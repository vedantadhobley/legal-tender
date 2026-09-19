package occurrence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/klauspost/compress/zstd"
)

const artifactCompressionLevel = 3

type countingHashWriter struct {
	writer io.Writer
	hash   hash.Hash
	bytes  uint64
}

func (writer *countingHashWriter) Write(content []byte) (int, error) {
	written, err := writer.writer.Write(content)
	if written > 0 {
		writer.bytes += uint64(written)
		_, _ = writer.hash.Write(content[:written])
	}
	return written, err
}

type artifactWriter struct {
	context       context.Context
	storageRoot   string
	basePath      string
	kind          string
	file          *os.File
	temporaryPath string
	compressed    *countingHashWriter
	uncompressed  *countingHashWriter
	encoder       *zstd.Encoder
	jsonEncoder   *json.Encoder
	records       uint64
	closed        bool
}

func newArtifactWriter(ctx context.Context, storageRoot, temporaryDirectory, kind string) (*artifactWriter, error) {
	return newArtifactWriterAt(ctx, storageRoot, temporaryDirectory, filepath.Join("evidence", "fec", "schedule-a"), kind)
}

func newArtifactWriterAt(ctx context.Context, storageRoot, temporaryDirectory, basePath, kind string) (*artifactWriter, error) {
	if basePath == "" || filepath.IsAbs(basePath) {
		return nil, fmt.Errorf("relative evidence base path is required")
	}
	cleanBase := filepath.Clean(basePath)
	if cleanBase == "." || cleanBase == ".." || strings.HasPrefix(cleanBase, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("evidence base path escapes storage root")
	}
	if err := os.MkdirAll(temporaryDirectory, 0o750); err != nil {
		return nil, err
	}
	file, err := os.CreateTemp(temporaryDirectory, "."+kind+"-*.jsonl.zst")
	if err != nil {
		return nil, err
	}
	if err := file.Chmod(0o640); err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return nil, err
	}
	compressed := &countingHashWriter{writer: file, hash: sha256.New()}
	encoder, err := zstd.NewWriter(
		compressed,
		zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(artifactCompressionLevel)),
		zstd.WithEncoderConcurrency(2),
	)
	if err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return nil, err
	}
	uncompressed := &countingHashWriter{writer: encoder, hash: sha256.New()}
	return &artifactWriter{
		context:       ctx,
		storageRoot:   storageRoot,
		basePath:      filepath.ToSlash(cleanBase),
		kind:          kind,
		file:          file,
		temporaryPath: file.Name(),
		compressed:    compressed,
		uncompressed:  uncompressed,
		encoder:       encoder,
		jsonEncoder:   json.NewEncoder(uncompressed),
	}, nil
}

func (writer *artifactWriter) WriteJSON(value any) error {
	if writer.closed {
		return fmt.Errorf("%s artifact writer is closed", writer.kind)
	}
	if err := writer.context.Err(); err != nil {
		return err
	}
	if err := writer.jsonEncoder.Encode(value); err != nil {
		return err
	}
	writer.records++
	return nil
}

func (writer *artifactWriter) Abort() {
	if writer == nil || writer.closed {
		return
	}
	writer.closed = true
	_ = writer.encoder.Close()
	_ = writer.file.Close()
	_ = os.Remove(writer.temporaryPath)
}

func (writer *artifactWriter) Finalize() (Artifact, error) {
	if writer.closed {
		return Artifact{}, fmt.Errorf("%s artifact writer is closed", writer.kind)
	}
	writer.closed = true
	failed := true
	defer func() {
		_ = writer.file.Close()
		if failed {
			_ = os.Remove(writer.temporaryPath)
		}
	}()
	if err := writer.context.Err(); err != nil {
		return Artifact{}, err
	}
	if err := writer.encoder.Close(); err != nil {
		return Artifact{}, err
	}
	if err := writer.file.Sync(); err != nil {
		return Artifact{}, err
	}
	if err := writer.file.Close(); err != nil {
		return Artifact{}, err
	}

	artifact := Artifact{
		RecordCount:        writer.records,
		UncompressedBytes:  writer.uncompressed.bytes,
		UncompressedSHA256: hex.EncodeToString(writer.uncompressed.hash.Sum(nil)),
		CompressedBytes:    writer.compressed.bytes,
		CompressedSHA256:   hex.EncodeToString(writer.compressed.hash.Sum(nil)),
		Compression:        "zstd",
	}
	storageKey := filepath.ToSlash(filepath.Join(
		writer.basePath, writer.kind, "sha256",
		artifact.CompressedSHA256[:2], artifact.CompressedSHA256+".jsonl.zst",
	))
	destination, err := resolveStorageKey(writer.storageRoot, storageKey)
	if err != nil {
		return Artifact{}, err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o750); err != nil {
		return Artifact{}, err
	}
	if info, err := os.Stat(destination); err == nil {
		if !info.Mode().IsRegular() || info.Size() < 0 || uint64(info.Size()) != artifact.CompressedBytes {
			return Artifact{}, fmt.Errorf("existing %s artifact does not match %s", writer.kind, artifact.CompressedSHA256)
		}
		if err := verifyArtifact(writer.context, destination, artifact); err != nil {
			return Artifact{}, fmt.Errorf("existing %s artifact is corrupt: %w", writer.kind, err)
		}
		_ = os.Remove(writer.temporaryPath)
	} else if !os.IsNotExist(err) {
		return Artifact{}, err
	} else {
		if err := os.Link(writer.temporaryPath, destination); err != nil {
			if !os.IsExist(err) {
				return Artifact{}, err
			}
			if verifyErr := verifyArtifact(writer.context, destination, artifact); verifyErr != nil {
				return Artifact{}, fmt.Errorf("concurrently published %s artifact is corrupt: %w", writer.kind, verifyErr)
			}
		} else if err := syncDirectory(filepath.Dir(destination)); err != nil {
			return Artifact{}, err
		}
		_ = os.Remove(writer.temporaryPath)
	}
	artifact.StorageKey = storageKey
	failed = false
	return artifact, nil
}

func verifyArtifact(ctx context.Context, path string, expected Artifact) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	compressed := &countingHashReader{reader: &contextReader{ctx: ctx, reader: file}, hash: sha256.New()}
	decoder, err := zstd.NewReader(compressed, zstd.WithDecoderConcurrency(2), zstd.WithDecoderLowmem(true), zstd.WithDecoderMaxMemory(64<<20))
	if err != nil {
		return err
	}
	defer decoder.Close()
	uncompressed := &countingHashReader{reader: &contextReader{ctx: ctx, reader: decoder}, hash: sha256.New()}
	if _, err := io.Copy(io.Discard, uncompressed); err != nil {
		return err
	}
	if compressed.bytes != expected.CompressedBytes || hex.EncodeToString(compressed.hash.Sum(nil)) != expected.CompressedSHA256 {
		return fmt.Errorf("compressed identity mismatch")
	}
	if uncompressed.bytes != expected.UncompressedBytes || hex.EncodeToString(uncompressed.hash.Sum(nil)) != expected.UncompressedSHA256 {
		return fmt.Errorf("uncompressed identity mismatch")
	}
	return nil
}

type countingHashReader struct {
	reader io.Reader
	hash   hash.Hash
	bytes  uint64
}

func (reader *countingHashReader) Read(destination []byte) (int, error) {
	read, err := reader.reader.Read(destination)
	if read > 0 {
		reader.bytes += uint64(read)
		_, _ = reader.hash.Write(destination[:read])
	}
	return read, err
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader *contextReader) Read(destination []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	return reader.reader.Read(destination)
}

func resolveStorageKey(storageRoot, storageKey string) (string, error) {
	if storageRoot == "" || storageKey == "" || filepath.IsAbs(storageKey) {
		return "", fmt.Errorf("storage root and relative storage key are required")
	}
	clean := filepath.Clean(storageKey)
	if clean == "." || clean == ".." || filepath.IsAbs(clean) || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("storage key escapes storage root")
	}
	return filepath.Join(storageRoot, clean), nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = directory.Close() }()
	return directory.Sync()
}
