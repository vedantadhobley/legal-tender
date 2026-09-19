// Package artifact publishes and replays immutable content-addressed zstd
// JSONL artifacts shared by source and calculation layers.
package artifact

import (
	"bufio"
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

const compressionLevel = 3

type Descriptor struct {
	RecordCount        uint64 `json:"record_count"`
	UncompressedBytes  uint64 `json:"uncompressed_byte_count"`
	UncompressedSHA256 string `json:"uncompressed_sha256"`
	CompressedBytes    uint64 `json:"compressed_byte_count"`
	CompressedSHA256   string `json:"compressed_sha256"`
	Compression        string `json:"compression"`
	StorageKey         string `json:"storage_key"`
}

type Writer struct {
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

func NewWriter(ctx context.Context, storageRoot, temporaryDirectory, basePath, kind string) (*Writer, error) {
	if basePath == "" || filepath.IsAbs(basePath) || kind == "" || strings.ContainsAny(kind, `/\\`) {
		return nil, fmt.Errorf("relative artifact base path and simple kind are required")
	}
	cleanBase := filepath.Clean(basePath)
	if cleanBase == "." || cleanBase == ".." || strings.HasPrefix(cleanBase, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("artifact base path escapes storage root")
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
	encoder, err := zstd.NewWriter(compressed, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(compressionLevel)), zstd.WithEncoderConcurrency(2))
	if err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return nil, err
	}
	uncompressed := &countingHashWriter{writer: encoder, hash: sha256.New()}
	return &Writer{
		context: ctx, storageRoot: storageRoot, basePath: filepath.ToSlash(cleanBase), kind: kind,
		file: file, temporaryPath: file.Name(), compressed: compressed, uncompressed: uncompressed,
		encoder: encoder, jsonEncoder: json.NewEncoder(uncompressed),
	}, nil
}

func (writer *Writer) WriteJSON(value any) error {
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

func (writer *Writer) Abort() {
	if writer == nil || writer.closed {
		return
	}
	writer.closed = true
	_ = writer.encoder.Close()
	_ = writer.file.Close()
	_ = os.Remove(writer.temporaryPath)
}

func (writer *Writer) Finalize() (Descriptor, error) {
	if writer.closed {
		return Descriptor{}, fmt.Errorf("%s artifact writer is closed", writer.kind)
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
		return Descriptor{}, err
	}
	if err := writer.encoder.Close(); err != nil {
		return Descriptor{}, err
	}
	if err := writer.file.Sync(); err != nil {
		return Descriptor{}, err
	}
	if err := writer.file.Close(); err != nil {
		return Descriptor{}, err
	}
	descriptor := Descriptor{
		RecordCount: writer.records, UncompressedBytes: writer.uncompressed.bytes,
		UncompressedSHA256: hex.EncodeToString(writer.uncompressed.hash.Sum(nil)),
		CompressedBytes:    writer.compressed.bytes, CompressedSHA256: hex.EncodeToString(writer.compressed.hash.Sum(nil)), Compression: "zstd",
	}
	descriptor.StorageKey = filepath.ToSlash(filepath.Join(writer.basePath, writer.kind, "sha256", descriptor.CompressedSHA256[:2], descriptor.CompressedSHA256+".jsonl.zst"))
	destination, err := Resolve(writer.storageRoot, descriptor.StorageKey)
	if err != nil {
		return Descriptor{}, err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o750); err != nil {
		return Descriptor{}, err
	}
	if info, err := os.Stat(destination); err == nil {
		if !info.Mode().IsRegular() || info.Size() < 0 || uint64(info.Size()) != descriptor.CompressedBytes {
			return Descriptor{}, fmt.Errorf("existing %s artifact does not match %s", writer.kind, descriptor.CompressedSHA256)
		}
		if err := Verify(writer.context, destination, descriptor); err != nil {
			return Descriptor{}, fmt.Errorf("existing %s artifact is corrupt: %w", writer.kind, err)
		}
		_ = os.Remove(writer.temporaryPath)
	} else if !os.IsNotExist(err) {
		return Descriptor{}, err
	} else {
		if err := os.Link(writer.temporaryPath, destination); err != nil {
			if !os.IsExist(err) {
				return Descriptor{}, err
			}
			if verifyErr := Verify(writer.context, destination, descriptor); verifyErr != nil {
				return Descriptor{}, fmt.Errorf("concurrently published %s artifact is corrupt: %w", writer.kind, verifyErr)
			}
		} else if err := syncDirectory(filepath.Dir(destination)); err != nil {
			return Descriptor{}, err
		}
		_ = os.Remove(writer.temporaryPath)
	}
	failed = false
	return descriptor, nil
}

func Verify(ctx context.Context, path string, expected Descriptor) error {
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
	return verifyCounts(compressed, uncompressed, 0, false, expected)
}

type Reader[T any] struct {
	expected     Descriptor
	file         *os.File
	compressed   *countingHashReader
	decoder      *zstd.Decoder
	uncompressed *countingHashReader
	jsonDecoder  *json.Decoder
	records      uint64
	finished     bool
}

func Open[T any](ctx context.Context, storageRoot string, expected Descriptor) (*Reader[T], error) {
	path, err := Resolve(storageRoot, expected.StorageKey)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	compressed := &countingHashReader{reader: &contextReader{ctx: ctx, reader: file}, hash: sha256.New()}
	decoder, err := zstd.NewReader(compressed, zstd.WithDecoderConcurrency(2), zstd.WithDecoderLowmem(true), zstd.WithDecoderMaxMemory(64<<20))
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	uncompressed := &countingHashReader{reader: decoder, hash: sha256.New()}
	jsonDecoder := json.NewDecoder(bufio.NewReaderSize(uncompressed, 256<<10))
	jsonDecoder.DisallowUnknownFields()
	return &Reader[T]{expected: expected, file: file, compressed: compressed, decoder: decoder, uncompressed: uncompressed, jsonDecoder: jsonDecoder}, nil
}

func (reader *Reader[T]) Next() (T, bool, error) {
	var value T
	if reader.finished {
		return value, false, nil
	}
	err := reader.jsonDecoder.Decode(&value)
	if err == nil {
		reader.records++
		return value, true, nil
	}
	if err != io.EOF {
		return value, false, err
	}
	if err := verifyCounts(reader.compressed, reader.uncompressed, reader.records, true, reader.expected); err != nil {
		return value, false, err
	}
	reader.finished = true
	return value, false, nil
}

func (reader *Reader[T]) Close() error {
	if reader == nil {
		return nil
	}
	reader.decoder.Close()
	err := reader.file.Close()
	if !reader.finished {
		return fmt.Errorf("artifact reader closed before verified EOF")
	}
	return err
}

func (reader *Reader[T]) Abort() {
	if reader == nil {
		return
	}
	reader.decoder.Close()
	_ = reader.file.Close()
}

func Resolve(storageRoot, storageKey string) (string, error) {
	if storageRoot == "" || storageKey == "" || filepath.IsAbs(storageKey) {
		return "", fmt.Errorf("storage root and relative storage key are required")
	}
	clean := filepath.Clean(storageKey)
	if clean == "." || clean == ".." || filepath.IsAbs(clean) || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("storage key escapes storage root")
	}
	return filepath.Join(storageRoot, clean), nil
}

func verifyCounts(compressed, uncompressed *countingHashReader, records uint64, checkRecords bool, expected Descriptor) error {
	if checkRecords && records != expected.RecordCount {
		return fmt.Errorf("artifact record count mismatch: got %d want %d", records, expected.RecordCount)
	}
	if compressed.bytes != expected.CompressedBytes || hex.EncodeToString(compressed.hash.Sum(nil)) != expected.CompressedSHA256 {
		return fmt.Errorf("compressed identity mismatch")
	}
	if uncompressed.bytes != expected.UncompressedBytes || hex.EncodeToString(uncompressed.hash.Sum(nil)) != expected.UncompressedSHA256 {
		return fmt.Errorf("uncompressed identity mismatch")
	}
	return nil
}

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

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = directory.Close() }()
	return directory.Sync()
}
