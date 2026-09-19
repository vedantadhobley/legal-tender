package release

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zstd"
)

const zstdCompressionLevel = 3

type streamMetrics struct {
	UncompressedBytes  uint64
	UncompressedSHA256 string
	CompressedBytes    uint64
	CompressedSHA256   string
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

func streamToZstd(ctx context.Context, temporaryDirectory string, producer func(io.Writer) error, budget *writeBudget) (string, streamMetrics, error) {
	if err := os.MkdirAll(temporaryDirectory, 0o750); err != nil {
		return "", streamMetrics{}, err
	}
	file, err := os.CreateTemp(temporaryDirectory, ".selected-*.zst")
	if err != nil {
		return "", streamMetrics{}, err
	}
	temporaryPath := file.Name()
	failed := true
	defer func() {
		_ = file.Close()
		if failed {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := file.Chmod(0o640); err != nil {
		return "", streamMetrics{}, err
	}

	var destination io.Writer = file
	if budget != nil {
		destination, err = budget.writer(file, true)
		if err != nil {
			return "", streamMetrics{}, err
		}
	}
	compressed := &countingHashWriter{writer: destination, hash: sha256.New()}
	encoder, err := zstd.NewWriter(
		compressed,
		zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(zstdCompressionLevel)),
		zstd.WithEncoderConcurrency(2),
	)
	if err != nil {
		return "", streamMetrics{}, err
	}
	uncompressed := &countingHashWriter{writer: encoder, hash: sha256.New()}
	if err := producer(uncompressed); err != nil {
		encoder.Close()
		return "", streamMetrics{}, err
	}
	if err := ctx.Err(); err != nil {
		encoder.Close()
		return "", streamMetrics{}, err
	}
	if err := encoder.Close(); err != nil {
		return "", streamMetrics{}, err
	}
	if err := file.Sync(); err != nil {
		return "", streamMetrics{}, err
	}
	if err := file.Close(); err != nil {
		return "", streamMetrics{}, err
	}

	metrics := streamMetrics{
		UncompressedBytes:  uncompressed.bytes,
		UncompressedSHA256: hex.EncodeToString(uncompressed.hash.Sum(nil)),
		CompressedBytes:    compressed.bytes,
		CompressedSHA256:   hex.EncodeToString(compressed.hash.Sum(nil)),
	}
	if err := verifyZstdStream(ctx, temporaryPath, metrics); err != nil {
		return "", streamMetrics{}, err
	}
	failed = false
	return temporaryPath, metrics, nil
}

func verifyZstdStream(ctx context.Context, path string, expected streamMetrics) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	compressedHasher := sha256.New()
	compressed := &countingHashReader{
		reader: &contextReader{ctx: ctx, reader: file},
		hash:   compressedHasher,
	}
	decoder, err := zstd.NewReader(compressed, zstd.WithDecoderConcurrency(2), zstd.WithDecoderLowmem(true), zstd.WithDecoderMaxMemory(64<<20))
	if err != nil {
		return err
	}
	defer decoder.Close()
	hasher := sha256.New()
	count, err := io.Copy(hasher, &contextReader{ctx: ctx, reader: decoder})
	if err != nil {
		return err
	}
	if count < 0 || uint64(count) != expected.UncompressedBytes || hex.EncodeToString(hasher.Sum(nil)) != expected.UncompressedSHA256 {
		return fmt.Errorf("zstd decompression does not conserve the staged stream")
	}
	if expected.CompressedBytes > 0 && (compressed.bytes != expected.CompressedBytes || hex.EncodeToString(compressedHasher.Sum(nil)) != expected.CompressedSHA256) {
		return fmt.Errorf("zstd stream does not conserve its compressed identity")
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

func finalizeStagedStream(ctx context.Context, storageRoot, temporaryPath, sourceID string, metrics streamMetrics) (string, error) {
	storageKey := stagedStorageKey(sourceID, metrics.CompressedSHA256)
	destination, err := resolveStorageKey(storageRoot, storageKey)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o750); err != nil {
		return "", err
	}
	if info, err := os.Stat(destination); err == nil {
		if !info.Mode().IsRegular() || info.Size() < 0 || uint64(info.Size()) != metrics.CompressedBytes {
			return "", fmt.Errorf("existing staged object does not match %s", metrics.CompressedSHA256)
		}
		if verifyErr := verifyZstdStream(ctx, destination, metrics); verifyErr != nil {
			return "", fmt.Errorf("existing staged object failed decompression validation: %w", verifyErr)
		}
		_ = os.Remove(temporaryPath)
		return storageKey, nil
	} else if !os.IsNotExist(err) {
		return "", err
	}
	if err := os.Link(temporaryPath, destination); err != nil {
		return "", err
	}
	if err := syncDirectory(filepath.Dir(destination)); err != nil {
		return "", err
	}
	_ = os.Remove(temporaryPath)
	return storageKey, nil
}
