package occurrence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/parquet-go/parquet-go"
	parquetzstd "github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleb"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulebparquet"
)

type scheduleBColumnarShardWriter struct {
	ctx            context.Context
	storageRoot    string
	index          uint64
	firstOrdinal   uint64
	firstRawOffset uint64
	temporaryPath  string
	file           *os.File
	counted        *countingHashWriter
	writer         *parquet.Writer
	schema         *schedulebparquet.Schema
	semantic       *schedulebparquet.SemanticHasher
	row            parquet.Row
	facts          uint64
	closed         bool
}

func newScheduleBColumnarShardWriter(
	ctx context.Context,
	storageRoot, stagingDirectory string,
	index, firstOrdinal, firstRawOffset, rowsPerRowGroup uint64,
	schema *schedulebparquet.Schema,
) (*scheduleBColumnarShardWriter, error) {
	if err := os.MkdirAll(stagingDirectory, 0o750); err != nil {
		return nil, err
	}
	temporaryPath := filepath.Join(stagingDirectory, fmt.Sprintf("part-%05d.parquet.partial", index))
	if err := os.Remove(temporaryPath); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	file, err := os.OpenFile(temporaryPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		return nil, err
	}
	counted := &countingHashWriter{writer: file, hash: sha256.New()}
	writer := parquet.NewWriter(
		counted,
		schema.Parquet(),
		parquet.Compression(&parquetzstd.Codec{Level: parquetzstd.SpeedDefault, Concurrency: 1}),
		parquet.MaxRowsPerRowGroup(int64(rowsPerRowGroup)),
		parquet.PageBufferSize(256<<10),
		parquet.WriteBufferSize(256<<10),
		parquet.DataPageStatistics(true),
		parquet.CreatedBy("legal-tender", schedulebparquet.PhysicalSchemaVersion, ScheduleBColumnarPublisherVersion),
	)
	return &scheduleBColumnarShardWriter{
		ctx: ctx, storageRoot: storageRoot, index: index, firstOrdinal: firstOrdinal,
		firstRawOffset: firstRawOffset, temporaryPath: temporaryPath, file: file,
		counted: counted, writer: writer, schema: schema, semantic: schedulebparquet.NewSemanticHasher(),
	}, nil
}

func (writer *scheduleBColumnarShardWriter) Write(row *scheduleb.Row, metadata schedulebparquet.Metadata, derived schedulebparquet.Derived) error {
	if writer.closed {
		return fmt.Errorf("Schedule B columnar shard writer is closed")
	}
	if err := writer.ctx.Err(); err != nil {
		return err
	}
	encoded, err := writer.schema.Encode(writer.row, row, metadata, derived)
	if err != nil {
		return err
	}
	writer.row = encoded
	if _, err := writer.writer.WriteRows([]parquet.Row{writer.row}); err != nil {
		return err
	}
	if err := writer.semantic.AddSource(row, metadata); err != nil {
		return err
	}
	writer.facts++
	return nil
}

func (writer *scheduleBColumnarShardWriter) Close(lastOrdinal, lastRawEnd, sourceRows uint64) (ScheduleBColumnarShard, error) {
	var descriptor ScheduleBColumnarShard
	if writer.closed {
		return descriptor, fmt.Errorf("Schedule B columnar shard writer is closed")
	}
	writer.closed = true
	failed := true
	defer func() {
		_ = writer.file.Close()
		if failed {
			_ = os.Remove(writer.temporaryPath)
		}
	}()
	if err := writer.ctx.Err(); err != nil {
		return descriptor, err
	}
	if err := writer.writer.Close(); err != nil {
		return descriptor, err
	}
	if err := writer.file.Sync(); err != nil {
		return descriptor, err
	}
	if err := writer.file.Close(); err != nil {
		return descriptor, err
	}

	verification, err := schedulebparquet.VerifyFile(writer.temporaryPath, writer.schema)
	if err != nil {
		return descriptor, fmt.Errorf("verify Schedule B Parquet shard %d: %w", writer.index, err)
	}
	if verification.Rows != writer.facts || verification.SemanticSHA256 != writer.semantic.Sum() {
		return descriptor, fmt.Errorf("Schedule B Parquet shard %d failed semantic round trip", writer.index)
	}
	info, err := os.Stat(writer.temporaryPath)
	if err != nil {
		return descriptor, err
	}
	if info.Size() < 0 || writer.counted.bytes != uint64(info.Size()) {
		return descriptor, fmt.Errorf("Schedule B Parquet shard %d write length changed after close", writer.index)
	}
	streamDigest := hex.EncodeToString(writer.counted.hash.Sum(nil))
	digest, err := scheduleBColumnarFileSHA256(writer.ctx, writer.temporaryPath)
	if err != nil {
		return descriptor, fmt.Errorf("hash completed Schedule B Parquet shard %d: %w", writer.index, err)
	}
	if digest != streamDigest {
		return descriptor, fmt.Errorf("Schedule B Parquet shard %d changed after close", writer.index)
	}
	descriptor = ScheduleBColumnarShard{
		Index: writer.index, FirstSourceRowOrdinal: writer.firstOrdinal, LastSourceRowOrdinal: lastOrdinal,
		FirstRawByteOffset: writer.firstRawOffset, LastRawByteEnd: lastRawEnd, SourceRows: sourceRows,
		Facts:     writer.facts,
		RowGroups: verification.RowGroups, Bytes: uint64(info.Size()), SHA256: digest,
		SemanticSHA256: verification.SemanticSHA256,
	}
	descriptor.StorageKey = filepath.ToSlash(filepath.Join(
		scheduleBColumnarBase(), "shards", "sha256", digest[:2], digest+".parquet",
	))
	destination, err := resolveStorageKey(writer.storageRoot, descriptor.StorageKey)
	if err != nil {
		return ScheduleBColumnarShard{}, err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o750); err != nil {
		return ScheduleBColumnarShard{}, err
	}
	if existing, err := os.Stat(destination); err == nil {
		if !existing.Mode().IsRegular() || existing.Size() < 0 || uint64(existing.Size()) != descriptor.Bytes {
			return ScheduleBColumnarShard{}, fmt.Errorf("existing Schedule B Parquet shard does not match %s", digest)
		}
		existingDigest, err := scheduleBColumnarFileSHA256(writer.ctx, destination)
		if err != nil || existingDigest != digest {
			return ScheduleBColumnarShard{}, fmt.Errorf("existing Schedule B Parquet shard %s is corrupt", digest)
		}
		_ = os.Remove(writer.temporaryPath)
	} else if !os.IsNotExist(err) {
		return ScheduleBColumnarShard{}, err
	} else {
		if err := os.Link(writer.temporaryPath, destination); err != nil {
			if !os.IsExist(err) {
				return ScheduleBColumnarShard{}, err
			}
			existingDigest, digestErr := scheduleBColumnarFileSHA256(writer.ctx, destination)
			if digestErr != nil || existingDigest != digest {
				return ScheduleBColumnarShard{}, fmt.Errorf("concurrently published Schedule B Parquet shard %s is corrupt", digest)
			}
		} else if err := syncDirectory(filepath.Dir(destination)); err != nil {
			return ScheduleBColumnarShard{}, err
		}
		_ = os.Remove(writer.temporaryPath)
	}
	failed = false
	return descriptor, nil
}

func (writer *scheduleBColumnarShardWriter) Abort() {
	if writer == nil || writer.closed {
		return
	}
	writer.closed = true
	_ = writer.writer.Close()
	_ = writer.file.Close()
	_ = os.Remove(writer.temporaryPath)
}

func scheduleBColumnarFileSHA256(ctx context.Context, path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }()
	hasher := sha256.New()
	if _, err := io.Copy(hasher, &contextReader{ctx: ctx, reader: file}); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}
