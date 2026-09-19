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
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulea"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleaparquet"
)

type scheduleAColumnarShardWriter struct {
	ctx            context.Context
	storageRoot    string
	index          uint64
	firstOrdinal   uint64
	firstRawOffset uint64
	temporaryPath  string
	file           *os.File
	counted        *countingHashWriter
	writer         *parquet.Writer
	schema         *scheduleaparquet.Schema
	semantic       *scheduleaparquet.SemanticHasher
	row            parquet.Row
	facts          uint64
	validFacts     uint64
	invalidFacts   uint64
	closed         bool
}

func newScheduleAColumnarShardWriter(
	ctx context.Context,
	storageRoot, stagingDirectory string,
	index, firstOrdinal, firstRawOffset, rowsPerRowGroup uint64,
	schema *scheduleaparquet.Schema,
) (*scheduleAColumnarShardWriter, error) {
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
		parquet.CreatedBy("legal-tender", scheduleaparquet.PhysicalSchemaVersion, ScheduleAColumnarPublisherVersion),
	)
	return &scheduleAColumnarShardWriter{
		ctx: ctx, storageRoot: storageRoot, index: index, firstOrdinal: firstOrdinal,
		firstRawOffset: firstRawOffset, temporaryPath: temporaryPath, file: file,
		counted: counted, writer: writer, schema: schema, semantic: scheduleaparquet.NewSemanticHasher(),
	}, nil
}

func (writer *scheduleAColumnarShardWriter) Write(row *schedulea.Row, metadata scheduleaparquet.Metadata, derived scheduleaparquet.Derived) error {
	if writer.closed {
		return fmt.Errorf("Schedule A columnar shard writer is closed")
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
	if derived.NormalizationState == "valid" {
		writer.validFacts++
	} else {
		writer.invalidFacts++
	}
	return nil
}

func (writer *scheduleAColumnarShardWriter) Close(lastOrdinal, lastRawEnd, sourceRows uint64) (ScheduleAColumnarShard, error) {
	var descriptor ScheduleAColumnarShard
	if writer.closed {
		return descriptor, fmt.Errorf("Schedule A columnar shard writer is closed")
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

	verification, err := scheduleaparquet.VerifyFile(writer.temporaryPath, writer.schema)
	if err != nil {
		return descriptor, fmt.Errorf("verify Schedule A Parquet shard %d: %w", writer.index, err)
	}
	if verification.Rows != writer.facts || verification.SemanticSHA256 != writer.semantic.Sum() {
		return descriptor, fmt.Errorf("Schedule A Parquet shard %d failed semantic round trip", writer.index)
	}
	info, err := os.Stat(writer.temporaryPath)
	if err != nil {
		return descriptor, err
	}
	if info.Size() < 0 || writer.counted.bytes != uint64(info.Size()) {
		return descriptor, fmt.Errorf("Schedule A Parquet shard %d write length changed after close", writer.index)
	}
	streamDigest := hex.EncodeToString(writer.counted.hash.Sum(nil))
	digest, err := scheduleAColumnarFileSHA256(writer.ctx, writer.temporaryPath)
	if err != nil {
		return descriptor, fmt.Errorf("hash completed Schedule A Parquet shard %d: %w", writer.index, err)
	}
	if digest != streamDigest {
		return descriptor, fmt.Errorf("Schedule A Parquet shard %d changed after close", writer.index)
	}
	descriptor = ScheduleAColumnarShard{
		Index: writer.index, FirstSourceRowOrdinal: writer.firstOrdinal, LastSourceRowOrdinal: lastOrdinal,
		FirstRawByteOffset: writer.firstRawOffset, LastRawByteEnd: lastRawEnd, SourceRows: sourceRows,
		Facts: writer.facts, ValidFacts: writer.validFacts, InvalidFacts: writer.invalidFacts,
		RowGroups: verification.RowGroups, Bytes: uint64(info.Size()), SHA256: digest,
		SemanticSHA256: verification.SemanticSHA256,
	}
	descriptor.StorageKey = filepath.ToSlash(filepath.Join(
		scheduleAColumnarBase(), "shards", "sha256", digest[:2], digest+".parquet",
	))
	destination, err := resolveStorageKey(writer.storageRoot, descriptor.StorageKey)
	if err != nil {
		return ScheduleAColumnarShard{}, err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o750); err != nil {
		return ScheduleAColumnarShard{}, err
	}
	if existing, err := os.Stat(destination); err == nil {
		if !existing.Mode().IsRegular() || existing.Size() < 0 || uint64(existing.Size()) != descriptor.Bytes {
			return ScheduleAColumnarShard{}, fmt.Errorf("existing Schedule A Parquet shard does not match %s", digest)
		}
		existingDigest, err := scheduleAColumnarFileSHA256(writer.ctx, destination)
		if err != nil || existingDigest != digest {
			return ScheduleAColumnarShard{}, fmt.Errorf("existing Schedule A Parquet shard %s is corrupt", digest)
		}
		_ = os.Remove(writer.temporaryPath)
	} else if !os.IsNotExist(err) {
		return ScheduleAColumnarShard{}, err
	} else {
		if err := os.Link(writer.temporaryPath, destination); err != nil {
			if !os.IsExist(err) {
				return ScheduleAColumnarShard{}, err
			}
			existingDigest, digestErr := scheduleAColumnarFileSHA256(writer.ctx, destination)
			if digestErr != nil || existingDigest != digest {
				return ScheduleAColumnarShard{}, fmt.Errorf("concurrently published Schedule A Parquet shard %s is corrupt", digest)
			}
		} else if err := syncDirectory(filepath.Dir(destination)); err != nil {
			return ScheduleAColumnarShard{}, err
		}
		_ = os.Remove(writer.temporaryPath)
	}
	failed = false
	return descriptor, nil
}

func (writer *scheduleAColumnarShardWriter) Abort() {
	if writer == nil || writer.closed {
		return
	}
	writer.closed = true
	_ = writer.writer.Close()
	_ = writer.file.Close()
	_ = os.Remove(writer.temporaryPath)
}

func scheduleAColumnarFileSHA256(ctx context.Context, path string) (string, error) {
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
