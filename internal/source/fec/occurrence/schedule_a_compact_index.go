package occurrence

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/klauspost/compress/zstd"
)

type compactStageShard struct {
	path   string
	file   *os.File
	buffer *bufio.Writer
}

type compactStageSet struct {
	context context.Context
	shards  []compactStageShard
	closed  bool
}

func newCompactStageSet(ctx context.Context, directory string, partitions int) (*compactStageSet, error) {
	if partitions <= 0 {
		return nil, fmt.Errorf("compact index partition count must be positive")
	}
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return nil, err
	}
	set := &compactStageSet{context: ctx, shards: make([]compactStageShard, partitions)}
	for partition := range set.shards {
		path := filepath.Join(directory, fmt.Sprintf("%04d.stage", partition))
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			set.Remove()
			return nil, err
		}
		set.shards[partition] = compactStageShard{path: path, file: file, buffer: bufio.NewWriterSize(file, 256<<10)}
	}
	return set, nil
}

func compactPartition(subID uint64, partitions int) int {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(strconv.FormatUint(subID, 10)))
	return int(hasher.Sum32() % uint32(partitions))
}

func (set *compactStageSet) Add(entry compactStageEntry) error {
	if set.closed {
		return fmt.Errorf("compact index staging is closed")
	}
	if err := set.context.Err(); err != nil {
		return err
	}
	partition := compactPartition(entry.SubID, len(set.shards))
	var encoded [ScheduleACompactStageRecordBytes]byte
	binary.BigEndian.PutUint64(encoded[0:8], entry.SubID)
	binary.BigEndian.PutUint64(encoded[8:16], entry.RowOrdinal)
	binary.BigEndian.PutUint64(encoded[16:24], entry.RawByteOffset)
	binary.BigEndian.PutUint64(encoded[24:32], entry.RawByteLength)
	if entry.Valid {
		encoded[32] = 1
	}
	copy(encoded[33:], entry.Semantic[:])
	_, err := set.shards[partition].buffer.Write(encoded[:])
	return err
}

func (set *compactStageSet) Close() error {
	if set.closed {
		return nil
	}
	set.closed = true
	var first error
	for index := range set.shards {
		if set.shards[index].buffer != nil {
			if err := set.shards[index].buffer.Flush(); err != nil && first == nil {
				first = err
			}
		}
		if set.shards[index].file != nil {
			if err := set.shards[index].file.Close(); err != nil && first == nil {
				first = err
			}
		}
	}
	return first
}

func (set *compactStageSet) Remove() {
	_ = set.Close()
	for _, shard := range set.shards {
		if shard.path != "" {
			_ = os.Remove(shard.path)
		}
	}
}

func readCompactStageEntries(path string) ([]compactStageEntry, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(content)%ScheduleACompactStageRecordBytes != 0 {
		return nil, fmt.Errorf("compact staging shard has a partial record")
	}
	entries := make([]compactStageEntry, 0, len(content)/ScheduleACompactStageRecordBytes)
	for offset := 0; offset < len(content); offset += ScheduleACompactStageRecordBytes {
		record := content[offset : offset+ScheduleACompactStageRecordBytes]
		entry := compactStageEntry{
			SubID: binary.BigEndian.Uint64(record[0:8]), RowOrdinal: binary.BigEndian.Uint64(record[8:16]),
			RawByteOffset: binary.BigEndian.Uint64(record[16:24]), RawByteLength: binary.BigEndian.Uint64(record[24:32]),
			Valid: record[32] == 1,
		}
		copy(entry.Semantic[:], record[33:])
		if entry.SubID == 0 || entry.RowOrdinal == 0 || entry.RawByteLength == 0 || record[32] > 1 {
			return nil, fmt.Errorf("compact staging shard contains an invalid record")
		}
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(left, right int) bool {
		if entries[left].SubID == entries[right].SubID {
			return entries[left].RowOrdinal < entries[right].RowOrdinal
		}
		return entries[left].SubID < entries[right].SubID
	})
	return entries, nil
}

type compactIndexWriter struct {
	context       context.Context
	storageRoot   string
	temporaryPath string
	file          *os.File
	compressed    *countingHashWriter
	uncompressed  *countingHashWriter
	encoder       *zstd.Encoder
	records       uint64
	closed        bool
}

func newCompactIndexWriter(ctx context.Context, storageRoot, temporaryDirectory string) (*compactIndexWriter, error) {
	file, err := os.CreateTemp(temporaryDirectory, ".key-index-*.bin.zst")
	if err != nil {
		return nil, err
	}
	if err := file.Chmod(0o640); err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return nil, err
	}
	compressed := &countingHashWriter{writer: file, hash: sha256.New()}
	encoder, err := zstd.NewWriter(compressed, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(2))
	if err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return nil, err
	}
	uncompressed := &countingHashWriter{writer: encoder, hash: sha256.New()}
	return &compactIndexWriter{
		context: ctx, storageRoot: storageRoot, temporaryPath: file.Name(), file: file,
		compressed: compressed, uncompressed: uncompressed, encoder: encoder,
	}, nil
}

func (writer *compactIndexWriter) Write(state compactKeyState) error {
	if writer.closed {
		return fmt.Errorf("compact index writer is closed")
	}
	if state.State != "unique" || state.SubID == 0 || state.RowOrdinal == 0 || state.OccurrenceCount != 1 {
		return fmt.Errorf("compact index accepts only complete unique states")
	}
	if err := writer.context.Err(); err != nil {
		return err
	}
	var encoded [ScheduleACompactIndexRecordBytes]byte
	binary.BigEndian.PutUint64(encoded[0:8], state.SubID)
	binary.BigEndian.PutUint64(encoded[8:16], state.RowOrdinal)
	copy(encoded[16:], state.ComparisonDigest[:])
	if _, err := writer.uncompressed.Write(encoded[:]); err != nil {
		return err
	}
	writer.records++
	return nil
}

func (writer *compactIndexWriter) Abort() {
	if writer == nil || writer.closed {
		return
	}
	writer.closed = true
	_ = writer.encoder.Close()
	_ = writer.file.Close()
	_ = os.Remove(writer.temporaryPath)
}

func (writer *compactIndexWriter) Finalize(partition, partitions int) (ScheduleACompactArtifact, error) {
	if writer.closed {
		return ScheduleACompactArtifact{}, fmt.Errorf("compact index writer is closed")
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
		return ScheduleACompactArtifact{}, err
	}
	if err := writer.encoder.Close(); err != nil {
		return ScheduleACompactArtifact{}, err
	}
	if err := writer.file.Sync(); err != nil {
		return ScheduleACompactArtifact{}, err
	}
	if err := writer.file.Close(); err != nil {
		return ScheduleACompactArtifact{}, err
	}
	artifact := ScheduleACompactArtifact{
		RecordCount: writer.records, RecordBytes: ScheduleACompactIndexRecordBytes,
		UncompressedBytes: writer.uncompressed.bytes, UncompressedSHA256: hex.EncodeToString(writer.uncompressed.hash.Sum(nil)),
		CompressedBytes: writer.compressed.bytes, CompressedSHA256: hex.EncodeToString(writer.compressed.hash.Sum(nil)),
		Compression: "zstd", Encoding: "uint64-be-sub-id,uint64-be-row-ordinal,sha256-semantic-digest",
	}
	storageKey := filepath.ToSlash(filepath.Join(
		"evidence", "fec", "schedule-a", "compact", "key-index", "sha256",
		artifact.CompressedSHA256[:2], artifact.CompressedSHA256+".bin.zst",
	))
	destination, err := resolveStorageKey(writer.storageRoot, storageKey)
	if err != nil {
		return ScheduleACompactArtifact{}, err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o750); err != nil {
		return ScheduleACompactArtifact{}, err
	}
	if info, err := os.Stat(destination); err == nil {
		if !info.Mode().IsRegular() || info.Size() < 0 || uint64(info.Size()) != artifact.CompressedBytes {
			return ScheduleACompactArtifact{}, fmt.Errorf("existing compact index artifact does not match its digest")
		}
		_ = os.Remove(writer.temporaryPath)
	} else if !os.IsNotExist(err) {
		return ScheduleACompactArtifact{}, err
	} else {
		if err := os.Link(writer.temporaryPath, destination); err != nil {
			if !os.IsExist(err) {
				return ScheduleACompactArtifact{}, err
			}
		} else if err := syncDirectory(filepath.Dir(destination)); err != nil {
			return ScheduleACompactArtifact{}, err
		}
		_ = os.Remove(writer.temporaryPath)
	}
	artifact.StorageKey = storageKey
	if _, err := readCompactIndexStates(writer.context, writer.storageRoot, artifact, partition, partitions); err != nil {
		return ScheduleACompactArtifact{}, fmt.Errorf("verify compact index partition %d: %w", partition, err)
	}
	failed = false
	return artifact, nil
}

func readCompactIndexStates(ctx context.Context, storageRoot string, artifact ScheduleACompactArtifact, partition, partitions int) ([]compactKeyState, error) {
	path, err := resolveStorageKey(storageRoot, artifact.StorageKey)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	compressed := &countingHashReader{reader: &contextReader{ctx: ctx, reader: file}, hash: sha256.New()}
	decoder, err := zstd.NewReader(compressed, zstd.WithDecoderConcurrency(2), zstd.WithDecoderLowmem(true), zstd.WithDecoderMaxMemory(64<<20))
	if err != nil {
		return nil, err
	}
	defer decoder.Close()
	uncompressed := &countingHashReader{reader: decoder, hash: sha256.New()}
	states := make([]compactKeyState, 0, artifact.RecordCount)
	var previous uint64
	for {
		var encoded [ScheduleACompactIndexRecordBytes]byte
		_, err := io.ReadFull(uncompressed, encoded[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read compact index record: %w", err)
		}
		state := compactKeyState{
			SubID: binary.BigEndian.Uint64(encoded[0:8]), State: "unique", OccurrenceCount: 1,
			RowOrdinal: binary.BigEndian.Uint64(encoded[8:16]),
		}
		copy(state.ComparisonDigest[:], encoded[16:])
		if state.SubID == 0 || state.RowOrdinal == 0 || previous != 0 && state.SubID <= previous || compactPartition(state.SubID, partitions) != partition {
			return nil, fmt.Errorf("compact index ordering or partition membership is invalid")
		}
		previous = state.SubID
		states = append(states, state)
	}
	if uint64(len(states)) != artifact.RecordCount || uncompressed.bytes != artifact.UncompressedBytes ||
		hex.EncodeToString(uncompressed.hash.Sum(nil)) != artifact.UncompressedSHA256 {
		return nil, fmt.Errorf("compact index uncompressed identity mismatch")
	}
	if compressed.bytes != artifact.CompressedBytes || hex.EncodeToString(compressed.hash.Sum(nil)) != artifact.CompressedSHA256 {
		return nil, fmt.Errorf("compact index compressed identity mismatch")
	}
	return states, nil
}

func readCompactKeyExceptionStates(ctx context.Context, storageRoot string, artifact Artifact, partition, partitions int) ([]compactKeyState, error) {
	path, err := resolveStorageKey(storageRoot, artifact.StorageKey)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	compressed := &countingHashReader{reader: &contextReader{ctx: ctx, reader: file}, hash: sha256.New()}
	decoder, err := zstd.NewReader(compressed, zstd.WithDecoderConcurrency(2), zstd.WithDecoderLowmem(true), zstd.WithDecoderMaxMemory(64<<20))
	if err != nil {
		return nil, err
	}
	defer decoder.Close()
	uncompressed := &countingHashReader{reader: decoder, hash: sha256.New()}
	jsonDecoder := json.NewDecoder(bufio.NewReaderSize(uncompressed, 256<<10))
	states := make([]compactKeyState, 0, artifact.RecordCount)
	var previous uint64
	for {
		var entry ScheduleACompactKeyException
		if err := jsonDecoder.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		key, err := strconv.ParseUint(entry.SubID, 10, 64)
		if err != nil || key == 0 || entry.Partition != partition || compactPartition(key, partitions) != partition || previous != 0 && key <= previous ||
			(entry.State != "invalid" && entry.State != "duplicate") || entry.OccurrenceCount == 0 || !digestPattern.MatchString(entry.ComparisonDigest) {
			return nil, fmt.Errorf("compact key exception is invalid")
		}
		if entry.State == "invalid" && (entry.OccurrenceCount != 1 || entry.RowOrdinal == 0) {
			return nil, fmt.Errorf("compact invalid key exception is incomplete")
		}
		if entry.State == "duplicate" && (entry.OccurrenceCount < 2 || entry.RowOrdinal != 0) {
			return nil, fmt.Errorf("compact duplicate key exception is invalid")
		}
		digest, err := decodeCompactDigest(entry.ComparisonDigest)
		if err != nil {
			return nil, err
		}
		states = append(states, compactKeyState{
			SubID: key, State: entry.State, OccurrenceCount: entry.OccurrenceCount,
			RowOrdinal: entry.RowOrdinal, ComparisonDigest: digest,
		})
		previous = key
	}
	if uint64(len(states)) != artifact.RecordCount || uncompressed.bytes != artifact.UncompressedBytes ||
		hex.EncodeToString(uncompressed.hash.Sum(nil)) != artifact.UncompressedSHA256 ||
		compressed.bytes != artifact.CompressedBytes || hex.EncodeToString(compressed.hash.Sum(nil)) != artifact.CompressedSHA256 {
		return nil, fmt.Errorf("compact key exception artifact identity mismatch")
	}
	return states, nil
}

func mergeCompactStates(unique, exceptions []compactKeyState) ([]compactKeyState, error) {
	states := make([]compactKeyState, 0, len(unique)+len(exceptions))
	left, right := 0, 0
	for left < len(unique) || right < len(exceptions) {
		switch {
		case right >= len(exceptions) || left < len(unique) && unique[left].SubID < exceptions[right].SubID:
			states = append(states, unique[left])
			left++
		case left >= len(unique) || exceptions[right].SubID < unique[left].SubID:
			states = append(states, exceptions[right])
			right++
		default:
			return nil, fmt.Errorf("compact key appears in both unique and exception indexes")
		}
	}
	return states, nil
}

func decodeCompactDigest(value string) ([32]byte, error) {
	var digest [32]byte
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != len(digest) {
		return digest, fmt.Errorf("compact digest is invalid")
	}
	copy(digest[:], decoded)
	return digest, nil
}
