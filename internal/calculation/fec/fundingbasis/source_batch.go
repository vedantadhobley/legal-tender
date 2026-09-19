package fundingbasis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"slices"
	"strconv"

	"github.com/parquet-go/parquet-go"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

// ReadSourceOccurrences verifies each touched shard once and uses the same
// complete-row decoder as paging. The caller must load the immutable manifest.
// This is a bounded review API, not a whole-cycle processing strategy.
func ReadSourceOccurrences(ctx context.Context, root string, m occ.ScheduleAColumnarManifest, ordinals []uint64) ([]Receipt, error) {
	if len(ordinals) == 0 || len(ordinals) > 4096 {
		return nil, fmt.Errorf("1..4096 distinct review ordinals required")
	}
	ids := slices.Clone(ordinals)
	slices.Sort(ids)
	for i, n := range ids {
		if n == 0 || n > m.Counts.SourceOccurrences || (i > 0 && ids[i-1] == n) {
			return nil, fmt.Errorf("invalid or duplicate source ordinal")
		}
	}
	cycle, err := strconv.ParseInt(m.Cycle, 10, 64)
	if err != nil {
		return nil, err
	}
	var out []Receipt
	for _, shard := range m.Shards {
		if len(ids) == 0 {
			break
		}
		if ids[0] > shard.LastSourceRowOrdinal {
			continue
		}
		if ids[0] < shard.FirstSourceRowOrdinal {
			return nil, fmt.Errorf("source ordinal has no backing shard")
		}
		count := 0
		for count < len(ids) && ids[count] <= shard.LastSourceRowOrdinal {
			count++
		}
		rows, err := readSourceBatchShard(ctx, root, shard, ids[:count])
		if err != nil {
			return nil, err
		}
		for i := range rows {
			v, err := SourceEvidenceFromReceipt(rows[i])
			if err != nil {
				return nil, err
			}
			if err = v.Validate(rows[i].Ordinal, cycle); err != nil {
				return nil, err
			}
			rows[i].Key = classify(v.input().row)
		}
		out = append(out, rows...)
		ids = ids[count:]
	}
	if len(ids) != 0 {
		return nil, fmt.Errorf("unread source ordinals")
	}
	return out, nil
}

func readSourceBatchShard(ctx context.Context, root string, s occ.ScheduleAColumnarShard, ids []uint64) ([]Receipt, error) {
	f, p, err := openShard(root, s)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	h := sha256.New()
	buf := make([]byte, 256<<10)
	var count uint64
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n, err := f.Read(buf)
		if n > 0 {
			h.Write(buf[:n])
			count += uint64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return nil, io.ErrNoProgress
		}
	}
	if count != s.Bytes || hex.EncodeToString(h.Sum(nil)) != s.SHA256 {
		return nil, fmt.Errorf("source batch shard digest mismatch")
	}
	selected := make([]Receipt, len(ids))
	reader := parquet.NewGenericReader[receiptRow](p)
	defer reader.Close()
	buffer := make([]receiptRow, 1)
	for i, n := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := reader.SeekToRow(int64(n - s.FirstSourceRowOrdinal)); err != nil {
			return nil, err
		}
		count, err := reader.Read(buffer)
		if count != 1 || (err != nil && err != io.EOF) {
			return nil, fmt.Errorf("exact source batch read: %v", err)
		}
		if err := validRow(buffer[0], n, buffer[0].Cycle); err != nil {
			return nil, err
		}
		selected[i] = Receipt{Ordinal: n, ShardSHA256: s.SHA256, Key: classify(buffer[0])}
	}
	return readFullReceipts(ctx, p, s.FirstSourceRowOrdinal, selected)
}
