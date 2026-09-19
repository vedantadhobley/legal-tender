package fundingbasis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"

	"github.com/parquet-go/parquet-go"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
)

// Query returns source occurrences, never name-grouped donor identities. Empty
// Component selects every component. The cursor is an exclusive source ordinal.
func (r *Reader) Query(ctx context.Context, q Query) (Page, error) {
	if !committeeflows.ValidCommitteeID(&q.Committee) || q.Limit < 1 || q.Limit > 100 ||
		(q.Component != "" && !validComponent(q.Component)) || q.After > r.result.Input.Facts {
		return Page{}, fmt.Errorf("exact committee ID, valid component/cursor, and limit 1..100 required")
	}
	page := Page{SchemaVersion: "legal-tender.fec.funding-receipt-page.v1", CalculationID: r.result.CalculationID,
		Input: r.result.Input, Committee: q.Committee, Component: q.Component, After: q.After, Receipts: []Receipt{}}
	bits := make([]byte, (len(r.manifest.Shards)+7)/8)
	for _, b := range r.result.Buckets {
		if b.Key.Recipient.Present && b.Key.Recipient.Value == q.Committee && (q.Component == "" || b.Key.Component == q.Component) && b.Last > q.After {
			for i, v := range b.Shards {
				bits[i] |= v
			}
		}
	}
	for i, s := range r.manifest.Shards {
		if bits[i/8]&(1<<uint(i%8)) == 0 || s.LastSourceRowOrdinal <= q.After {
			continue
		}
		rows, err := r.queryShard(ctx, i, q, q.Limit+1-len(page.Receipts))
		if err != nil {
			return Page{}, err
		}
		page.Receipts = append(page.Receipts, rows...)
		if len(page.Receipts) > q.Limit {
			page.HasMore = true
			page.Receipts = page.Receipts[:q.Limit]
			break
		}
	}
	if err := ctx.Err(); err != nil {
		return Page{}, err
	}
	if page.HasMore {
		v := page.Receipts[len(page.Receipts)-1].Ordinal
		page.NextAfter = &v
	}
	return page, nil
}

func (r *Reader) queryShard(ctx context.Context, index int, q Query, limit int) ([]Receipt, error) {
	return r.queryShardFile(ctx, index, q, limit, "")
}

func (r *Reader) queryShardFile(ctx context.Context, index int, q Query, limit int, file string) ([]Receipt, error) {
	s := r.manifest.Shards[index]
	f, p, err := openShard(r.root, s)
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
		n, e := f.Read(buf)
		if n > 0 {
			h.Write(buf[:n])
			count += uint64(n)
		}
		if e == io.EOF {
			break
		}
		if e != nil {
			return nil, e
		}
		if n == 0 {
			return nil, io.ErrNoProgress
		}
	}
	if count != s.Bytes || hex.EncodeToString(h.Sum(nil)) != s.SHA256 {
		return nil, fmt.Errorf("source shard changed after inventory verification")
	}
	reader := parquet.NewGenericReader[receiptRow](p)
	defer reader.Close()
	start := s.FirstSourceRowOrdinal
	if q.After >= start {
		start = q.After + 1
	}
	if err := reader.SeekToRow(int64(start - s.FirstSourceRowOrdinal)); err != nil {
		return nil, err
	}
	cycle, err := strconv.ParseInt(r.result.Cycle, 10, 64)
	if err != nil {
		return nil, err
	}
	buffer := make([]receiptRow, 8192)
	type reportNumber struct {
		File *string `parquet:"file_num"`
	}
	var files *parquet.GenericReader[reportNumber]
	var fileBuffer []reportNumber
	if file != "" {
		files = parquet.NewGenericReader[reportNumber](p)
		defer files.Close()
		if err := files.SeekToRow(int64(start - s.FirstSourceRowOrdinal)); err != nil {
			return nil, err
		}
		fileBuffer = make([]reportNumber, len(buffer))
	}
	var selected []Receipt
	ordinal := start
	for len(selected) < limit {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n, e := reader.Read(buffer)
		if files != nil && n > 0 {
			got, err := files.Read(fileBuffer[:n])
			if got != n || err != nil && err != io.EOF {
				return nil, fmt.Errorf("report column read mismatch: %v", err)
			}
		}
		for rowIndex, row := range buffer[:n] {
			if err := validRow(row, ordinal, cycle); err != nil {
				return nil, err
			}
			ordinal++
			if files != nil && (fileBuffer[rowIndex].File == nil || *fileBuffer[rowIndex].File != file) {
				continue
			}
			if q.Committee != "" && (row.Recipient == nil || *row.Recipient != q.Committee) {
				continue
			}
			key := classify(row)
			if q.Component != "" && key.Component != q.Component {
				continue
			}
			selected = append(selected, Receipt{Ordinal: uint64(row.Ordinal), ShardSHA256: s.SHA256, Key: key})
			if len(selected) == limit {
				break
			}
		}
		if e == io.EOF {
			break
		}
		if e != nil {
			return nil, e
		}
		if n == 0 {
			return nil, io.ErrNoProgress
		}
	}
	return readFullReceipts(ctx, p, s.FirstSourceRowOrdinal, selected)
}

// readFullReceipts is shared by paging and bounded exact-ordinal source review.
func readFullReceipts(ctx context.Context, p *parquet.File, first uint64, selected []Receipt) ([]Receipt, error) {
	full := parquet.NewReader(p)
	defer full.Close()
	columns := p.Schema().Columns()
	raw := make([]parquet.Row, 1)
	for i := range selected {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := full.SeekToRow(int64(selected[i].Ordinal - first)); err != nil {
			return nil, err
		}
		n, err := full.ReadRows(raw)
		if n != 1 || err != nil && err != io.EOF {
			return nil, fmt.Errorf("read full receipt row: %w", err)
		}
		fields := make(map[string]any, len(columns))
		for _, v := range raw[0] {
			if v.Column() < 0 || v.Column() >= len(columns) || len(columns[v.Column()]) != 1 {
				return nil, fmt.Errorf("unexpected source column")
			}
			name := columns[v.Column()][0]
			if _, exists := fields[name]; exists {
				return nil, fmt.Errorf("duplicate source column")
			}
			switch {
			case v.IsNull():
				fields[name] = nil
			case v.Kind() == parquet.ByteArray:
				fields[name] = string(v.ByteArray())
			case v.Kind() == parquet.Boolean:
				fields[name] = v.Boolean()
			case v.Kind() == parquet.Int64:
				fields[name] = strconv.FormatInt(v.Int64(), 10)
			case v.Kind() == parquet.Int32:
				fields[name] = v.Int32()
			default:
				return nil, fmt.Errorf("unexpected source physical type")
			}
		}
		if len(fields) != len(columns) || fields["lt_source_row_ordinal"] != strconv.FormatUint(selected[i].Ordinal, 10) {
			return nil, fmt.Errorf("source row reconstruction mismatch")
		}
		selected[i].Fields = fields
	}
	return selected, nil
}
