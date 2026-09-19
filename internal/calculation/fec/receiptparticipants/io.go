package receiptparticipants

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/parquet-go/parquet-go"
	parquetzstd "github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/fundingbasis"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleaparquet"
	"github.com/vedantadhobley/legal-tender/internal/storage/artifact"
	"github.com/vedantadhobley/legal-tender/internal/storage/narrowparquet"
)

type budget struct {
	mu          sync.Mutex
	used, limit uint64
}
type cappedWriter struct {
	ctx    context.Context
	writer io.Writer
	budget *budget
}

func (w cappedWriter) Write(b []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	w.budget.mu.Lock()
	defer w.budget.mu.Unlock()
	if uint64(len(b)) > w.budget.limit-w.budget.used {
		return 0, fmt.Errorf("participant output byte cap exceeded")
	}
	n, err := w.writer.Write(b)
	w.budget.used += uint64(n)
	return n, err
}

// Verify the bytes of the exact opened file before the reader uses them.
func verifyOpen(ctx context.Context, f *os.File, size uint64, want string) error {
	h := sha256.New()
	buffer := make([]byte, 128<<10)
	var seen uint64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := f.Read(buffer)
		if n > 0 {
			h.Write(buffer[:n])
			seen += uint64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	if seen != size || hex.EncodeToString(h.Sum(nil)) != want {
		return fmt.Errorf("opened participant/source file digest or size mismatch")
	}
	return nil
}

func scanSource(ctx context.Context, root string, shard occ.ScheduleAColumnarShard, cycle int64, visit func([]fundingbasis.SourceEvidenceRow) error) error {
	path, err := artifact.Resolve(root, shard.StorageKey)
	if err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if err = verifyOpen(ctx, f, shard.Bytes, shard.SHA256); err != nil {
		return err
	}
	p, err := parquet.OpenFile(f, int64(shard.Bytes))
	if err != nil {
		return err
	}
	schema, err := scheduleaparquet.NewSchema()
	if err != nil {
		return err
	}
	if p.Schema().String() != schema.Parquet().String() || p.NumRows() != int64(shard.Facts) {
		return fmt.Errorf("full Schedule A schema/count mismatch")
	}
	r, err := narrowparquet.New[fundingbasis.SourceEvidenceRow](p)
	if err != nil {
		return err
	}
	defer r.Close()
	buffer := make([]fundingbasis.SourceEvidenceRow, 8192)
	var seen uint64
	for {
		if err = ctx.Err(); err != nil {
			return err
		}
		n, readErr := r.Read(buffer)
		for _, row := range buffer[:n] {
			if err = row.Validate(shard.FirstSourceRowOrdinal+seen, cycle); err != nil {
				return err
			}
			for _, s := range []*string{row.Recipient, row.Contributor, row.CleanContributor, row.ConduitID, row.Entity, row.ReceiptType, row.File, row.Transaction, row.BackReference, row.BackSchedule} {
				if s != nil && len(*s) > 4096 {
					return fmt.Errorf("source role/identifier exceeds 4096-byte contract at %d", row.Ordinal)
				}
			}
			if (row.MemoText != nil && len(*row.MemoText) > 1<<20) || (row.ConduitName != nil && len(*row.ConduitName) > 1<<20) {
				return fmt.Errorf("source evidence text exceeds 1MiB contract at %d", row.Ordinal)
			}
			seen++
		}
		if n > 0 {
			if err = visit(buffer[:n]); err != nil {
				return err
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	if seen != shard.Facts {
		return fmt.Errorf("source occurrence conservation")
	}
	return nil
}

type sourceScan func(context.Context, func([]fundingbasis.SourceEvidenceRow) error) error

func writeShard(ctx context.Context, dir string, shard occ.ScheduleAColumnarShard, cap *budget, scan sourceScan) (File, Census, error) {
	desc := File{Name: fmt.Sprintf("participants-%06d.parquet", shard.Index), First: shard.FirstSourceRowOrdinal, Last: shard.LastSourceRowOrdinal, SourceSHA256: shard.SHA256}
	census := newCensus()
	f, err := os.OpenFile(filepath.Join(dir, desc.Name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0640)
	if err != nil {
		return File{}, Census{}, err
	}
	defer f.Close()
	physical, values := sha256.New(), sha256.New()
	w := parquet.NewGenericWriter[Row](cappedWriter{ctx, io.MultiWriter(f, physical), cap}, parquet.Compression(&parquetzstd.Codec{Concurrency: 1}), parquet.MaxRowsPerRowGroup(32768))
	buffer := make([]Row, 8192)
	canonical := make([]byte, 0, 2048)
	err = scan(ctx, func(rows []fundingbasis.SourceEvidenceRow) error {
		for offset := 0; offset < len(rows); offset += len(buffer) {
			batch := rows[offset:min(offset+len(buffer), len(rows))]
			for i, source := range batch {
				if uint64(source.Ordinal) != desc.First+desc.Rows {
					return fmt.Errorf("participant source ordinal mismatch")
				}
				row := project(source)
				buffer[i] = row
				canonical = row.canonical(canonical)
				values.Write(canonical)
				census.observe(row)
				desc.Rows++
			}
			if n, e := w.Write(buffer[:len(batch)]); e != nil {
				return e
			} else if n != len(batch) {
				return io.ErrShortWrite
			}
			clear(buffer[:len(batch)])
		}
		return nil
	})
	// Do not flush partial buffered output on failure or exhausted budget.
	if err != nil {
		return File{}, Census{}, err
	}
	if desc.Rows != shard.Facts || census.Rows != desc.Rows {
		return File{}, Census{}, fmt.Errorf("participant shard conservation")
	}
	if err = ctx.Err(); err != nil {
		return File{}, Census{}, err
	}
	if err = w.Close(); err != nil {
		return File{}, Census{}, err
	}
	if err = f.Sync(); err != nil {
		return File{}, Census{}, err
	}
	info, err := f.Stat()
	if err != nil {
		return File{}, Census{}, err
	}
	if err = f.Close(); err != nil {
		return File{}, Census{}, err
	}
	desc.Bytes = uint64(info.Size())
	desc.SHA256 = hex.EncodeToString(physical.Sum(nil))
	desc.ValuesSHA256 = hex.EncodeToString(values.Sum(nil))
	got, err := ReadShard(ctx, dir, desc, nil)
	if err != nil {
		return File{}, Census{}, err
	}
	if !equalCensus(got, census) {
		return File{}, Census{}, fmt.Errorf("participant disposition readback mismatch")
	}
	return desc, census, nil
}

// ReadShard verifies full bytes, schema, source ordering, values and population.
// The callback borrows each row only until it returns.
func ReadShard(ctx context.Context, dir string, d File, visit func(Row) error) (Census, error) {
	if d.Name == "" || filepath.Base(d.Name) != d.Name || !digest(d.SHA256) || !digest(d.ValuesSHA256) || d.Rows == 0 || d.First == 0 || d.Last < d.First || d.Last-d.First+1 != d.Rows {
		return Census{}, fmt.Errorf("invalid participant shard descriptor")
	}
	f, err := os.Open(filepath.Join(dir, d.Name))
	if err != nil {
		return Census{}, err
	}
	defer f.Close()
	if err = verifyOpen(ctx, f, d.Bytes, d.SHA256); err != nil {
		return Census{}, err
	}
	p, err := parquet.OpenFile(f, int64(d.Bytes))
	if err != nil {
		return Census{}, err
	}
	if p.Schema().String() != parquet.SchemaOf(new(Row)).String() || p.NumRows() != int64(d.Rows) {
		return Census{}, fmt.Errorf("participant schema/count mismatch")
	}
	r := parquet.NewGenericReader[Row](p)
	defer r.Close()
	buffer := make([]Row, 8192)
	values := sha256.New()
	canonical := make([]byte, 0, 2048)
	out := newCensus()
	for {
		if err = ctx.Err(); err != nil {
			return Census{}, err
		}
		n, readErr := r.Read(buffer)
		for _, row := range buffer[:n] {
			if row.Ordinal <= 0 || uint64(row.Ordinal) != d.First+out.Rows || !((row.Amount == nil && row.AmountState == "source_null") || (row.Amount != nil && row.AmountState == "reported_value")) {
				return Census{}, fmt.Errorf("participant ordinal/amount state mismatch")
			}
			canonical = row.canonical(canonical)
			values.Write(canonical)
			out.observe(row)
			if visit != nil {
				if err = visit(row); err != nil {
					return Census{}, err
				}
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return Census{}, readErr
		}
		if n == 0 {
			return Census{}, io.ErrNoProgress
		}
	}
	if out.Rows != d.Rows || hex.EncodeToString(values.Sum(nil)) != d.ValuesSHA256 {
		return Census{}, fmt.Errorf("participant values/count readback mismatch")
	}
	return out, nil
}

func parseCycle(cycle string) (int64, error) { return strconv.ParseInt(cycle, 10, 64) }
