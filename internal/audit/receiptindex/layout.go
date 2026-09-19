package receiptindex

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"

	"github.com/parquet-go/parquet-go"
	parquetzstd "github.com/parquet-go/parquet-go/compress/zstd"
)

type File struct {
	Name         string `json:"name"`
	Rows         uint64 `json:"rows"`
	Bytes        uint64 `json:"bytes"`
	SHA256       string `json:"sha256"`
	ValuesSHA256 string `json:"values_sha256"`
}

// A shared cumulative cap includes both layouts and failed/partial writes.
type budget struct{ used, limit uint64 }
type budgetWriter struct {
	ctx    context.Context
	w      io.Writer
	budget *budget
}

func (w budgetWriter) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	if uint64(len(p)) > w.budget.limit-w.budget.used {
		return 0, fmt.Errorf("benchmark output byte budget exceeded")
	}
	n, err := w.w.Write(p)
	w.budget.used += uint64(n)
	return n, err
}

// measure writes and reads back every projected value while the bounded input
// run is still in memory. Neither a matching count nor a digest alone suffices.
func measure(ctx context.Context, dir, name string, rows []Row, cap *budget) (File, error) {
	var result File
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		return result, err
	}
	defer f.Close()
	h := sha256.New()
	w := parquet.NewGenericWriter[Row](budgetWriter{ctx, io.MultiWriter(f, h), cap},
		parquet.Compression(&parquetzstd.Codec{Concurrency: 1}), parquet.MaxRowsPerRowGroup(32768))
	if n, err := w.Write(rows); err != nil || n != len(rows) {
		// Do not flush more data after a budget/cancellation failure.
		if err == nil {
			err = io.ErrShortWrite
		}
		return result, err
	}
	if err := w.Close(); err != nil {
		return result, err
	}
	if err := f.Sync(); err != nil {
		return result, err
	}
	info, err := f.Stat()
	if err != nil {
		return result, err
	}
	if err := f.Close(); err != nil {
		return result, err
	}
	digest, err := verify(ctx, filepath.Join(dir, name), rows)
	if err != nil {
		return result, err
	}
	return File{Name: name, Rows: uint64(len(rows)), Bytes: uint64(info.Size()), SHA256: hex.EncodeToString(h.Sum(nil)), ValuesSHA256: digest}, nil
}

func verify(ctx context.Context, path string, expected []Row) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return "", err
	}
	p, err := parquet.OpenFile(f, info.Size())
	if err != nil {
		return "", err
	}
	if p.Schema().String() != parquet.SchemaOf(new(Row)).String() || p.NumRows() != int64(len(expected)) {
		return "", fmt.Errorf("index readback schema/count mismatch")
	}
	r := parquet.NewGenericReader[Row](p)
	defer r.Close()
	buffer := make([]Row, 4096)
	h := sha256.New()
	encoder := json.NewEncoder(h)
	seen := 0
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		n, err := r.Read(buffer)
		for _, row := range buffer[:n] {
			if seen >= len(expected) || !reflect.DeepEqual(row, expected[seen]) {
				return "", fmt.Errorf("index readback mismatch at position %d", seen)
			}
			if err := encoder.Encode(row); err != nil {
				return "", err
			}
			seen++
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
		if n == 0 {
			return "", io.ErrNoProgress
		}
	}
	if seen != len(expected) {
		return "", fmt.Errorf("index readback row count mismatch")
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
