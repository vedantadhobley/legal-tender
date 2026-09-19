package identityassertions

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/parquet-go/parquet-go"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleaparquet"
	"github.com/vedantadhobley/legal-tender/internal/storage/artifact"
	"github.com/vedantadhobley/legal-tender/internal/storage/narrowparquet"
)

// scanReceipt checks the exact opened source file and complete original schema,
// not merely the selected fields. The proof covers every selected field value.
func scanReceipt(ctx context.Context, root string, s occ.ScheduleAColumnarShard, cycle int64, visit func(Receipt) error) (ReceiptProof, error) {
	var out ReceiptProof
	path, err := artifact.Resolve(root, s.StorageKey)
	if err != nil {
		return out, err
	}
	f, err := os.Open(path)
	if err != nil {
		return out, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return out, err
	}
	if !info.Mode().IsRegular() || info.Size() < 0 || uint64(info.Size()) != s.Bytes {
		return out, fmt.Errorf("assertion source file size/type mismatch")
	}
	h := sha256.New()
	buf := make([]byte, 128<<10)
	for {
		if err = ctx.Err(); err != nil {
			return out, err
		}
		n, e := f.Read(buf)
		h.Write(buf[:n])
		if e == io.EOF {
			break
		}
		if e != nil {
			return out, e
		}
		if n == 0 {
			return out, io.ErrNoProgress
		}
	}
	if hex.EncodeToString(h.Sum(nil)) != s.SHA256 {
		return out, fmt.Errorf("assertion source file digest mismatch")
	}
	p, err := parquet.OpenFile(f, info.Size())
	if err != nil {
		return out, err
	}
	schema, err := scheduleaparquet.NewSchema()
	if err != nil {
		return out, err
	}
	if p.Schema().String() != schema.Parquet().String() || p.NumRows() != int64(s.Facts) {
		return out, fmt.Errorf("assertion source full schema/count mismatch")
	}
	r, err := narrowparquet.New[Receipt](p)
	if err != nil {
		return out, err
	}
	defer r.Close()
	out.Source = s
	values := sha256.New()
	canonical := make([]byte, 0, 4096)
	rows := make([]Receipt, 2048)
	for {
		if err = ctx.Err(); err != nil {
			return ReceiptProof{}, err
		}
		n, e := r.Read(rows)
		for _, row := range rows[:n] {
			if err = row.validate(s.FirstSourceRowOrdinal+out.Rows, cycle); err != nil {
				return ReceiptProof{}, err
			}
			canonical = row.canonical(canonical)
			values.Write(canonical)
			for j, p := range row.fields() {
				out.Fields[j].observe(p)
			}
			out.Rows++
			if visit != nil {
				if err = visit(row); err != nil {
					return ReceiptProof{}, err
				}
			}
		}
		clear(rows[:n])
		if e == io.EOF {
			break
		}
		if e != nil {
			return ReceiptProof{}, e
		}
		if n == 0 {
			return ReceiptProof{}, io.ErrNoProgress
		}
	}
	if err = ctx.Err(); err != nil {
		return ReceiptProof{}, err
	}
	if out.Rows != s.Facts || s.LastSourceRowOrdinal != s.FirstSourceRowOrdinal+out.Rows-1 {
		return ReceiptProof{}, fmt.Errorf("assertion source conservation mismatch")
	}
	out.ValuesSHA256 = hex.EncodeToString(values.Sum(nil))
	return out, nil
}

func scanCommittee(ctx context.Context, root string, m occ.ClassicFactManifest, visit func(Committee) error) (CommitteeProof, error) {
	var out CommitteeProof
	a := m.Facts
	if a.RecordCount != m.Counts.Facts || a.RecordCount > maxCommitteeFacts || a.UncompressedBytes > 512<<20 {
		return out, fmt.Errorf("committee assertion population/count/resource cap")
	}
	d := artifact.Descriptor{RecordCount: a.RecordCount, UncompressedBytes: a.UncompressedBytes, UncompressedSHA256: a.UncompressedSHA256,
		CompressedBytes: a.CompressedBytes, CompressedSHA256: a.CompressedSHA256, Compression: a.Compression, StorageKey: a.StorageKey}
	r, err := artifact.Open[occ.ClassicFact](ctx, root, d)
	if err != nil {
		return out, err
	}
	defer r.Abort()
	out.Source = a
	seen := make(map[string]bool)
	values := sha256.New()
	canonical := make([]byte, 0, 4096)
	for {
		if err = ctx.Err(); err != nil {
			return CommitteeProof{}, err
		}
		f, ok, e := r.Next()
		if e != nil {
			return CommitteeProof{}, e
		}
		if !ok {
			break
		}
		if len(seen) >= maxCommitteeFacts || f.FactID == "" || seen[f.FactID] || f.OccurrenceID == "" || f.SchemaVersion != m.FactSchemaVersion || f.FactType != m.FactType || f.Dataset != "committee-master" || f.Cycle != m.Cycle || f.OccurrenceSetID != m.OccurrenceSetID || f.SourceReleaseID != m.SourceReleaseID || f.SourceContract != m.SourceContract || f.State != "valid" {
			return CommitteeProof{}, fmt.Errorf("committee assertion fact envelope/identity mismatch")
		}
		seen[f.FactID] = true
		row, e := committeeFields(f)
		if e != nil {
			return CommitteeProof{}, e
		}
		canonical = appendText(canonical[:0], &row.FactID)
		canonical = appendText(canonical, &row.OccurrenceID)
		for i, p := range row.fields() {
			canonical = appendText(canonical, p)
			out.Fields[i].observe(p)
		}
		values.Write(canonical)
		out.Rows++
		if visit != nil {
			if err = visit(row); err != nil {
				return CommitteeProof{}, err
			}
		}
	}
	if err = r.Close(); err != nil {
		return CommitteeProof{}, err
	}
	if err = ctx.Err(); err != nil {
		return CommitteeProof{}, err
	}
	if out.Rows != m.Counts.Facts {
		return CommitteeProof{}, fmt.Errorf("committee assertion conservation")
	}
	out.ValuesSHA256 = hex.EncodeToString(values.Sum(nil))
	return out, nil
}

func committeeFields(f occ.ClassicFact) (Committee, error) {
	var out Committee
	var fields [4]string
	for i, name := range CommitteeColumns() {
		v, ok := f.SourceFields[name]
		if !ok || len(v) > maxTextBytes {
			return out, fmt.Errorf("committee assertion raw field absent/oversized: %s", name)
		}
		fields[i] = v
	}
	b, err := json.Marshal(f.TypedFields)
	if err != nil {
		return out, err
	}
	var typed occ.CommitteeTypedFields
	if err = json.Unmarshal(b, &typed); err != nil {
		return out, err
	}
	if typed.CommitteeID != fields[0] || typed.Name != fields[1] || typed.OrganizationTypeCode != fields[2] || typed.ConnectedOrganization != fields[3] || strconv.Itoa(typed.SourceCycle) != f.Cycle {
		return out, fmt.Errorf("committee assertion raw/typed disagreement")
	}
	return Committee{f.FactID, f.OccurrenceID, fields[0], fields[1], fields[2], fields[3]}, nil
}
