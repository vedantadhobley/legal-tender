package identityassertions

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"

	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/release"
)

type Options struct {
	StorageRoot, ReceiptManifest, CommitteeManifest, Cycle, BuildSHA256 string
	ExpectedViewID                                                      string
	Workers                                                             int
	Progress                                                            func(string)
}

// Consumer callbacks are optional and provisional until Scan returns success.
// Receipt callbacks are serial within a worker, concurrent between workers;
// strings/pointers are borrowed until return. Committee callbacks are serial.
// No callback may mutate the supplied values. Source fact set + ordinal derives
// the same contributor appearance ID as receiptparticipants.AppearanceID.
type Consumer struct {
	Receipt   func(worker int, row Receipt) error
	Committee func(row Committee) error
}

// Scan publishes a deterministic source-backed view as a result, with no new
// text artifact or database write. A caller can retain that result as a manifest
// and replay with ExpectedViewID; source artifacts must remain immutable.
func Scan(ctx context.Context, o Options, consume Consumer) (Result, error) {
	if o.StorageRoot == "" || o.ReceiptManifest == "" || o.CommitteeManifest == "" || !digest(o.BuildSHA256) || o.Workers < 1 || o.Workers > 8 || (o.ExpectedViewID != "" && !digest(o.ExpectedViewID)) {
		return Result{}, fmt.Errorf("exact source manifests, executable digest and 1..8 workers required")
	}
	cycle, err := strconv.ParseInt(o.Cycle, 10, 64)
	if err != nil || cycle <= 0 || strconv.FormatInt(cycle, 10) != o.Cycle {
		return Result{}, fmt.Errorf("canonical positive source cycle required")
	}
	log := o.Progress
	if log == nil {
		log = func(string) {}
	}
	if err = ctx.Err(); err != nil {
		return Result{}, err
	}
	log("verifying immutable receipt and committee fact inputs")
	a, ah, err := occ.LoadPublishedScheduleAColumnarManifest(ctx, o.StorageRoot, o.ReceiptManifest)
	if err != nil {
		return Result{}, err
	}
	c, ch, err := occ.LoadPublishedClassicFactManifest(o.StorageRoot, o.CommitteeManifest, "committee-master")
	if err != nil {
		return Result{}, err
	}
	if filepath.Base(o.ReceiptManifest) != a.FactSetID+".json" || filepath.Base(o.CommitteeManifest) != c.FactSetID+".json" || a.Cycle != o.Cycle || c.Cycle != o.Cycle || a.Counts.Facts != a.Counts.ValidFacts || a.Counts.SourceOccurrences != a.Counts.Facts || a.Counts.InvalidFacts != 0 || a.Counts.ExcludedOccurrences != 0 || c.Counts.Facts != c.Counts.ValidFacts || c.Counts.InvalidFacts != 0 {
		return Result{}, fmt.Errorf("exact immutable valid fact cycle required")
	}
	if err = dense(a); err != nil {
		return Result{}, err
	}
	committees, err := scanCommittee(ctx, o.StorageRoot, c, consume.Committee)
	if err != nil {
		return Result{}, err
	}
	log(fmt.Sprintf("verified %d committee organization assertion records", committees.Rows))
	receipts, err := scanReceipts(ctx, o.StorageRoot, a, cycle, o.Workers, consume.Receipt, log)
	if err != nil {
		return Result{}, err
	}
	out := Result{SchemaVersion: Version, Policy: Policy, BuildSHA256: o.BuildSHA256, Cycle: o.Cycle,
		State: "complete_published_fact_assertion_view", Storage: "references_immutable_source_facts",
		ReceiptSource:   Source{a.FactSetID, ah, a.SourceReleaseID, a.SourceReleaseManifestSHA256, a.Counts.Facts, a.Counts.SourceOccurrences, a.Counts.ExcludedOccurrences},
		CommitteeSource: Source{c.FactSetID, ch, c.SourceReleaseID, c.SourceReleaseManifestSHA256, c.Counts.Facts, c.Counts.SourceOccurrences, c.Counts.ExcludedOccurrences},
		ReceiptColumns:  ReceiptColumns(), CommitteeColumns: CommitteeColumns(), Receipts: receipts, Committee: committees}
	for _, proof := range receipts {
		for j, field := range proof.Fields {
			out.ReceiptFields[j].Null += field.Null
			out.ReceiptFields[j].Empty += field.Empty
			out.ReceiptFields[j].Nonempty += field.Nonempty
		}
	}
	out.ViewID = logicalID(out)
	if err = out.validate(); err != nil {
		return Result{}, err
	}
	if o.ExpectedViewID != "" && out.ViewID != o.ExpectedViewID {
		return Result{}, fmt.Errorf("reported identity view differs from expected identity")
	}
	if err = ctx.Err(); err != nil {
		return Result{}, err
	}
	return out, nil
}

func dense(m occ.ScheduleAColumnarManifest) error {
	if m.Counts.Facts == 0 || m.Counts.Facts > math.MaxInt64 {
		return fmt.Errorf("nonempty representable receipt population required")
	}
	var rows uint64
	for i, s := range m.Shards {
		if s.Index != uint64(i) || s.Facts == 0 || s.Facts > m.Counts.Facts-rows || s.Facts != s.SourceRows || s.Facts != s.ValidFacts || s.FirstSourceRowOrdinal != rows+1 || s.LastSourceRowOrdinal != rows+s.Facts {
			return fmt.Errorf("non-dense receipt assertion source")
		}
		rows += s.Facts
	}
	if rows != m.Counts.Facts {
		return fmt.Errorf("receipt assertion source conservation")
	}
	return nil
}

func scanReceipts(ctx context.Context, root string, m occ.ScheduleAColumnarManifest, cycle int64, workers int, visit func(int, Receipt) error, log func(string)) ([]ReceiptProof, error) {
	if workers < 1 || workers > 8 {
		return nil, fmt.Errorf("1..8 receipt assertion workers required")
	}
	if err := dense(m); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		index int
		proof ReceiptProof
		err   error
	}
	results := make(chan result, workers)
	var next atomic.Int64
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Go(func() {
			for {
				if ctx.Err() != nil {
					return
				}
				i := int(next.Add(1) - 1)
				if i >= len(m.Shards) {
					return
				}
				var callback func(Receipt) error
				if visit != nil {
					callback = func(r Receipt) error { return visit(worker, r) }
				}
				proof, err := scanReceipt(ctx, root, m.Shards[i], cycle, callback)
				results <- result{i, proof, err}
				if err != nil {
					cancel()
					return
				}
			}
		})
	}
	go func() { wg.Wait(); close(results) }()
	out := make([]ReceiptProof, len(m.Shards))
	var first error
	completed := 0
	for r := range results {
		if r.err != nil {
			if first == nil || errors.Is(first, context.Canceled) {
				first = r.err
			}
			cancel()
			continue
		}
		out[r.index] = r.proof
		completed++
		if log != nil {
			log(fmt.Sprintf("verified reported identity shard %d (%d/%d), %d records", r.index, completed, len(out), r.proof.Rows))
		}
	}
	if first != nil {
		return nil, first
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if completed != len(out) {
		return nil, fmt.Errorf("receipt assertion scan incomplete")
	}
	return out, nil
}

// Decode validates an exact stored result. Replay through Scan verifies source
// bytes and all projected values as well; Decode alone is not backing validation.
func Decode(b []byte, expected string) (Result, error) {
	var r Result
	if len(b) > 8<<20 || !digest(expected) {
		return r, fmt.Errorf("bounded assertion manifest and expected digest required")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(&r); err != nil {
		return Result{}, err
	}
	if d.Decode(new(any)) != io.EOF {
		return Result{}, fmt.Errorf("trailing assertion manifest data")
	}
	if r.ViewID != expected {
		return Result{}, fmt.Errorf("assertion manifest identity mismatch")
	}
	if err := r.validate(); err != nil {
		return Result{}, err
	}
	return r, nil
}

func (r Result) validate() error {
	cycle, err := strconv.ParseInt(r.Cycle, 10, 64)
	if err != nil || cycle <= 0 || strconv.FormatInt(cycle, 10) != r.Cycle || r.SchemaVersion != Version || r.Policy != Policy || r.State != "complete_published_fact_assertion_view" || r.Storage != "references_immutable_source_facts" || !digest(r.BuildSHA256) || r.ViewID != logicalID(r) || r.ReceiptColumns != ReceiptColumns() || r.CommitteeColumns != CommitteeColumns() || r.IdentityResolved || r.EmploymentVerified || r.OwnershipVerified || r.TerminalPolicyAdopted || r.FinancialAttribution {
		return fmt.Errorf("assertion view identity/mapping/evidence boundary mismatch")
	}
	for _, s := range []Source{r.ReceiptSource, r.CommitteeSource} {
		if !digest(s.FactSetID) || !digest(s.ManifestSHA256) || !release.ValidReleaseID(s.SourceReleaseID) || !digest(s.SourceReleaseSHA256) || s.PublishedFacts == 0 || s.PublishedFacts > math.MaxInt64 || s.SourceOccurrences < s.PublishedFacts || s.ExcludedOccurrences != s.SourceOccurrences-s.PublishedFacts {
			return fmt.Errorf("assertion source identity/population mismatch")
		}
	}
	if r.ReceiptSource.ExcludedOccurrences != 0 {
		return fmt.Errorf("receipt assertion occurrence gap")
	}
	var rows uint64
	var counts [18]FieldCounts
	for i, p := range r.Receipts {
		s := p.Source
		if p.Rows == 0 || p.Rows > r.ReceiptSource.PublishedFacts-rows || s.Index != uint64(i) || s.Facts != p.Rows || s.SourceRows != p.Rows || s.ValidFacts != p.Rows || s.FirstSourceRowOrdinal != rows+1 || s.LastSourceRowOrdinal != rows+p.Rows || s.Bytes == 0 || !digest(s.SHA256) || !digest(p.ValuesSHA256) || s.StorageKey == "" {
			return fmt.Errorf("receipt assertion proof/range mismatch")
		}
		rows += p.Rows
		for j, c := range p.Fields {
			if !c.valid(p.Rows) {
				return fmt.Errorf("receipt assertion field count mismatch")
			}
			counts[j].Null += c.Null
			counts[j].Empty += c.Empty
			counts[j].Nonempty += c.Nonempty
		}
	}
	if rows != r.ReceiptSource.PublishedFacts || !reflect.DeepEqual(counts, r.ReceiptFields) || r.Committee.Rows != r.CommitteeSource.PublishedFacts || r.Committee.Rows != r.Committee.Source.RecordCount || !digest(r.Committee.ValuesSHA256) {
		return fmt.Errorf("assertion view conservation mismatch")
	}
	for _, c := range r.Committee.Fields {
		if c.Null != 0 || !c.valid(r.Committee.Rows) {
			return fmt.Errorf("committee assertion field count mismatch")
		}
	}
	return nil
}
