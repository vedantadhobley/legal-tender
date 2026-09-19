package flowreconciliation

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"

	receiver "github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/disbursements"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleaparquet"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulebparquet"
	artifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

// Calculate writes content-addressed cohort and assertion artifacts, then
// returns their verified result. It does not advance a pointer or write a graph.
func Calculate(ctx context.Context, o Options) (Result, error) {
	if err := prepareOptions(&o); err != nil {
		return Result{}, err
	}
	o.Progress("verifying both fact sets and coordinated source-release ancestry")
	inputs, a, b, err := loadInputs(ctx, o)
	if err != nil {
		return Result{}, err
	}
	return calculateLoaded(ctx, o, inputs, a, b)
}

func prepareOptions(o *Options) error {
	if o.Workers == 0 {
		o.Workers = 4
	}
	period, err := strconv.ParseInt(o.Cycle, 10, 64)
	if err != nil || len(o.Cycle) != 4 || period < 1976 || period%2 != 0 || o.Workers < 1 || o.Workers > 16 || o.OutputRoot == "" {
		return fmt.Errorf("even-year cycle, output root, and 1-16 workers required")
	}
	if o.Progress == nil {
		o.Progress = func(string) {}
	}
	return nil
}

func currentPolicy() Policy {
	return Policy{SenderPolicy, receiver.PolicyVersion, disbursements.PolicyVersion, MatchPolicy, SenderRules()}
}

func calculationIdentity(cycle string, inputs Inputs, policy Policy) string {
	return hashJSON(struct {
		Version, Cycle string
		Inputs         Inputs
		Policy         Policy
	}{Version, cycle, inputs, policy})
}

// Call only after loading and verifying the pinned source inputs.
func calculateLoaded(ctx context.Context, o Options, inputs Inputs, a occurrence.ScheduleAColumnarManifest, b occurrence.ScheduleBColumnarManifest) (Result, error) {
	period, _ := strconv.ParseInt(o.Cycle, 10, 64)
	policy := currentPolicy()
	id := calculationIdentity(o.Cycle, inputs, policy)
	as, err := scheduleaparquet.NewSchema()
	if err != nil {
		return Result{}, err
	}
	bs, err := schedulebparquet.NewSchema()
	if err != nil {
		return Result{}, err
	}
	ashards := make([]shard, len(a.Shards))
	for i, s := range a.Shards {
		if s.SourceRows != s.Facts {
			return Result{}, fmt.Errorf("non-dense Schedule A shard")
		}
		ashards[i] = shard{s.StorageKey, s.FirstSourceRowOrdinal, s.LastSourceRowOrdinal, s.Facts, s.Bytes}
	}
	bshards := make([]shard, len(b.Shards))
	for i, s := range b.Shards {
		bshards[i] = shard{s.StorageKey, s.FirstSourceRowOrdinal, s.LastSourceRowOrdinal, s.Facts, s.Bytes}
	}
	o.Progress("selecting receiver-reported Schedule A observations")
	ar, err := scan(ctx, o.StorageRoot, ashards, as.Parquet(), period, o.Workers, selectA, func(r aRow) (int64, int64, *int64) { return r.Ordinal, r.Period, r.Amount }, o.Progress)
	if err != nil {
		return Result{}, err
	}
	o.Progress("selecting sender-reported Schedule B observations")
	br, err := scan(ctx, o.StorageRoot, bshards, bs.Parquet(), period, o.Workers, selectB, func(r bRow) (int64, int64, *int64) { return r.Ordinal, r.Period, r.Amount }, o.Progress)
	if err != nil {
		return Result{}, err
	}
	if ar.total.Rows != inputs.A.Facts || br.total.Rows != inputs.B.Facts {
		return Result{}, fmt.Errorf("source facts not conserved")
	}
	o.Progress(fmt.Sprintf("reconciling %d A and %d B observations without greedy pairing", len(ar.observations), len(br.observations)))
	assertions, err := Reconcile(ctx, ar.observations, br.observations, id)
	if err != nil {
		return Result{}, err
	}
	summary, err := verifyAssertions(ar.observations, br.observations, assertions, id)
	if err != nil {
		return Result{}, err
	}
	aSide, err := writeSide(ctx, o.OutputRoot, id, "schedule-a", ar)
	if err != nil {
		return Result{}, err
	}
	bSide, err := writeSide(ctx, o.OutputRoot, id, "schedule-b", br)
	if err != nil {
		return Result{}, err
	}
	evidence, err := writeVerified(ctx, o.OutputRoot, id, "assertions", assertions)
	if err != nil {
		return Result{}, err
	}
	return Result{Version, id, o.Cycle, "complete_candidate_reconciliation", inputs, policy, aSide, bSide, summary, evidence, false}, nil
}

func writeSide(ctx context.Context, root, id, kind string, scan scanned) (Side, error) {
	result := Side{Total: scan.total, Decisions: make([]Bucket, 0, len(scan.decisions))}
	for key, m := range scan.decisions {
		result.Decisions = append(result.Decisions, Bucket{key, m})
	}
	sort.Slice(result.Decisions, func(i, j int) bool {
		a, b := result.Decisions[i].Key, result.Decisions[j].Key
		if a.State != b.State {
			return a.State < b.State
		}
		if a.ReportingRole != b.ReportingRole {
			return a.ReportingRole < b.ReportingRole
		}
		if a.Type.Present != b.Type.Present {
			return !a.Type.Present
		}
		return a.Type.Value < b.Type.Value
	})
	for _, o := range scan.observations {
		if err := merge(&result.Selected, Measures{1, 1, o.Amount}); err != nil {
			return Side{}, err
		}
	}
	descriptor, err := writeVerified(ctx, root, id, kind, scan.observations)
	result.Observations = descriptor
	return result, err
}

func writeVerified[T any](ctx context.Context, root, id, kind string, rows []T) (artifact.Descriptor, error) {
	base := filepath.ToSlash(filepath.Join("evidence", id))
	w, err := artifact.NewWriter(ctx, root, filepath.Join(root, ".work"), base, kind)
	if err != nil {
		return artifact.Descriptor{}, err
	}
	defer w.Abort()
	for _, r := range rows {
		if err := w.WriteJSON(r); err != nil {
			return artifact.Descriptor{}, err
		}
	}
	d, err := w.Finalize()
	if err != nil {
		return d, err
	}
	reader, err := artifact.Open[T](ctx, root, d)
	if err != nil {
		return d, err
	}
	defer reader.Abort()
	for i := 0; ; i++ {
		r, ok, err := reader.Next()
		if err != nil {
			return d, err
		}
		if !ok {
			if i != len(rows) {
				return d, fmt.Errorf("artifact readback count mismatch")
			}
			break
		}
		if i >= len(rows) || !reflect.DeepEqual(r, rows[i]) {
			return d, fmt.Errorf("artifact readback differs at row %d", i)
		}
	}
	return d, reader.Close()
}

func verifyAssertions(a, b []Observation, assertions []Assertion, id string) ([]Summary, error) {
	ai, bi := map[uint64]Observation{}, map[uint64]Observation{}
	for _, o := range a {
		if _, ok := ai[o.Ordinal]; ok {
			return nil, fmt.Errorf("duplicate A ordinal")
		}
		ai[o.Ordinal] = o
	}
	for _, o := range b {
		if _, ok := bi[o.Ordinal]; ok {
			return nil, fmt.Errorf("duplicate B ordinal")
		}
		bi[o.Ordinal] = o
	}
	states := map[string]*Summary{}
	for _, r := range assertions {
		if r.ID != hashJSON(struct {
			Calculation string
			A, B        []uint64
		}{id, r.A, r.B}) || len(r.A)+len(r.B) == 0 {
			return nil, fmt.Errorf("invalid assertion identity")
		}
		var left, right Observation
		if len(r.A) > 0 {
			left = ai[r.A[0]]
		}
		if len(r.B) > 0 {
			right = bi[r.B[0]]
		}
		if r.State != componentState(len(r.A), len(r.B), left, right) {
			return nil, fmt.Errorf("assertion state differs from membership")
		}
		sum := states[r.State]
		if sum == nil {
			sum = &Summary{State: r.State}
			states[r.State] = sum
		}
		sum.Components++
		var am, bm Measures
		for _, ordinal := range r.A {
			o, ok := ai[ordinal]
			if !ok {
				return nil, fmt.Errorf("reused or missing A occurrence")
			}
			if err := merge(&am, Measures{1, 1, o.Amount}); err != nil {
				return nil, err
			}
			delete(ai, ordinal)
		}
		for _, ordinal := range r.B {
			o, ok := bi[ordinal]
			if !ok {
				return nil, fmt.Errorf("reused or missing B occurrence")
			}
			if err := merge(&bm, Measures{1, 1, o.Amount}); err != nil {
				return nil, err
			}
			delete(bi, ordinal)
		}
		if am.Amount != r.AAmount || bm.Amount != r.BAmount {
			return nil, fmt.Errorf("assertion changed a source amount")
		}
		if err := merge(&sum.A, am); err != nil {
			return nil, err
		}
		if err := merge(&sum.B, bm); err != nil {
			return nil, err
		}
	}
	if len(ai) != 0 || len(bi) != 0 {
		return nil, fmt.Errorf("assertions omitted selected source facts")
	}
	out := make([]Summary, 0, len(states))
	for _, s := range states {
		out = append(out, *s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].State < out[j].State })
	return out, nil
}
