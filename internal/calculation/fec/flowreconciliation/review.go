package flowreconciliation

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"

	receiver "github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/disbursements"
	artifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

const ReviewVersion = "legal-tender.fec.committee-flow-review.v1"

type ReviewOptions struct{ ResultPath, EvidenceRoot, StorageRoot string }
type ReviewKey struct {
	State             string `json:"state"`
	ATypes            string `json:"schedule_a_types"`
	BTypes            string `json:"schedule_b_types"`
	Cardinality       string `json:"cardinality"`
	HasExactSignature bool   `json:"has_shared_exact_signature"`
}
type ReviewShape struct {
	Key                   ReviewKey `json:"key"`
	Components            uint64    `json:"components"`
	A                     Measures  `json:"schedule_a"`
	B                     Measures  `json:"schedule_b"`
	SharedExactSignatures uint64    `json:"shared_exact_signatures"`
	UniqueExactSignatures uint64    `json:"one_per_side_exact_signatures"`
}
type ReviewDate struct {
	AMinusBDays int64  `json:"a_minus_b_days"`
	Components  uint64 `json:"components"`
}
type ReviewExample struct {
	Reasons    []string      `json:"selection_reasons"`
	Assertion  Assertion     `json:"assertion"`
	A          []Observation `json:"schedule_a_observations"`
	B          []Observation `json:"schedule_b_observations"`
	ATruncated bool          `json:"schedule_a_examples_truncated"`
	BTruncated bool          `json:"schedule_b_examples_truncated"`
}
type SourceExample struct {
	Side        string         `json:"side"`
	FactSetID   string         `json:"fact_set_id"`
	Ordinal     uint64         `json:"source_row_ordinal"`
	ShardSHA256 string         `json:"shard_sha256"`
	Fields      map[string]any `json:"fields"`
}
type ReviewResult struct {
	SchemaVersion        string          `json:"schema_version"`
	ResultSHA256         string          `json:"result_sha256"`
	CalculationID        string          `json:"calculation_set_id"`
	Shapes               []ReviewShape   `json:"shapes"`
	Dates                []ReviewDate    `json:"one_to_one_date_differences"`
	Examples             []ReviewExample `json:"examples"`
	SourceExamples       []SourceExample `json:"source_examples"`
	VerifiedSourceShards uint64          `json:"verified_source_shards"`
	GraphEligible        bool            `json:"graph_eligible"`
}

// Review replays the complete saved candidate evidence, profiles every component,
// and reads only selected source examples. It does not revise membership or
// classify any economic flow. Sampling is targeted, not statistically random.
func Review(ctx context.Context, o ReviewOptions) (ReviewResult, error) {
	r, a, b, assertions, digest, err := readReviewEvidence(ctx, o)
	if err != nil {
		return ReviewResult{}, err
	}
	out, err := profileComponents(ctx, a, b, assertions)
	if err != nil {
		return out, err
	}
	out.SchemaVersion, out.ResultSHA256, out.CalculationID = ReviewVersion, digest, r.CalculationSetID
	out.SourceExamples, out.VerifiedSourceShards, err = reviewSourceExamples(ctx, o.StorageRoot, r, out.Examples)
	return out, err
}

func readReviewEvidence(ctx context.Context, o ReviewOptions) (Result, []Observation, []Observation, []Assertion, string, error) {
	var r Result
	fail := func(err error) (Result, []Observation, []Observation, []Assertion, string, error) {
		return r, nil, nil, nil, "", err
	}
	content, err := os.ReadFile(o.ResultPath)
	if err != nil {
		return fail(err)
	}
	if err := strictJSON(content, &r); err != nil {
		return fail(err)
	}
	expectedPolicy := Policy{SenderPolicy, receiver.PolicyVersion, disbursements.PolicyVersion, MatchPolicy, SenderRules()}
	id := hashJSON(struct {
		Version, Cycle string
		Inputs         Inputs
		Policy         Policy
	}{Version, r.Cycle, r.Input, r.Policy})
	if r.SchemaVersion != Version || r.State != "complete_candidate_reconciliation" || r.GraphEligible || !reflect.DeepEqual(r.Policy, expectedPolicy) || r.CalculationSetID != id {
		return fail(fmt.Errorf("unsupported or inconsistent reconciliation result"))
	}
	a, err := readReviewArtifact[Observation](ctx, o.EvidenceRoot, r.A.Observations, maxSelected)
	if err != nil {
		return fail(err)
	}
	b, err := readReviewArtifact[Observation](ctx, o.EvidenceRoot, r.B.Observations, maxSelected)
	if err != nil {
		return fail(err)
	}
	assertions, err := readReviewArtifact[Assertion](ctx, o.EvidenceRoot, r.Assertions, 2*maxSelected)
	if err != nil {
		return fail(err)
	}
	replay, err := Reconcile(ctx, a, b, id)
	if err != nil {
		return fail(err)
	}
	if !reflect.DeepEqual(replay, assertions) {
		return fail(fmt.Errorf("saved assertions differ from full candidate replay"))
	}
	summary, err := verifyAssertions(a, b, assertions, id)
	if err != nil {
		return fail(err)
	}
	if !reflect.DeepEqual(summary, r.Summary) {
		return fail(fmt.Errorf("saved summary differs from evidence"))
	}
	for i, side := range []Side{r.A, r.B} {
		var total, selected Measures
		facts := r.Input.A.Facts
		if i == 1 {
			facts = r.Input.B.Facts
		}
		if side.Total.Rows != facts || side.Total.Known > side.Total.Rows {
			return fail(fmt.Errorf("source counts differ from input identity"))
		}
		seen := map[DecisionKey]bool{}
		for _, d := range side.Decisions {
			if d.Measures.Known > d.Measures.Rows || d.Measures.Known == 0 && d.Measures.Amount != 0 {
				return fail(fmt.Errorf("invalid selection measures"))
			}
			if seen[d.Key] {
				return fail(fmt.Errorf("duplicate selection bucket"))
			}
			seen[d.Key] = true
			if err := merge(&total, d.Measures); err != nil {
				return fail(err)
			}
		}
		rows := a
		if i == 1 {
			rows = b
		}
		for _, row := range rows {
			if err := merge(&selected, Measures{1, 1, row.Amount}); err != nil {
				return fail(err)
			}
		}
		if total != side.Total || selected != side.Selected {
			return fail(fmt.Errorf("saved selection totals differ"))
		}
	}
	return r, a, b, assertions, hashBytes(content), nil
}
func readReviewArtifact[T any](ctx context.Context, root string, d artifact.Descriptor, limit uint64) ([]T, error) {
	if d.RecordCount > limit {
		return nil, fmt.Errorf("review evidence capacity exceeded")
	}
	reader, err := artifact.Open[T](ctx, root, d)
	if err != nil {
		return nil, err
	}
	defer reader.Abort()
	rows := make([]T, 0, int(d.RecordCount))
	for {
		r, ok, err := reader.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		if uint64(len(rows)) >= limit {
			return nil, fmt.Errorf("review evidence capacity exceeded")
		}
		rows = append(rows, r)
	}
	return rows, reader.Close()
}

func profileComponents(ctx context.Context, a, b []Observation, assertions []Assertion) (ReviewResult, error) {
	out := ReviewResult{Shapes: []ReviewShape{}, Dates: []ReviewDate{}, Examples: []ReviewExample{}, SourceExamples: []SourceExample{}}
	ai, bi := map[uint64]Observation{}, map[uint64]Observation{}
	for _, o := range a {
		ai[o.Ordinal] = o
	}
	for _, o := range b {
		bi[o.Ordinal] = o
	}
	groups := map[ReviewKey]*ReviewShape{}
	sampled := map[ReviewKey]bool{}
	dates := map[int64]uint64{}
	chosen := map[int][]string{}
	largest := map[string]int{}
	maxGapIndex := -1
	var maxGap uint64
	for i, r := range assertions {
		if ctx.Err() != nil {
			return out, ctx.Err()
		}
		left, right := make([]Observation, 0, len(r.A)), make([]Observation, 0, len(r.B))
		for _, id := range r.A {
			o, ok := ai[id]
			if !ok {
				return out, fmt.Errorf("unknown A occurrence")
			}
			left = append(left, o)
		}
		for _, id := range r.B {
			o, ok := bi[id]
			if !ok {
				return out, fmt.Errorf("unknown B occurrence")
			}
			right = append(right, o)
		}
		shared, unique := exactSignatures(left, right)
		key := ReviewKey{r.State, reviewTypes(left), reviewTypes(right), cardinality(len(left)) + ":" + cardinality(len(right)), shared > 0}
		g := groups[key]
		if g == nil {
			if len(groups) >= 10000 {
				return out, fmt.Errorf("review profile capacity exceeded")
			}
			g = &ReviewShape{Key: key}
			groups[key] = g
		}
		// Retain complete type profiles, but sample type combinations only for
		// conflicts. Ambiguity examples span cardinality and exact-signature
		// presence, not every combination in a large component's type set.
		sampleKey := key
		if r.State != "conflicting_role" && r.State != "conflicting_amount" {
			sampleKey.ATypes = ""
			sampleKey.BTypes = ""
		}
		if !sampled[sampleKey] {
			sampled[sampleKey] = true
			chosen[i] = append(chosen[i], "first_component_in_review_stratum")
		}
		g.Components++
		g.SharedExactSignatures += shared
		g.UniqueExactSignatures += unique
		if err := merge(&g.A, Measures{uint64(len(left)), uint64(len(left)), r.AAmount}); err != nil {
			return out, err
		}
		if err := merge(&g.B, Measures{uint64(len(right)), uint64(len(right)), r.BAmount}); err != nil {
			return out, err
		}
		prior, exists := largest[r.State]
		if !exists || len(r.A)+len(r.B) > len(assertions[prior].A)+len(assertions[prior].B) {
			largest[r.State] = i
		}
		if len(left) == 1 && len(right) == 1 && left[0].Date != nil && right[0].Date != nil {
			gap := int64(*left[0].Date) - int64(*right[0].Date)
			dates[gap]++
			if absReview(gap) > maxGap {
				maxGap = absReview(gap)
				maxGapIndex = i
			}
		}
	}
	for state, i := range largest {
		chosen[i] = append(chosen[i], "largest_component_in_state:"+state)
	}
	if maxGapIndex >= 0 {
		chosen[maxGapIndex] = append(chosen[maxGapIndex], "largest_one_to_one_date_gap")
	}
	if len(chosen) > 128 {
		return out, fmt.Errorf("review sample shape capacity exceeded")
	}
	for _, g := range groups {
		out.Shapes = append(out.Shapes, *g)
	}
	sort.Slice(out.Shapes, func(i, j int) bool { return reviewKeyString(out.Shapes[i].Key) < reviewKeyString(out.Shapes[j].Key) })
	for day, n := range dates {
		out.Dates = append(out.Dates, ReviewDate{day, n})
	}
	sort.Slice(out.Dates, func(i, j int) bool { return out.Dates[i].AMinusBDays < out.Dates[j].AMinusBDays })
	indices := make([]int, 0, len(chosen))
	for i := range chosen {
		indices = append(indices, i)
	}
	sort.Ints(indices)
	for _, i := range indices {
		r := assertions[i]
		reasons := chosen[i]
		sort.Strings(reasons)
		e := ReviewExample{Reasons: reasons, Assertion: r, A: []Observation{}, B: []Observation{}, ATruncated: len(r.A) > 4, BTruncated: len(r.B) > 4}
		for _, ordinal := range r.A[:min(4, len(r.A))] {
			e.A = append(e.A, ai[ordinal])
		}
		for _, ordinal := range r.B[:min(4, len(r.B))] {
			e.B = append(e.B, bi[ordinal])
		}
		out.Examples = append(out.Examples, e)
	}
	return out, nil
}
func absReview(v int64) uint64 {
	if v < 0 {
		return uint64(-(v + 1)) + 1
	}
	return uint64(v)
}
func cardinality(n int) string {
	if n == 0 {
		return "0"
	}
	if n == 1 {
		return "1"
	}
	return "many"
}
func reviewTypes(rows []Observation) string {
	m := map[string]bool{}
	for _, r := range rows {
		m[r.Type] = true
	}
	keys := []string{}
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}
func reviewKeyString(k ReviewKey) string {
	return fmt.Sprintf("%s|%s|%s|%s|%t", k.State, k.ATypes, k.BTypes, k.Cardinality, k.HasExactSignature)
}
func exactSignatures(a, b []Observation) (shared, unique uint64) {
	type key struct {
		Role   string
		Amount int64
		Date   int32
	}
	counts := map[key][2]int{}
	for side, rows := range [][]Observation{a, b} {
		for _, r := range rows {
			if r.Date == nil {
				continue
			}
			k := key{r.Role, r.Amount, *r.Date}
			v := counts[k]
			v[side]++
			counts[k] = v
		}
	}
	for _, v := range counts {
		if v[0] > 0 && v[1] > 0 {
			shared++
			if v[0] == 1 && v[1] == 1 {
				unique++
			}
		}
	}
	return
}
