package disbursements

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"sort"
	"strconv"
	"sync"

	"github.com/parquet-go/parquet-go"
	occurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulebparquet"
	artifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

const maxGroups = 250_000
const membership = "Evaluate(policy_version, source_fact_set_id, source_row_ordinal); no per-transaction deduplication"

type factRow struct {
	Ordinal          int64   `parquet:"lt_source_row_ordinal"`
	Period           int64   `parquet:"lt_two_year_transaction_period"`
	Amount           *int64  `parquet:"lt_disbursement_amount_minor_units,optional"`
	AmountState      string  `parquet:"lt_disbursement_amount_state"`
	Memoed           bool    `parquet:"lt_memoed_subtotal"`
	Sender           *string `parquet:"cmte_id,optional"`
	Form             string  `parquet:"filing_form"`
	Line             *string `parquet:"line_num,optional"`
	Schedule         *string `parquet:"schedule_type,optional"`
	MemoCode         *string `parquet:"memo_cd,optional"`
	DisbursementType *string `parquet:"disb_tp,optional"`
	RawRecipient     *string `parquet:"recipient_cmte_id,optional"`
	CleanRecipient   *string `parquet:"clean_recipient_cmte_id,optional"`
	BeneficiaryName  *string `parquet:"benef_cmte_nm,optional"`
	ConduitName      *string `parquet:"conduit_cmte_nm,optional"`
}

func (row factRow) input() Input {
	return Input{Sender: row.Sender, RawRecipient: row.RawRecipient, CleanRecipient: row.CleanRecipient, Form: row.Form, Line: row.Line, Schedule: row.Schedule, MemoCode: row.MemoCode, DisbursementType: row.DisbursementType, BeneficiaryName: row.BeneficiaryName, ConduitName: row.ConduitName, Memoed: row.Memoed, AmountState: row.AmountState, Amount: row.Amount}
}

// Calculate produces a complete deterministic reporting calculation. It never
// changes a fact, publication pointer, graph, or another calculation.
func Calculate(ctx context.Context, options Options) (Result, error) {
	if options.Workers == 0 {
		options.Workers = 4
	}
	if options.Workers < 1 || options.Workers > 16 {
		return Result{}, fmt.Errorf("workers must be between 1 and 16")
	}
	if options.Progress == nil {
		options.Progress = func(string) {}
	}
	options.Progress("verifying Schedule B manifest and all backing shard digests")
	manifest, digest, err := occurrence.LoadPublishedScheduleBColumnarManifest(ctx, options.StorageRoot, options.FactManifestPath)
	if err != nil {
		return Result{}, err
	}
	if manifest.Cycle != options.Cycle {
		return Result{}, fmt.Errorf("fact cycle differs from requested cycle")
	}
	scan, err := scanFacts(ctx, options, manifest)
	if err != nil {
		return Result{}, err
	}
	return assemble(manifest, digest, scan)
}

func assemble(manifest occurrence.ScheduleBColumnarManifest, digest string, scan scanResult) (Result, error) {
	result := Result{SchemaVersion: Version, PolicyVersion: PolicyVersion, State: "complete", Cycle: manifest.Cycle, Input: Source{manifest.FactSetID, digest, manifest.SourceReleaseID, manifest.SourceArtifactSHA256, manifest.PhysicalSchemaVersion, manifest.Counts.Facts}, LineRules: LineRules(), Total: scan.total, Decisions: map[string]Measures{}, Groups: make([]Group, 0, len(scan.groups)), Membership: membership}
	for _, d := range []string{Included, Memo, Separate, Unresolved} {
		result.Decisions[d] = Measures{}
	}
	for _, g := range scan.groups {
		result.Groups = append(result.Groups, *g)
		m := result.Decisions[g.Key.Decision]
		if err := merge(&m, g.Measures); err != nil {
			return Result{}, err
		}
		result.Decisions[g.Key.Decision] = m
	}
	sort.Slice(result.Groups, func(i, j int) bool { return result.Groups[i].FirstOrdinal < result.Groups[j].FirstOrdinal })
	result.GroupsSHA256 = hashJSON(result.Groups)
	result.PolicySHA256 = policyDigest()
	result.CalculationSetID = calculationID(result)
	if result.Decisions[Unresolved].Rows > 0 {
		result.State = "complete_with_unresolved"
	}
	if err := Validate(result); err != nil {
		return Result{}, err
	}
	return result, nil
}

type scanResult struct {
	groups map[Key]*Group
	total  Measures
	err    error
}

func scanFacts(ctx context.Context, options Options, manifest occurrence.ScheduleBColumnarManifest) (scanResult, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan occurrence.ScheduleBColumnarShard)
	results := make(chan scanResult, options.Workers)
	var wg sync.WaitGroup
	for range options.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for shard := range jobs {
				if ctx.Err() != nil {
					return
				}
				r := scanShard(ctx, options.StorageRoot, manifest.Cycle, shard)
				results <- r
				if r.err != nil {
					cancel()
					return
				}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, s := range manifest.Shards {
			select {
			case jobs <- s:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() { wg.Wait(); close(results) }()
	combined := scanResult{groups: map[Key]*Group{}}
	completed := 0
	var firstErr error
	for r := range results {
		if r.err != nil && firstErr == nil {
			firstErr = r.err
			cancel()
		}
		if firstErr != nil {
			continue
		}
		if err := merge(&combined.total, r.total); err != nil {
			firstErr = err
			cancel()
			continue
		}
		for key, g := range r.groups {
			if existing, ok := combined.groups[key]; ok {
				if err := merge(&existing.Measures, g.Measures); err != nil {
					firstErr = err
					cancel()
					break
				}
				if g.FirstOrdinal < existing.FirstOrdinal {
					existing.FirstOrdinal = g.FirstOrdinal
				}
			} else {
				if len(combined.groups) >= maxGroups {
					firstErr = fmt.Errorf("reporting group limit exceeded")
					cancel()
					break
				}
				combined.groups[key] = g
			}
		}
		completed++
		if completed%16 == 0 || completed == len(manifest.Shards) {
			options.Progress(fmt.Sprintf("classified %d/%d shards, %d rows, %d reporting groups", completed, len(manifest.Shards), combined.total.Rows, len(combined.groups)))
		}
	}
	if firstErr != nil {
		return scanResult{}, firstErr
	}
	if ctx.Err() != nil {
		return scanResult{}, ctx.Err()
	}
	if completed != len(manifest.Shards) || combined.total.Rows != manifest.Counts.Facts {
		return scanResult{}, fmt.Errorf("incomplete Schedule B scan")
	}
	return combined, nil
}

func scanShard(ctx context.Context, root, cycle string, shard occurrence.ScheduleBColumnarShard) (r scanResult) {
	r.groups = map[Key]*Group{}
	period, err := strconv.ParseInt(cycle, 10, 64)
	if err != nil {
		r.err = err
		return
	}
	path, err := artifact.Resolve(root, shard.StorageKey)
	if err != nil {
		r.err = err
		return
	}
	f, err := os.Open(path)
	if err != nil {
		r.err = err
		return
	}
	defer f.Close()
	physical, err := parquet.OpenFile(f, int64(shard.Bytes))
	if err != nil {
		r.err = err
		return
	}
	schema, err := schedulebparquet.NewSchema()
	if err != nil {
		r.err = err
		return
	}
	if physical.Schema().String() != schema.Parquet().String() || physical.NumRows() != int64(shard.Facts) {
		r.err = fmt.Errorf("Schedule B physical schema or row count mismatch")
		return
	}
	reader := parquet.NewGenericReader[factRow](physical)
	defer reader.Close()
	buffer := make([]factRow, 8192)
	for {
		if err := ctx.Err(); err != nil {
			r.err = err
			return
		}
		n, readErr := reader.Read(buffer)
		for i := range n {
			row := &buffer[i]
			if row.Ordinal <= 0 || uint64(row.Ordinal) != shard.FirstSourceRowOrdinal+r.total.Rows || uint64(row.Ordinal) > shard.LastSourceRowOrdinal || row.Period != period {
				r.err = fmt.Errorf("Schedule B ordinal or cycle mismatch")
				return
			}
			key, err := Evaluate(row.input())
			if err != nil {
				r.err = fmt.Errorf("ordinal %d: %w", row.Ordinal, err)
				return
			}
			g, ok := r.groups[key]
			if !ok {
				if len(r.groups) >= maxGroups {
					r.err = fmt.Errorf("shard reporting group limit exceeded")
					return
				}
				g = &Group{Key: key, FirstOrdinal: uint64(row.Ordinal)}
				r.groups[key] = g
			}
			m := measure(row.Amount)
			if err := merge(&g.Measures, m); err != nil {
				r.err = err
				return
			}
			if err := merge(&r.total, m); err != nil {
				r.err = err
				return
			}
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				r.err = readErr
			}
			break
		}
		if n == 0 {
			r.err = io.ErrNoProgress
			return
		}
	}
	if r.err == nil && r.total.Rows != shard.Facts {
		r.err = fmt.Errorf("Schedule B shard not conserved")
	}
	return
}

func measure(amount *int64) Measures {
	m := Measures{Rows: 1}
	if amount == nil {
		m.MissingAmountRows = 1
		return m
	}
	m.AmountRows = 1
	m.Amount = *amount
	switch {
	case *amount > 0:
		m.PositiveRows = 1
	case *amount < 0:
		m.NegativeRows = 1
	default:
		m.ZeroRows = 1
	}
	return m
}

func merge(dst *Measures, src Measures) error {
	counts := []struct {
		d *uint64
		s uint64
	}{{&dst.Rows, src.Rows}, {&dst.AmountRows, src.AmountRows}, {&dst.MissingAmountRows, src.MissingAmountRows}, {&dst.PositiveRows, src.PositiveRows}, {&dst.NegativeRows, src.NegativeRows}, {&dst.ZeroRows, src.ZeroRows}}
	for _, c := range counts {
		if math.MaxUint64-*c.d < c.s {
			return fmt.Errorf("row count overflow")
		}
		*c.d += c.s
	}
	if (src.Amount > 0 && dst.Amount > math.MaxInt64-src.Amount) || (src.Amount < 0 && dst.Amount < math.MinInt64-src.Amount) {
		return fmt.Errorf("signed cent overflow")
	}
	dst.Amount += src.Amount
	return nil
}

func hashJSON(value any) string {
	b, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func policyDigest() string {
	return hashJSON(struct {
		Version string
		Rules   []LineRule
	}{PolicyVersion, LineRules()})
}
func calculationID(r Result) string {
	return hashJSON(struct {
		Version, Cycle, Policy string
		Input                  Source
	}{Version, r.Cycle, r.PolicySHA256, r.Input})
}

// Validate checks identities, policy drift, complete membership, and exact
// independent group/decision/source conservation before a result is accepted.
func Validate(r Result) error {
	cycle, err := strconv.Atoi(r.Cycle)
	if err != nil || len(r.Cycle) != 4 || cycle < 1976 || cycle%2 != 0 || !validDigest(r.Input.FactSetID) || !validDigest(r.Input.ManifestSHA256) || !validDigest(r.Input.SourceArtifactSHA256) || len(r.Input.SourceReleaseID) != 68 || r.Input.SourceReleaseID[:4] != "fec-" || !validDigest(r.Input.SourceReleaseID[4:]) || r.Input.PhysicalSchemaVersion != schedulebparquet.PhysicalSchemaVersion {
		return fmt.Errorf("invalid source identity or cycle")
	}
	if r.SchemaVersion != Version || r.PolicyVersion != PolicyVersion || r.PolicySHA256 != policyDigest() || !reflect.DeepEqual(r.LineRules, LineRules()) || r.CalculationSetID != calculationID(r) || r.GroupsSHA256 != hashJSON(r.Groups) || r.Membership != membership || r.GraphEligible {
		return fmt.Errorf("reporting identity or policy mismatch")
	}
	if len(r.Groups) == 0 || len(r.Groups) > maxGroups || r.Input.Facts == 0 || r.Total.Rows != r.Input.Facts {
		return fmt.Errorf("invalid reporting coverage")
	}
	decisions := map[string]Measures{Included: {}, Memo: {}, Separate: {}, Unresolved: {}}
	seen := map[Key]bool{}
	var total Measures
	var previous uint64
	for _, g := range r.Groups {
		if !validKey(g.Key) {
			return fmt.Errorf("reporting key differs from policy")
		}
		if g.FirstOrdinal <= previous || g.FirstOrdinal > r.Input.Facts || seen[g.Key] || g.Measures.Rows == 0 || !validMeasures(g.Measures) {
			return fmt.Errorf("invalid reporting group")
		}
		if _, ok := decisions[g.Key.Decision]; !ok {
			return fmt.Errorf("unknown reporting decision")
		}
		seen[g.Key] = true
		previous = g.FirstOrdinal
		if err := merge(&total, g.Measures); err != nil {
			return err
		}
		m := decisions[g.Key.Decision]
		if err := merge(&m, g.Measures); err != nil {
			return err
		}
		decisions[g.Key.Decision] = m
	}
	if total != r.Total || !reflect.DeepEqual(decisions, r.Decisions) {
		return fmt.Errorf("reporting amounts or rows do not conserve")
	}
	state := "complete"
	if decisions[Unresolved].Rows > 0 {
		state = "complete_with_unresolved"
	}
	if r.State != state {
		return fmt.Errorf("reporting state mismatch")
	}
	return nil
}

func validDigest(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == 32 && hex.EncodeToString(b) == s
}

func validKey(k Key) bool {
	for _, c := range []Cell{k.Sender, k.Line, k.Schedule, k.DisbursementType} {
		if !c.Present && c.Value != "" {
			return false
		}
	}
	scope, role := "unresolved", "unresolved"
	if rule, ok := lineIndex[[2]string{k.Form, k.Line.Value}]; ok && k.Line.Present && k.Schedule == (Cell{true, "SB"}) {
		scope, role = rule.Scope, rule.Role
	}
	if k.Scope != scope || k.Role != role {
		return false
	}
	switch k.RecipientIdentity {
	case "exact_matching_committee_id", "conflicting_committee_ids", "raw_committee_id_only", "clean_committee_id_only", "no_valid_committee_id":
	default:
		return false
	}
	switch k.Decision {
	case Included:
		return k.Reason == "reviewed_regular_form_line" && scope == "regular_committee" && k.Sender.Present && validCommittee(&k.Sender.Value)
	case Memo:
		return k.Reason == "memo_code_x"
	case Separate:
		return k.Reason == "distinct_reporting_scope" && scope != "regular_committee" && scope != "unresolved"
	case Unresolved:
		switch k.Reason {
		case "missing_amount":
			return true
		case "unreviewed_schedule":
			return k.Schedule != (Cell{true, "SB"})
		case "unreviewed_form_line":
			return k.Schedule == (Cell{true, "SB"}) && scope == "unresolved"
		case "invalid_filer_id":
			return scope == "regular_committee" && (!k.Sender.Present || !validCommittee(&k.Sender.Value))
		}
	}
	return false
}
func validMeasures(m Measures) bool {
	if m.AmountRows > m.Rows || m.MissingAmountRows != m.Rows-m.AmountRows || m.PositiveRows > m.AmountRows || m.NegativeRows > m.AmountRows-m.PositiveRows || m.ZeroRows != m.AmountRows-m.PositiveRows-m.NegativeRows {
		return false
	}
	return m.AmountRows != 0 || m.Amount == 0
}
