package fundinggeneration

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	flow "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
	receipt "github.com/vedantadhobley/legal-tender/internal/projection/arango/receiptgraph"
)

const PathsGateVersion = "legal-tender.funding-paths-gate.v1"

type PathCase struct {
	Kind   string       `json:"kind"`
	State  string       `json:"state"`
	Result *PathsResult `json:"result"`
}
type PathsGate struct {
	SchemaVersion string     `json:"schema_version"`
	GateID        string     `json:"gate_id"`
	GenerationID  string     `json:"generation_id"`
	BuildSHA256   string     `json:"consumer_executable_sha256"`
	Scope         string     `json:"scope"`
	Cases         []PathCase `json:"cases"`
}

// Select a two-observation witness followed by a candidate context. An origin
// already carrying that candidate ending is excluded so the live case must
// exercise a committee hop, not just a direct authorization/spending edge.
func selectPathWitness(chain pathTopology, endings []graphread.Link) (string, string, bool) {
	incoming := pathTopology{}
	for _, links := range chain {
		for _, e := range links {
			incoming[e.To] = append(incoming[e.To], e)
		}
	}
	for _, es := range incoming {
		sort.Slice(es, func(i, j int) bool { return es[i].ID() < es[j].ID() })
	}
	sorted := append([]graphread.Link{}, endings...)
	sort.Slice(sorted, func(i, j int) bool {
		a, b := sorted[i], sorted[j]
		if a.To != b.To {
			return a.To < b.To
		}
		if a.From != b.From {
			return a.From < b.From
		}
		return a.ID() < b.ID()
	})
	direct := map[string]map[string]bool{}
	for _, e := range sorted {
		if direct[e.To] == nil {
			direct[e.To] = map[string]bool{}
		}
		direct[e.To][e.From] = true
	}
	for _, end := range sorted {
		for _, second := range incoming[end.From] {
			if second.From == second.To {
				continue
			}
			for _, first := range incoming[second.From] {
				if first.From == first.To || first.From == second.To || direct[end.To][first.From] {
					continue
				}
				return first.From, end.To, true
			}
		}
	}
	return "", "", false
}

func (r *Reader) ValidatePaths(ctx context.Context, expected string, progress func(string)) (PathsGate, error) {
	if expected != "" && !validDigest(expected) {
		return PathsGate{}, fmt.Errorf("invalid expected path gate identity")
	}
	if progress == nil {
		progress = func(string) {}
	}
	out := PathsGate{SchemaVersion: PathsGateVersion, GenerationID: r.queryGenerationID(), BuildSHA256: r.consumerBuild, Scope: "data_selected_typed_paths_not_all_entities_or_terminal_sources", Cases: []PathCase{}}
	run := func(kind string, q PathQuery, requirePath bool) error {
		progress("reading typed path case " + kind)
		result, err := r.Paths(ctx, q)
		if err != nil {
			return fmt.Errorf("%s: %w", kind, err)
		}
		if requirePath && len(result.Paths) == 0 {
			return fmt.Errorf("selected path witness not returned: %s (%s)", kind, result.Search.State)
		}
		if requirePath {
			for _, p := range result.Paths {
				if len(p.Links) < 2 {
					return fmt.Errorf("multi-hop witness collapsed to direct context")
				}
			}
		}
		out.Cases = append(out.Cases, PathCase{Kind: kind, State: "verified", Result: &result})
		return nil
	}
	var baseline *PathQuery
	for _, side := range []flow.Ledger{flow.ScheduleA, flow.ScheduleB} {
		chain, err := r.chainTopology(ctx, side)
		if err != nil {
			return out, err
		}
		for _, ending := range []string{"candidate_authorization_context", "independent_support", "independent_opposition"} {
			links, err := r.endingLinks(ending)
			if err != nil {
				return out, err
			}
			from, target, ok := selectPathWitness(chain, links)
			kind := string(side) + "_" + ending
			if !ok {
				out.Cases = append(out.Cases, PathCase{Kind: kind, State: "no_matching_selection_population"})
				continue
			}
			q := PathQuery{From: from, Ledger: side, Target: target, Ending: ending, MaxHops: 2, Limit: 2, Budget: 100000}
			if err := run(kind, q, true); err != nil {
				return out, err
			}
			if baseline == nil {
				copy := q
				baseline = &copy
			}
		}
	}
	if baseline != nil {
		// Obtain an entry from the exact published receipt page for the selected
		// origin, rather than deriving appearance identity from names or amounts.
		page, err := r.receipts.Page(ctx, "reported_receipt", baseline.From, "", 1)
		if err != nil {
			return out, err
		}
		if len(page.Items) > 0 {
			var doc struct {
				Ordinal uint64 `json:"source_row_ordinal"`
			}
			if err := json.Unmarshal(page.Items[0].Document, &doc); err != nil || doc.Ordinal == 0 {
				return out, fmt.Errorf("invalid selected receipt witness")
			}
			q := *baseline
			q.From = ""
			q.EntryFamily = "reported_receipt"
			q.ReceiptOrdinal = doc.Ordinal
			if err := run("receipt_to_candidate", q, true); err != nil {
				return out, err
			}
		} else {
			out.Cases = append(out.Cases, PathCase{Kind: "receipt_to_candidate", State: "no_matching_selection_population"})
		}
		bounded := *baseline
		bounded.MaxHops = 0
		if err := run("hop_bound", bounded, false); err != nil {
			return out, err
		}
		v := out.Cases[len(out.Cases)-1].Result
		if len(v.Paths) != 0 || v.Search.HopFrontiers == 0 {
			return out, fmt.Errorf("hop bound witness did not expose frontier")
		}
		bounded = *baseline
		bounded.Budget = 1
		if err := run("expansion_budget", bounded, false); err != nil {
			return out, err
		}
		if out.Cases[len(out.Cases)-1].Result.Search.State != "truncated_expansion_budget" {
			return out, fmt.Errorf("budget witness did not truncate")
		}
	}
	// Conduit entry crosses to a committee via association, never extra money.
	conduit, err := r.receipts.ConduitWitnessID(ctx)
	if err != nil {
		return out, err
	}
	if conduit != "" {
		page, err := r.receipts.Page(ctx, "conduit_association", conduit, "", 1)
		if err != nil {
			return out, err
		}
		chain, err := r.chainTopology(ctx, flow.ScheduleA)
		if err != nil {
			return out, err
		}
		target := ""
		for _, edge := range chain[conduit] {
			if edge.To != conduit {
				target = edge.To
				break
			}
		}
		if len(page.Items) > 0 && target != "" {
			var doc struct {
				Decision struct {
					Ordinal uint64 `json:"source_row_ordinal"`
				} `json:"decision"`
			}
			if err := json.Unmarshal(page.Items[0].Document, &doc); err != nil || doc.Decision.Ordinal == 0 {
				return out, fmt.Errorf("invalid conduit witness ordinal")
			}
			q := PathQuery{ReceiptOrdinal: doc.Decision.Ordinal, EntryFamily: "conduit_association", Ledger: flow.ScheduleA, Target: target, MaxHops: 1, Limit: 2, Budget: 100000}
			if err := run("conduit_to_committee", q, true); err != nil {
				return out, err
			}
		} else {
			out.Cases = append(out.Cases, PathCase{Kind: "conduit_to_committee", State: "no_matching_selection_population"})
		}
	}
	if r.shared != nil {
		id := r.shared.WitnessID()
		if id == "" {
			out.Cases = append(out.Cases, PathCase{Kind: receipt.SharedFamily, State: "no_matching_selection_population"})
		} else {
			page, err := r.shared.Page(ctx, id, "", 1)
			if err != nil {
				return out, err
			}
			if len(page.Items) != 1 {
				return out, fmt.Errorf("shared witness missing")
			}
			var evidence receipt.SharedEvidence
			if json.Unmarshal(page.Items[0].Evidence, &evidence) != nil {
				return out, fmt.Errorf("invalid shared witness")
			}
			q := PathQuery{ReceiptOrdinal: evidence.Decision.Ordinal, EntryFamily: receipt.SharedFamily, Ledger: flow.ScheduleA, Target: id, MaxHops: 0, Limit: 1, Budget: 1}
			if err := run(receipt.SharedFamily, q, false); err != nil {
				return out, err
			}
			v := out.Cases[len(out.Cases)-1].Result
			if len(v.Paths) != 1 || len(v.Links) != 1 || v.Entry == nil || v.Entry.State != "available" {
				return out, fmt.Errorf("shared entry path missing")
			}
		}
	}
	if err := r.VerifyCompletion(ctx); err != nil {
		return out, err
	}
	out.GateID = valueID(out)
	if expected != "" && expected != out.GateID {
		return PathsGate{}, fmt.Errorf("path gate replay differs")
	}
	return out, nil
}
