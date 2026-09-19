package fundinggeneration

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	resolution "github.com/vedantadhobley/legal-tender/internal/calculation/fec/candidateresolution"
	effective "github.com/vedantadhobley/legal-tender/internal/calculation/fec/independentexpenditures"
	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
	ie "github.com/vedantadhobley/legal-tender/internal/projection/arango/independentexpenditures"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	"github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

type spendingReference struct {
	Generation, SourceHash, DecisionHash string
	Member                               ie.DatedMember
}
type spendingGateCensus struct {
	Facts     map[string]spendingReference
	Parents   map[string]resolution.AggregateResult
	Summaries []spendingCensusSummary
}
type spendingCensusSummary struct {
	Generation           string `json:"generation_id"`
	Facts                uint64 `json:"source_facts"`
	Decisions            uint64 `json:"candidate_decisions"`
	Projectable          uint64 `json:"projectable_facts"`
	ExpenditureUnknown   uint64 `json:"unknown_expenditure_dates"`
	DisseminationUnknown uint64 `json:"unknown_dissemination_dates"`
	DifferentKnownDates  uint64 `json:"different_known_date_fields"`
	SourceSHA            string `json:"source_artifact_sha256"`
	DecisionsSHA         string `json:"decision_artifact_sha256"`
}

func spendingGroupKey(generation, spender, candidate, stance string) string {
	return generation + ":" + spender + ":" + candidate + ":" + stance
}

// Independent artifact decoding, not VisitDatedMembers, windowTopology or the
// production date selector. Reuse the accepted effective predicate, not a new
// test-only money policy. Candidate decisions are joined by exact fact ID.
func loadSpendingCensus(t *testing.T, ctx context.Context, r *WindowReader, root string) spendingGateCensus {
	t.Helper()
	out := spendingGateCensus{Facts: map[string]spendingReference{}, Parents: map[string]resolution.AggregateResult{}}
	for _, p := range r.partitions {
		g := p.publication.GenerationID
		input := p.publication.Generation.OutsideSpending.Inputs
		m, sha, err := occ.LoadPublishedScheduleEFactManifest(ctx, root, filepath.Join(root, "facts/fec/schedule-e/manifests", input.ScheduleEFactSetID+".json"))
		liveCheck(t, err)
		if sha != input.ScheduleEManifestSHA256 {
			t.Fatal("independent source manifest differs")
		}
		d, sha, err := resolution.LoadPublishedManifest(ctx, root, filepath.Join(root, "calculations/fec/independent-expenditure-candidate-resolution/manifests", input.CandidateResolutionCalculationSetID+".json"))
		liveCheck(t, err)
		if sha != input.CandidateResolutionManifestSHA256 {
			t.Fatal("independent resolution manifest differs")
		}
		a, sha, err := resolution.LoadPublishedAggregateManifest(ctx, root, filepath.Join(root, "calculations/fec/resolved-independent-expenditures/manifests", input.CalculationSetID+".json"))
		liveCheck(t, err)
		if sha != input.CalculationManifestSHA256 {
			t.Fatal("independent aggregate manifest differs")
		}
		ar, err := artifact.Open[resolution.AggregateResult](ctx, root, a.Results)
		liveCheck(t, err)
		for {
			v, ok, err := ar.Next()
			liveCheck(t, err)
			if !ok {
				break
			}
			k := spendingGroupKey(g, v.SpenderCommitteeID, v.CandidateID, v.SupportOppose)
			if _, exists := out.Parents[k]; exists {
				t.Fatal("duplicate independent parent")
			}
			out.Parents[k] = v
		}
		liveCheck(t, ar.Close())
		decisions := map[string]resolution.Decision{}
		dr, err := artifact.Open[resolution.Decision](ctx, root, d.Decisions)
		liveCheck(t, err)
		for {
			v, ok, err := dr.Next()
			liveCheck(t, err)
			if !ok {
				break
			}
			if _, exists := decisions[v.FactID]; exists {
				t.Fatal("duplicate independent decision")
			}
			decisions[v.FactID] = v
		}
		liveCheck(t, dr.Close())
		summary := spendingCensusSummary{Generation: g, Decisions: uint64(len(decisions)), SourceSHA: m.Facts.CompressedSHA256, DecisionsSHA: d.Decisions.CompressedSHA256}
		descriptor := artifact.Descriptor{RecordCount: m.Facts.RecordCount, UncompressedBytes: m.Facts.UncompressedBytes, UncompressedSHA256: m.Facts.UncompressedSHA256,
			CompressedBytes: m.Facts.CompressedBytes, CompressedSHA256: m.Facts.CompressedSHA256, Compression: m.Facts.Compression, StorageKey: m.Facts.StorageKey}
		fr, err := artifact.Open[occ.ScheduleEFact](ctx, root, descriptor)
		liveCheck(t, err)
		for {
			f, ok, err := fr.Next()
			liveCheck(t, err)
			if !ok {
				break
			}
			if len(out.Facts) >= int(MaxWindowObservations) {
				t.Fatal("independent census exceeds memory guard")
			}
			if _, exists := out.Facts[f.FactID]; exists {
				t.Fatal("duplicate independent source fact")
			}
			eval := effective.EvaluateFact(f)
			member := ie.DatedMember{FactID: f.FactID, EffectiveState: eval.Decision, AmountReason: eval.AmountReason,
				RouteReasons: eval.RouteReasons, ResolutionState: "not_admitted_by_effective_policy"}
			if f.TypedFields.Candidate.SupportOpposeCode != nil {
				member.Stance = *f.TypedFields.Candidate.SupportOpposeCode
			}
			if eval.Amount != nil {
				a := eval.Amount.String()
				member.Amount = &a
			}
			member.ExpenditureDate, err = independentSpendingDay(f.TypedFields.Expenditure.ExpenditureOn)
			liveCheck(t, err)
			member.DisseminationDate, err = independentSpendingDay(f.TypedFields.Expenditure.DisseminatedOn)
			liveCheck(t, err)
			ref := spendingReference{Generation: g, SourceHash: valueID(f)}
			decision, exists := decisions[f.FactID]
			admitted := eval.Decision == effective.DecisionIncluded && len(eval.RouteReasons) == 0
			if admitted != exists {
				t.Fatal("source/decision membership differs")
			}
			if exists {
				delete(decisions, f.FactID)
				ref.DecisionHash = valueID(decision)
				member.DecisionID, member.ResolutionState = decision.DecisionID, decision.State
				if decision.AmountMinorUnits != *member.Amount || decision.SpenderCommitteeID != *f.TypedFields.Spender.CommitteeID || decision.SupportOppose != member.Stance {
					t.Fatal("decision source fields differ")
				}
				if decision.ResolvedCandidateID != nil {
					parent, ok := out.Parents[spendingGroupKey(g, decision.SpenderCommitteeID, *decision.ResolvedCandidateID, decision.SupportOppose)]
					if !ok {
						t.Fatal("independent parent absent")
					}
					family := "independent_support"
					if member.Stance == "O" {
						family = "independent_opposition"
					}
					member.Link = &graphread.Link{Family: family + "_observation", Key: f.FactID, From: decision.SpenderCommitteeID, To: *decision.ResolvedCandidateID}
					member.Parent = &graphread.Link{Family: family, Key: parent.ResultID, From: member.Link.From, To: member.Link.To}
					summary.Projectable++
				}
			}
			summary.Facts++
			if member.ExpenditureDate == nil {
				summary.ExpenditureUnknown++
			}
			if member.DisseminationDate == nil {
				summary.DisseminationUnknown++
			}
			if member.ExpenditureDate != nil && member.DisseminationDate != nil && *member.ExpenditureDate != *member.DisseminationDate {
				summary.DifferentKnownDates++
			}
			ref.Member = member
			out.Facts[f.FactID] = ref
		}
		liveCheck(t, fr.Close())
		if summary.Facts != m.Counts.Facts || summary.Decisions != d.Counts.SourceEffectiveFacts || summary.Projectable != a.Counts.ProjectableDecisions || len(decisions) != 0 {
			t.Fatal("independent source census not conserved")
		}
		out.Summaries = append(out.Summaries, summary)
	}
	return out
}

func independentSpendingDay(s *string) (*int32, error) {
	if s == nil {
		return nil, nil
	}
	d, err := time.Parse(time.DateOnly, *s)
	if err != nil || d.Format(time.DateOnly) != *s {
		return nil, fmt.Errorf("non-day source field")
	}
	day := int32(d.Unix() / 86400)
	return &day, nil
}
