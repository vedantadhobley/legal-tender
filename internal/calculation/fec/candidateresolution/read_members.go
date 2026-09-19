package candidateresolution

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"

	effective "github.com/vedantadhobley/legal-tender/internal/calculation/fec/independentexpenditures"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	artifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

// VisitPublishedFacts replays the publisher's own policy against the exact
// source ancestry and compares every resulting decision to its publication.
// Every source fact is visited, including memo, amount and routing exceptions.
// A nil decision means the effective policy did not admit candidate resolution.
// Callbacks must not publish partial output: success requires complete replay.
func VisitPublishedFacts(ctx context.Context, root, resolutionID, expectedSHA string,
	visit func(occ.ScheduleEFact, effective.FactEvaluation, *Decision) error,
) error {
	if !validDigest(resolutionID) || !validDigest(expectedSHA) || visit == nil {
		return fmt.Errorf("pinned candidate resolution and visitor required")
	}
	m, sha, err := LoadPublishedManifest(ctx, root, filepath.Join(root, calculationBase(), "manifests", resolutionID+".json"))
	if err != nil {
		return err
	}
	if sha != expectedSHA || m.CalculationSetID != resolutionID {
		return fmt.Errorf("candidate-resolution ancestry changed")
	}
	e, esha, err := effective.LoadPublishedManifest(ctx, root, filepath.Join(root, "calculations/fec/effective-independent-expenditures/manifests", m.InputCalculation.CalculationSetID+".json"))
	if err != nil {
		return err
	}
	s, ssha, err := occ.LoadPublishedScheduleEFactManifest(ctx, root, filepath.Join(root, "facts/fec/schedule-e/manifests", m.InputCalculation.ScheduleEFactSetID+".json"))
	if err != nil {
		return err
	}
	// The replay holds duplicate identities, not full facts. Fail before that
	// allocation rather than silently truncating a larger source publication.
	if s.Counts.Facts > 2_000_000 {
		return fmt.Errorf("Schedule E member replay exceeds 2000000-fact memory guard")
	}
	c, csha, err := occ.LoadPublishedClassicFactManifest(root, filepath.Join(root, "facts/fec/classic/candidate-master/manifests", m.InputCandidateFactSet.FactSetID+".json"), "candidate-master")
	if err != nil {
		return err
	}
	if esha != m.InputCalculation.ManifestSHA256 || e.CalculationSetID != m.InputCalculation.CalculationSetID ||
		ssha != m.InputCalculation.ScheduleEManifestSHA256 || s.FactSetID != m.InputCalculation.ScheduleEFactSetID ||
		e.InputFactSet.FactSetID != s.FactSetID || e.InputFactSet.ManifestSHA256 != ssha ||
		csha != m.InputCandidateFactSet.ManifestSHA256 || c.FactSetID != m.InputCandidateFactSet.FactSetID ||
		s.Cycle != m.Cycle || e.Cycle != m.Cycle || c.Cycle != m.Cycle ||
		s.SourceReleaseID != m.SourceReleaseID || e.SourceReleaseID != m.SourceReleaseID || c.SourceReleaseID != m.SourceReleaseID {
		return fmt.Errorf("candidate-resolution source ancestry differs")
	}
	index, candidateCount, usableCount, err := buildCandidateIndex(ctx, root, c)
	if err != nil {
		return err
	}
	decisions, err := artifact.Open[Decision](ctx, root, m.Decisions)
	if err != nil {
		return err
	}
	defer decisions.Abort()
	counts, amounts, err := resolveEffectiveFacts(ctx, root, s, e, m.CalculationSetID, index,
		func(f occ.ScheduleEFact, evaluation effective.FactEvaluation, want *Decision) error {
			if want != nil {
				got, ok, err := decisions.Next()
				if err != nil {
					return err
				}
				if !ok || !reflect.DeepEqual(got, *want) {
					return fmt.Errorf("published candidate decision differs from source replay for fact %s", f.FactID)
				}
			}
			return visit(f, evaluation, want)
		}, nil)
	if err != nil {
		return err
	}
	counts.CandidateFacts, counts.UsableCandidateFacts = candidateCount, usableCount
	if counts != m.Counts || amounts != m.Amounts {
		return fmt.Errorf("candidate-resolution replay counts or signed amounts differ")
	}
	if _, ok, err := decisions.Next(); err != nil {
		return err
	} else if ok {
		return fmt.Errorf("extra candidate-resolution decisions")
	}
	return decisions.Close()
}
