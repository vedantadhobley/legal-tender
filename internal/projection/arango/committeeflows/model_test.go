package committeeflows

import (
	"strings"
	"testing"

	fecflows "github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
)

func TestAnalyzeTopologyFindsComponentsPathsAndCycles(t *testing.T) {
	t.Parallel()
	adjacency := map[string][]string{
		"A": {"B"},
		"B": {"C"},
		"C": {"A"},
		"D": {"E"},
		"E": {},
	}
	metrics, source, target, cycle := analyzeTopology(adjacency)
	if metrics.WeakComponents != 2 || metrics.StrongComponents != 3 ||
		metrics.CyclicStrongComponents != 1 || metrics.CommitteesInCycles != 3 {
		t.Fatalf("unexpected topology: %+v", metrics)
	}
	if source == "" || target == "" || metrics.RepresentativePathHops != 2 {
		t.Fatalf("unexpected representative path %s -> %s: %+v", source, target, metrics)
	}
	if cycle == "" || metrics.RepresentativeCycleHops != 3 {
		t.Fatalf("unexpected representative cycle %s: %+v", cycle, metrics)
	}
}

func TestValidateCalculationResultRejectsNonCommitteeEndpoint(t *testing.T) {
	t.Parallel()
	manifest := fixtureCalculationManifest()
	result := fixtureFlowResult()
	result.SourceCommitteeID = "P00000001"
	if err := validateCalculationResult(result, manifest); err == nil {
		t.Fatal("noncommittee endpoint accepted")
	}
}

func fixtureCalculationManifest() fecflows.Manifest {
	return fecflows.Manifest{CalculationSetID: strings.Repeat("a", 64), Cycle: "2024"}
}

func fixtureFlowResult() fecflows.Result {
	return fecflows.Result{
		SchemaVersion: fecflows.ResultSchemaVersion, ResultID: strings.Repeat("b", 64),
		CalculationSetID: strings.Repeat("a", 64), Cycle: "2024",
		SourceCommitteeID: "C00000001", RecipientCommitteeID: "C00000002",
		ReceiptRole:            fecflows.RoleRegisteredFilerContribution,
		SignedAmountMinorUnits: "100", ReceiptCount: 1, PositiveCount: 1,
	}
}
