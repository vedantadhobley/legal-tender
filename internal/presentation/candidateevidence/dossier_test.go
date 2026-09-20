package candidateevidence

import (
	"context"
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/candidateresolution"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/fundingbasis"
)

func TestDossierCandidateRolesKeepEndpointsSeparate(t *testing.T) {
	candidate := "S6OH00163"
	other := "S6PA00217"
	for _, test := range []struct {
		name  string
		value candidateresolution.CandidateInterpretation
		want  []string
	}{
		{
			name: "reported and safe default overlap",
			value: candidateresolution.CandidateInterpretation{
				ReportedCandidate:      candidateresolution.ReportedCandidate{CandidateID: candidate},
				SafeDefaultCandidateID: &candidate,
			},
			want: []string{OutsideReportedEndpoint, OutsideSafeDefaultEndpoint},
		},
		{
			name: "inferred alternative is not reported",
			value: candidateresolution.CandidateInterpretation{
				ReportedCandidate: candidateresolution.ReportedCandidate{CandidateID: other},
				State:             candidateresolution.InterpretationInferred, AlternativeCandidateID: &candidate,
			},
			want: []string{OutsideInferredAlternativeEndpoint},
		},
		{
			name: "conflicting alternative remains separate",
			value: candidateresolution.CandidateInterpretation{
				ReportedCandidate: candidateresolution.ReportedCandidate{CandidateID: other},
				State:             candidateresolution.InterpretationConflicting, AlternativeCandidateID: &candidate,
			},
			want: []string{OutsideConflictingAlternativeEndpoint},
		},
		{
			name: "unrelated",
			value: candidateresolution.CandidateInterpretation{
				ReportedCandidate: candidateresolution.ReportedCandidate{CandidateID: other},
			},
			want: []string{},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := dossierCandidateRoles(candidate, test.value); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("roles = %v, want %v", got, test.want)
			}
		})
	}
}

func TestOpenDossierAuthenticatesContent(t *testing.T) {
	digestValue := strings.Repeat("a", 64)
	dossier := Dossier{
		SchemaVersion: DossierVersion, Policy: DossierPolicy, ExecutableSHA256: digestValue,
		CandidateID: "H0AA00001", Cycle: "2024", CandidateName: EntityName{EntityID: "H0AA00001"},
		Inputs: DossierInput{
			ParentReportID: digestValue, ParentReportSHA256: digestValue, ParentEvidenceID: digestValue,
			ReceiptFactSetID: digestValue, ReceiptManifestSHA256: digestValue,
			CommitteeFlowCalculationSetID: digestValue, CandidateInterpretationSetID: digestValue,
			CandidateInterpretationManifestSHA: digestValue, CandidateInterpretationArtifactSHA: digestValue,
		},
		Receipts:        ReceiptEvidence{CandidateLinkedCommittees: []fundingbasis.EvidenceCommittee{{CommitteeID: "C00000001"}}},
		OutsideSpending: OutsideSpendingEvidenceSet{ViewsAreNonAdditive: true},
	}
	id, err := contentID(dossier)
	if err != nil {
		t.Fatal(err)
	}
	dossier.DossierID = id
	raw, err := json.Marshal(dossier)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "dossier.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	opened, sha, err := OpenDossier(context.Background(), path, id)
	if err != nil || opened.DossierID != id || len(sha) != 64 {
		t.Fatal(opened.DossierID, sha, err)
	}
	if _, _, err := OpenDossier(context.Background(), path, strings.Repeat("b", 64)); err == nil {
		t.Fatal("wrong expected identity accepted")
	}
}

func TestDossierMeasuresPreserveSignsAndStances(t *testing.T) {
	var values stanceAccumulator
	values.add("S", big.NewInt(100))
	values.add("S", big.NewInt(-25))
	values.add("O", big.NewInt(0))
	got := values.measures()
	wantTotal := AmountMeasures{
		Rows: 3, PositiveRows: 1, NegativeRows: 1, ZeroRows: 1,
		SignedMinorUnits: "75", PositiveMinorUnits: "100", NegativeMinorUnits: "-25",
	}
	if got.Total != wantTotal || got.Supporting.Rows != 2 || got.Supporting.SignedMinorUnits != "75" || got.Opposing.Rows != 1 || got.Opposing.SignedMinorUnits != "0" {
		t.Fatalf("measures = %#v", got)
	}
}

func TestDossierCandidateNameRequiresOneExactSelectedCandidate(t *testing.T) {
	report := Report{Evidence: pathFixture(), Names: []EntityName{{EntityID: "H0AA00001", Kind: "candidate", State: "name_source_not_requested", Assertions: []NameAssertion{}}}}
	if name, err := dossierCandidateName(report); err != nil || name.EntityID != "H0AA00001" {
		t.Fatal(name, err)
	}
	report.Names = append(report.Names, report.Names[0])
	if _, err := dossierCandidateName(report); err == nil {
		t.Fatal("duplicate candidate name evidence accepted")
	}
	report.Names = nil
	if _, err := dossierCandidateName(report); err == nil {
		t.Fatal("missing candidate name evidence accepted")
	}
}
