package organizationresolution

import (
	"os"
	"reflect"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/gleif"
)

func TestRegistryNameAmbiguityNotIdentityApproval(t *testing.T) {
	body, err := os.ReadFile("../../source/gleif/testdata/name-search.json")
	if err != nil {
		t.Fatal(err)
	}
	page, err := gleif.ParseNames(body, "RIDGELINE INC")
	if err != nil {
		t.Fatal(err)
	}
	name := "RIDGELINE INC"
	r := gleif.NameReplay{CaptureUsable: true, Manifest: gleif.NameManifest{Plan: gleif.NamePlan{Inputs: []gleif.NameInput{{Name: &name}, {Name: &name}, {}}}}, Observations: []gleif.NameObservation{{Query: gleif.NameQuery{Inputs: []int{0, 1}}, Page: &page}}}
	got := DiscoverRegistryNames(r, "build")
	if got.IdentityApproved || got.EmploymentVerified || got.GraphPublicationApproved || got.FinancialAttribution || got.ExhaustiveDiscovery || len(got.Decisions) != 3 {
		t.Fatal("approval/conservation")
	}
	for _, d := range got.Decisions[:2] {
		if d.State != "ambiguous_name_correspondences" || len(d.Candidates) != 3 || len(d.CorrespondingLEIs) != 2 || !d.WindowComplete {
			t.Fatal("rival discarded", d)
		}
	}
	if got.Decisions[2].State != "no_searchable_name" || got.Decisions[2].Query != nil {
		t.Fatal("missing name")
	}
	if !reflect.DeepEqual(got, DiscoverRegistryNames(r, "build")) {
		t.Fatal("replay")
	}
	// Prior/transliterated names are evidence, not replacements for the legal name.
	r.Observations[0].Page = &gleif.NamePage{WindowComplete: false, Records: []gleif.Record{{LEI: "source:test", LegalName: gleif.Name{Name: "Different Parent"}, OtherNames: []gleif.Name{{Name: name, Type: "PREVIOUS_LEGAL_NAME", Language: "en"}}, TransliteratedNames: []gleif.Name{{Name: name, Type: "AUTO_ASCII_TRANSLITERATED_LEGAL_NAME", Language: "en"}}, EntityStatus: "INACTIVE", RegistrationStatus: "LAPSED"}}}
	d := DiscoverRegistryNames(r, "build").Decisions[0]
	if d.State != "name_correspondence_identity_unresolved" || d.WindowComplete || len(d.Candidates[0].Names) != 2 || d.Candidates[0].Names[0].Type != "PREVIOUS_LEGAL_NAME" || len(d.Blockers) != 4 {
		t.Fatal("history/status/coverage lost", d)
	}
	if d.Candidates[0].EntityStatus != "INACTIVE" || d.Candidates[0].RegistrationStatus != "LAPSED" {
		t.Fatal("status filtered")
	}
	r.Observations[0].Page = &gleif.NamePage{WindowComplete: true}
	if d := DiscoverRegistryNames(r, "build").Decisions[0]; d.State != "no_records_in_query_window" {
		t.Fatal("empty window")
	}
	r.Observations[0].Issue = "http_status_not_ok"
	r.CaptureUsable = false
	if d := DiscoverRegistryNames(r, "build").Decisions[0]; d.State != "source_unusable_or_unattempted" {
		t.Fatal("failure became absence")
	}
}
