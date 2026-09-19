package organizationresolution

import (
	"encoding/json"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/gleif"
	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

const registryLEI = "300300SRCLQKVTFFOM15"
const otherLEI = "LCUAWMT4M5H8DJ8DFH49"

func leiStatement(id, rank, qualifiers string) string {
	return `{"id":"Q100$claim-` + id + `","type":"statement","rank":"` + rank + `","mainsnak":{"property":"P1278","datatype":"external-id","snaktype":"value","datavalue":{"type":"string","value":"` + id + `"}}` + qualifiers + `}`
}

func registryFixture(t *testing.T) (wikimedia.Replay, gleif.Replay) {
	t.Helper()
	o := observation(t)
	o.Query.Text = "Example Corporation"
	e := o.Entities["Q100"]
	e.Claims["P1278"] = json.RawMessage("[" + leiStatement(registryLEI, "normal", "") + "]")
	o.Entities[e.ID] = e
	w := wikimedia.Replay{CaptureSHA256: wikimedia.Hash([]byte("wm")), Observations: []wikimedia.Observation{o}}
	req, err := RegistryRequests(w)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile("../../source/gleif/testdata/record.json")
	if err != nil {
		t.Fatal(err)
	}
	r, err := gleif.Parse(b, registryLEI)
	if err != nil {
		t.Fatal(err)
	}
	g := gleif.Replay{CaptureSHA256: wikimedia.Hash([]byte("registry")), Manifest: gleif.Manifest{Contract: gleif.Contract, Requests: req}, Observations: []gleif.Observation{{LEI: registryLEI, Record: &r}}}
	return w, g
}

func TestRegistryClaimGrainAndEligibility(t *testing.T) {
	for _, tc := range []struct{ raw, state string }{
		{leiStatement(registryLEI, "normal", ""), "unqualified_nondeprecated"},
		{leiStatement(registryLEI, "preferred", ""), "unqualified_nondeprecated"},
		{leiStatement(registryLEI, "deprecated", ""), "deprecated"},
		{leiStatement(registryLEI, "new-rank", ""), "unsupported_rank"},
		{leiStatement(registryLEI, "normal", `,"qualifiers":{"P582":[{"snaktype":"somevalue"}]}`), "qualified_validity_unassessed"},
		{leiStatement(registryLEI, "normal", `,"qualifiers":null`), "unsupported_claim_shape"},
		{leiStatement(registryLEI, "normal", `,"qualifiers-order":null`), "unsupported_claim_shape"},
		{leiStatement(registryLEI[:19]+"6", "normal", ""), "invalid_lei"},
		{strings.Replace(leiStatement(registryLEI, "normal", ""), `"property":"P1278"`, `"property":"P856"`, 1), "unsupported_claim_shape"},
		{strings.Replace(leiStatement(registryLEI, "normal", ""), `"value":"`+registryLEI+`"`, `"value":null`, 1), "invalid_lei"},
	} {
		e := wikimedia.Entity{ID: "Q100", Claims: map[string]json.RawMessage{"P1278": json.RawMessage("[" + tc.raw + "]")}}
		cs := RegistryClaims(e)
		if len(cs) != 1 || cs[0].State != tc.state || cs[0].Index != 0 || string(cs[0].Raw) != tc.raw {
			t.Fatal(tc.state, cs)
		}
	}
	for _, raw := range []string{"null", `{}`, `"unexpected"`} {
		c := RegistryClaims(wikimedia.Entity{ID: "Q100", Claims: map[string]json.RawMessage{"P1278": json.RawMessage(raw)}})
		if len(c) != 1 || c[0].Index != -1 || c[0].State != "unsupported_claim_shape" || string(c[0].Raw) != raw {
			t.Fatal("property issue lost")
		}
	}
}

func TestRegistryCorroborationIsNotIdentityAcceptance(t *testing.T) {
	w, g := registryFixture(t)
	build := wikimedia.Hash([]byte("build"))
	before, _ := json.Marshal(w)
	r, err := Corroborate(w, g, build)
	if err != nil {
		t.Fatal(err)
	}
	c := r.Cases[0].Candidates[0]
	if c.State != "name_and_lei_correspondence_fec_identity_unresolved" || !c.Comparisons[0].RecordObserved || !slices.Contains(c.Blockers, "fec_input_has_no_independent_lei_binding") {
		t.Fatal(c)
	}
	if r.IdentityPublicationApproved || r.EmploymentVerified || r.OwnershipVerified || r.FinancialAttribution || r.Proposals.IdentityResolved {
		t.Fatal("corroboration became identity or money")
	}
	again, err := Corroborate(w, g, build)
	if err != nil || !reflect.DeepEqual(r, again) {
		t.Fatal("nonrepeatable result", err)
	}
	after, _ := json.Marshal(w)
	if string(before) != string(after) {
		t.Fatal("evidence mutated")
	}
	w.CaptureSHA256 = wikimedia.Hash([]byte("other"))
	if _, err = Corroborate(w, g, build); err == nil {
		t.Fatal("cross-capture substitution accepted")
	}
}

func TestRegistryHardCasesRemainExplicit(t *testing.T) {
	for _, tc := range []struct {
		name, blocker string
		change        func(*wikimedia.Replay, *gleif.Replay)
	}{
		{"subsidiary_name", "", func(_ *wikimedia.Replay, g *gleif.Replay) { g.Observations[0].Record.LegalName.Name = "Example Bank" }},
		{"parent_name", "", func(_ *wikimedia.Replay, g *gleif.Replay) {
			g.Observations[0].Record.LegalName.Name = "Example Holdings Corporation"
		}},
		{"former_name", "", func(_ *wikimedia.Replay, g *gleif.Replay) {
			g.Observations[0].Record.LegalName.Name = "New Organization Corporation"
			g.Observations[0].Record.OtherNames[0].Name = "Example Corporation"
		}},
		{"qualified_claim", "unsupported_or_qualified_identifier_claim", func(w *wikimedia.Replay, _ *gleif.Replay) {
			e := w.Observations[0].Entities["Q100"]
			e.Claims["P1278"] = json.RawMessage("[" + leiStatement(registryLEI, "normal", `,"qualifiers":{"P582":[{"snaktype":"somevalue"}]}`) + "]")
			w.Observations[0].Entities[e.ID] = e
		}},
		{"deprecated_claim", "", func(w *wikimedia.Replay, _ *gleif.Replay) {
			e := w.Observations[0].Entities["Q100"]
			e.Claims["P1278"] = json.RawMessage("[" + leiStatement(registryLEI, "deprecated", "") + "]")
			w.Observations[0].Entities[e.ID] = e
		}},
		{"source_failure", "", func(_ *wikimedia.Replay, g *gleif.Replay) {
			g.Observations[0].Issue = "http_status_not_ok"
			g.Observations[0].Record = nil
		}},
		{"two_current_identifiers", "competing_nondeprecated_identifiers", func(w *wikimedia.Replay, g *gleif.Replay) {
			e := w.Observations[0].Entities["Q100"]
			e.Claims["P1278"] = json.RawMessage("[" + leiStatement(registryLEI, "normal", "") + "," + leiStatement(otherLEI, "normal", "") + "]")
			w.Observations[0].Entities[e.ID] = e
			r := *g.Observations[0].Record
			r.LEI = otherLEI
			g.Observations = append(g.Observations, gleif.Observation{LEI: otherLEI, Record: &r})
			g.Manifest.Requests.LEIs = append(g.Manifest.Requests.LEIs, otherLEI)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, g := registryFixture(t)
			tc.change(&w, &g)
			r, err := Corroborate(w, g, wikimedia.Hash([]byte("build")))
			if err != nil {
				t.Fatal(err)
			}
			c := r.Cases[0].Candidates[0]
			if c.State == "name_and_lei_correspondence_fec_identity_unresolved" || r.IdentityPublicationApproved {
				t.Fatal("unsupported correspondence", c)
			}
			if tc.blocker != "" && !slices.Contains(c.Blockers, tc.blocker) {
				t.Fatal("missing blocker", c)
			}
			if tc.name == "former_name" && !c.Comparisons[0].Names[1].ReportedNameExact {
				t.Fatal("historical name evidence lost")
			}
		})
	}
}

func TestRegistryLapsedAndUnknownStatusAreNotEntityAbsence(t *testing.T) {
	for _, status := range []string{"LAPSED", "RETIRED", "UNKNOWN_FUTURE_STATUS"} {
		w, g := registryFixture(t)
		g.Observations[0].Record.RegistrationStatus = status
		r, err := Corroborate(w, g, wikimedia.Hash([]byte("build")))
		if err != nil {
			t.Fatal(err)
		}
		c := r.Cases[0].Candidates[0]
		if !c.Comparisons[0].RecordObserved || !slices.Contains(c.Blockers, "registry_status_requires_review") || r.IdentityPublicationApproved {
			t.Fatal("status collapsed", c)
		}
	}
}

func TestRegistryRequestsConserveDuplicatesAndMissingClaims(t *testing.T) {
	w, g := registryFixture(t)
	w.Observations = append(w.Observations, w.Observations[0])
	r, err := RegistryRequests(w)
	if err != nil || !reflect.DeepEqual(r, g.Manifest.Requests) {
		t.Fatal("duplicate observation created request", err)
	}
	// Same valid LEI asserted twice is retained as two statements, not two votes.
	e := w.Observations[0].Entities["Q100"]
	e.Claims["P1278"] = json.RawMessage("[" + leiStatement(registryLEI, "normal", "") + "," + leiStatement(registryLEI, "normal", "") + "]")
	w.Observations[0].Entities[e.ID] = e
	if len(RegistryClaims(e)) != 2 {
		t.Fatal("claim grain collapsed")
	}
	r, err = RegistryRequests(w)
	if err != nil || len(r.LEIs) != 1 {
		t.Fatal("duplicate identifier fetched twice")
	}
	delete(e.Claims, "P1278")
	r, err = RegistryRequests(w)
	if err != nil || len(r.LEIs) != 0 || r.LEIs == nil {
		t.Fatal("missing claim invented request")
	}
	g.Manifest.Requests = r
	g.Observations = []gleif.Observation{}
	result, err := Corroborate(w, g, wikimedia.Hash([]byte("build")))
	if err != nil || result.Cases[0].Candidates[0].State != "no_lei_claim" {
		t.Fatal("no claim collapsed", err)
	}
}
