package personaffiliation

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

// Reviewed overlays exercise the new representation. They are not model answers,
// repairs to saved responses, or evidence of better automatic interpretation.
func TestProseReferentRetainedCounterexamples(t *testing.T) {
	for _, c := range proseInputCases(t, groundingFixture) {
		if c.ID != "grounding-ambiguous-conditional" && c.ID != "grounding-anonymous-correction" {
			continue
		}
		t.Run(c.ID, func(t *testing.T) {
			path := filepath.Join(fixtureDir, groundingFixture, c.ID+".json")
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var capture modelRecord
			if err := strictjson.Decode(raw, &capture); err != nil {
				t.Fatal(err)
			}
			names := groundingNameInput(t, c)
			content, err := modelContent(capture.Response)
			if err != nil {
				t.Fatal(err)
			}
			var original groundedRoleInput
			if err := strictjson.Decode(content, &original); err != nil {
				t.Fatal(err)
			}
			old, err := inspectGroundedRoles(c, names, content, "model_proposal")
			if err != nil {
				t.Fatal(err)
			}
			in := referentInput{Grounding: original, Referents: []roleReferents{}, Corrections: []correctionTargets{}}
			for _, p := range original.Interpretations {
				if p.Kind == "role" {
					in.Referents = append(in.Referents, roleReferents{Interpretation: p.ID, Subject: &referentChoice{State: "unassessed", Candidates: []string{}}, Organization: &referentChoice{State: "unassessed", Candidates: []string{}}})
				}
			}
			if c.ID == "grounding-anonymous-correction" {
				if _, err := inspectReferentContract(c, names, referentJSON(t, in), "model_proposal", "reviewed_fixture"); err == nil {
					t.Fatal("omitted correction assessment silently accepted")
				}
				in.Corrections = []correctionTargets{{Correction: "i1", State: "unresolved", Targets: []string{}}}
			}
			out, err := inspectReferentContract(c, names, referentJSON(t, in), "model_proposal", "reviewed_fixture")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(out.Proposal.Grounding, original) || !reflect.DeepEqual(out.Derived, old) || out.AssessmentOrigin != "reviewed_fixture" || out.Derived.Review.Derived.Review.Review.Origin != "model_proposal" {
				t.Fatal("original model evidence or annotation origin changed")
			}
			if c.ID == "grounding-ambiguous-conditional" {
				if old.Surfaces[0].Subject != "exact_surface" || out.Referents[0].Subject != "unverified_unassessed" {
					t.Fatal("surname match resolved a person")
				}
				// Independent reviewed interpretation: preserve the two named alternatives.
				in.Referents[0].Subject = &referentChoice{State: "ambiguous", Candidates: []string{"n0", "n1"}}
				in.Referents[0].Organization = &referentChoice{State: "proposed", Candidates: []string{"n2"}}
				out, err = inspectReferentContract(c, names, referentJSON(t, in), "model_proposal", "reviewed_fixture")
				if err != nil || out.Referents[0].Subject != "unverified_ambiguous" || out.Derived.Proposal.Interpretations[0].Binding != "direct" || !reflect.DeepEqual(out.Derived.Proposal.Interpretations[0].Subjects, []string{"n3"}) {
					t.Fatal("review overlay overwrote the original surname proposal", err)
				}
			} else {
				if len(out.Derived.Proposal.Links) != 0 {
					t.Fatal("unresolved correction invented retraction")
				}
				in.Corrections[0] = correctionTargets{Correction: "i1", State: "proposed", Targets: []string{"i0"}}
				out, err = inspectReferentContract(c, names, referentJSON(t, in), "model_proposal", "reviewed_fixture")
				if err != nil || len(out.Proposal.Grounding.Links) != 0 || len(out.Derived.Proposal.Links) != 1 || out.Derived.Review.Derived.Review.Review.Interpretations[0].State != "context_blocked" {
					t.Fatal("explicit target not applied separately", err)
				}
			}
			r := out.Derived.Review.Derived.Review.Review
			if !reflect.DeepEqual(out.Proposal.Grounding, original) || r.IdentityApproved || r.GraphPublicationApproved || r.FinancialAttribution {
				t.Fatal("grounding rewritten or identity approved")
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(raw, after) {
				t.Fatal("capture changed")
			}
			encoded, _ := json.Marshal(old)
			var compact bytes.Buffer
			if err := json.Compact(&compact, capture.EvidenceAttachment); err != nil || !bytes.Equal(encoded, compact.Bytes()) {
				t.Fatal("old trial result changed")
			}
		})
	}
}
