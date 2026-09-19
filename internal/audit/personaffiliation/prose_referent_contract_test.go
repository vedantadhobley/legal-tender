package personaffiliation

import (
	"encoding/json"
	"fmt"
	"slices"

	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

// Offline contract only. Candidate IDs are source mentions, never canonical
// people/companies. Exact spelling does not populate these choices implicitly.
type referentChoice struct {
	State      string   `json:"state"`
	Candidates []string `json:"candidates"`
}
type roleReferents struct {
	Interpretation string          `json:"interpretation"`
	Subject        *referentChoice `json:"subject"`
	Organization   *referentChoice `json:"organization"`
}
type correctionTargets struct {
	Correction string   `json:"correction"`
	State      string   `json:"state"`
	Targets    []string `json:"targets"`
}
type referentInput struct {
	Grounding   groundedRoleInput   `json:"grounding"`
	Referents   []roleReferents     `json:"referents"`
	Corrections []correctionTargets `json:"corrections"`
}
type referentReview struct {
	Interpretation string `json:"interpretation"`
	Subject        string `json:"subject_state"`
	Organization   string `json:"organization_state"`
}
type referentReport struct {
	Method           string             `json:"method"`
	AssessmentOrigin string             `json:"assessment_origin"`
	Proposal         referentInput      `json:"supplied_proposal"`
	Referents        []referentReview   `json:"referent_review"`
	Derived          groundedRoleReport `json:"derived_grounding"`
}

func inspectReferentContract(c modelCase, names, raw []byte, groundingOrigin, assessmentOrigin string) (referentReport, error) {
	fail := func(msg string) (referentReport, error) {
		return referentReport{}, fmt.Errorf("referent contract: %s", msg)
	}
	if len(raw) > 128<<10 {
		return fail("input budget")
	}
	if !slices.Contains([]string{"synthetic_test", "reviewed_fixture", "model_proposal"}, assessmentOrigin) {
		return fail("unsupported assessment origin")
	}
	var in referentInput
	if err := strictjson.Decode(raw, &in); err != nil {
		return referentReport{}, err
	}
	if in.Referents == nil || in.Corrections == nil || len(in.Referents) > 32 || len(in.Corrections) > 32 {
		return fail("missing arrays or scope budget")
	}
	// Only one retraction authority in this new contract. Conflict links retain
	// their old meaning; old model formats and validators remain unchanged.
	for _, link := range in.Grounding.Links {
		if link.Kind == "retracts" {
			return fail("retraction must declare correction targets")
		}
	}
	encoded, err := json.Marshal(in.Grounding)
	if err != nil {
		return referentReport{}, err
	}
	base, err := inspectGroundedRoles(c, names, encoded, groundingOrigin)
	if err != nil {
		return referentReport{}, err
	}
	kinds := map[string]string{}
	for _, n := range base.Review.Candidates {
		kinds[n.ID] = n.Kind
	}
	items := map[string]groundedRoleProposal{}
	for _, p := range in.Grounding.Interpretations {
		items[p.ID] = p
	}
	validChoice := func(choice *referentChoice, kind string) bool {
		if choice == nil || choice.Candidates == nil || len(choice.Candidates) > 64 {
			return false
		}
		switch choice.State {
		case "unassessed", "unresolved":
			if len(choice.Candidates) != 0 {
				return false
			}
		case "proposed":
			if len(choice.Candidates) != 1 {
				return false
			}
		case "ambiguous":
			if len(choice.Candidates) < 2 {
				return false
			}
		default:
			return false
		}
		seen := map[string]bool{}
		for _, id := range choice.Candidates {
			if seen[id] || kinds[id] != kind {
				return false
			}
			seen[id] = true
		}
		return true
	}
	seenReferents := map[string]bool{}
	refs := []referentReview{}
	for _, r := range in.Referents {
		if items[r.Interpretation].Kind != "role" || seenReferents[r.Interpretation] || !validChoice(r.Subject, "person") || !validChoice(r.Organization, "organization") {
			return fail("invalid or duplicate role referents")
		}
		seenReferents[r.Interpretation] = true
		refs = append(refs, referentReview{r.Interpretation, "unverified_" + r.Subject.State, "unverified_" + r.Organization.State})
	}
	derived := in.Grounding
	derived.Links = slices.Clone(in.Grounding.Links)
	seenCorrections := map[string]bool{}
	for _, correction := range in.Corrections {
		if items[correction.Correction].Kind != "correction" || seenCorrections[correction.Correction] || correction.Targets == nil || len(correction.Targets) > 32 {
			return fail("invalid or duplicate correction assessment")
		}
		switch correction.State {
		case "proposed":
			if len(correction.Targets) == 0 {
				return fail("proposed correction requires targets")
			}
		case "ambiguous":
			if len(correction.Targets) < 2 {
				return fail("ambiguous correction requires alternatives")
			}
		case "unresolved":
			if len(correction.Targets) != 0 {
				return fail("unresolved correction cannot select targets")
			}
		default:
			return fail("unsupported correction target state")
		}
		seenTargets := map[string]bool{}
		for _, target := range correction.Targets {
			if items[target].Kind != "role" || seenTargets[target] {
				return fail("correction target is not a distinct role")
			}
			seenTargets[target] = true
			if correction.State == "proposed" {
				derived.Links = append(derived.Links, contextProposal{From: correction.Correction, To: target, Kind: "retracts"})
			}
		}
		seenCorrections[correction.Correction] = true
	}
	for _, p := range in.Grounding.Interpretations {
		if p.Kind == "role" && !seenReferents[p.ID] {
			return fail("role referent assessment missing")
		}
		if p.Kind == "correction" && !seenCorrections[p.ID] {
			return fail("correction target assessment missing")
		}
	}
	// The caller's proposal remains untouched. Only explicit proposed correction
	// targets generate links; alternatives and missing targets never retract all.
	encoded, err = json.Marshal(derived)
	if err != nil {
		return referentReport{}, err
	}
	out, err := inspectGroundedRoles(c, names, encoded, groundingOrigin)
	if err != nil {
		return referentReport{}, err
	}
	return referentReport{Method: "mention-referents-and-correction-targets.v1", AssessmentOrigin: assessmentOrigin, Proposal: in, Referents: refs, Derived: out}, nil
}
