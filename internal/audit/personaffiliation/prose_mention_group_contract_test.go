package personaffiliation

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/vedantadhobley/legal-tender/internal/source/companypage"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

// This is an offline representation contract. Occurrences are immutable source
// mentions, mention groups are unverified discourse-level proposals, and
// canonical candidates belong to a later source-backed input.
type mentionGroupProposal struct {
	ID          string   `json:"id"`
	Kind        string   `json:"kind"`
	Antecedent  string   `json:"antecedent"`
	Occurrences []string `json:"occurrences"`
}

type mentionGroupChoice struct {
	State  string   `json:"state"`
	Groups []string `json:"groups"`
}

type groupedRoleReferents struct {
	Interpretation string              `json:"interpretation"`
	Subject        *mentionGroupChoice `json:"subject"`
	Organization   *mentionGroupChoice `json:"organization"`
}

type mentionGroupingInput struct {
	Grounding   groundedRoleInput      `json:"grounding"`
	Groups      []mentionGroupProposal `json:"mention_groups"`
	Referents   []groupedRoleReferents `json:"referents"`
	Corrections []correctionTargets    `json:"corrections"`
}

type mentionGroupReview struct {
	ID          string   `json:"id"`
	Kind        string   `json:"kind"`
	Antecedent  string   `json:"antecedent"`
	Occurrences []string `json:"occurrences"`
	State       string   `json:"state"`
}

type groupedReferentReview struct {
	Interpretation     string   `json:"interpretation"`
	SubjectState       string   `json:"subject_state"`
	SubjectGroups      []string `json:"subject_groups"`
	OrganizationState  string   `json:"organization_state"`
	OrganizationGroups []string `json:"organization_groups"`
}

type mentionGroupingReport struct {
	Method                   string                  `json:"method"`
	GroupingOrigin           string                  `json:"grouping_origin"`
	Occurrences              []bindingCandidate      `json:"literal_occurrences"`
	Proposal                 mentionGroupingInput    `json:"supplied_proposal"`
	Groups                   []mentionGroupReview    `json:"mention_group_review"`
	Referents                []groupedReferentReview `json:"referent_review"`
	Derived                  groundedRoleReport      `json:"derived_grounding"`
	IdentityApproved         bool                    `json:"identity_approved"`
	GraphPublicationApproved bool                    `json:"graph_publication_approved"`
	FinancialAttribution     bool                    `json:"financial_attribution"`
}

func validProposalOrigin(origin string) bool {
	return slices.Contains([]string{"synthetic_test", "reviewed_fixture", "model_proposal"}, origin)
}

func inspectMentionGrouping(c modelCase, names, raw []byte, groundingOrigin, groupingOrigin string) (mentionGroupingReport, error) {
	fail := func(msg string) (mentionGroupingReport, error) {
		return mentionGroupingReport{}, fmt.Errorf("mention grouping: %s", msg)
	}
	if len(raw) > 128<<10 {
		return fail("input budget")
	}
	if !validProposalOrigin(groupingOrigin) {
		return fail("unsupported grouping origin")
	}
	var in mentionGroupingInput
	if err := strictjson.Decode(raw, &in); err != nil {
		return mentionGroupingReport{}, err
	}
	if in.Groups == nil || in.Referents == nil || in.Corrections == nil || len(in.Groups) > 64 || len(in.Referents) > 32 || len(in.Corrections) > 32 {
		return fail("missing arrays or scope budget")
	}
	groundingRaw, err := json.Marshal(in.Grounding)
	if err != nil {
		return mentionGroupingReport{}, err
	}
	base, err := inspectGroundedRoles(c, names, groundingRaw, groundingOrigin)
	if err != nil {
		return mentionGroupingReport{}, err
	}
	_, occurrences, err := bindingCandidates(c, names, groundingOrigin)
	if err != nil {
		return mentionGroupingReport{}, err
	}
	occurrenceKinds := map[string]string{}
	for _, occurrence := range occurrences {
		occurrenceKinds[occurrence.ID] = occurrence.Kind
	}

	groups := map[string]mentionGroupProposal{}
	membership := map[string]string{}
	groupReview := make([]mentionGroupReview, 0, len(in.Groups))
	for _, group := range in.Groups {
		if group.ID == "" || occurrenceKinds[group.ID] != "" || groups[group.ID].ID != "" || !slices.Contains([]string{"person", "organization"}, group.Kind) || group.Occurrences == nil || len(group.Occurrences) == 0 || len(group.Occurrences) > 64 {
			return fail("invalid or duplicate mention group")
		}
		seen := map[string]bool{}
		hasAntecedent := false
		for _, occurrence := range group.Occurrences {
			if seen[occurrence] || occurrenceKinds[occurrence] != group.Kind || membership[occurrence] != "" {
				return fail("mention group has duplicate, overlapping or wrongly typed occurrence")
			}
			seen[occurrence] = true
			membership[occurrence] = group.ID
			hasAntecedent = hasAntecedent || occurrence == group.Antecedent
		}
		if !hasAntecedent {
			return fail("mention group antecedent is not a member")
		}
		groups[group.ID] = group
		groupReview = append(groupReview, mentionGroupReview{group.ID, group.Kind, group.Antecedent, slices.Clone(group.Occurrences), "unverified_proposed_equivalence"})
	}

	interpretations := map[string]groundedRoleProposal{}
	for _, interpretation := range in.Grounding.Interpretations {
		interpretations[interpretation.ID] = interpretation
	}
	validChoice := func(choice *mentionGroupChoice, kind string) bool {
		if choice == nil || choice.Groups == nil || len(choice.Groups) > 64 {
			return false
		}
		switch choice.State {
		case "unassessed", "unresolved":
			if len(choice.Groups) != 0 {
				return false
			}
		case "proposed":
			if len(choice.Groups) != 1 {
				return false
			}
		case "ambiguous":
			if len(choice.Groups) < 2 {
				return false
			}
		default:
			return false
		}
		seen := map[string]bool{}
		for _, id := range choice.Groups {
			if seen[id] || groups[id].Kind != kind {
				return false
			}
			seen[id] = true
		}
		return true
	}
	seenReferents := map[string]bool{}
	referentReview := make([]groupedReferentReview, 0, len(in.Referents))
	for _, referent := range in.Referents {
		if interpretations[referent.Interpretation].Kind != "role" || seenReferents[referent.Interpretation] || !validChoice(referent.Subject, "person") || !validChoice(referent.Organization, "organization") {
			return fail("invalid or duplicate grouped role referents")
		}
		seenReferents[referent.Interpretation] = true
		referentReview = append(referentReview, groupedReferentReview{
			Interpretation: referent.Interpretation,
			SubjectState:   "unverified_" + referent.Subject.State, SubjectGroups: slices.Clone(referent.Subject.Groups),
			OrganizationState: "unverified_" + referent.Organization.State, OrganizationGroups: slices.Clone(referent.Organization.Groups),
		})
	}
	for _, interpretation := range in.Grounding.Interpretations {
		if interpretation.Kind == "role" && !seenReferents[interpretation.ID] {
			return fail("role referent assessment missing")
		}
	}

	// Keep correction semantics on the existing validated path. The adapter uses
	// unassessed occurrence choices only to obtain the same immutable grounding
	// and explicitly derived correction links; it does not translate groups back
	// into occurrence identities.
	legacyReferents := make([]roleReferents, 0, len(in.Referents))
	for _, interpretation := range in.Grounding.Interpretations {
		if interpretation.Kind == "role" {
			legacyReferents = append(legacyReferents, roleReferents{Interpretation: interpretation.ID,
				Subject: &referentChoice{State: "unassessed", Candidates: []string{}}, Organization: &referentChoice{State: "unassessed", Candidates: []string{}}})
		}
	}
	legacyRaw, err := json.Marshal(referentInput{Grounding: in.Grounding, Referents: legacyReferents, Corrections: in.Corrections})
	if err != nil {
		return mentionGroupingReport{}, err
	}
	legacy, err := inspectReferentContract(c, names, legacyRaw, groundingOrigin, groupingOrigin)
	if err != nil {
		return mentionGroupingReport{}, err
	}
	if !reflect.DeepEqual(base.Proposal, in.Grounding) || !reflect.DeepEqual(legacy.Proposal.Grounding, in.Grounding) {
		return fail("grounding changed during join")
	}
	return mentionGroupingReport{
		Method: "literal-mention-groups.v1", GroupingOrigin: groupingOrigin,
		Occurrences: occurrences, Proposal: in, Groups: groupReview, Referents: referentReview, Derived: legacy.Derived,
	}, nil
}

// Canonical candidates are source records offered to a later, independently
// joined stage. They are not members of a mention group and are not accepted
// identities merely because a mapping is proposed.
type canonicalEntityCandidate struct {
	ID        string             `json:"id"`
	Kind      string             `json:"kind"`
	Namespace string             `json:"namespace"`
	RecordID  string             `json:"record_id"`
	Source    companypage.Source `json:"source"`
}

type canonicalEntityChoice struct {
	State      string   `json:"state"`
	Candidates []string `json:"candidates"`
}

type canonicalGroupAssessment struct {
	Group  string                 `json:"mention_group"`
	Choice *canonicalEntityChoice `json:"choice"`
}

type canonicalCandidateInput struct {
	MentionGroupingSHA256 string                     `json:"mention_grouping_sha256"`
	Candidates            []canonicalEntityCandidate `json:"canonical_candidates"`
	Assessments           []canonicalGroupAssessment `json:"assessments"`
}

type canonicalAssessmentReview struct {
	Group      string   `json:"mention_group"`
	State      string   `json:"state"`
	Candidates []string `json:"candidates"`
}

type canonicalCandidateReport struct {
	Method                   string                      `json:"method"`
	CandidateOrigin          string                      `json:"candidate_origin"`
	AssessmentOrigin         string                      `json:"assessment_origin"`
	MentionGroupingSHA256    string                      `json:"mention_grouping_sha256"`
	Proposal                 canonicalCandidateInput     `json:"supplied_proposal"`
	Review                   []canonicalAssessmentReview `json:"assessment_review"`
	IdentityApproved         bool                        `json:"identity_approved"`
	GraphPublicationApproved bool                        `json:"graph_publication_approved"`
	FinancialAttribution     bool                        `json:"financial_attribution"`
}

func mentionGroupingDigest(grouping mentionGroupingReport) (string, error) {
	raw, err := json.Marshal(grouping)
	if err != nil {
		return "", err
	}
	return hash(raw), nil
}

func inspectCanonicalCandidates(grouping mentionGroupingReport, raw []byte, candidateOrigin, assessmentOrigin string) (canonicalCandidateReport, error) {
	fail := func(msg string) (canonicalCandidateReport, error) {
		return canonicalCandidateReport{}, fmt.Errorf("canonical candidates: %s", msg)
	}
	if len(raw) > 128<<10 {
		return fail("input budget")
	}
	if !slices.Contains([]string{"synthetic_test", "reviewed_fixture", "source_adapter"}, candidateOrigin) || !validProposalOrigin(assessmentOrigin) {
		return fail("unsupported candidate or assessment origin")
	}
	var in canonicalCandidateInput
	if err := strictjson.Decode(raw, &in); err != nil {
		return canonicalCandidateReport{}, err
	}
	if in.Candidates == nil || in.Assessments == nil || len(in.Candidates) > 64 || len(in.Assessments) > 64 {
		return fail("missing arrays or scope budget")
	}
	digest, err := mentionGroupingDigest(grouping)
	if err != nil {
		return canonicalCandidateReport{}, err
	}
	if in.MentionGroupingSHA256 != digest {
		return fail("mention grouping digest mismatch")
	}
	reserved := map[string]bool{}
	for _, occurrence := range grouping.Occurrences {
		reserved[occurrence.ID] = true
	}
	groupKinds := map[string]string{}
	for _, group := range grouping.Groups {
		reserved[group.ID] = true
		groupKinds[group.ID] = group.Kind
	}
	candidates := map[string]canonicalEntityCandidate{}
	records := map[string]bool{}
	for _, candidate := range in.Candidates {
		recordKey := candidate.Namespace + "\x00" + candidate.RecordID
		if candidate.ID == "" || reserved[candidate.ID] || candidates[candidate.ID].ID != "" || !slices.Contains([]string{"person", "organization"}, candidate.Kind) || strings.TrimSpace(candidate.Namespace) == "" || strings.TrimSpace(candidate.RecordID) == "" || len(candidate.Namespace) > 128 || len(candidate.RecordID) > 512 || records[recordKey] {
			return fail("invalid, duplicate or namespace-colliding canonical candidate")
		}
		if err := candidate.Source.Validate(); err != nil {
			return fail("canonical candidate source is not pinned")
		}
		candidates[candidate.ID] = candidate
		records[recordKey] = true
	}
	validChoice := func(choice *canonicalEntityChoice, kind string) bool {
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
			if seen[id] || candidates[id].Kind != kind {
				return false
			}
			seen[id] = true
		}
		return true
	}
	seenGroups := map[string]bool{}
	review := make([]canonicalAssessmentReview, 0, len(in.Assessments))
	for _, assessment := range in.Assessments {
		if groupKinds[assessment.Group] == "" || seenGroups[assessment.Group] || !validChoice(assessment.Choice, groupKinds[assessment.Group]) {
			return fail("invalid or duplicate canonical assessment")
		}
		seenGroups[assessment.Group] = true
		review = append(review, canonicalAssessmentReview{assessment.Group, "unverified_" + assessment.Choice.State, slices.Clone(assessment.Choice.Candidates)})
	}
	for group := range groupKinds {
		if !seenGroups[group] {
			return fail("mention group canonical assessment missing")
		}
	}
	return canonicalCandidateReport{
		Method: "source-backed-canonical-candidates.v1", CandidateOrigin: candidateOrigin, AssessmentOrigin: assessmentOrigin,
		MentionGroupingSHA256: digest, Proposal: in, Review: review,
	}, nil
}
