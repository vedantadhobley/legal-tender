package personaffiliation

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/companypage"
)

func mentionGroupingJSON(t *testing.T, in mentionGroupingInput) []byte {
	t.Helper()
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func proposedGroup(id string) *mentionGroupChoice {
	return &mentionGroupChoice{State: "proposed", Groups: []string{id}}
}

func unresolvedGroup() *mentionGroupChoice {
	return &mentionGroupChoice{State: "unresolved", Groups: []string{}}
}

func assessmentSeedByID(t *testing.T, id string) referentAssessmentSeed {
	t.Helper()
	seeds, _ := proseReferentAssessmentSeeds(t)
	for _, seed := range seeds {
		if seed.Case.ID == id {
			return seed
		}
	}
	t.Fatal("missing assessment seed", id)
	return referentAssessmentSeed{}
}

func decodeGrounding(t *testing.T, raw []byte) groundedRoleInput {
	t.Helper()
	var grounding groundedRoleInput
	if err := json.Unmarshal(raw, &grounding); err != nil {
		t.Fatal(err)
	}
	return grounding
}

func microsoftGroupingInput(t *testing.T) (referentAssessmentSeed, mentionGroupingInput) {
	t.Helper()
	seed := assessmentSeedByID(t, "microsoft-hood")
	return seed, mentionGroupingInput{
		Grounding: decodeGrounding(t, seed.Grounding),
		Groups: []mentionGroupProposal{
			{ID: "mg_amy_hood", Kind: "person", Antecedent: "n0", Occurrences: []string{"n0", "n1"}},
			{ID: "mg_satya_nadella", Kind: "person", Antecedent: "n2", Occurrences: []string{"n2"}},
			{ID: "mg_microsoft", Kind: "organization", Antecedent: "n3", Occurrences: []string{"n3"}},
		},
		Referents: []groupedRoleReferents{
			{Interpretation: "hood-evp", Subject: proposedGroup("mg_amy_hood"), Organization: proposedGroup("mg_microsoft")},
			{Interpretation: "hood-cfo", Subject: proposedGroup("mg_amy_hood"), Organization: proposedGroup("mg_microsoft")},
			{Interpretation: "nadella-ceo", Subject: proposedGroup("mg_satya_nadella"), Organization: proposedGroup("mg_microsoft")},
		},
		Corrections: []correctionTargets{},
	}
}

func TestMentionGroupsSeparateOccurrencesFromProposedAntecedents(t *testing.T) {
	seed, in := microsoftGroupingInput(t)
	out, err := inspectMentionGrouping(seed.Case, seed.Names, mentionGroupingJSON(t, in), seed.GroundingOrigin, "reviewed_fixture")
	if err != nil {
		t.Fatal(err)
	}
	_, occurrences, err := bindingCandidates(seed.Case, seed.Names, seed.GroundingOrigin)
	if err != nil {
		t.Fatal(err)
	}
	if out.Method != "literal-mention-groups.v1" || !reflect.DeepEqual(out.Occurrences, occurrences) || !reflect.DeepEqual(out.Proposal, in) || out.Groups[0].Antecedent != "n0" || !slices.Equal(out.Groups[0].Occurrences, []string{"n0", "n1"}) || out.Groups[0].State != "unverified_proposed_equivalence" {
		t.Fatal("literal occurrence or proposed group changed")
	}
	if out.Referents[0].SubjectState != "unverified_proposed" || !slices.Equal(out.Referents[1].SubjectGroups, []string{"mg_amy_hood"}) || out.Referents[2].SubjectGroups[0] == out.Referents[1].SubjectGroups[0] {
		t.Fatal("role did not reference discourse groups")
	}
	if out.IdentityApproved || out.GraphPublicationApproved || out.FinancialAttribution || !reflect.DeepEqual(out.Derived.Proposal, in.Grounding) {
		t.Fatal("group proposal approved identity or changed grounding")
	}
	again, err := inspectMentionGrouping(seed.Case, seed.Names, mentionGroupingJSON(t, in), seed.GroundingOrigin, "reviewed_fixture")
	if err != nil || !reflect.DeepEqual(out, again) {
		t.Fatal("mention grouping is not deterministic", err)
	}
}

func TestMentionGroupsHandleShortOrganizationWithoutNameRule(t *testing.T) {
	seed := assessmentSeedByID(t, "amd-su")
	in := mentionGroupingInput{
		Grounding: decodeGrounding(t, seed.Grounding),
		Groups: []mentionGroupProposal{
			{ID: "mg_su", Kind: "person", Antecedent: "n0", Occurrences: []string{"n0", "n1"}},
			{ID: "mg_amd", Kind: "organization", Antecedent: "n2", Occurrences: []string{"n2"}},
			{ID: "mg_freescale", Kind: "organization", Antecedent: "n3", Occurrences: []string{"n3", "n4"}},
			{ID: "mg_ibm", Kind: "organization", Antecedent: "n5", Occurrences: []string{"n5"}},
			{ID: "mg_ti", Kind: "organization", Antecedent: "n6", Occurrences: []string{"n6"}},
		},
		Referents: []groupedRoleReferents{
			{Interpretation: "su-chair", Subject: proposedGroup("mg_su"), Organization: proposedGroup("mg_amd")},
			{Interpretation: "su-coo", Subject: proposedGroup("mg_su"), Organization: proposedGroup("mg_amd")},
			{Interpretation: "su-freescale-svp", Subject: proposedGroup("mg_su"), Organization: proposedGroup("mg_freescale")},
			{Interpretation: "su-freescale-cto", Subject: proposedGroup("mg_su"), Organization: proposedGroup("mg_freescale")},
			{Interpretation: "su-ibm-vp", Subject: proposedGroup("mg_su"), Organization: proposedGroup("mg_ibm")},
			{Interpretation: "su-ti-staff", Subject: proposedGroup("mg_su"), Organization: proposedGroup("mg_ti")},
		}, Corrections: []correctionTargets{},
	}
	out, err := inspectMentionGrouping(seed.Case, seed.Names, mentionGroupingJSON(t, in), seed.GroundingOrigin, "reviewed_fixture")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(out.Groups[2].Occurrences, []string{"n3", "n4"}) || out.Groups[2].Antecedent != "n3" || !slices.Equal(out.Referents[2].OrganizationGroups, out.Referents[3].OrganizationGroups) {
		t.Fatal("full and short organization occurrences were not represented separately from their proposed group")
	}
}

func TestMentionGroupAmbiguityDoesNotMergePeople(t *testing.T) {
	seed := assessmentSeedByID(t, "grounding-ambiguous-conditional")
	in := mentionGroupingInput{
		Grounding: decodeGrounding(t, seed.Grounding),
		Groups: []mentionGroupProposal{
			{ID: "mg_luca", Kind: "person", Antecedent: "n0", Occurrences: []string{"n0"}},
			{ID: "mg_leila", Kind: "person", Antecedent: "n1", Occurrences: []string{"n1"}},
			{ID: "mg_juniper", Kind: "organization", Antecedent: "n2", Occurrences: []string{"n2"}},
		},
		Referents: []groupedRoleReferents{{
			Interpretation: "i0",
			Subject:        &mentionGroupChoice{State: "ambiguous", Groups: []string{"mg_luca", "mg_leila"}},
			Organization:   proposedGroup("mg_juniper"),
		}}, Corrections: []correctionTargets{},
	}
	out, err := inspectMentionGrouping(seed.Case, seed.Names, mentionGroupingJSON(t, in), seed.GroundingOrigin, "reviewed_fixture")
	if err != nil {
		t.Fatal(err)
	}
	for _, group := range out.Groups {
		if slices.Contains(group.Occurrences, "n3") {
			t.Fatal("ambiguous surname occurrence was merged into an alternative")
		}
	}
	if out.Referents[0].SubjectState != "unverified_ambiguous" || !slices.Equal(out.Referents[0].SubjectGroups, []string{"mg_luca", "mg_leila"}) || len(out.Groups[0].Occurrences) != 1 || len(out.Groups[1].Occurrences) != 1 {
		t.Fatal("person alternatives collapsed")
	}
}

func TestMentionGroupingDoesNotInferFromSpelling(t *testing.T) {
	seed, in := microsoftGroupingInput(t)
	in.Groups = []mentionGroupProposal{}
	for i := range in.Referents {
		in.Referents[i].Subject = unresolvedGroup()
		in.Referents[i].Organization = unresolvedGroup()
	}
	out, err := inspectMentionGrouping(seed.Case, seed.Names, mentionGroupingJSON(t, in), seed.GroundingOrigin, "reviewed_fixture")
	if err != nil || len(out.Groups) != 0 || len(out.Occurrences) == 0 {
		t.Fatal("literal spelling manufactured a group", err)
	}
}

func TestMentionGroupingPreservesCorrectionTargets(t *testing.T) {
	c, names, old := referentUnitInput(t)
	_, occurrences, err := bindingCandidates(c, names, "synthetic_test")
	if err != nil {
		t.Fatal(err)
	}
	in := mentionGroupingInput{Grounding: old.Grounding, Groups: []mentionGroupProposal{}, Referents: []groupedRoleReferents{}, Corrections: old.Corrections}
	for _, occurrence := range occurrences {
		in.Groups = append(in.Groups, mentionGroupProposal{ID: "mg_" + occurrence.ID, Kind: occurrence.Kind, Antecedent: occurrence.ID, Occurrences: []string{occurrence.ID}})
	}
	choice := func(ids []string) *mentionGroupChoice {
		if len(ids) == 0 {
			return unresolvedGroup()
		}
		groups := make([]string, len(ids))
		for i, id := range ids {
			groups[i] = "mg_" + id
		}
		state := "proposed"
		if len(groups) > 1 {
			state = "ambiguous"
		}
		return &mentionGroupChoice{State: state, Groups: groups}
	}
	for _, interpretation := range old.Grounding.Interpretations {
		if interpretation.Kind == "role" {
			in.Referents = append(in.Referents, groupedRoleReferents{Interpretation: interpretation.ID, Subject: choice(interpretation.Subjects), Organization: choice(interpretation.Organizations)})
		}
	}
	out, err := inspectMentionGrouping(c, names, mentionGroupingJSON(t, in), "synthetic_test", "synthetic_test")
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Proposal.Grounding.Links) != 1 || len(out.Derived.Proposal.Links) != 2 || out.Derived.Proposal.Links[1] != (contextProposal{From: "notice", To: "positive", Kind: "retracts"}) {
		t.Fatal("explicit correction target changed during group join")
	}
}

func TestMentionGroupingRejectsConflationAndForgery(t *testing.T) {
	for name, mutate := range map[string]func(*mentionGroupingInput){
		"occurrence used as group":          func(v *mentionGroupingInput) { v.Referents[0].Subject.Groups = []string{"n0"} },
		"group ID collides with occurrence": func(v *mentionGroupingInput) { v.Groups[0].ID = "n0" },
		"overlapping groups": func(v *mentionGroupingInput) {
			v.Groups = append(v.Groups, mentionGroupProposal{ID: "mg_overlap", Kind: "person", Antecedent: "n1", Occurrences: []string{"n1"}})
		},
		"wrongly typed member":     func(v *mentionGroupingInput) { v.Groups[0].Occurrences = append(v.Groups[0].Occurrences, "n3") },
		"antecedent outside group": func(v *mentionGroupingInput) { v.Groups[0].Antecedent = "n2" },
		"missing role":             func(v *mentionGroupingInput) { v.Referents = v.Referents[1:] },
		"duplicate role":           func(v *mentionGroupingInput) { v.Referents = append(v.Referents, v.Referents[0]) },
		"singleton ambiguity":      func(v *mentionGroupingInput) { v.Referents[0].Subject.State = "ambiguous" },
		"wrong group type":         func(v *mentionGroupingInput) { v.Referents[0].Subject.Groups = []string{"mg_microsoft"} },
		"unresolved with group":    func(v *mentionGroupingInput) { v.Referents[0].Subject.State = "unresolved" },
		"nil groups":               func(v *mentionGroupingInput) { v.Groups = nil },
		"scope budget":             func(v *mentionGroupingInput) { v.Groups = make([]mentionGroupProposal, 65) },
	} {
		t.Run(name, func(t *testing.T) {
			seed, in := microsoftGroupingInput(t)
			mutate(&in)
			out, err := inspectMentionGrouping(seed.Case, seed.Names, mentionGroupingJSON(t, in), seed.GroundingOrigin, "reviewed_fixture")
			if err == nil || !reflect.DeepEqual(out, mentionGroupingReport{}) {
				t.Fatal("invalid grouping returned partial evidence")
			}
		})
	}
	seed, in := microsoftGroupingInput(t)
	raw := string(mentionGroupingJSON(t, in))
	for _, bad := range []string{raw + raw, `{"canonical_candidates":[],` + raw[1:], `{"identity_approved":true,` + raw[1:], raw + strings.Repeat(" ", 128<<10)} {
		if _, err := inspectMentionGrouping(seed.Case, seed.Names, []byte(bad), seed.GroundingOrigin, "reviewed_fixture"); err == nil {
			t.Fatal("canonical data, approval, trailing JSON or excess input entered grouping stage")
		}
	}
}

func canonicalInputForGrouping(t *testing.T, grouping mentionGroupingReport) canonicalCandidateInput {
	t.Helper()
	digest, err := mentionGroupingDigest(grouping)
	if err != nil {
		t.Fatal(err)
	}
	source := func(id string) companypage.Source {
		return companypage.Source{URL: "https://registry.example/records/" + id, ObservedOn: "2026-09-19", SHA256: hash([]byte("retained registry record " + id))}
	}
	return canonicalCandidateInput{
		MentionGroupingSHA256: digest,
		Candidates: []canonicalEntityCandidate{
			{ID: "ce_amy", Kind: "person", Namespace: "test_registry", RecordID: "person-1", Source: source("person-1")},
			{ID: "ce_ms_primary", Kind: "organization", Namespace: "test_registry", RecordID: "organization-1", Source: source("organization-1")},
			{ID: "ce_ms_alternative", Kind: "organization", Namespace: "test_registry", RecordID: "organization-2", Source: source("organization-2")},
		},
		Assessments: []canonicalGroupAssessment{
			{Group: "mg_amy_hood", Choice: &canonicalEntityChoice{State: "proposed", Candidates: []string{"ce_amy"}}},
			{Group: "mg_satya_nadella", Choice: &canonicalEntityChoice{State: "unresolved", Candidates: []string{}}},
			{Group: "mg_microsoft", Choice: &canonicalEntityChoice{State: "ambiguous", Candidates: []string{"ce_ms_primary", "ce_ms_alternative"}}},
		},
	}
}

func canonicalJSON(t *testing.T, in canonicalCandidateInput) []byte {
	t.Helper()
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestCanonicalCandidatesAreSeparateSourceBackedStage(t *testing.T) {
	seed, in := microsoftGroupingInput(t)
	grouping, err := inspectMentionGrouping(seed.Case, seed.Names, mentionGroupingJSON(t, in), seed.GroundingOrigin, "reviewed_fixture")
	if err != nil {
		t.Fatal(err)
	}
	canonical := canonicalInputForGrouping(t, grouping)
	out, err := inspectCanonicalCandidates(grouping, canonicalJSON(t, canonical), "source_adapter", "reviewed_fixture")
	if err != nil {
		t.Fatal(err)
	}
	if out.Method != "source-backed-canonical-candidates.v1" || out.MentionGroupingSHA256 != canonical.MentionGroupingSHA256 || !reflect.DeepEqual(out.Proposal, canonical) || out.Review[0].State != "unverified_proposed" || out.Review[2].State != "unverified_ambiguous" {
		t.Fatal("canonical candidate boundary changed")
	}
	if out.IdentityApproved || out.GraphPublicationApproved || out.FinancialAttribution || !reflect.DeepEqual(grouping.Proposal, in) {
		t.Fatal("canonical candidate proposal approved or mutated mention evidence")
	}
	again, err := inspectCanonicalCandidates(grouping, canonicalJSON(t, canonical), "source_adapter", "reviewed_fixture")
	if err != nil || !reflect.DeepEqual(out, again) {
		t.Fatal("canonical candidate boundary is not deterministic", err)
	}
}

func TestCanonicalCandidateBoundaryRejectsCrossLayerReferences(t *testing.T) {
	for name, mutate := range map[string]func(*canonicalCandidateInput){
		"occurrence as candidate": func(v *canonicalCandidateInput) { v.Assessments[0].Choice.Candidates = []string{"n0"} },
		"group as candidate":      func(v *canonicalCandidateInput) { v.Candidates[0].ID = "mg_amy_hood" },
		"candidate as group":      func(v *canonicalCandidateInput) { v.Assessments[0].Group = "ce_amy" },
		"wrong kind":              func(v *canonicalCandidateInput) { v.Assessments[0].Choice.Candidates = []string{"ce_ms_primary"} },
		"duplicate external record": func(v *canonicalCandidateInput) {
			v.Candidates[1].RecordID = "person-1"
			v.Candidates[1].Namespace = "test_registry"
		},
		"unpinned source":            func(v *canonicalCandidateInput) { v.Candidates[0].Source.SHA256 = "" },
		"wrong grouping digest":      func(v *canonicalCandidateInput) { v.MentionGroupingSHA256 = strings.Repeat("0", 64) },
		"missing group assessment":   func(v *canonicalCandidateInput) { v.Assessments = v.Assessments[1:] },
		"duplicate group assessment": func(v *canonicalCandidateInput) { v.Assessments = append(v.Assessments, v.Assessments[0]) },
		"singleton ambiguity": func(v *canonicalCandidateInput) {
			v.Assessments[2].Choice.Candidates = v.Assessments[2].Choice.Candidates[:1]
		},
		"nil candidates": func(v *canonicalCandidateInput) { v.Candidates = nil },
	} {
		t.Run(name, func(t *testing.T) {
			seed, in := microsoftGroupingInput(t)
			grouping, err := inspectMentionGrouping(seed.Case, seed.Names, mentionGroupingJSON(t, in), seed.GroundingOrigin, "reviewed_fixture")
			if err != nil {
				t.Fatal(err)
			}
			canonical := canonicalInputForGrouping(t, grouping)
			mutate(&canonical)
			out, err := inspectCanonicalCandidates(grouping, canonicalJSON(t, canonical), "source_adapter", "reviewed_fixture")
			if err == nil || !reflect.DeepEqual(out, canonicalCandidateReport{}) {
				t.Fatal("invalid canonical input returned partial evidence")
			}
		})
	}
	seed, in := microsoftGroupingInput(t)
	grouping, err := inspectMentionGrouping(seed.Case, seed.Names, mentionGroupingJSON(t, in), seed.GroundingOrigin, "reviewed_fixture")
	if err != nil {
		t.Fatal(err)
	}
	canonical := canonicalInputForGrouping(t, grouping)
	if _, err := inspectCanonicalCandidates(grouping, canonicalJSON(t, canonical), "model_proposal", "reviewed_fixture"); err == nil {
		t.Fatal("model supplied its own canonical source candidates")
	}
}
