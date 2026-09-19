package personaffiliation

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	screen "github.com/vedantadhobley/legal-tender/internal/calculation/personaffiliation"
	"github.com/vedantadhobley/legal-tender/internal/source/companypage"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

//go:embed prose_referent_assessment_prompt.txt
var proseReferentAssessmentPrompt string

const referentAssessmentFixture = "referent-assessment-model-v1"

type referentAssessmentInput struct {
	Referents   []roleReferents     `json:"referents"`
	Corrections []correctionTargets `json:"corrections"`
}

type referentAssessmentReport struct {
	Method          string                  `json:"method"`
	GroundingSHA256 string                  `json:"grounding_sha256"`
	GroundingOrigin string                  `json:"grounding_origin"`
	Assessment      referentAssessmentInput `json:"supplied_assessment"`
	Joined          referentReport          `json:"joined_review"`
}

type referentAssessmentCase struct {
	ID              string   `json:"id"`
	GroundingOrigin string   `json:"grounding_origin"`
	GroundingSHA256 string   `json:"grounding_sha256"`
	Checks          []string `json:"checks"`
}

type referentAssessmentSeed struct {
	Case            modelCase
	Names           []byte
	Grounding       []byte
	GroundingOrigin string
}

type reviewedGroundingRole struct {
	ID                     string
	Entry                  int
	Focus                  string
	FocusOccurrence        int
	Kind                   string
	Normalized             string
	Role                   screen.Role
	Subjects               []string
	Organizations          []string
	SubjectSurface         string
	SubjectOccurrence      int
	OrganizationSurface    string
	OrganizationOccurrence int
	Binding                string
	Status                 string
}

type reviewedGroundingSpec struct {
	Roles []reviewedGroundingRole
	Links []contextProposal
}

func reviewedAssessmentGrounding(t *testing.T, c modelCase, names []byte, spec reviewedGroundingSpec) []byte {
	t.Helper()
	cat, err := newCitationCatalog(c)
	if err != nil {
		t.Fatal(err)
	}
	links := append([]contextProposal{}, spec.Links...)
	in := groundedRoleInput{Selections: []citationSelection{}, Interpretations: []groundedRoleProposal{}, Links: links}
	selected := map[string]string{}
	selectText := func(entry int, text string, occurrence int) string {
		t.Helper()
		if text == "" {
			return ""
		}
		key := fmt.Sprintf("%d\x00%s\x00%d", entry, text, occurrence)
		if id := selected[key]; id != "" {
			return id
		}
		options, err := cat.literalOptions(entry, text)
		if err != nil || occurrence < 0 || occurrence >= len(options.Matches) || options.Matches[occurrence].Range == nil {
			t.Fatalf("reviewed grounding literal %q occurrence %d: %v", text, occurrence, err)
		}
		r := *options.Matches[occurrence].Range
		id := fmt.Sprintf("g%d", len(in.Selections))
		in.Selections = append(in.Selections, citationSelection{id, &r.Entry, &r.First, &r.Last})
		selected[key] = id
		return id
	}
	for _, role := range spec.Roles {
		subject := selectText(role.Entry, role.SubjectSurface, role.SubjectOccurrence)
		organization := selectText(role.Entry, role.OrganizationSurface, role.OrganizationOccurrence)
		in.Interpretations = append(in.Interpretations, groundedRoleProposal{
			ID: role.ID, Entry: &role.Entry,
			Focus: selectText(role.Entry, role.Focus, role.FocusOccurrence),
			Kind:  role.Kind, Normalized: role.Normalized, Role: role.Role,
			Subjects: role.Subjects, Organizations: role.Organizations,
			SubjectSurface: &subject, OrganizationSurface: &organization,
			AdditionalEvidence: []string{}, Binding: role.Binding, Status: role.Status,
		})
	}
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := inspectGroundedRoles(c, names, raw, "reviewed_fixture"); err != nil {
		t.Fatal("invalid reviewed grounding", c.ID, err)
	}
	return raw
}

func assessmentGroundingSpecs() map[string]reviewedGroundingSpec {
	role := func(id string, entry int, focus, kind, normalized string, class screen.Role, subjects, organizations []string, subject, organization, binding, status string) reviewedGroundingRole {
		return reviewedGroundingRole{ID: id, Entry: entry, Focus: focus, Kind: kind, Normalized: normalized, Role: class, Subjects: subjects, Organizations: organizations, SubjectSurface: subject, OrganizationSurface: organization, Binding: binding, Status: status}
	}
	return map[string]reviewedGroundingSpec{
		"microsoft-hood": {Roles: []reviewedGroundingRole{
			role("hood-evp", 59, "executive vice president", "role", "executive vice president", screen.Executive, []string{"n0"}, []string{"n3"}, "Amy Hood", "Microsoft", "direct", "asserted"),
			role("hood-cfo", 60, "CFO", "role", "chief financial officer", screen.Executive, []string{"n1"}, []string{"n3"}, "Hood", "Microsoft", "direct", "asserted"),
			role("nadella-ceo", 60, "CEO", "role", "chief executive officer", screen.Executive, []string{"n2"}, []string{"n3"}, "Satya Nadella", "Microsoft", "direct", "asserted"),
		}},
		"amd-su": {Roles: []reviewedGroundingRole{
			role("su-chair", 53, "Chair", "role", "chair", screen.BoardDirector, []string{"n0"}, []string{"n2"}, "Lisa T. Su", "AMD", "direct", "asserted"),
			role("su-coo", 53, "Chief Operating Officer", "role", "chief operating officer", screen.Executive, []string{"n0"}, []string{"n2"}, "she", "AMD’s", "coreference", "asserted"),
			role("su-freescale-svp", 53, "Senior Vice President and General Manager, Networking and Multimedia", "role", "senior vice president and general manager", screen.Executive, []string{"n0"}, []string{"n3"}, "Dr. Su", "Freescale Semiconductor, Inc.", "coreference", "asserted"),
			role("su-freescale-cto", 53, "Chief Technology Officer", "role", "chief technology officer", screen.Executive, []string{"n0"}, []string{"n4"}, "Dr. Su", "Freescale", "coreference", "asserted"),
			role("su-ibm-vp", 53, "Vice President of the Semiconductor Research and Development Center", "role", "vice president", screen.Executive, []string{"n0"}, []string{"n5"}, "Dr. Su", "IBM", "coreference", "asserted"),
			role("su-ti-staff", 53, "member of the technical staff", "role", "member of the technical staff", screen.Employee, []string{"n0"}, []string{"n6"}, "she", "Texas Instruments Incorporated", "coreference", "asserted"),
		}},
		"fresh-mcclure": {Roles: []reviewedGroundingRole{
			role("mcclure-cfo", 67, "Chief Financial Officer", "role", "chief financial officer", screen.Executive, []string{"n0"}, []string{"n1"}, "KC McClure", "Accenture", "direct", "asserted"),
			role("mcclure-goldman-board", 67, "board", "role", "board member", screen.BoardDirector, []string{"n0"}, []string{"n2"}, "Ms. McClure", "The Goldman Sachs Group, Inc.", "coreference", "asserted"),
			role("mcclure-visitor-board", 67, "member", "role", "board of visitors member", screen.UnknownRole, []string{"n0"}, []string{"n3"}, "Ms. McClure", "Smeal College of Business Board of Visitors", "coreference", "asserted"),
		}},
		"grounding-nearby-names": {Roles: []reviewedGroundingRole{
			role("inez-secretary", 1, "secretary", "role", "secretary", screen.UnknownRole, []string{"n0"}, []string{"n3"}, "Inez Rao", "Ash Works", "direct", "asserted"),
		}},
		"new-pronoun-employer-change": {Roles: []reviewedGroundingRole{
			role("nadia-cto", 0, "chief technology officer", "role", "chief technology officer", screen.Executive, []string{"n0"}, []string{"n1"}, "She", "its", "coreference", "asserted"),
			role("nadia-ceo", 1, "chief executive officer", "role", "chief executive officer", screen.Executive, []string{"n0"}, []string{"n2"}, "she", "Larch Robotics", "coreference", "asserted"),
		}},
		"new-correction-and-conflict": {Roles: []reviewedGroundingRole{
			role("vale-asserted", 0, "CEO", "role", "chief executive officer", screen.Executive, []string{"n0"}, []string{"n1"}, "Owen Price", "Vale Devices", "direct", "asserted"),
			role("vale-denied", 1, "CEO", "role", "chief executive officer", screen.Executive, []string{"n0"}, []string{"n1"}, "Owen Price", "Vale Devices", "direct", "denied"),
			role("elm-trustee", 2, "trustee", "role", "trustee", screen.BoardDirector, []string{"n0"}, []string{"n2"}, "Owen Price", "Elm Museum", "direct", "asserted"),
			role("vale-correction", 3, "Correction", "correction", "withdrawal", "", []string{}, []string{}, "", "", "unresolved", "asserted"),
		}, Links: []contextProposal{{From: "vale-asserted", To: "vale-denied", Kind: "contradicts"}}},
	}
}

func retainedAssessmentGrounding(t *testing.T, id string) ([]byte, string) {
	t.Helper()
	path := filepath.Join(fixtureDir, groundingFixture, id+".json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var record modelRecord
	if err := strictjson.Decode(raw, &record); err != nil || record.HTTPStatus != 200 || record.Failure != "" || record.CitationCheck != "literal_citations_valid_semantics_unassessed" {
		t.Fatal("invalid retained grounding capture", id, err)
	}
	content, err := modelContent(record.Response)
	if err != nil {
		t.Fatal(err)
	}
	return content, "model_proposal"
}

func proseReferentAssessmentSeeds(t *testing.T) ([]referentAssessmentSeed, []referentAssessmentCase) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(fixtureDir, referentAssessmentFixture, "cases.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest []referentAssessmentCase
	if err := strictjson.Decode(raw, &manifest); err != nil || len(manifest) != 10 {
		t.Fatal("invalid referent assessment manifest", err)
	}
	available := map[string]bindingSeed{}
	referentSeeds, _ := proseReferentSeeds(t)
	for _, seed := range referentSeeds {
		available[seed.Case.ID] = seed
	}
	for _, seed := range proseBindingSeeds(t) {
		available[seed.Case.ID] = seed
	}
	specs := assessmentGroundingSpecs()
	seeds := make([]referentAssessmentSeed, 0, len(manifest))
	seen := map[string]bool{}
	for _, item := range manifest {
		seed, ok := available[item.ID]
		if !ok || seen[item.ID] || len(item.Checks) == 0 {
			t.Fatal("unknown, duplicate or unreviewed assessment case", item.ID)
		}
		seen[item.ID] = true
		var grounding []byte
		origin := item.GroundingOrigin
		if spec, ok := specs[item.ID]; ok {
			if origin != "reviewed_fixture" {
				t.Fatal("reviewed grounding origin changed", item.ID)
			}
			grounding = reviewedAssessmentGrounding(t, seed.Case, seed.Names, spec)
		} else {
			grounding, origin = retainedAssessmentGrounding(t, item.ID)
			if item.GroundingOrigin != origin {
				t.Fatal("retained grounding origin changed", item.ID)
			}
		}
		if item.GroundingSHA256 != hash(grounding) {
			t.Fatalf("grounding hash changed %s: got %s", item.ID, hash(grounding))
		}
		if _, err := inspectGroundedRoles(seed.Case, seed.Names, grounding, origin); err != nil {
			t.Fatal("manifest grounding is not validated", item.ID, err)
		}
		seeds = append(seeds, referentAssessmentSeed{Case: seed.Case, Names: seed.Names, Grounding: grounding, GroundingOrigin: origin})
	}
	return seeds, manifest
}

func referentAssessmentSchema(grounding groundedRoleInput, candidates []bindingCandidate) map[string]any {
	object := func(fields map[string]any, required ...string) map[string]any {
		return map[string]any{"type": "object", "properties": fields, "required": required, "additionalProperties": false}
	}
	array := func(item any, min, max int) map[string]any {
		return map[string]any{"type": "array", "items": item, "minItems": min, "maxItems": max}
	}
	enum := func(values []string) map[string]any {
		out := map[string]any{"type": "string"}
		if len(values) > 0 {
			out["enum"] = values
		}
		return out
	}
	idsByKind := map[string][]string{"person": {}, "organization": {}}
	for _, candidate := range candidates {
		idsByKind[candidate.Kind] = append(idsByKind[candidate.Kind], candidate.ID)
	}
	roles, corrections := []string{}, []string{}
	for _, interpretation := range grounding.Interpretations {
		switch interpretation.Kind {
		case "role":
			roles = append(roles, interpretation.ID)
		case "correction":
			corrections = append(corrections, interpretation.ID)
		}
	}
	choice := func(kind string) map[string]any {
		ids := idsByKind[kind]
		items := enum(ids)
		return object(map[string]any{
			"state":      enum([]string{"unresolved", "proposed", "ambiguous"}),
			"candidates": array(items, 0, len(ids)),
		}, "state", "candidates")
	}
	roleID := enum(roles)
	correctionID := enum(corrections)
	targetID := enum(roles)
	roleItem := object(map[string]any{"interpretation": roleID, "subject": choice("person"), "organization": choice("organization")}, "interpretation", "subject", "organization")
	correctionItem := object(map[string]any{"correction": correctionID, "state": enum([]string{"proposed", "ambiguous", "unresolved"}), "targets": array(targetID, 0, len(roles))}, "correction", "state", "targets")
	return object(map[string]any{
		"referents":   array(roleItem, len(roles), len(roles)),
		"corrections": array(correctionItem, len(corrections), len(corrections)),
	}, "referents", "corrections")
}

func proseReferentAssessmentRequest(seed referentAssessmentSeed, profile modelProfile) ([]byte, error) {
	var grounding groundedRoleInput
	if err := strictjson.Decode(seed.Grounding, &grounding); err != nil {
		return nil, err
	}
	if _, err := inspectGroundedRoles(seed.Case, seed.Names, seed.Grounding, seed.GroundingOrigin); err != nil {
		return nil, err
	}
	_, candidates, err := bindingCandidates(seed.Case, seed.Names, seed.GroundingOrigin)
	if err != nil {
		return nil, err
	}
	input, err := json.Marshal(struct {
		Source          companypage.Source `json:"source"`
		Entries         []modelEntry       `json:"source_entries"`
		Candidates      []bindingCandidate `json:"name_candidates"`
		GroundingSHA256 string             `json:"grounding_sha256"`
		Grounding       groundedRoleInput  `json:"grounding"`
	}{seed.Case.Source, seed.Case.Entries, candidates, hash(seed.Grounding), grounding})
	if err != nil || len(input) > 128<<10 {
		return nil, fmt.Errorf("referent assessment input budget")
	}
	request := map[string]any{
		"model":      profile.Model,
		"messages":   []map[string]string{{"role": "system", "content": proseReferentAssessmentPrompt}, {"role": "user", "content": string(input)}},
		"max_tokens": profile.MaxTokens, "reasoning_effort": profile.ReasoningEffort,
		"seed": 1, "stream": false,
		"response_format": map[string]any{"type": "json_schema", "json_schema": map[string]any{"name": "referent_and_correction_assessments", "strict": true, "schema": referentAssessmentSchema(grounding, candidates)}},
	}
	return json.Marshal(request)
}

func inspectReferentAssessment(seed referentAssessmentSeed, raw []byte) (referentAssessmentReport, error) {
	if len(raw) > 128<<10 {
		return referentAssessmentReport{}, fmt.Errorf("referent assessment: input budget")
	}
	var assessment referentAssessmentInput
	if err := strictjson.Decode(raw, &assessment); err != nil {
		return referentAssessmentReport{}, err
	}
	for _, referent := range assessment.Referents {
		if (referent.Subject != nil && referent.Subject.State == "unassessed") || (referent.Organization != nil && referent.Organization.State == "unassessed") {
			return referentAssessmentReport{}, fmt.Errorf("referent assessment: model cannot return unassessed")
		}
	}
	var grounding groundedRoleInput
	if err := strictjson.Decode(seed.Grounding, &grounding); err != nil {
		return referentAssessmentReport{}, err
	}
	joinedRaw, err := json.Marshal(referentInput{Grounding: grounding, Referents: assessment.Referents, Corrections: assessment.Corrections})
	if err != nil {
		return referentAssessmentReport{}, err
	}
	joined, err := inspectReferentContract(seed.Case, seed.Names, joinedRaw, seed.GroundingOrigin, "model_proposal")
	if err != nil {
		return referentAssessmentReport{}, err
	}
	if !reflect.DeepEqual(joined.Proposal.Grounding, grounding) {
		return referentAssessmentReport{}, fmt.Errorf("referent assessment: grounding changed during join")
	}
	return referentAssessmentReport{Method: "grounding-bound-referent-assessment.v1", GroundingSHA256: hash(seed.Grounding), GroundingOrigin: seed.GroundingOrigin, Assessment: assessment, Joined: joined}, nil
}

func inspectReferentAssessmentModelAnswer(seed referentAssessmentSeed, raw []byte) (json.RawMessage, error) {
	out, err := inspectReferentAssessment(seed, raw)
	if err != nil {
		return nil, err
	}
	return json.Marshal(out)
}

func TestProseReferentAssessmentModelLive(t *testing.T) {
	if os.Getenv("LT_PROSE_REFERENT_ASSESSMENT_TRIAL") != "1" || os.Getenv("LT_PROSE_MODEL_URL") == "" {
		t.Skip("explicit opt-in referent assessment trial; offline by default")
	}
	seeds, _ := proseReferentAssessmentSeeds(t)
	byID := map[string]referentAssessmentSeed{}
	cases := make([]modelCase, 0, len(seeds))
	for _, seed := range seeds {
		byID[seed.Case.ID] = seed
		cases = append(cases, seed.Case)
	}
	runProseModelTrial(t, proseReferentAssessmentPrompt, func(*testing.T) []modelCase { return cases }, func(c modelCase, profile modelProfile) []byte {
		raw, err := proseReferentAssessmentRequest(byID[c.ID], profile)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}, func(c modelCase, raw []byte) (json.RawMessage, error) {
		return inspectReferentAssessmentModelAnswer(byID[c.ID], raw)
	})
}

func TestProseReferentAssessmentModelRequiresSpecificOptIn(t *testing.T) {
	t.Setenv("LT_PROSE_REFERENT_ASSESSMENT_TRIAL", "")
	t.Setenv("LT_PROSE_MODEL_URL", ":invalid-url")
	if !t.Run("generic opt-in is insufficient", TestProseReferentAssessmentModelLive) {
		t.Fatal("assessment trial started without its specific opt-in")
	}
}
