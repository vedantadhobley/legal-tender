package candidateresolution

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"unicode"

	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

type candidateAssertion struct {
	factID      string
	candidateID string
	contextKey  string
}

type candidateIndex struct {
	byID      map[string][]candidateAssertion
	byContext map[string][]candidateAssertion
}

type resolution struct {
	state               string
	method              string
	resolvedCandidateID *string
	candidateFactIDs    []string
	evidenceCodes       []string
}

func newCandidateIndex() candidateIndex {
	return candidateIndex{
		byID:      make(map[string][]candidateAssertion),
		byContext: make(map[string][]candidateAssertion),
	}
}

func (index candidateIndex) add(fact fecoccurrence.ClassicFact, fields fecoccurrence.CandidateTypedFields) bool {
	assertion := candidateAssertion{factID: fact.FactID, candidateID: fields.CandidateID}
	key, _, usable := contextKey(fields.Name, fields.Office, fields.OfficeState, fields.OfficeDistrict)
	if usable {
		assertion.contextKey = key
		index.byContext[key] = append(index.byContext[key], assertion)
	}
	index.byID[fields.CandidateID] = append(index.byID[fields.CandidateID], assertion)
	return usable
}

func (index candidateIndex) resolve(candidate fecoccurrence.ScheduleECandidateFields) resolution {
	reportedID := pointerValue(candidate.CandidateID)
	key, contextIssues, usable := contextKey(
		pointerValue(candidate.Name),
		pointerValue(candidate.OfficeCode),
		pointerValue(candidate.OfficeState),
		pointerValue(candidate.OfficeDistrict),
	)
	reportedAssertions := index.byID[reportedID]
	if !usable {
		codes := []string{"reported_context_insufficient"}
		if len(reportedAssertions) != 0 {
			resolvedID := reportedID
			codes = append(codes, "reported_id_present_in_candidate_master")
			codes = append(codes, contextIssues...)
			return resolution{
				state: StateUnverified, method: MethodReportedIDInsufficient,
				resolvedCandidateID: &resolvedID,
				candidateFactIDs:    factIDs(reportedAssertions), evidenceCodes: codes,
			}
		}
		codes = append(codes, "reported_id_absent_from_candidate_master")
		codes = append(codes, contextIssues...)
		return resolution{
			state: StateUnresolved, method: MethodInsufficientContext,
			candidateFactIDs: factIDs(reportedAssertions), evidenceCodes: codes,
		}
	}

	contextAssertions := index.byContext[key]
	if matching := assertionsForID(contextAssertions, reportedID); len(matching) != 0 {
		resolvedID := reportedID
		return resolution{
			state: StateConfirmed, method: MethodReportedIDExactContext,
			resolvedCandidateID: &resolvedID, candidateFactIDs: factIDs(matching),
			evidenceCodes: []string{"reported_id_present_in_candidate_master", "exact_name_office_context"},
		}
	}

	byCandidate := assertionsByCandidateID(contextAssertions)
	if len(byCandidate) == 1 {
		for candidateID, assertions := range byCandidate {
			resolvedID := candidateID
			codes := []string{"unique_exact_name_office_context"}
			if len(reportedAssertions) == 0 {
				codes = append(codes, "reported_id_absent_from_candidate_master")
			} else {
				codes = append(codes, "reported_id_context_conflict")
			}
			return resolution{
				state: StateResolved, method: MethodUniqueExactContext,
				resolvedCandidateID: &resolvedID, candidateFactIDs: factIDs(assertions), evidenceCodes: codes,
			}
		}
	}
	if len(byCandidate) > 1 {
		codes := []string{"multiple_exact_name_office_candidates"}
		if len(reportedAssertions) == 0 {
			codes = append(codes, "reported_id_absent_from_candidate_master")
		} else {
			codes = append(codes, "reported_id_context_conflict")
		}
		return resolution{
			state: StateAmbiguous, method: MethodMultipleExactContext,
			candidateFactIDs: factIDs(contextAssertions), evidenceCodes: codes,
		}
	}
	if len(reportedAssertions) != 0 {
		resolvedID := reportedID
		return resolution{
			state: StateUnverified, method: MethodReportedIDUnverified,
			resolvedCandidateID: &resolvedID,
			candidateFactIDs:    factIDs(reportedAssertions),
			evidenceCodes:       []string{"reported_id_present_in_candidate_master", "reported_context_not_corroborated", "no_exact_name_office_candidate"},
		}
	}
	return resolution{
		state: StateUnresolved, method: MethodNoExactContext,
		candidateFactIDs: []string{},
		evidenceCodes:    []string{"reported_id_absent_from_candidate_master", "no_exact_name_office_candidate"},
	}
}

func contextKey(name, office, state, district string) (string, []string, bool) {
	normalizedName := normalizeName(name)
	normalizedOffice := strings.ToUpper(strings.TrimSpace(office))
	issues := make([]string, 0, 3)
	if normalizedName == "" {
		issues = append(issues, "candidate_name_missing")
	}
	if normalizedOffice != "H" && normalizedOffice != "S" && normalizedOffice != "P" {
		issues = append(issues, "candidate_office_missing_or_invalid")
	}
	if len(issues) != 0 {
		return "", issues, false
	}
	parts := []string{normalizedName, normalizedOffice}
	if normalizedOffice == "P" {
		return strings.Join(parts, "\x00"), nil, true
	}
	normalizedState := strings.ToUpper(strings.TrimSpace(state))
	if len(normalizedState) != 2 {
		issues = append(issues, "candidate_office_state_missing_or_invalid")
	} else {
		parts = append(parts, normalizedState)
	}
	if normalizedOffice == "H" {
		normalizedDistrict, ok := normalizeDistrict(district)
		if !ok {
			issues = append(issues, "candidate_office_district_missing_or_invalid")
		} else {
			parts = append(parts, normalizedDistrict)
		}
	}
	if len(issues) != 0 {
		return "", issues, false
	}
	return strings.Join(parts, "\x00"), nil, true
}

func normalizeName(value string) string {
	tokens := make([]string, 0, 6)
	var token []rune
	flush := func() {
		if len(token) == 0 {
			return
		}
		tokens = append(tokens, string(token))
		token = token[:0]
	}
	for _, character := range strings.ToUpper(value) {
		if unicode.IsLetter(character) || unicode.IsDigit(character) {
			token = append(token, character)
		} else {
			flush()
		}
	}
	flush()
	sort.Strings(tokens)
	return strings.Join(tokens, " ")
}

func normalizeDistrict(value string) (string, bool) {
	trimmed := strings.ToUpper(strings.TrimSpace(value))
	if trimmed == "AL" || trimmed == "AT LARGE" || trimmed == "AT-LARGE" {
		return "00", true
	}
	if trimmed == "" {
		return "", false
	}
	for _, character := range trimmed {
		if character < '0' || character > '9' {
			return "", false
		}
	}
	if len(trimmed) == 1 {
		return "0" + trimmed, true
	}
	if len(trimmed) == 2 {
		return trimmed, true
	}
	return "", false
}

func decodeCandidateFields(value any) (fecoccurrence.CandidateTypedFields, error) {
	content, err := json.Marshal(value)
	if err != nil {
		return fecoccurrence.CandidateTypedFields{}, err
	}
	var fields fecoccurrence.CandidateTypedFields
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&fields); err != nil {
		return fecoccurrence.CandidateTypedFields{}, err
	}
	if fields.CandidateID == "" {
		return fecoccurrence.CandidateTypedFields{}, fmt.Errorf("candidate ID is empty")
	}
	return fields, nil
}

func assertionsForID(assertions []candidateAssertion, candidateID string) []candidateAssertion {
	result := make([]candidateAssertion, 0)
	for _, assertion := range assertions {
		if assertion.candidateID == candidateID {
			result = append(result, assertion)
		}
	}
	return result
}

func assertionsByCandidateID(assertions []candidateAssertion) map[string][]candidateAssertion {
	result := make(map[string][]candidateAssertion)
	for _, assertion := range assertions {
		result[assertion.candidateID] = append(result[assertion.candidateID], assertion)
	}
	return result
}

func factIDs(assertions []candidateAssertion) []string {
	unique := make(map[string]struct{}, len(assertions))
	for _, assertion := range assertions {
		unique[assertion.factID] = struct{}{}
	}
	result := make([]string, 0, len(unique))
	for factID := range unique {
		result = append(result, factID)
	}
	sort.Strings(result)
	return result
}

func pointerValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
