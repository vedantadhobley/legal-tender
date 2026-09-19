package candidateresolution

import (
	"reflect"
	"testing"

	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

func TestResolveCandidateReferencePolicy(t *testing.T) {
	index := newCandidateIndex()
	addCandidate := func(factID, candidateID, name, office, state, district string) {
		t.Helper()
		if !index.add(
			fecoccurrence.ClassicFact{FactID: factID},
			fecoccurrence.CandidateTypedFields{
				CandidateID:    candidateID,
				Name:           name,
				Office:         office,
				OfficeState:    state,
				OfficeDistrict: district,
			},
		) {
			t.Fatalf("candidate %s did not form a usable context", candidateID)
		}
	}
	addCandidate(digestParts("candidate-smith"), "H4AA00001", "SMITH, JANE", "H", "AA", "01")
	addCandidate(digestParts("candidate-doe"), "H4AA00002", "DOE, JOHN", "H", "AA", "02")
	addCandidate(digestParts("candidate-lee-one"), "H4AA00003", "LEE, ALEX", "H", "AA", "03")
	addCandidate(digestParts("candidate-lee-two"), "H4AA00004", "LEE, ALEX", "H", "AA", "03")
	addCandidate(digestParts("candidate-rivera"), "H4AA00005", "RIVERA, MARIA", "H", "AA", "05")

	tests := []struct {
		name       string
		candidate  fecoccurrence.ScheduleECandidateFields
		state      string
		method     string
		resolvedID *string
		factCount  int
	}{
		{
			name:      "confirmed reported ID and exact context",
			candidate: scheduleECandidate("H4AA00001", "Jane Smith", "H", "AA", "1"),
			state:     StateConfirmed, method: MethodReportedIDExactContext,
			resolvedID: textPointer("H4AA00001"), factCount: 1,
		},
		{
			name:      "resolved absent ID by unique exact context",
			candidate: scheduleECandidate("H4AA99999", "John Doe", "H", "AA", "02"),
			state:     StateResolved, method: MethodUniqueExactContext,
			resolvedID: textPointer("H4AA00002"), factCount: 1,
		},
		{
			name:      "ambiguous exact context",
			candidate: scheduleECandidate("H4AA99998", "Alex Lee", "H", "AA", "03"),
			state:     StateAmbiguous, method: MethodMultipleExactContext,
			factCount: 2,
		},
		{
			name:      "present reported ID is retained when context is unverified",
			candidate: scheduleECandidate("H4AA00005", "Morgan Quinn", "H", "AA", "06"),
			state:     StateUnverified, method: MethodReportedIDUnverified,
			resolvedID: textPointer("H4AA00005"), factCount: 1,
		},
		{
			name:      "present reported ID is retained when context is insufficient",
			candidate: scheduleECandidate("H4AA00005", "", "H", "AA", "05"),
			state:     StateUnverified, method: MethodReportedIDInsufficient,
			resolvedID: textPointer("H4AA00005"), factCount: 1,
		},
		{
			name:      "absent reported ID has no exact context",
			candidate: scheduleECandidate("H4AA99997", "Casey Unknown", "H", "AA", "07"),
			state:     StateUnresolved, method: MethodNoExactContext,
			factCount: 0,
		},
		{
			name:      "missing name is insufficient context",
			candidate: scheduleECandidate("H4AA99996", "", "H", "AA", "08"),
			state:     StateUnresolved, method: MethodInsufficientContext,
			factCount: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := index.resolve(test.candidate)
			if actual.state != test.state || actual.method != test.method {
				t.Fatalf("got %s/%s, want %s/%s", actual.state, actual.method, test.state, test.method)
			}
			if !reflect.DeepEqual(actual.resolvedCandidateID, test.resolvedID) {
				t.Fatalf("resolved candidate = %#v, want %#v", actual.resolvedCandidateID, test.resolvedID)
			}
			if len(actual.candidateFactIDs) != test.factCount {
				t.Fatalf("candidate fact count = %d, want %d", len(actual.candidateFactIDs), test.factCount)
			}
			if len(actual.evidenceCodes) == 0 {
				t.Fatal("resolution omitted evidence codes")
			}
		})
	}
}

func TestContextKeyRequiresOfficeSpecificFields(t *testing.T) {
	tests := []struct {
		name     string
		office   string
		state    string
		district string
		usable   bool
	}{
		{name: "president", office: "P", usable: true},
		{name: "senate", office: "S", state: "NY", usable: true},
		{name: "senate missing state", office: "S", usable: false},
		{name: "house", office: "H", state: "NY", district: "7", usable: true},
		{name: "house missing district", office: "H", state: "NY", usable: false},
		{name: "invalid office", office: "X", state: "NY", district: "01", usable: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, usable := contextKey("DOE, JANE", test.office, test.state, test.district)
			if usable != test.usable {
				t.Fatalf("usable = %t, want %t", usable, test.usable)
			}
		})
	}
}

func TestNormalizeNameUsesExactTokenMultiset(t *testing.T) {
	if normalizeName("Smith, Jane A.") != normalizeName("JANE A SMITH") {
		t.Fatal("punctuation and source name order should normalize equally")
	}
	if normalizeName("Smith, Jane") == normalizeName("Smith, Janet") {
		t.Fatal("normalization must not perform fuzzy or nickname matching")
	}
}

// Characterize the current interpretation for the pre-attribution review.
// These tests record behavior; they do not establish real-world identity.
func TestReviewCandidateResolutionPrecedence(t *testing.T) {
	index := newCandidateIndex()
	for _, c := range []struct{ id, name string }{
		{"H4AA00001", "LEE, ALEX"},
		{"H4AA00002", "ALEX LEE"},
		{"H4AA00003", "MORGAN QUINN"},
	} {
		index.add(fecoccurrence.ClassicFact{FactID: digestParts(c.id)}, fecoccurrence.CandidateTypedFields{
			CandidateID: c.id, Name: c.name, Office: "H", OfficeState: "AA", OfficeDistrict: "01",
		})
	}
	t.Run("reported ID wins within a non-unique normalized context", func(t *testing.T) {
		r := index.resolve(scheduleECandidate("H4AA00001", "Alex Lee", "H", "AA", "1"))
		if r.state != StateConfirmed || r.resolvedCandidateID == nil || *r.resolvedCandidateID != "H4AA00001" {
			t.Fatalf("reported ID precedence changed: %+v", r)
		}
	})
	t.Run("absent ID cannot choose between normalized context collisions", func(t *testing.T) {
		r := index.resolve(scheduleECandidate("H4AA99999", "Alex Lee", "H", "AA", "1"))
		if r.state != StateAmbiguous || r.resolvedCandidateID != nil || len(r.candidateFactIDs) != 2 {
			t.Fatalf("context collision was not preserved: %+v", r)
		}
	})
	t.Run("unique context can replace an ID that exists in the master", func(t *testing.T) {
		in := scheduleECandidate("H4AA00001", "Morgan Quinn", "H", "AA", "1")
		r := index.resolve(in)
		if r.state != StateResolved || r.resolvedCandidateID == nil || *r.resolvedCandidateID != "H4AA00003" || *in.CandidateID != "H4AA00001" {
			t.Fatalf("replacement interpretation or original assertion changed: %+v", r)
		}
	})
}

func TestReviewNameNormalizationBoundaries(t *testing.T) {
	for _, names := range [][2]string{
		{"Alex Lee", "Alex Alex Lee"}, // Multiplicity is retained.
		{"Alex Lee", "Alex Lee Jr"},   // Suffixes are not discarded.
		{"Jose Lee", "José Lee"},      // No accent folding.
	} {
		if normalizeName(names[0]) == normalizeName(names[1]) {
			t.Fatalf("unexpected name equivalence for %q and %q", names[0], names[1])
		}
	}
}

func scheduleECandidate(candidateID, name, office, state, district string) fecoccurrence.ScheduleECandidateFields {
	return fecoccurrence.ScheduleECandidateFields{
		CandidateID: textPointer(candidateID), Name: textPointer(name), OfficeCode: textPointer(office),
		OfficeState: textPointer(state), OfficeDistrict: textPointer(district),
	}
}

func textPointer(value string) *string {
	return &value
}
