package summaryassertion

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/committeesummary"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/summarypublication"
)

func records(t *testing.T, rows ...map[string]string) []summarypublication.Fact {
	t.Helper()
	var data bytes.Buffer
	w := csv.NewWriter(&data)
	fields := committeesummary.Fields()
	_ = w.Write(fields)
	for _, input := range rows {
		row := make([]string, len(fields))
		for i, name := range fields {
			row[i] = input[name]
		}
		_ = w.Write(row)
	}
	w.Flush()
	if err := w.Error(); err != nil {
		t.Fatal(err)
	}
	expected := committeesummary.Expected{Cycle: "2024", Bytes: int64(data.Len()), SHA256: digest(data.Bytes())}
	out := []summarypublication.Fact{}
	_, err := committeesummary.Verify(context.Background(), bytes.NewReader(data.Bytes()), expected, func(r *committeesummary.Record) error {
		out = append(out, summarypublication.Fact{FactType: summarypublication.FactType, FactID: fmt.Sprintf("%064d", r.Ordinal), OccurrenceID: fmt.Sprintf("%064d", r.Ordinal), Cycle: "2024", OriginSnapshotID: expected.SHA256, Record: *r})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func source() map[string]string {
	return map[string]string{"CMTE_ID": "C00000001", "CMTE_NM": "Synthetic", "FEC_ELECTION_YR": "2024", "CAND_ID": "H0AA00001", "CMTE_TP": "H", "CVG_START_DT": "20230101", "CVG_END_DT": "20241231", "COH_BOP": "10", "TTL_RECEIPTS": "20", "TTL_DISB": "5", "COH_COP": "25", "INDV_ITEM_CONTB": "2", "INDV_UNITEM_CONTB": "3", "INDV_CONTB": "5"}
}

func grouped(t *testing.T, facts []summarypublication.Fact) Result {
	t.Helper()
	b := newBuilder("2024", Input{FactSetID: strings.Repeat("a", 64), ManifestSHA256: strings.Repeat("b", 64), SourceReleaseID: "fec-" + strings.Repeat("c", 64), SourceReleaseSHA256: strings.Repeat("d", 64), SourceArtifactSHA256: facts[0].OriginSnapshotID})
	for _, f := range facts {
		if err := b.add(f); err != nil {
			t.Fatal(err)
		}
	}
	r, err := b.finish()
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestCandidateFanoutAndExactDuplicatesRetainAllMembers(t *testing.T) {
	a, b := source(), source()
	b["CAND_ID"] = "not-a-candidate"
	facts := records(t, a, b, a)
	r := grouped(t, facts)
	if r.Counts != (Counts{SourceRows: 3, IndexedRows: 3, Committees: 1, Assertions: 1, RepeatedEvidenceRows: 2}) || r.TerminalAttributionEligible || r.FinancialUseEligible {
		t.Fatal(r.Counts)
	}
	x := r.Committees[0].Assertions[0]
	if len(x.Members) != 3 || x.Members[1].Candidate.State != committeesummary.Invalid || x.Members[0].FactID == x.Members[2].FactID || x.Equations["cash"].State != "equal" {
		t.Fatal(x)
	}
	if !reflect.DeepEqual(r, grouped(t, facts)) {
		t.Fatal("unstable replay")
	}
	if path := os.Getenv("LT_SUMMARY_ASSERTION_FIXTURE"); path != "" {
		raw, _ := json.MarshalIndent(r, "", "  ")
		if err := os.WriteFile(path, append(raw, '\n'), 0o640); err != nil {
			t.Fatal(err)
		}
	}
}

func TestEveryNonCandidateFieldParticipatesInEquality(t *testing.T) {
	for _, field := range committeesummary.Fields() {
		if field == "CAND_ID" || field == "CMTE_ID" || field == "FEC_ELECTION_YR" {
			continue
		}
		t.Run(field, func(t *testing.T) {
			a, b := source(), source()
			b[field] = "changed"
			r := grouped(t, records(t, a, b))
			g := r.Committees[0]
			if g.State != "conflicting_assertions" || len(g.Assertions) != 2 || !slices.Equal(g.ConflictFields, []string{field}) {
				t.Fatal(g)
			}
		})
	}
	for _, pair := range [][2]string{{"", "0"}, {"1", "1.00"}, {"1", "1 "}} {
		a, b := source(), source()
		a["TTL_RECEIPTS"], b["TTL_RECEIPTS"] = pair[0], pair[1]
		if grouped(t, records(t, a, b)).Counts.ConflictingCommittees != 1 {
			t.Fatal("normalized unequal evidence")
		}
	}
}

func TestConflictFieldsAcrossThreeVariantsAndCommitteeIsolation(t *testing.T) {
	a, b, c, d := source(), source(), source(), source()
	b["TTL_RECEIPTS"] = "21"
	c["CMTE_NM"] = "changed"
	d["CMTE_ID"] = "C00000002"
	facts := records(t, a, b, c, d)
	for range 20 {
		r := grouped(t, facts)
		if r.Counts.Committees != 2 || r.Counts.ConflictingCommittees != 1 || !slices.Equal(r.Committees[0].ConflictFields, []string{"CMTE_NM", "TTL_RECEIPTS"}) {
			t.Fatal(r.Counts)
		}
	}
}

func TestUnindexedRowsDoNotBecomeOneAnonymousCommittee(t *testing.T) {
	a, b := source(), source()
	a["CMTE_ID"] = ""
	b["CMTE_ID"] = "bad"
	r := grouped(t, records(t, a, b))
	if r.Counts.UnindexedRows != 2 || len(r.Committees) != 0 || len(r.Unindexed) != 2 {
		t.Fatal(r)
	}
}

func TestExactSignedDiagnosticsAndUnavailableOperands(t *testing.T) {
	a := source()
	a["INDV_ITEM_CONTB"] = ".32"
	a["INDV_UNITEM_CONTB"] = "-.01"
	a["INDV_CONTB"] = ".30"
	x := grouped(t, records(t, a)).Committees[0].Assertions[0]
	if *x.Equations["individual"].Delta != "1" || x.Equations["cash_federal_columns"].State != "missing" || x.Equations["cash_federal_columns"].Delta != nil {
		t.Fatal(x)
	}
	a["INDV_ITEM_CONTB"] = "bad"
	a["INDV_UNITEM_CONTB"] = ""
	x = grouped(t, records(t, a)).Committees[0].Assertions[0]
	if x.Equations["individual"].State != "invalid" || x.Equations["individual"].Delta != nil {
		t.Fatal(x)
	}
	a = source()
	a["COH_BOP"] = "92233720368547758.07"
	a["TTL_RECEIPTS"] = a["COH_BOP"]
	a["TTL_DISB"] = "0"
	a["COH_COP"] = "0"
	x = grouped(t, records(t, a)).Committees[0].Assertions[0]
	if *x.Equations["cash"].Delta != "18446744073709551614" {
		t.Fatal("overflow", x)
	}
}

func TestRejectInvalidInputsAndOrdering(t *testing.T) {
	if _, err := Run(context.Background(), "", "", ""); err == nil {
		t.Fatal("accepted empty inputs")
	}
	f := records(t, source())[0]
	b := newBuilder("2024", Input{SourceArtifactSHA256: f.OriginSnapshotID})
	if err := b.add(f); err != nil {
		t.Fatal(err)
	}
	if err := b.add(f); err == nil {
		t.Fatal("accepted repeated ordinal/fact ID")
	}
	b = newBuilder("2024", Input{SourceArtifactSHA256: "different"})
	if err := b.add(f); err == nil {
		t.Fatal("accepted other source")
	}
}

func TestCompiledEquationPolicy(t *testing.T) {
	raw, err := os.ReadFile("../../../../contracts/calculations/fec/committee-summary-assertions/v1/policy.json")
	if err != nil {
		t.Fatal(err)
	}
	var policy struct {
		Policy    string                          `json:"policy"`
		Excluded  []string                        `json:"excluded_grouping_fields"`
		Equations map[string][][2]json.RawMessage `json:"equations"`
	}
	if err := json.Unmarshal(raw, &policy); err != nil {
		t.Fatal(err)
	}
	if policy.Policy != Policy || !slices.Equal(policy.Excluded, []string{"CAND_ID"}) || len(policy.Equations) != len(equations) {
		t.Fatal("policy identity drift")
	}
	for name, terms := range equations {
		pinned := policy.Equations[name]
		if len(pinned) != len(terms) {
			t.Fatal("equation arity drift", name)
		}
		for i, term := range terms {
			var field string
			var coefficient int
			if json.Unmarshal(pinned[i][0], &field) != nil || json.Unmarshal(pinned[i][1], &coefficient) != nil || field != term.field || coefficient != term.coefficient {
				t.Fatal("equation policy drift", name, i)
			}
		}
	}
}

func TestPublishedSummaryAssertionCorpus(t *testing.T) {
	root, audit, out := os.Getenv("LT_SUMMARY_STORAGE_ROOT"), os.Getenv("LT_SUMMARY_RELEASE_AUDIT"), os.Getenv("LT_SUMMARY_ASSERTION_OUTPUT")
	if root == "" || audit == "" || out == "" {
		t.Skip("requires read-only published summaries and audit output")
	}
	var release struct {
		Periods []string `json:"periods"`
	}
	raw, err := os.ReadFile(filepath.Join(audit, "release.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &release); err != nil {
		t.Fatal(err)
	}
	for _, cycle := range release.Periods {
		t.Run(cycle, func(t *testing.T) {
			manifest := filepath.Join(audit, "summaries", cycle, "manifest.json")
			a, err := Run(context.Background(), root, manifest, cycle)
			if err != nil {
				t.Fatal(err)
			}
			b, err := Run(context.Background(), root, manifest, cycle)
			if err != nil {
				t.Fatal(err)
			}
			first, _ := json.MarshalIndent(a, "", "  ")
			second, _ := json.MarshalIndent(b, "", "  ")
			if !bytes.Equal(first, second) {
				t.Fatal("nonidentical real replay")
			}
			if err := os.WriteFile(filepath.Join(out, cycle+".json"), append(first, '\n'), 0o640); err != nil {
				t.Fatal(err)
			}
			if _, err := Run(context.Background(), root, manifest, "2018"); err == nil {
				t.Fatal("accepted wrong requested cycle")
			}
			t.Logf("cycle=%s counts=%+v replay=identical", cycle, a.Counts)
		})
	}
}
