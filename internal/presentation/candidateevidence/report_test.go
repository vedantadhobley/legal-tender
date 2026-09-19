package candidateevidence

import (
	"context"
	"errors"
	"io"
	"reflect"
	"slices"
	"strings"
	"testing"

	upstream "github.com/vedantadhobley/legal-tender/internal/calculation/fec/candidateupstream"
	flow "github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/fundingbasis"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

func ptr[T any](v T) *T { return &v }

func pathFixture() fundingbasis.CandidateEvidence {
	return fundingbasis.CandidateEvidence{
		CandidateID: "H0AA00001", Cycle: "2024",
		Trace: upstream.Result{Nodes: []upstream.Node{
			{CommitteeID: "C00000000", Authorized: true},
			{CommitteeID: "C00000001", Hops: 1, WitnessOrdinal: ptr(uint64(1))},
			{CommitteeID: "C00000002", Hops: 2, WitnessOrdinal: ptr(uint64(2))},
			{CommitteeID: "C00000003", Hops: 3, WitnessOrdinal: ptr(uint64(3))},
			{CommitteeID: "C00000004", Hops: 4, WitnessOrdinal: ptr(uint64(4))},
			{CommitteeID: "C00000005", Hops: 1, WitnessOrdinal: ptr(uint64(5))},
		}},
		Witnesses: []flow.Observation{
			{Ordinal: 1, Sender: "C00000001", Recipient: "C00000000", Date: ptr(int32(19000)), Amount: 101},
			{Ordinal: 2, Sender: "C00000002", Recipient: "C00000001", Date: ptr(int32(19001)), Amount: -1},
			{Ordinal: 3, Sender: "C00000003", Recipient: "C00000002", Date: nil, Amount: 0},
			{Ordinal: 4, Sender: "C00000004", Recipient: "C00000003", Date: ptr(int32(19002)), Amount: 999},
			{Ordinal: 5, Sender: "C00000005", Recipient: "C00000000", Date: ptr(int32(19002)), Amount: 99999},
		},
	}
}

func TestPathsAreExactDeterministicCompleteWitnessChains(t *testing.T) {
	e := pathFixture()
	p, err := pathExamples(context.Background(), e)
	if err != nil || len(p) != 3 {
		t.Fatal(p, err)
	}
	for i, path := range p {
		if path.From != e.Trace.Nodes[i+1].CommitteeID || path.To != "C00000000" || len(path.Hops) != i+1 || path.AllocatedAmount != nil {
			t.Fatal(path)
		}
		for j, hop := range path.Hops {
			if !reflect.DeepEqual(hop, e.Witnesses[i-j]) {
				t.Fatal("witness changed")
			}
		}
	}
	if p[0].DateReversal || !p[1].DateReversal || !p[1].Nonpositive || !p[2].MissingDates || !p[2].DateReversal {
		t.Fatal("lost date/sign gaps", p)
	}
	slices.Reverse(e.Trace.Nodes)
	slices.Reverse(e.Witnesses)
	replay, err := pathExamples(context.Background(), e)
	if err != nil || !reflect.DeepEqual(p, replay) {
		t.Fatal("input ordering changed examples", err)
	}
	e = pathFixture()
	e.Witnesses[1].Date = e.Witnesses[0].Date
	p, err = pathExamples(context.Background(), e)
	if err != nil || !p[1].SameDay || p[1].DateReversal {
		t.Fatal("same-day order lost", err)
	}
}

func TestPathFailuresAndCancellation(t *testing.T) {
	for _, edit := range []func(*fundingbasis.CandidateEvidence){
		func(e *fundingbasis.CandidateEvidence) { e.Witnesses = append(e.Witnesses, e.Witnesses[0]) },
		func(e *fundingbasis.CandidateEvidence) { e.Trace.Nodes[1].WitnessOrdinal = nil },
		func(e *fundingbasis.CandidateEvidence) { e.Trace.Nodes[1].WitnessOrdinal = ptr(uint64(900)) },
		func(e *fundingbasis.CandidateEvidence) { e.Witnesses[0].Sender = "C99999999" },
		func(e *fundingbasis.CandidateEvidence) { e.Witnesses[0].Recipient = "C00000002" },
	} {
		e := pathFixture()
		edit(&e)
		if _, err := pathExamples(context.Background(), e); err == nil {
			t.Fatal("broken witness accepted")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := pathExamples(ctx, pathFixture()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if p, err := pathExamples(context.Background(), fundingbasis.CandidateEvidence{}); err != nil || p == nil || len(p) != 0 {
		t.Fatal(p, err)
	}
}

func TestRawNamesAndMissingConflictStates(t *testing.T) {
	for _, tc := range []struct {
		names []NameAssertion
		state string
	}{
		{nil, "no_reference_record"},
		{[]NameAssertion{{RawName: ""}}, "source_name_blank"},
		{[]NameAssertion{{RawName: "Raw Name "}, {RawName: "Raw Name "}}, "reported_name"},
		{[]NameAssertion{{RawName: "Raw Name"}, {RawName: "RAW NAME"}}, "conflicting_reported_names"},
		{[]NameAssertion{{RawName: ""}, {RawName: "Named"}}, "conflicting_reported_names"},
	} {
		n := nameFor(map[string][]NameAssertion{"id": tc.names}, "id", "candidate")
		if n.State != tc.state || n.Assertions == nil || len(n.Assertions) != len(tc.names) {
			t.Fatal(n)
		}
		if len(tc.names) > 0 && n.Assertions[0] != tc.names[0] {
			t.Fatal("raw name changed")
		}
	}
	f := occ.ClassicFact{Dataset: "candidate-master", SourceFields: map[string]string{"CAND_ID": "H0AA00001", "CAND_NAME": "  RAW <Name> "}, TypedFields: occ.CandidateTypedFields{CandidateID: "H0AA00001", Name: "  RAW <Name> ", SourceCycle: 2024}}
	if id, raw, err := factName(f, "2024"); err != nil || id != "H0AA00001" || raw != "  RAW <Name> " {
		t.Fatal(id, raw, err)
	}
	if _, _, err := factName(f, "2022"); err == nil {
		t.Fatal("wrong cycle accepted")
	}
	f.SourceFields["CAND_NAME"] = "other"
	if _, _, err := factName(f, "2024"); err == nil {
		t.Fatal("name disagreement accepted")
	}
	f = occ.ClassicFact{Dataset: "committee-master", SourceFields: map[string]string{"CMTE_ID": "C00000001", "CMTE_NM": ""}, TypedFields: occ.CommitteeTypedFields{CommitteeID: "C00000001", SourceCycle: 2024}}
	if _, raw, err := factName(f, "2024"); err != nil || raw != "" {
		t.Fatal(raw, err)
	}
	delete(f.SourceFields, "CMTE_NM")
	if _, _, err := factName(f, "2024"); err == nil {
		t.Fatal("absent name field accepted as blank")
	}
	if _, _, err := loadNames(context.Background(), t.TempDir(), "/missing/manifest", "2024", "candidate-master"); err == nil {
		t.Fatal("missing publication accepted")
	}
}

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }

func TestMarkdownEscapesLabelsKeepsMemoSeparateAndMoneyExact(t *testing.T) {
	for _, code := range []string{"contribution", "in_kind", "affiliated_transfer", "refund_or_repayment", "earmarked", "unresolved", "memo_subtotal", "INDV_CONTB"} {
		if strings.HasPrefix(label(code), "Untranslated") {
			t.Fatal("missing source label", code)
		}
	}
	for input, want := range map[string]string{"0": "$0.00", "1": "$0.01", "-1": "-$0.01", "100001": "$1,000.01", "123456789012345678901": "$1,234,567,890,123,456,789.01"} {
		if got := money(input); got != want {
			t.Fatal(got, want)
		}
	}
	e := pathFixture()
	e.Committees = []fundingbasis.EvidenceCommittee{{CommitteeID: "C00000000", Authorization: "authorized", Receipts: fundingbasis.ReceiptPopulation{Components: []fundingbasis.ReceiptRoleCoverage{
		{Component: "itemized_individual_only", Role: "unresolved", Measures: fundingbasis.Measures{Rows: 1, Signed: 100}},
		{Component: "memo_subtotal", Role: "earmarked", Measures: fundingbasis.Measures{Rows: 2, Signed: 999}},
	}}}}
	raw := "<script>|[Click](https://example.invalid)\n# Title\u202e"
	r := Report{Evidence: e, Names: []EntityName{{EntityID: e.CandidateID, State: "reported_name", Assertions: []NameAssertion{{RawName: raw}}}}}
	r.Paths, _ = pathExamples(context.Background(), e)
	text := Markdown(r)
	for _, bad := range []string{"<script>", "[Click]", "\n# Title", "\u202e"} {
		if strings.Contains(text, bad) {
			t.Fatal("unsafe name", bad)
		}
	}
	if r.Names[0].Assertions[0].RawName != raw {
		t.Fatal("source name changed")
	}
	for _, want := range []string{"Memo evidence — separate, not additional funding", "Not classified by the committee-flow role map", "ordinary individual receipts", "earlier reported date", "zero or negative observation", "unknown, not zero"} {
		if !strings.Contains(text, want) {
			t.Fatal("missing explanation", want)
		}
	}
	if strings.Index(text, "$9.99") < strings.Index(text, "Memo evidence — separate") {
		t.Fatal("memo mixed into receipt table")
	}
	if err := WriteMarkdown(shortWriter{}, r); !errors.Is(err, io.ErrShortWrite) {
		t.Fatal(err)
	}
}
