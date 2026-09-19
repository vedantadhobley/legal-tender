package candidateevidence

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	upstream "github.com/vedantadhobley/legal-tender/internal/calculation/fec/candidateupstream"
	flow "github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/fundingbasis"
)

func sealReport(t *testing.T, r Report) Report {
	t.Helper()
	var err error
	r.Evidence.ResultID = ""
	r.Evidence.ResultID, err = contentID(r.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	r.ReportID = ""
	r.ReportID, err = contentID(r)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func savedReport(t *testing.T) Report {
	e := pathFixture()
	e.SchemaVersion, e.Policy, e.ExecutableSHA256 = fundingbasis.CandidateEvidenceVersion, fundingbasis.CandidateEvidencePolicy, strings.Repeat("a", 64)
	e.Trace.Candidate, e.Trace.Cycle = e.CandidateID, e.Cycle
	e.ReceiptInput = fundingbasis.Input{FactSetID: strings.Repeat("b", 64), ManifestSHA256: strings.Repeat("c", 64), Facts: 5}
	e.Trace.Inputs.Sources.A = flow.FactReference{FactSetID: e.ReceiptInput.FactSetID, ManifestSHA256: e.ReceiptInput.ManifestSHA256, Facts: 5}
	e.Trace.Inputs.Reconciliation = flow.CalculationReference{CalculationSetID: strings.Repeat("d", 64), ManifestSHA256: strings.Repeat("e", 64)}
	e.Trace.CandidateObservations = []upstream.CandidateObservation{{Observation: e.Witnesses[0]}}
	return sealReport(t, Report{SchemaVersion: Version, Policy: Policy, PathPolicy: PathPolicy, Evidence: e})
}

func TestConnectionSelectionIsExactAndScopedToConcreteReportRows(t *testing.T) {
	r := savedReport(t)
	o, membership, err := selectConnection(r, 1)
	if err != nil || !reflect.DeepEqual(o, r.Evidence.Witnesses[0]) || !reflect.DeepEqual(membership, []string{"connection_witness", "candidate_boundary_observation"}) {
		t.Fatal(o, membership, err)
	}
	if _, _, err := selectConnection(r, 99); err == nil {
		t.Fatal("foreign connection selected")
	}
	r.Evidence.Trace.CandidateObservations[0].Observation.Amount++
	if _, _, err := selectConnection(r, 1); err == nil {
		t.Fatal("conflicting observation accepted")
	}
	r = savedReport(t)
	r.Evidence.Witnesses = append(r.Evidence.Witnesses, r.Evidence.Witnesses[0])
	if _, _, err := selectConnection(r, 1); err == nil {
		t.Fatal("duplicate witness accepted")
	}
	// Candidate-boundary observations need not be the shortest-hop witness.
	r = savedReport(t)
	r.Evidence.Trace.CandidateObservations[0].Observation.Ordinal = 99
	if _, m, err := selectConnection(r, 99); err != nil || !reflect.DeepEqual(m, []string{"candidate_boundary_observation"}) {
		t.Fatal(m, err)
	}
}

func TestParentReportRequiresExpectedIdentityAndPreservedContract(t *testing.T) {
	r := savedReport(t)
	if err := verifyReport(r, r.ReportID); err != nil {
		t.Fatal(err)
	}
	if err := verifyReport(r, strings.Repeat("f", 64)); err == nil {
		t.Fatal("wrong expected identity accepted")
	}
	for _, edit := range []func(*Report){
		func(r *Report) { r.Evidence.Witnesses[0].Amount++ },
		func(r *Report) { r.SchemaVersion = "future" },
		func(r *Report) { r.Evidence.TerminalEligible = true },
		func(r *Report) { r.Evidence.AllocationPolicy = ptr("proportional") },
		func(r *Report) { r.Evidence.Trace.Candidate = "H0ZZ00001" },
		func(r *Report) { r.Evidence.ReceiptInput.ManifestSHA256 = strings.Repeat("f", 64) },
	} {
		bad := savedReport(t)
		edit(&bad)
		if err := verifyReport(bad, r.ReportID); err == nil {
			t.Fatal("changed report accepted under original identity")
		}
	}
	bad := savedReport(t)
	bad.Evidence.TerminalEligible = true
	bad = sealReport(t, bad)
	if err := verifyReport(bad, bad.ReportID); err == nil {
		t.Fatal("rehashed financial promotion accepted")
	}
	bad = savedReport(t)
	bad.Evidence.ResultID = strings.Repeat("f", 64)
	bad.ReportID = ""
	bad.ReportID, _ = contentID(bad)
	if err := verifyReport(bad, bad.ReportID); err == nil {
		t.Fatal("wrong nested identity accepted")
	}
}

func TestReportInputIsClosedBoundedAndCancellable(t *testing.T) {
	r := savedReport(t)
	raw, _ := json.Marshal(r)
	path := filepath.Join(t.TempDir(), "report.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	got, sha, err := openReport(context.Background(), path, r.ReportID)
	if err != nil || !reflect.DeepEqual(got, r) || !digest(sha) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := openReport(ctx, path, r.ReportID); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for _, bad := range [][]byte{
		append(append([]byte{}, raw...), []byte("{}")...),
		bytes.Replace(raw, []byte(`"schema_version":`), []byte(`"schema_version":"duplicate","schema_version":`), 1),
		append(append([]byte{}, raw[:len(raw)-1]...), []byte(`,"unknown":1}`)...),
	} {
		if err := os.WriteFile(path, bad, 0600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := openReport(context.Background(), path, r.ReportID); err == nil {
			t.Fatal("lossy/unknown JSON accepted")
		}
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(maxReportBytes + 1); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if _, _, err := openReport(context.Background(), path, r.ReportID); err == nil {
		t.Fatal("oversized report accepted")
	}
}

func TestConnectionMarkdownPreservesScalarStatesAndEscapesSource(t *testing.T) {
	c := Connection{Source: flow.SourceExample{Fields: map[string]any{"null": nil, "empty": "", "zero": "0", "flag": false, "memo": "<script>|[link](x)\nnext"}}}
	text := ConnectionMarkdown(c)
	for _, want := range []string{"null", "&#34;&#34;", "&#34;0&#34;", "false", "full source row", "not recalculate the whole parent", "unknown, not zero"} {
		if !strings.Contains(text, want) {
			t.Fatal("missing representation", want)
		}
	}
	if strings.Contains(text, "<script>") || strings.Contains(text, "[link]") {
		t.Fatal("unescaped source value")
	}
	if safe("\"'") != "&#34;&#39;" {
		t.Fatal("Markdown escaping split HTML entities")
	}
	if err := WriteConnectionMarkdown(shortWriter{}, c); !errors.Is(err, io.ErrShortWrite) {
		t.Fatal(err)
	}
}
