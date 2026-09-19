package fundingbasis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/candidateupstream"
	flow "github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/receipts"
)

func evidenceTrace(r *Reader) candidateupstream.Result {
	zero := candidateupstream.Amount{Signed: "0", Positive: "0", Negative: "0"}
	return candidateupstream.Result{SchemaVersion: candidateupstream.Version, CalculationID: strings.Repeat("b", 64),
		Policy: candidateupstream.Policy, Cycle: r.result.Cycle, Candidate: "H0AA00001",
		Inputs:           candidateupstream.Inputs{Sources: flow.Inputs{A: flow.FactReference{FactSetID: r.result.Input.FactSetID, ManifestSHA256: r.result.Input.ManifestSHA256, Facts: r.result.Input.Facts}}},
		Relationships:    []receipts.CommitteeRelationship{{CommitteeID: "C00000001", State: "authorized"}, {CommitteeID: "C00000003", State: "unresolved"}},
		Nodes:            []candidateupstream.Node{{CommitteeID: "C00000001", Authorized: true}, {CommitteeID: "C00000002", Hops: 1}},
		CyclicComponents: []candidateupstream.Component{{ID: strings.Repeat("c", 64), Committees: []string{"C00000002"}}},
		Accounting:       candidateupstream.Accounting{CandidateLinked: zero, External: zero, Internal: zero, UnresolvedScope: zero, TerminalAllocated: "0", UnresolvedAttribution: zero}}
}

func TestCandidateEvidenceJoinsPreservedPopulationsAndLocalSummaryBlockers(t *testing.T) {
	r, summary := summaryReviewFixture(t)
	r.result.Policy = Policy
	summary.Input.SourceReleaseID = "fec-" + strings.Repeat("d", 64)
	trace := evidenceTrace(r)
	out, err := r.candidateEvidence(context.Background(), trace, []flow.Observation{}, &summary, strings.Repeat("e", 64))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out.Trace, trace) || out.TerminalAmount != nil || out.TerminalPolicy != nil || out.AllocationPolicy != nil || out.TerminalEligible {
		t.Fatal("upstream or unresolved policies changed")
	}
	if out.Overview != (EvidenceOverview{1, 1, 2, 2, 1, 1}) || len(out.Committees) != 3 {
		t.Fatal(out.Overview)
	}
	first := out.Committees[0]
	if first.Receipts.Total.Rows != 4 || first.Receipts.Total.Unknown != 1 || first.Receipts.Total.Signed != 200 || first.Receipts.Overlap.Signed != -200 {
		t.Fatal("source populations changed", first)
	}
	if first.Summary == nil || first.Summary.SourceAlignment != "different_source_release" || first.Summary.ComparisonReady || first.Summary.FundingEligible {
		t.Fatal("summary mismatch lost or promoted")
	}
	if !reflect.DeepEqual(first.Summary.Receipts, first.Receipts) {
		t.Fatal("summary and inventory disagree")
	}
	if out.Committees[1].Summary != nil || !out.Committees[1].Reached || out.Committees[2].Reached || out.Committees[2].Summary == nil {
		t.Fatal("summary or reachability scope wrong")
	}
	replay, err := r.candidateEvidence(context.Background(), trace, []flow.Observation{}, &summary, strings.Repeat("e", 64))
	if err != nil || !reflect.DeepEqual(out, replay) {
		t.Fatal("replay differs", err)
	}
	id := out.ResultID
	out.ResultID = ""
	b, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(b)
	if hex.EncodeToString(h[:]) != id {
		t.Fatal("result identity does not bind result")
	}
	out.ResultID = id
	other, err := r.candidateEvidence(context.Background(), trace, []flow.Observation{}, nil, strings.Repeat("e", 64))
	if err != nil || other.ResultID == id || other.SummaryState != "not_requested" {
		t.Fatal("optional source identity not bound")
	}
	other, err = r.candidateEvidence(context.Background(), trace, []flow.Observation{}, &summary, strings.Repeat("f", 64))
	if err != nil || other.ResultID == id {
		t.Fatal("build identity not bound")
	}
	text := CandidateEvidenceMarkdown(out)
	for _, expected := range []string{"-$2.00", "unknown, not zero", "different_source_release", "H0AA00001", id, "No receipt rows", "memo_subtotal"} {
		if !strings.Contains(text, expected) {
			t.Fatal("report omitted", expected)
		}
	}
	if path := os.Getenv("LT_CANDIDATE_EVIDENCE_FIXTURE"); path != "" {
		b, err := json.Marshal(out)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(b, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCandidateEvidenceRejectsForeignSourceAndCancellation(t *testing.T) {
	r, _ := summaryReviewFixture(t)
	trace := evidenceTrace(r)
	trace.Inputs.Sources.A.ManifestSHA256 = strings.Repeat("f", 64)
	if _, err := r.candidateEvidence(context.Background(), trace, nil, nil, ""); err == nil {
		t.Fatal("foreign receipt manifest accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.candidateEvidence(ctx, evidenceTrace(r), nil, nil, ""); err == nil {
		t.Fatal("cancellation ignored")
	}
	if _, err := BuildCandidateEvidence(context.Background(), CandidateEvidenceOptions{ExecutableSHA256: "unversioned"}); err == nil {
		t.Fatal("unversioned executable accepted")
	}
}

func TestEvidenceMoneyFormattingAndReportFailure(t *testing.T) {
	for in, want := range map[string]string{"0": "$0.00", "1": "$0.01", "-101": "-$1.01", "92233720368547758080": "$922337203685477580.80"} {
		if got := decimalCents(in); got != want {
			t.Fatal(got, want)
		}
	}
	if err := WriteCandidateEvidenceReport(shortReportWriter{}, CandidateEvidence{}); err != io.ErrShortWrite {
		t.Fatal(err)
	}
}

type shortReportWriter struct{}

func (shortReportWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }
