package candidateevidence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"

	flow "github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/fundingbasis"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

const ConnectionVersion = "legal-tender.fec.candidate-connection.v1"
const ConnectionPolicy = "fec/candidate-connection-source-drilldown@1.0.0"
const maxReportBytes = 128 << 20

type Connection struct {
	SchemaVersion      string                    `json:"schema_version"`
	ConnectionID       string                    `json:"connection_id"`
	Policy             string                    `json:"policy"`
	ExecutableSHA256   string                    `json:"executable_sha256"`
	ReportID           string                    `json:"parent_report_id"`
	ReportSHA256       string                    `json:"parent_document_sha256"`
	EvidenceID         string                    `json:"parent_evidence_id"`
	CandidateID        string                    `json:"candidate_id"`
	Cycle              string                    `json:"cycle"`
	ReportVerification string                    `json:"parent_verification"`
	SourceVerification string                    `json:"source_verification"`
	Membership         []string                  `json:"report_membership"`
	Calculation        flow.CalculationReference `json:"calculation"`
	Inputs             flow.Inputs               `json:"inputs"`
	Observation        flow.Observation          `json:"observation"`
	Source             flow.SourceExample        `json:"source"`
	Names              []EntityName              `json:"parent_name_assertions"`
	TerminalEligible   bool                      `json:"terminal_attribution_eligible"`
	AllocatedAmount    *string                   `json:"allocated_amount_minor_units"`
}

// InspectConnection authenticates content identity, not authorship. The
// selected observation is independently checked against published membership
// and the exact Parquet row. It does not rerun all parent financial calculations.
func InspectConnection(ctx context.Context, root, reportPath, expectedReportID string, ordinal uint64, executable string) (Connection, error) {
	if root == "" || ordinal == 0 || !digest(executable) {
		return Connection{}, fmt.Errorf("storage, source ordinal and executable digest required")
	}
	r, rawSHA, err := openReport(ctx, reportPath, expectedReportID)
	if err != nil {
		return Connection{}, err
	}
	o, membership, err := selectConnection(r, ordinal)
	if err != nil {
		return Connection{}, err
	}
	e := r.Evidence
	ref := e.Trace.Inputs.Reconciliation
	reader, err := flow.OpenSourceReader(ctx, root, filepath.Join(root, flow.PublicationBase, "manifests", ref.CalculationSetID+".json"))
	if err != nil {
		return Connection{}, err
	}
	source, err := reader.LookupExpected(ctx, flow.SourceExpectation{Calculation: ref, Inputs: e.Trace.Inputs.Sources, Cycle: e.Cycle,
		Locator: flow.SourceLocator{Side: "schedule_a", FactSetID: e.ReceiptInput.FactSetID, Ordinal: ordinal}, Observation: o})
	if err != nil {
		return Connection{}, err
	}
	out := Connection{SchemaVersion: ConnectionVersion, Policy: ConnectionPolicy, ExecutableSHA256: executable,
		ReportID: r.ReportID, ReportSHA256: rawSHA, EvidenceID: e.ResultID, CandidateID: e.CandidateID, Cycle: e.Cycle,
		ReportVerification: "content_identity_checked_not_full_recalculation", SourceVerification: "published_membership_and_full_source_row_verified",
		Membership: membership, Calculation: ref, Inputs: e.Trace.Inputs.Sources, Observation: o, Source: source, Names: []EntityName{}}
	for _, n := range r.Names {
		if n.Kind == "committee" && (n.EntityID == o.Sender || n.EntityID == o.Recipient) {
			out.Names = append(out.Names, n)
		}
	}
	if out.ConnectionID, err = contentID(out); err != nil {
		return Connection{}, err
	}
	return out, ctx.Err()
}

func digest(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == sha256.Size && hex.EncodeToString(b) == s
}
func contentID(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}

func openReport(ctx context.Context, path, expectedID string) (Report, string, error) {
	if err := ctx.Err(); err != nil {
		return Report{}, "", err
	}
	if !digest(expectedID) {
		return Report{}, "", fmt.Errorf("expected v2 report ID required")
	}
	f, err := os.Open(path)
	if err != nil {
		return Report{}, "", err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return Report{}, "", err
	}
	if !info.Mode().IsRegular() || info.Size() > maxReportBytes {
		return Report{}, "", fmt.Errorf("report must be a regular file no larger than 128 MiB")
	}
	raw, err := io.ReadAll(io.LimitReader(f, maxReportBytes+1))
	if err != nil {
		return Report{}, "", err
	}
	if len(raw) > maxReportBytes {
		return Report{}, "", fmt.Errorf("report exceeds size limit")
	}
	var r Report
	if err := strictjson.Decode(raw, &r); err != nil {
		return Report{}, "", err
	}
	if err := verifyReport(r, expectedID); err != nil {
		return Report{}, "", err
	}
	h := sha256.Sum256(raw)
	return r, hex.EncodeToString(h[:]), ctx.Err()
}

func verifyReport(r Report, expectedID string) error {
	if r.SchemaVersion != Version || r.Policy != Policy || r.PathPolicy != PathPolicy || r.ReportID != expectedID || !digest(expectedID) {
		return fmt.Errorf("report version, policy or expected identity mismatch")
	}
	r.ReportID = ""
	if id, err := contentID(r); err != nil || id != expectedID {
		return fmt.Errorf("report content identity mismatch")
	}
	e := r.Evidence
	if e.SchemaVersion != fundingbasis.CandidateEvidenceVersion || e.Policy != fundingbasis.CandidateEvidencePolicy || !digest(e.ResultID) || !digest(e.ExecutableSHA256) || e.CandidateID != e.Trace.Candidate || e.Cycle != e.Trace.Cycle || e.TerminalEligible || e.TerminalPolicy != nil || e.AllocationPolicy != nil || e.TerminalAmount != nil {
		return fmt.Errorf("parent evidence contract mismatch")
	}
	identity := e.ResultID
	e.ResultID = ""
	if id, err := contentID(e); err != nil || id != identity {
		return fmt.Errorf("parent evidence content identity mismatch")
	}
	ref, sources := e.Trace.Inputs.Reconciliation, e.Trace.Inputs.Sources
	if !digest(ref.CalculationSetID) || !digest(ref.ManifestSHA256) || !digest(e.ReceiptInput.FactSetID) || e.ReceiptInput.FactSetID != sources.A.FactSetID || e.ReceiptInput.ManifestSHA256 != sources.A.ManifestSHA256 || e.ReceiptInput.Facts != sources.A.Facts {
		return fmt.Errorf("parent receipt and connection sources disagree")
	}
	return nil
}

func selectConnection(r Report, ordinal uint64) (flow.Observation, []string, error) {
	var selected *flow.Observation
	membership := []string{}
	groups := [][]flow.Observation{r.Evidence.Witnesses, {}}
	for _, c := range r.Evidence.Trace.CandidateObservations {
		groups[1] = append(groups[1], c.Observation)
	}
	for i, rows := range groups {
		seen := map[uint64]bool{}
		for _, o := range rows {
			if seen[o.Ordinal] {
				return flow.Observation{}, nil, fmt.Errorf("duplicate report connection")
			}
			seen[o.Ordinal] = true
			if o.Ordinal != ordinal {
				continue
			}
			if selected != nil && !reflect.DeepEqual(*selected, o) {
				return flow.Observation{}, nil, fmt.Errorf("conflicting report connection")
			}
			copy := o
			selected = &copy
			membership = append(membership, []string{"connection_witness", "candidate_boundary_observation"}[i])
		}
	}
	if selected == nil {
		return flow.Observation{}, nil, fmt.Errorf("ordinal is not a concrete connection in this report")
	}
	return *selected, membership, nil
}
