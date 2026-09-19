// Package receiptconduits qualifies reported memo associations, not payments.
package receiptconduits

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"

	policy "github.com/vedantadhobley/legal-tender/internal/calculation/fec/earmarkassociation"
	participants "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
	refs "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptreferences"
	xsort "github.com/vedantadhobley/legal-tender/internal/storage/externalsort"
)

const Version = "legal-tender.fec.receipt-conduit-associations.v1"
const Policy = "fec/cycle-receipt-conduit-evidence@1.0.0"
const GroupVersion = "legal-tender.fec.receipt-conduit-associations.v2"
const GroupPublicationPolicy = "fec/cycle-receipt-conduit-evidence@2.0.0"
const Unassessed = "no_reference_incident_transaction_uniqueness_unassessed"

// Wire dictionary, not source codes or entity-specific matching rules.
var states = []string{Unassessed, "ambiguous_or_incomplete_reference_evidence", "multiple_related_records_unresolved", "shared_related_record_unresolved", "original_contributor_role_unresolved", "related_role_unresolved", "related_committee_id_unresolved", "conflicting_committee_evidence", "reported_earmark_memo_association", policy.SharedAssociation}
var amounts = []string{"not_assessed", "unknown_reported_amount", "different_reported_amount", "same_reported_amount"}

type Decision struct {
	Ordinal          uint64  `json:"source_row_ordinal"`
	Related          uint64  `json:"sole_related_source_row_ordinal"`
	State            string  `json:"state"`
	AmountComparison string  `json:"amount_comparison"`
	ConduitID        *string `json:"reported_conduit_committee_id"`
}

func key(n uint64) string { return string(binary.BigEndian.AppendUint64(nil, n)) }
func (d Decision) record() (xsort.Record, error) {
	s, a := slices.Index(states, d.State), slices.Index(amounts, d.AmountComparison)
	if s < 0 || a < 0 || IsAssociation(d.State) != (d.ConduitID != nil) {
		return xsort.Record{}, fmt.Errorf("invalid association decision")
	}
	b := binary.BigEndian.AppendUint64(nil, d.Related)
	b = append(b, byte(s), byte(a))
	if d.ConduitID != nil {
		b = append(b, (*d.ConduitID)...)
	}
	r := xsort.Record{Key: key(d.Ordinal), Ordinal: d.Ordinal, Data: b}
	if _, err := DecodeDecision(r, ^uint64(0)); err != nil {
		return xsort.Record{}, err
	}
	return r, nil
}
func DecodeDecision(r xsort.Record, rows uint64) (Decision, error) {
	if r.Ordinal == 0 || r.Ordinal > rows || r.Key != key(r.Ordinal) || r.Tag != 0 || len(r.Data) < 10 || int(r.Data[8]) >= len(states) || int(r.Data[9]) >= len(amounts) {
		return Decision{}, fmt.Errorf("invalid conduit record")
	}
	d := Decision{Ordinal: r.Ordinal, Related: binary.BigEndian.Uint64(r.Data), State: states[r.Data[8]], AmountComparison: amounts[r.Data[9]]}
	if d.Related > rows || d.Related == d.Ordinal || (d.Related == 0 && d.AmountComparison != "not_assessed") {
		return d, fmt.Errorf("invalid related occurrence")
	}
	if ((r.Data[8] == 0 || r.Data[8] == 2) && d.Related != 0) || (r.Data[8] >= 3 && (d.Related == 0 || d.AmountComparison == "not_assessed")) {
		return d, fmt.Errorf("decision state/peer mismatch")
	}
	if IsAssociation(d.State) {
		if len(r.Data) != 19 || r.Data[10] != 'C' || d.Related == 0 || d.AmountComparison == "not_assessed" {
			return d, fmt.Errorf("invalid positive association")
		}
		for _, c := range r.Data[11:] {
			if c < '0' || c > '9' {
				return d, fmt.Errorf("invalid reported conduit ID")
			}
		}
		id := string(r.Data[10:])
		d.ConduitID = &id
	} else if len(r.Data) != 10 {
		return d, fmt.Errorf("unexpected conduit ID")
	}
	return d, nil
}

func IsAssociation(state string) bool {
	return state == "reported_earmark_memo_association" || state == policy.SharedAssociation
}
func evidence(r participants.Row) policy.Evidence {
	return policy.Evidence{Memo: r.Memo, ReceiptType: r.ReceiptType, Entity: r.Entity, Contributor: r.Contributor, CleanContributor: r.CleanContributor, ConduitID: r.ConduitID, Amount: r.Amount}
}
func topology(e refs.Endpoint) policy.Topology {
	return policy.Topology{Unsafe: e.UnsafeReasons != 0, Peers: e.Peers}
}
func decide(r participants.Row, e refs.Endpoint, related *policy.Related) (Decision, error) {
	d := Decision{Ordinal: uint64(r.Ordinal), Related: e.OnlyPeer, State: Unassessed, AmountComparison: "not_assessed"}
	if e.Peers == 0 && e.UnsafeReasons == 0 {
		return d, nil
	}
	v, err := policy.Decide(evidence(r), topology(e), related)
	if err != nil {
		return d, err
	}
	if v.AdditionalAmount != "0" || v.TerminalEligible {
		return d, fmt.Errorf("association monetary boundary changed")
	}
	d.State, d.AmountComparison, d.ConduitID = v.State, v.AmountComparison, v.ConduitID
	return d, nil
}

type Options struct {
	Participants, ParticipantID, Topology, TopologyID, OutputDirectory, BuildSHA256 string
	Workers, RunRows, FanIn                                                         int
	MaxWorkspaceBytes                                                               uint64
	Progress                                                                        func(string)
	profile                                                                         *sharedProfileCollector
	GroupBaseline, GroupBaselineID                                                  string
	groups                                                                          *groupWork
}
type Result struct {
	SchemaVersion        string            `json:"schema_version"`
	State                string            `json:"state"`
	CalculationID        string            `json:"calculation_id"`
	BuildSHA256          string            `json:"executable_sha256"`
	Policy               string            `json:"policy"`
	AssociationPolicy    string            `json:"association_policy"`
	ParticipantID        string            `json:"participant_calculation_id"`
	ParticipantSHA256    string            `json:"participant_manifest_sha256"`
	TopologyID           string            `json:"topology_calculation_id"`
	TopologySHA256       string            `json:"topology_manifest_sha256"`
	FactSetID            string            `json:"fact_set_id"`
	FactManifestSHA256   string            `json:"fact_manifest_sha256"`
	Cycle                string            `json:"cycle"`
	SourceRows           uint64            `json:"source_rows"`
	EligibleRoleRows     uint64            `json:"non_memo_earmark_rows"`
	OtherRows            uint64            `json:"other_source_rows"`
	OtherDisposition     string            `json:"other_source_disposition"`
	Decisions            xsort.File        `json:"decisions"`
	States               map[string]uint64 `json:"states"`
	Amounts              map[string]uint64 `json:"amount_comparisons"`
	Qualified            uint64            `json:"qualified_associations"`
	AdditionalAmount     string            `json:"additional_amount_minor_units"`
	FinancialEligibility bool              `json:"financial_eligibility"`
	IdentityResolved     bool              `json:"conduit_identity_resolved"`
	Workers              int               `json:"workers"`
	RunRows              int               `json:"sort_run_rows"`
	FanIn                int               `json:"merge_fan_in"`
	ElapsedMS            int64             `json:"elapsed_ms"`
	PeakRSSBytes         uint64            `json:"peak_rss_bytes"`
	PeakWorkspaceBytes   uint64            `json:"peak_workspace_bytes"`
	RetainedBytes        uint64            `json:"retained_bytes"`
	Groups               *GroupPublication `json:"shared_group_publication,omitempty"`
}

func digest(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 32 && hex.EncodeToString(b) == s
}
func identity(r Result) string {
	r.CalculationID = ""
	r.Workers = 0
	r.RunRows = 0
	r.FanIn = 0
	r.ElapsedMS = 0
	r.PeakRSSBytes = 0
	r.PeakWorkspaceBytes = 0
	r.RetainedBytes = 0
	r.Decisions.Name = ""
	r.Decisions.Bytes = 0
	r.Decisions.SHA256 = ""
	if r.Groups != nil {
		g := *r.Groups
		g.Decisions.Name, g.Decisions.SHA256, g.Decisions.Bytes = "", "", 0
		r.Groups = &g
	}
	b, _ := json.Marshal(r)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
