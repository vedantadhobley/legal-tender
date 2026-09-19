package fundingbasis

import (
	"context"
	"fmt"
	"strconv"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
)

const EvidencePolicy = "fec/reported-receipt-source-evidence@1.0.0"

// EvidenceDecision separates a reported money source from publisher aggregate
// membership and from a conduit. No state proves a resolved donor identity.
type EvidenceDecision struct {
	Ordinal                    uint64  `json:"source_row_ordinal"`
	SourceRoute                string  `json:"source_route"`
	ReportedSourceCommitteeID  *string `json:"reported_source_committee_id"`
	PublisherIndividualOverlap bool    `json:"publisher_individual_overlap"`
	IndividualEntityConflict   bool    `json:"individual_entity_type_conflict"`
	EarmarkState               string  `json:"earmark_state"`
	ConduitState               string  `json:"conduit_state"`
	ReportedConduitID          *string `json:"reported_conduit_committee_id"`
	ReportReferenceState       string  `json:"report_reference_state"`
	MemoTextPresent            bool    `json:"memo_text_present"`
	ConduitNamePresent         bool    `json:"conduit_name_present"`
	ConduitAmount              string  `json:"additional_conduit_amount_minor_units"`
	TerminalEligible           bool    `json:"terminal_attribution_eligible"`
}

type EvidencePage struct {
	SchemaVersion string             `json:"schema_version"`
	Policy        string             `json:"policy"`
	Page          Page               `json:"source_page"`
	Decisions     []EvidenceDecision `json:"decisions"`
}

type evidenceInput struct {
	row                                                                       receiptRow
	entity, conduitName, memo, file, transaction, backReference, backSchedule *string
}

// Missing fields and wrong physical types are failures, not null observations.
func decodeEvidence(r Receipt) (evidenceInput, error) {
	var out evidenceInput
	var firstError error
	str := func(name string) *string {
		v, exists := r.Fields[name]
		if !exists {
			firstError = fmt.Errorf("missing source field %s", name)
			return nil
		}
		if v == nil {
			return nil
		}
		s, ok := v.(string)
		if !ok {
			firstError = fmt.Errorf("invalid string source field %s", name)
			return nil
		}
		return &s
	}
	boolean := func(name string) *bool {
		v, exists := r.Fields[name]
		if !exists {
			firstError = fmt.Errorf("missing source field %s", name)
			return nil
		}
		if v == nil {
			return nil
		}
		b, ok := v.(bool)
		if !ok {
			firstError = fmt.Errorf("invalid boolean source field %s", name)
			return nil
		}
		return &b
	}
	integer := func(name string) *int64 {
		s := str(name)
		if s == nil {
			return nil
		}
		n, err := strconv.ParseInt(*s, 10, 64)
		if err != nil {
			firstError = fmt.Errorf("invalid integer source field %s", name)
			return nil
		}
		return &n
	}
	text := func(name string) string {
		s := str(name)
		if s == nil {
			firstError = fmt.Errorf("required source field %s is null", name)
			return ""
		}
		return *s
	}
	row := receiptRow{Recipient: str("cmte_id"), Contributor: str("contbr_id"), CleanContributor: str("clean_contbr_id"), Individual: boolean("is_individual"),
		ReceiptType: str("receipt_tp"), Amount: integer("lt_receipt_amount_minor_units"), AmountState: text("lt_receipt_amount_state"),
		Normalization: text("lt_normalization_state"), ConduitID: str("conduit_cmte_id")}
	ordinal, cycle, memo := integer("lt_source_row_ordinal"), integer("lt_two_year_transaction_period"), boolean("lt_memoed_subtotal")
	if ordinal == nil || cycle == nil || memo == nil {
		return out, fmt.Errorf("required typed source metadata is null")
	}
	row.Ordinal, row.Cycle, row.Memo = *ordinal, *cycle, *memo
	out = evidenceInput{row: row, entity: str("entity_tp"), conduitName: str("conduit_cmte_nm"), memo: str("memo_text"), file: str("file_num"),
		transaction: str("tran_id"), backReference: str("back_ref_tran_id"), backSchedule: str("back_ref_sched_nm")}
	if firstError != nil {
		return out, firstError
	}
	if err := validRow(row, r.Ordinal, row.Cycle); err != nil {
		return out, err
	}
	if classify(row) != r.Key {
		return out, fmt.Errorf("source fields disagree with inventory classification")
	}
	return out, nil
}

func present(p *string) bool { return p != nil && *p != "" }

func assessEvidence(in evidenceInput) EvidenceDecision {
	return assessClassifiedEvidence(in, classify(in.row))
}

func assessClassifiedEvidence(in evidenceInput, k Key) EvidenceDecision {
	r := in.row
	out := EvidenceDecision{Ordinal: uint64(r.Ordinal), SourceRoute: "other_or_unresolved_receipt", EarmarkState: "no_reviewed_earmark_code",
		ConduitState: "no_structured_conduit_assertion", ReportReferenceState: "no_report_reference",
		MemoTextPresent: present(in.memo), ConduitNamePresent: present(in.conduitName), ConduitAmount: "0"}
	switch k.Component {
	case "overlapping_individual_and_committee", "committee_flow_only":
		// Reuse reported committee evidence, without declaring registration,
		// entity identity, cash, or terminal provenance resolved.
		out.SourceRoute = "reported_committee_observation"
		out.ReportedSourceCommitteeID = r.Contributor
		out.PublisherIndividualOverlap = k.IndividualDecision == "included"
		out.IndividualEntityConflict = in.entity != nil && (*in.entity == "IND" || *in.entity == "CAN")
	case "itemized_individual_only":
		out.SourceRoute = "publisher_individual_identity_unresolved"
		if committeeflows.ValidCommitteeID(r.Contributor) || committeeflows.ValidCommitteeID(r.CleanContributor) {
			out.SourceRoute = "conflicting_contributor_evidence_unresolved"
		}
	case "memo_subtotal":
		out.SourceRoute = "memo_evidence_only"
	case "unknown_amount":
		out.SourceRoute = "unknown_amount"
	case "unresolved_recipient":
		out.SourceRoute = "unresolved_recipient"
	}
	if k.ReceiptRole == committeeflows.RoleEarmarked {
		out.EarmarkState = "reported_earmarked_receipt"
	}
	if r.ReceiptType != nil {
		switch *r.ReceiptType {
		case "15I", "15T":
			out.EarmarkState = "reported_intermediary_receipt"
		}
	}
	// Names, memo substrings, and unrelated back-references never manufacture
	// a conduit assertion. Only its dedicated source ID field can do so.
	switch {
	case present(r.ConduitID) && !committeeflows.ValidCommitteeID(r.ConduitID):
		out.ConduitState = "invalid_structured_conduit_id"
	case present(r.ConduitID) && out.EarmarkState != "no_reviewed_earmark_code":
		out.ConduitState = "reported_structured_id_identity_unverified"
		out.ReportedConduitID = r.ConduitID
	case present(r.ConduitID):
		out.ConduitState = "conduit_role_unresolved"
	case out.EarmarkState != "no_reviewed_earmark_code":
		out.ConduitState = "earmark_conduit_unresolved"
	case present(in.conduitName):
		out.ConduitState = "conduit_name_identity_unresolved"
	}
	if present(in.backReference) || present(in.backSchedule) {
		out.ReportReferenceState = "incomplete_report_reference"
		if present(in.backReference) && present(in.file) && present(in.transaction) && committeeflows.ValidCommitteeID(r.Recipient) {
			out.ReportReferenceState = "reported_reference_requires_same_filing_resolution"
		}
	}
	return out
}

func annotate(receipts []Receipt) ([]EvidenceDecision, error) {
	out := make([]EvidenceDecision, 0, len(receipts))
	for _, receipt := range receipts {
		in, err := decodeEvidence(receipt)
		if err != nil {
			return nil, err
		}
		out = append(out, assessEvidence(in))
	}
	return out, nil
}

// Inspect adds a source-role decision without changing the v1 raw page or
// either accepted monetary predicate. Source bytes are verified by Query.
func (r *Reader) Inspect(ctx context.Context, q Query) (EvidencePage, error) {
	page, err := r.Query(ctx, q)
	if err != nil {
		return EvidencePage{}, err
	}
	d, err := annotate(page.Receipts)
	if err != nil {
		return EvidencePage{}, err
	}
	return EvidencePage{SchemaVersion: "legal-tender.fec.receipt-source-evidence-page.v1", Policy: EvidencePolicy, Page: page, Decisions: d}, nil
}
