// Package directattribution calculates accepted occurrence-grain direct and
// explicitly earmarked candidate receipt attribution. Committee-chain money
// remains unresolved.
package directattribution

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"sort"
)

const (
	SchemaVersion     = "legal-tender.fec.direct-source-appearance-attribution.v1"
	CalculationPolicy = "fec/direct-source-appearance-attribution@1.0.0"
	TerminalPolicy    = "fec/reported-source-appearance-terminal-boundary@1.0.0"
	AllocationPolicy  = "fec/direct-and-explicit-earmark-allocation@1.0.0"
	PredicateVersion  = "legal-tender.fec.direct-source-appearance-membership.v1"
)

const (
	DispositionDirect     = "direct"
	DispositionEarmarked  = "explicitly_earmarked"
	DispositionUnresolved = "unresolved"
	DispositionMemo       = "excluded_memo_subtotal"
	DispositionOutside    = "outside_authorized_candidate_scope"
)

type Inputs struct {
	ParticipantCalculationID string `json:"participant_calculation_id"`
	ParticipantManifestSHA   string `json:"participant_manifest_sha256"`
	ScheduleAFactSetID       string `json:"schedule_a_fact_set_id"`
	ScheduleAManifestSHA     string `json:"schedule_a_manifest_sha256"`
	SourceReleaseID          string `json:"fec_source_release_id"`
	ReceiptBundleID          string `json:"receipt_fact_bundle_id"`
	ReceiptBundleSHA         string `json:"receipt_fact_bundle_sha256"`
	LinkageFactSetID         string `json:"candidate_committee_linkage_fact_set_id"`
	LinkageManifestSHA       string `json:"candidate_committee_linkage_manifest_sha256"`
	SourceRows               uint64 `json:"schedule_a_rows"`
	LinkageFacts             uint64 `json:"candidate_committee_linkage_facts"`
}

type Measures struct {
	Rows               uint64 `json:"rows"`
	KnownAmountRows    uint64 `json:"known_amount_rows"`
	UnknownAmountRows  uint64 `json:"unknown_amount_rows"`
	PositiveRows       uint64 `json:"positive_rows"`
	NegativeRows       uint64 `json:"negative_rows"`
	ZeroRows           uint64 `json:"zero_rows"`
	SignedMinorUnits   string `json:"signed_minor_units"`
	PositiveMinorUnits string `json:"positive_minor_units"`
	NegativeMinorUnits string `json:"negative_minor_units"`
}

type ReasonMeasures struct {
	Reason   string   `json:"reason"`
	Measures Measures `json:"measures"`
}

type CandidateResult struct {
	CandidateID          string           `json:"candidate_id"`
	State                string           `json:"state"`
	AuthorizedCommittees []string         `json:"authorized_committee_ids"`
	AuthorizedScope      Measures         `json:"authorized_committee_scope"`
	IncludedNonmemo      Measures         `json:"included_nonmemo_scope"`
	Direct               Measures         `json:"direct"`
	Earmarked            Measures         `json:"explicitly_earmarked"`
	Unresolved           Measures         `json:"unresolved"`
	ExcludedMemo         Measures         `json:"excluded_memo_subtotal"`
	UnresolvedReasons    []ReasonMeasures `json:"unresolved_reasons"`
}

type AuthorizationCensus struct {
	CandidatesWithLinkageFacts  uint64 `json:"candidates_with_linkage_facts"`
	CandidatesWithAuthorization uint64 `json:"candidates_with_authorized_committees"`
	AuthorizedCommittees        uint64 `json:"authorized_committees"`
	AuthorizedRelationships     uint64 `json:"authorized_relationships"`
	UnresolvedRelationships     uint64 `json:"unresolved_relationships"`
	UnauthorizedRelationships   uint64 `json:"unauthorized_relationships"`
}

type Census struct {
	CompleteParticipants      Measures `json:"complete_participant_population"`
	OutsideAuthorizationScope Measures `json:"outside_authorized_candidate_scope"`
	AuthorizedScope           Measures `json:"authorized_committee_scope"`
	IncludedNonmemo           Measures `json:"included_nonmemo_scope"`
	Direct                    Measures `json:"direct"`
	Earmarked                 Measures `json:"explicitly_earmarked"`
	Unresolved                Measures `json:"unresolved"`
	ExcludedMemo              Measures `json:"excluded_memo_subtotal"`
}

type PredicateRule struct {
	Disposition string   `json:"disposition"`
	All         []string `json:"all"`
	Meaning     string   `json:"meaning"`
}

type Predicate struct {
	Version           string          `json:"version"`
	AuthorizationRule string          `json:"authorization_rule"`
	DecisionOrder     []PredicateRule `json:"decision_order"`
	MembershipKey     []string        `json:"source_membership_key"`
}

type Result struct {
	SchemaVersion                     string              `json:"schema_version"`
	CalculationID                     string              `json:"calculation_id"`
	CalculationPolicy                 string              `json:"calculation_policy"`
	TerminalPolicy                    string              `json:"terminal_policy"`
	AllocationPolicy                  string              `json:"allocation_policy"`
	ExecutableSHA256                  string              `json:"executable_sha256"`
	Cycle                             string              `json:"cycle"`
	State                             string              `json:"state"`
	IdentityBoundary                  string              `json:"identity_boundary"`
	Inputs                            Inputs              `json:"inputs"`
	Predicate                         Predicate           `json:"predicate"`
	Authorization                     AuthorizationCensus `json:"authorization_census"`
	Census                            Census              `json:"census"`
	Candidates                        []CandidateResult   `json:"candidates"`
	AppearanceTerminalEligible        bool                `json:"source_appearance_terminal_eligible"`
	ResolvedEntityTerminalEligible    bool                `json:"resolved_entity_terminal_eligible"`
	CommitteeChainAllocationPerformed bool                `json:"committee_chain_allocation_performed"`
	Limitations                       []string            `json:"limitations"`
}

type accumulator struct {
	rows, known, unknown, positiveRows, negativeRows, zeroRows uint64
	signed, positive, negative                                 big.Int
}

func (a *accumulator) observe(amount *int64) {
	a.rows++
	if amount == nil {
		a.unknown++
		return
	}
	a.known++
	a.signed.Add(&a.signed, big.NewInt(*amount))
	switch {
	case *amount > 0:
		a.positiveRows++
		a.positive.Add(&a.positive, big.NewInt(*amount))
	case *amount < 0:
		a.negativeRows++
		a.negative.Add(&a.negative, big.NewInt(*amount))
	default:
		a.zeroRows++
	}
}

func (a *accumulator) add(value accumulator) {
	a.rows += value.rows
	a.known += value.known
	a.unknown += value.unknown
	a.positiveRows += value.positiveRows
	a.negativeRows += value.negativeRows
	a.zeroRows += value.zeroRows
	a.signed.Add(&a.signed, &value.signed)
	a.positive.Add(&a.positive, &value.positive)
	a.negative.Add(&a.negative, &value.negative)
}

func (a accumulator) equal(value accumulator) bool {
	return a.rows == value.rows && a.known == value.known && a.unknown == value.unknown &&
		a.positiveRows == value.positiveRows && a.negativeRows == value.negativeRows && a.zeroRows == value.zeroRows &&
		a.signed.Cmp(&value.signed) == 0 && a.positive.Cmp(&value.positive) == 0 && a.negative.Cmp(&value.negative) == 0
}

func (a accumulator) measures() Measures {
	return Measures{Rows: a.rows, KnownAmountRows: a.known, UnknownAmountRows: a.unknown,
		PositiveRows: a.positiveRows, NegativeRows: a.negativeRows, ZeroRows: a.zeroRows,
		SignedMinorUnits: a.signed.String(), PositiveMinorUnits: a.positive.String(), NegativeMinorUnits: a.negative.String()}
}

func digest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == value
}

func identity(value Result) (string, error) {
	value.CalculationID = ""
	body, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(body)
	return hex.EncodeToString(h[:]), nil
}

func predicate() Predicate {
	return Predicate{Version: PredicateVersion,
		AuthorizationRule: "accepted same-cycle A/P relationship; conflicting, invalid, mixed-designation, and shared authorization remains unresolved and outside financial routing",
		MembershipKey:     []string{"schedule_a_fact_set_id", "source_row_ordinal", "candidate_id"},
		DecisionOrder: []PredicateRule{
			{Disposition: DispositionOutside, All: []string{"recipient is not uniquely authorized to one candidate"}, Meaning: "preserve outside this calculation; do not copy money into a candidate"},
			{Disposition: DispositionMemo, All: []string{"recipient is uniquely authorized", "memoed_subtotal is true"}, Meaning: "evidence only; adds no second amount"},
			{Disposition: DispositionUnresolved, All: []string{"recipient is uniquely authorized", "reported amount is unknown"}, Meaning: "amount remains unresolved"},
			{Disposition: DispositionEarmarked, All: []string{"recipient is uniquely authorized", "nonmemo known amount", "inventory component is itemized_individual_only", "receipt role is earmarked"}, Meaning: "allocate once to the reported original-contributor appearance"},
			{Disposition: DispositionDirect, All: []string{"recipient is uniquely authorized", "nonmemo known amount", "inventory component is itemized_individual_only", "receipt role is not earmarked"}, Meaning: "allocate once to the reported direct source appearance"},
			{Disposition: DispositionUnresolved, All: []string{"recipient is uniquely authorized", "nonmemo known amount", "inventory component is not itemized_individual_only"}, Meaning: "retain exact amount without committee-chain or identity-role inference"},
		},
	}
}

func sortedReasonMeasures(values map[string]accumulator) []ReasonMeasures {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]ReasonMeasures, 0, len(keys))
	for _, key := range keys {
		result = append(result, ReasonMeasures{Reason: key, Measures: values[key].measures()})
	}
	return result
}
