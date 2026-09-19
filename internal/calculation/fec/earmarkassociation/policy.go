// Package earmarkassociation owns the reviewed role/conflict decision, separate
// from the execution plan used to establish complete same-report topology.
package earmarkassociation

import (
	"fmt"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
)

const Policy = "fec/same-report-earmark-memo-association@1.0.0"

type Evidence struct {
	Memo                                                          bool
	ReceiptType, Entity, Contributor, CleanContributor, ConduitID *string
	Amount                                                        *int64
}

// Topology must include invalid incident references, not just exact neighbors.
// Its caller owns complete scope, transaction uniqueness and reciprocal dedup.
type Topology struct {
	Unsafe bool
	Peers  uint64
}

type Related struct {
	Evidence Evidence
	Topology Topology
}

type Decision struct {
	State            string
	ConduitID        *string
	AmountComparison string
	AdditionalAmount string
	TerminalEligible bool
}

func Applies(v Evidence) bool {
	role, _ := committeeflows.ClassifyReceiptRole(v.ReceiptType)
	return !v.Memo && role == committeeflows.RoleEarmarked
}

// Decide does not discover neighbors, resolve identities or allocate money.
// Amount equality is a separate observation and never a qualification gate.
func Decide(v Evidence, topology Topology, related *Related) (Decision, error) {
	if !Applies(v) || (topology.Peers == 1) != (related != nil) {
		return Decision{}, fmt.Errorf("non-memo earmark and exactly one related evidence record iff one peer required")
	}
	d := Decision{State: "no_exact_related_memo", AdditionalAmount: "0", AmountComparison: "not_assessed"}
	switch {
	case topology.Unsafe:
		d.State = "ambiguous_or_incomplete_reference_evidence"
	case topology.Peers > 1:
		d.State = "multiple_related_records_unresolved"
	case related != nil:
		other := related.Evidence
		d.AmountComparison = "unknown_reported_amount"
		if v.Amount != nil && other.Amount != nil {
			d.AmountComparison = "different_reported_amount"
			if *v.Amount == *other.Amount {
				d.AmountComparison = "same_reported_amount"
			}
		}
		switch {
		case related.Topology.Unsafe:
			d.State = "ambiguous_or_incomplete_reference_evidence"
		case related.Topology.Peers != 1:
			d.State = "shared_related_record_unresolved"
		default:
			roles := InspectRoles(v, other)
			d.State = roles.State
			if roles.State == RolesCompatible {
				d.State = "reported_earmark_memo_association"
				d.ConduitID = roles.CommitteeID
			}
		}
	}
	return d, nil
}

const RolesCompatible = "roles_and_reported_ids_compatible"

// RoleInspection assesses only source roles and ID agreement. It does not
// establish reference safety, payment identity, or association eligibility.
type RoleInspection struct {
	State       string
	CommitteeID *string
}

// InspectRoles lets diagnostics inspect checks masked by topology rejection,
// without fabricating a one-to-one topology or relaxing Decide.
func InspectRoles(v, other Evidence) RoleInspection {
	state, id := committeeflows.ClassifySourceIdentity(other.Contributor, other.CleanContributor)
	role, _ := committeeflows.ClassifyReceiptRole(other.ReceiptType)
	switch {
	case !Applies(v):
		return RoleInspection{State: "original_not_non_memo_earmark"}
	case v.Entity == nil || (*v.Entity != "IND" && *v.Entity != "CAN"):
		return RoleInspection{State: "original_contributor_role_unresolved"}
	case !other.Memo || other.Entity == nil || (*other.Entity != "PAC" && *other.Entity != "PTY" && *other.Entity != "CCM"):
		return RoleInspection{State: "related_role_unresolved"}
	case present(other.ReceiptType) && role != committeeflows.RoleEarmarked:
		return RoleInspection{State: "related_role_unresolved"}
	case state != committeeflows.IdentityExactMatching:
		return RoleInspection{State: "related_committee_id_unresolved"}
	case conflicts(v.Contributor, id) || conflicts(v.CleanContributor, id) || conflicts(v.ConduitID, id) || conflicts(other.ConduitID, id):
		return RoleInspection{State: "conflicting_committee_evidence"}
	default:
		return RoleInspection{State: RolesCompatible, CommitteeID: &id}
	}
}

func present(v *string) bool              { return v != nil && *v != "" }
func conflicts(v *string, id string) bool { return present(v) && *v != id }
