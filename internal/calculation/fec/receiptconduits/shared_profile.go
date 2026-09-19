package receiptconduits

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"slices"
	"strings"
	"sync"

	policy "github.com/vedantadhobley/legal-tender/internal/calculation/fec/earmarkassociation"
	participants "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
	refs "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptreferences"
)

const SharedProfilePolicy = "fec/shared-reference-role-profile@1.0.0"
const sharedRejection = "shared_related_record_unresolved"

// AmountProfile is an occurrence sum, not an effective payment ledger. Missing
// amounts remain missing; the known signed sum uses arbitrary precision.
type AmountProfile struct {
	Known    uint64 `json:"known_rows"`
	Missing  uint64 `json:"missing_rows"`
	Positive uint64 `json:"positive_rows"`
	Negative uint64 `json:"negative_rows"`
	Zero     uint64 `json:"zero_rows"`
	Sum      string `json:"known_signed_sum_minor_units"`
}

type amountTally struct {
	AmountProfile
	sum big.Int
}

func (a *amountTally) add(v *int64) {
	if v == nil {
		a.Missing++
		return
	}
	a.Known++
	switch {
	case *v > 0:
		a.Positive++
	case *v < 0:
		a.Negative++
	default:
		a.Zero++
	}
	var n big.Int
	a.sum.Add(&a.sum, n.SetInt64(*v))
}
func (a *amountTally) merge(b *amountTally) {
	a.Known += b.Known
	a.Missing += b.Missing
	a.Positive += b.Positive
	a.Negative += b.Negative
	a.Zero += b.Zero
	a.sum.Add(&a.sum, &b.sum)
}
func (a *amountTally) result() AmountProfile {
	r := a.AmountProfile
	r.Sum = a.sum.String()
	return r
}

type ProfileOccurrence struct {
	Ordinal   uint64          `json:"source_row_ordinal"`
	Recipient *string         `json:"recipient_committee_id"`
	Evidence  policy.Evidence `json:"reported_role_evidence"`
	Topology  ProfileTopology `json:"reference_topology"`
}

// The request stream preserves degree, sole peer and safety, not direction.
// Do not report absent direction fields as observed zeros.
type ProfileTopology struct {
	Peers         uint64 `json:"distinct_exact_peers"`
	OnlyPeer      uint64 `json:"sole_peer_ordinal_or_zero"`
	UnsafeReasons uint8  `json:"unsafe_reason_bits"`
}
type RoleWitness struct {
	Role       string            `json:"role_inspection"`
	Occurrence ProfileOccurrence `json:"occurrence"`
}
type SharedGroupWitness struct {
	Related            ProfileOccurrence `json:"shared_related_occurrence"`
	SolePeerRequests   uint64            `json:"non_memo_earmark_sole_peer_requests"`
	SharedRejectedRows uint64            `json:"shared_rejected_rows"`
	OtherPeers         uint64            `json:"other_exact_peers_not_characterized"`
	Roles              map[string]uint64 `json:"role_rows"`
	OriginalAmounts    AmountProfile     `json:"shared_rejected_original_amounts"`
	Examples           []RoleWitness     `json:"first_original_per_role"`
}
type SharedGroupClass struct {
	Coverage         string             `json:"peer_coverage"`
	Roles            string             `json:"role_pattern"`
	AmountComparison string             `json:"original_sum_vs_related_amount"`
	Groups           uint64             `json:"groups"`
	Rows             uint64             `json:"shared_rejected_rows"`
	MaximumPeers     uint64             `json:"maximum_exact_peers"`
	OriginalAmounts  AmountProfile      `json:"shared_rejected_original_amounts"`
	RelatedAmounts   AmountProfile      `json:"related_amounts_once_per_group"`
	Witness          SharedGroupWitness `json:"lowest_related_ordinal_witness"`
}

type SharedProfile struct {
	SchemaVersion      string             `json:"schema_version"`
	Policy             string             `json:"policy"`
	ProfileID          string             `json:"profile_id"`
	State              string             `json:"state"`
	Baseline           Result             `json:"unchanged_association_calculation"`
	SharedRows         uint64             `json:"shared_rejected_rows"`
	SharedGroups       uint64             `json:"shared_related_occurrences"`
	Roles              map[string]uint64  `json:"role_rows"`
	Classes            []SharedGroupClass `json:"group_classes"`
	AdditionalAmount   string             `json:"additional_amount_minor_units"`
	AssociationChanges uint64             `json:"association_changes"`
	TerminalEligible   bool               `json:"terminal_eligible"`
}

type sharedGroup struct {
	rows     uint64
	roles    map[string]uint64
	amounts  amountTally
	examples map[string]RoleWitness
}

// Reader buffers may be reused. Retain only owned, bounded role evidence for
// the first original per role and the lowest peer ordinal per structural class.
func own[T any](v *T) *T {
	if v == nil {
		return nil
	}
	n := *v
	return &n
}
func profileOccurrence(r participants.Row, e refs.Endpoint) ProfileOccurrence {
	v := evidence(r)
	v.ReceiptType, v.Entity = own(v.ReceiptType), own(v.Entity)
	v.Contributor, v.CleanContributor = own(v.Contributor), own(v.CleanContributor)
	v.ConduitID, v.Amount = own(v.ConduitID), own(v.Amount)
	return ProfileOccurrence{Ordinal: uint64(r.Ordinal), Recipient: own(r.Recipient), Evidence: v, Topology: ProfileTopology{Peers: e.Peers, OnlyPeer: e.OnlyPeer, UnsafeReasons: e.UnsafeReasons}}
}
func (g *sharedGroup) observe(original participants.Row, e refs.Endpoint, peer participants.Row) {
	if g.roles == nil {
		g.roles = map[string]uint64{}
		g.examples = map[string]RoleWitness{}
	}
	role := policy.InspectRoles(evidence(original), evidence(peer)).State
	if g.roles[role] == 0 {
		g.examples[role] = RoleWitness{Role: role, Occurrence: profileOccurrence(original, e)}
	}
	g.rows++
	g.roles[role]++
	g.amounts.add(original.Amount)
}

type classKey struct{ coverage, roles, amount string }
type classTally struct {
	class              SharedGroupClass
	originals, related amountTally
}
type sharedProfileCollector struct {
	mu           sync.Mutex
	rows, groups uint64
	roles        map[string]uint64
	classes      map[classKey]*classTally
}

func newSharedProfileCollector() *sharedProfileCollector {
	return &sharedProfileCollector{roles: map[string]uint64{}, classes: map[classKey]*classTally{}}
}
func (p *sharedProfileCollector) add(peer participants.Row, e refs.Endpoint, requests uint64, g sharedGroup) error {
	if e.UnsafeReasons != 0 || e.Peers < 2 || g.rows > requests || requests > e.Peers {
		return fmt.Errorf("inconsistent shared-reference profile group")
	}
	k := classKey{coverage: "other_peers_not_characterized", roles: "no_compatible_roles", amount: "unknown_reported_amount"}
	if g.rows == e.Peers {
		k.coverage = "complete_safe_earmark_leaf_coverage"
	}
	if g.roles[policy.RolesCompatible] == g.rows {
		k.roles = "all_roles_compatible"
	} else if g.roles[policy.RolesCompatible] > 0 {
		k.roles = "mixed_roles"
	}
	if g.amounts.Missing == 0 && peer.Amount != nil {
		k.amount = "different_reported_amount"
		var n big.Int
		if g.amounts.sum.Cmp(n.SetInt64(*peer.Amount)) == 0 {
			k.amount = "same_reported_amount"
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rows += g.rows
	p.groups++
	for role, n := range g.roles {
		p.roles[role] += n
	}
	b := p.classes[k]
	if b == nil {
		b = &classTally{class: SharedGroupClass{Coverage: k.coverage, Roles: k.roles, AmountComparison: k.amount}}
		p.classes[k] = b
	}
	b.class.Groups++
	b.class.Rows += g.rows
	b.class.MaximumPeers = max(b.class.MaximumPeers, e.Peers)
	b.originals.merge(&g.amounts)
	b.related.add(peer.Amount)
	if b.class.Witness.Related.Ordinal == 0 || uint64(peer.Ordinal) < b.class.Witness.Related.Ordinal {
		w := SharedGroupWitness{Related: profileOccurrence(peer, e), SolePeerRequests: requests, SharedRejectedRows: g.rows, OtherPeers: e.Peers - g.rows, Roles: g.roles, OriginalAmounts: g.amounts.result()}
		for _, example := range g.examples {
			w.Examples = append(w.Examples, example)
		}
		slices.SortFunc(w.Examples, func(a, b RoleWitness) int { return strings.Compare(a.Role, b.Role) })
		b.class.Witness = w
	}
	return nil
}
func (p *sharedProfileCollector) finish(baseline Result) (SharedProfile, error) {
	if p.rows != baseline.States[sharedRejection] {
		return SharedProfile{}, fmt.Errorf("shared profile/association membership mismatch")
	}
	r := SharedProfile{SchemaVersion: "legal-tender.fec.shared-reference-profile.v1", Policy: SharedProfilePolicy, State: "complete_diagnostic_no_association_changes", Baseline: baseline, SharedRows: p.rows, SharedGroups: p.groups, Roles: p.roles, Classes: []SharedGroupClass{}, AdditionalAmount: "0"}
	for _, b := range p.classes {
		c := b.class
		c.OriginalAmounts, c.RelatedAmounts = b.originals.result(), b.related.result()
		r.Classes = append(r.Classes, c)
	}
	slices.SortFunc(r.Classes, func(a, b SharedGroupClass) int {
		return strings.Compare(a.Coverage+"/"+a.Roles+"/"+a.AmountComparison, b.Coverage+"/"+b.Roles+"/"+b.AmountComparison)
	})
	// Baseline calculation ID pins all source ancestry and executable bytes;
	// its paths, worker tuning and runtime metrics must not change this identity.
	var err error
	r.ProfileID, err = sharedProfileID(r)
	return r, err
}

func sharedProfileID(r SharedProfile) (string, error) {
	canonical := r
	canonical.ProfileID = ""
	canonical.Baseline = Result{CalculationID: r.Baseline.CalculationID}
	b, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}

// ProfileSharedReferences replays the unchanged publisher and observes its
// shared-degree rejections. No current pointer, database or graph is modified.
func ProfileSharedReferences(ctx context.Context, o Options) (SharedProfile, error) {
	p := newSharedProfileCollector()
	o.profile = p
	baseline, err := Run(ctx, o)
	if err != nil {
		return SharedProfile{}, err
	}
	r, err := p.finish(baseline)
	if err != nil {
		return r, err
	}
	if err = ctx.Err(); err != nil {
		return r, err
	}
	return r, saveJSON(o.OutputDirectory, "shared-reference-profile.json", r)
}
