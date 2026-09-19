package receiptconduits

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"slices"

	policy "github.com/vedantadhobley/legal-tender/internal/calculation/fec/earmarkassociation"
	xsort "github.com/vedantadhobley/legal-tender/internal/storage/externalsort"
)

// GroupRecord covers every root with at least one old shared-degree rejection.
// Requests are exact, distinct earmark leaves. Equality with the complete
// endpoint degree proves all-peer coverage; a smaller count proves no such thing.
type GroupRecord struct {
	Related    uint64               `json:"related_source_row_ordinal"`
	SharedRows uint64               `json:"previous_shared_rejected_rows"`
	Decision   policy.GroupDecision `json:"group_decision"`
}

type GroupPublication struct {
	Policy               string            `json:"policy"`
	BaselineID           string            `json:"baseline_calculation_id"`
	BaselineSHA256       string            `json:"baseline_manifest_sha256"`
	BaselineValues       string            `json:"baseline_decision_values_sha256"`
	BaselineStates       map[string]uint64 `json:"baseline_states"`
	Decisions            xsort.File        `json:"group_decisions"`
	States               map[string]uint64 `json:"group_states"`
	SharedRows           uint64            `json:"previous_shared_rejected_rows"`
	ChangedRows          uint64            `json:"new_shared_association_rows"`
	UnchangedRows        uint64            `json:"unchanged_decision_rows"`
	UncharacterizedPeers uint64            `json:"uncharacterized_exact_peers"`
}

type groupWork struct {
	baseline Result
	sha      string
	groups   []xsort.File
	changes  []xsort.File
}

func DecodeGroup(r xsort.Record, rows uint64) (GroupRecord, error) {
	var g GroupRecord
	d := json.NewDecoder(bytes.NewReader(r.Data))
	d.DisallowUnknownFields()
	if d.Decode(&g) != nil || d.Decode(new(any)) != io.EOF || r.Tag != 0 || r.Ordinal == 0 || r.Ordinal > rows || r.Ordinal != g.Related || r.Key != key(g.Related) {
		return g, fmt.Errorf("invalid group record")
	}
	v := g.Decision
	if v.Policy != policy.GroupPolicy || v.AdditionalAmount != "0" || v.TerminalEligible || v.ExpectedPeers < 2 || v.ExpectedPeers >= rows || v.ObservedPeers > v.ExpectedPeers || g.SharedRows == 0 || g.SharedRows > v.ObservedPeers || v.UnsafePeers > v.ObservedPeers || v.NonLeafPeers != 0 {
		return g, fmt.Errorf("group scope/count mismatch")
	}
	var n uint64
	for role, count := range v.Roles {
		switch role {
		case policy.RolesCompatible, "conflicting_committee_evidence", "original_contributor_role_unresolved", "related_role_unresolved", "related_committee_id_unresolved":
		default:
			return g, fmt.Errorf("unknown group role")
		}
		if count > v.ObservedPeers-n {
			return g, fmt.Errorf("group role overflow")
		}
		n += count
	}
	want := "unsupported_group_peer_roles"
	switch {
	case v.UnsafePeers > 0:
		want = "ambiguous_or_incomplete_reference_evidence"
	case v.ObservedPeers != v.ExpectedPeers:
		want = "incomplete_group_peer_coverage"
	case v.Roles["conflicting_committee_evidence"] > 0:
		want = "conflicting_group_committee_evidence"
	case v.Roles[policy.RolesCompatible] == v.ObservedPeers:
		want = policy.SharedAssociation
	}
	if n != v.ObservedPeers || v.State != want || (v.State == policy.SharedAssociation) != (v.ConduitID != nil) || (v.State == policy.SharedAssociation && g.SharedRows != v.ObservedPeers) {
		return g, fmt.Errorf("group decision/coverage mismatch")
	}
	if v.ConduitID != nil {
		probe := Decision{Ordinal: r.Ordinal, Related: 1, State: policy.SharedAssociation, AmountComparison: "unknown_reported_amount", ConduitID: v.ConduitID}
		if r.Ordinal == 1 {
			probe.Related = 2
		}
		if _, err := probe.record(); err != nil {
			return g, err
		}
	}
	return g, nil
}

// A second sequential read applies the now-complete group decision. No group
// members are buffered pending the last peer, including very large memo groups.
func (w work) groupChanges(ctx context.Context, requests, groups xsort.File) (xsort.File, error) {
	r, err := xsort.Open(ctx, w.space.Dir, requests)
	if err != nil {
		return xsort.File{}, err
	}
	defer r.Close()
	g, err := xsort.Open(ctx, w.space.Dir, groups)
	if err != nil {
		return xsort.File{}, err
	}
	defer g.Close()
	out, err := xsort.New(ctx, w.space, w.o.RunRows, w.o.FanIn)
	if err != nil {
		return xsort.File{}, err
	}
	next, nextErr := r.Next()
	var previous uint64
	for {
		record, err := g.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return xsort.File{}, err
		}
		group, err := DecodeGroup(record, w.p.SourceRows)
		if err != nil || group.Related <= previous {
			return xsort.File{}, fmt.Errorf("invalid ordered group: %w", err)
		}
		previous = group.Related
		var seen uint64
		for nextErr == nil && next.Key <= record.Key {
			row, endpoint, err := decodeRequest(next, w.p.SourceRows)
			if err != nil {
				return xsort.File{}, err
			}
			if next.Key == record.Key {
				seen++
				if group.Decision.State == policy.SharedAssociation {
					if endpoint.UnsafeReasons != 0 || !policy.Applies(evidence(row)) {
						return xsort.File{}, fmt.Errorf("unsafe or inapplicable group change")
					}
					b := binary.BigEndian.AppendUint64(nil, group.Related)
					b = append(b, (*group.Decision.ConduitID)...)
					if err := out.Add(xsort.Record{Key: key(next.Ordinal), Ordinal: next.Ordinal, Data: b}); err != nil {
						return xsort.File{}, err
					}
				}
			}
			next, nextErr = r.Next()
		}
		if seen != group.Decision.ObservedPeers {
			return xsort.File{}, fmt.Errorf("group/request replay count mismatch")
		}
	}
	// Exhaust and verify requests even beyond the last shared group.
	for nextErr == nil {
		if _, _, err := decodeRequest(next, w.p.SourceRows); err != nil {
			return xsort.File{}, err
		}
		next, nextErr = r.Next()
	}
	if nextErr != io.EOF {
		return xsort.File{}, nextErr
	}
	return out.Finish()
}

func (w work) applyGroups(r Result) (Result, error) {
	r.SourceRows = w.p.SourceRows
	b := w.o.groups.baseline
	if r.Decisions.ValuesSHA256 != b.Decisions.ValuesSHA256 || r.Decisions.Rows != b.Decisions.Rows || r.EligibleRoleRows != b.EligibleRoleRows || r.Qualified != b.Qualified || !reflect.DeepEqual(r.States, b.States) || !reflect.DeepEqual(r.Amounts, b.Amounts) {
		return r, fmt.Errorf("complete one-to-one baseline replay differs")
	}
	// Verify the actual accepted parent artifact, not only its descriptor.
	if err := verifyDecisionFile(w.ctx, filepath.Join(filepath.Dir(w.o.GroupBaseline), "data"), b); err != nil {
		return r, err
	}
	groups, err := w.mergeLevel(w.o.groups.groups)
	if err != nil {
		return r, err
	}
	changes, err := w.mergeLevel(w.o.groups.changes)
	if err != nil {
		return r, err
	}
	publication, err := summarizeGroups(w.ctx, w.space.Dir, groups, w.p.SourceRows)
	if err != nil {
		return r, err
	}
	publication.BaselineID, publication.BaselineSHA256 = b.CalculationID, w.o.groups.sha
	publication.BaselineValues, publication.BaselineStates = b.Decisions.ValuesSHA256, b.States
	if publication.SharedRows != b.States[sharedRejection] || publication.ChangedRows != changes.Rows {
		return r, fmt.Errorf("complete shared group population mismatch")
	}
	publication.UnchangedRows = r.EligibleRoleRows - publication.ChangedRows
	updated, err := replaceShared(w.ctx, w.space, r.Decisions, changes, w.p.SourceRows)
	if err != nil {
		return r, err
	}
	if err = w.space.Remove(r.Decisions); err != nil {
		return r, err
	}
	if err = w.space.Remove(changes); err != nil {
		return r, err
	}
	r.Decisions, r.Groups = updated, &publication
	r.States[sharedRejection] -= publication.ChangedRows
	if publication.ChangedRows != 0 {
		r.States[policy.SharedAssociation] = publication.ChangedRows
	}
	r.Qualified += publication.ChangedRows
	if err = verifyDecisionFile(w.ctx, w.space.Dir, r); err != nil {
		return r, err
	}
	return r, nil
}

func validateGroupManifest(r Result) error {
	if r.SchemaVersion == Version && r.Policy == Policy && r.Groups == nil && r.States[policy.SharedAssociation] == 0 {
		return nil
	}
	g := r.Groups
	if r.SchemaVersion != GroupVersion || r.Policy != GroupPublicationPolicy || g == nil || g.Policy != policy.GroupPolicy || !digest(g.BaselineID) || !digest(g.BaselineSHA256) || !digest(g.BaselineValues) || !digest(g.Decisions.SHA256) || !digest(g.Decisions.ValuesSHA256) || g.Decisions.Name == "" || filepath.Base(g.Decisions.Name) != g.Decisions.Name || g.Decisions.Name == r.Decisions.Name || g.ChangedRows > g.SharedRows || g.SharedRows > r.EligibleRoleRows || g.UnchangedRows != r.EligibleRoleRows-g.ChangedRows || g.ChangedRows != r.States[policy.SharedAssociation] || g.SharedRows != g.BaselineStates[sharedRejection] || r.States[sharedRejection] != g.SharedRows-g.ChangedRows {
		return fmt.Errorf("invalid group publication identity/counts")
	}
	var baselineRows, groups uint64
	for state, count := range g.BaselineStates {
		if state == policy.SharedAssociation || !slices.Contains(states, state) || count > r.EligibleRoleRows-baselineRows || (state != sharedRejection && r.States[state] != count) {
			return fmt.Errorf("group baseline changed outside shared state")
		}
		baselineRows += count
	}
	for state, count := range r.States {
		if state != sharedRejection && state != policy.SharedAssociation && g.BaselineStates[state] != count {
			return fmt.Errorf("new non-group disposition")
		}
	}
	for state, count := range g.States {
		switch state {
		case policy.SharedAssociation, "ambiguous_or_incomplete_reference_evidence", "incomplete_group_peer_coverage", "conflicting_group_committee_evidence", "unsupported_group_peer_roles":
		default:
			return fmt.Errorf("unknown publication group state")
		}
		if count > g.Decisions.Rows-groups {
			return fmt.Errorf("group census overflow")
		}
		groups += count
	}
	if baselineRows != r.EligibleRoleRows || groups != g.Decisions.Rows || groups > g.SharedRows || (g.ChangedRows == 0) != (g.States[policy.SharedAssociation] == 0) || g.States[policy.SharedAssociation] > g.ChangedRows/2 {
		return fmt.Errorf("group publication conservation failure")
	}
	return nil
}

// VerifyGroupBacking is required when a downstream consumer accepts v2. It is
// small compared with the complete occurrence stream but remains pinned evidence.
func VerifyGroupBacking(ctx context.Context, path string, r Result) error {
	if err := validateGroupManifest(r); err != nil {
		return err
	}
	if r.Groups == nil {
		return nil
	}
	g, err := summarizeGroups(ctx, filepath.Join(filepath.Dir(path), "data"), r.Groups.Decisions, r.SourceRows)
	if err != nil {
		return err
	}
	if !equalCounts(g.States, r.Groups.States) || g.SharedRows != r.Groups.SharedRows || g.ChangedRows != r.Groups.ChangedRows || g.UncharacterizedPeers != r.Groups.UncharacterizedPeers {
		return fmt.Errorf("group backing census differs")
	}
	return nil
}

func summarizeGroups(ctx context.Context, dir string, f xsort.File, rows uint64) (GroupPublication, error) {
	out := GroupPublication{Policy: policy.GroupPolicy, Decisions: f, States: map[string]uint64{}}
	r, err := xsort.Open(ctx, dir, f)
	if err != nil {
		return out, err
	}
	defer r.Close()
	var previous uint64
	for {
		record, err := r.Next()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		g, err := DecodeGroup(record, rows)
		if err != nil || g.Related <= previous {
			return out, fmt.Errorf("invalid group order/evidence: %w", err)
		}
		previous = g.Related
		out.States[g.Decision.State]++
		out.SharedRows += g.SharedRows
		out.UncharacterizedPeers += g.Decision.ExpectedPeers - g.Decision.ObservedPeers
		if g.Decision.State == policy.SharedAssociation {
			out.ChangedRows += g.SharedRows
		}
	}
}

func verifyDecisionFile(ctx context.Context, dir string, expected Result) error {
	r, err := xsort.Open(ctx, dir, expected.Decisions)
	if err != nil {
		return err
	}
	defer r.Close()
	states, amounts := map[string]uint64{}, map[string]uint64{}
	var previous, qualified, seen uint64
	for {
		record, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		d, err := DecodeDecision(record, expected.SourceRows)
		if err != nil || d.Ordinal <= previous {
			return fmt.Errorf("invalid complete decision stream: %w", err)
		}
		previous = d.Ordinal
		seen++
		states[d.State]++
		amounts[d.AmountComparison]++
		if d.ConduitID != nil {
			qualified++
		}
	}
	if seen != expected.EligibleRoleRows || qualified != expected.Qualified || !equalCounts(states, expected.States) || !equalCounts(amounts, expected.Amounts) {
		return fmt.Errorf("complete decision census mismatch")
	}
	return nil
}

func equalCounts(a, b map[string]uint64) bool {
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	for k, v := range b {
		if a[k] != v {
			return false
		}
	}
	return true
}

// Every unchanged record is copied verbatim. A change may alter only the old
// shared-degree state and add its proven ID; ordinal, peer and amount axis stay.
func replaceShared(ctx context.Context, space *xsort.Workspace, baseline, changes xsort.File, rows uint64) (xsort.File, error) {
	b, err := xsort.Open(ctx, space.Dir, baseline)
	if err != nil {
		return xsort.File{}, err
	}
	defer b.Close()
	c, err := xsort.Open(ctx, space.Dir, changes)
	if err != nil {
		return xsort.File{}, err
	}
	defer c.Close()
	w, err := space.Writer(ctx)
	if err != nil {
		return xsort.File{}, err
	}
	defer w.Abort()
	next, nextErr := c.Next()
	var previous, changed uint64
	for {
		record, err := b.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return xsort.File{}, err
		}
		d, err := DecodeDecision(record, rows)
		if err != nil || d.Ordinal <= previous {
			return xsort.File{}, fmt.Errorf("invalid baseline order: %w", err)
		}
		previous = d.Ordinal
		if nextErr != nil && nextErr != io.EOF {
			return xsort.File{}, nextErr
		}
		if nextErr == nil && next.Ordinal <= d.Ordinal {
			if next.Ordinal != d.Ordinal || next.Key != record.Key || next.Tag != 0 || len(next.Data) != 17 || binary.BigEndian.Uint64(next.Data) != d.Related || d.State != sharedRejection || d.ConduitID != nil {
				return xsort.File{}, fmt.Errorf("change outside shared baseline membership")
			}
			id := string(next.Data[8:])
			d.State, d.ConduitID = policy.SharedAssociation, &id
			record, err = d.record()
			if err != nil {
				return xsort.File{}, err
			}
			changed++
			next, nextErr = c.Next()
		}
		if err = w.Add(record); err != nil {
			return xsort.File{}, err
		}
	}
	if nextErr != io.EOF || changed != changes.Rows {
		return xsort.File{}, fmt.Errorf("unconsumed or duplicate group change")
	}
	return w.Finish()
}
