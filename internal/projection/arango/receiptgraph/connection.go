package receiptgraph

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	upstream "github.com/vedantadhobley/legal-tender/internal/calculation/fec/candidateupstream"
	flow "github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
	c "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptconduits"
	p "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
	"github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
)

const ConnectionVersion = "legal-tender.receipt-candidate-connection.v1"

type ConnectionOptions struct {
	Sources                                                      Options
	Manifest, ManifestSHA256, FlowBundle, Candidate, BuildSHA256 string
	Ordinal                                                      uint64
}

// Connection is a typed, nonfinancial witness, not a new payment ledger or a
// claim of complete integrated coverage. The receipt and each chain observation
// retain their own amounts, provenance and source occurrence identity.
type Connection struct {
	SchemaVersion        string            `json:"schema_version"`
	ConnectionID         string            `json:"connection_id"`
	BuildSHA256          string            `json:"executable_sha256"`
	Cycle                string            `json:"cycle"`
	Candidate            string            `json:"candidate_id"`
	State                string            `json:"state"`
	Projection           Reference         `json:"receipt_projection"`
	Inputs               Inputs            `json:"receipt_inputs"`
	UpstreamInputs       upstream.Inputs   `json:"upstream_inputs"`
	UpstreamCalculation  string            `json:"upstream_calculation_id"`
	FlowProjection       string            `json:"committee_flow_projection_id"`
	Source               p.Inspection      `json:"source"`
	Appearance           json.RawMessage   `json:"appearance"`
	Receipt              json.RawMessage   `json:"receipt"`
	Conduit              json.RawMessage   `json:"conduit_association"`
	CommitteePath        []json.RawMessage `json:"receiver_reported_committee_path"`
	Authorization        json.RawMessage   `json:"candidate_authorization"`
	Entities             []json.RawMessage `json:"receipt_graph_entities"`
	FinancialEligibility bool              `json:"financial_eligibility"`
	TerminalEligible     bool              `json:"terminal_attribution_eligible"`
	Limitations          []string          `json:"limitations"`
}

// InspectConnection uses only read APIs. Missing completion is an error, not a
// reason to finish the publisher. It selects no latest pointer or fallback cycle.
func InspectConnection(ctx context.Context, o ConnectionOptions) (Connection, error) {
	if o.Ordinal == 0 || !validDigest(o.BuildSHA256) || !candidatePattern.MatchString(o.Candidate) || o.FlowBundle == "" {
		return Connection{}, fmt.Errorf("source ordinal, consumer build, candidate and exact flow bundle required")
	}
	r, err := openConnection(ctx, o, false)
	if err != nil {
		return Connection{}, err
	}
	if o.Ordinal > r.v.Rows {
		return Connection{}, fmt.Errorf("source ordinal outside completed cycle")
	}
	selected := o.Sources
	selected.First, selected.Rows = o.Ordinal, 1
	var out Connection
	err = stream(ctx, selected, r.l, func(row p.Row, decision *c.Decision) error {
		var err error
		out, err = r.inspect(ctx, row, decision)
		return err
	})
	if err != nil {
		return Connection{}, err
	}
	return out, nil
}

// Share verified source and graph readers across gate cases. Nothing here is a
// process-global cache; a fresh invocation revalidates the pinned generation.
type connectionReader struct {
	o         ConnectionOptions
	v         completion
	raw       []byte
	l         loaded
	cl        *client
	freader   *flowevidence.Reader
	u         upstream.Result
	witnesses []flow.Observation
	selection CandidateSelection
}

func openConnection(ctx context.Context, o ConnectionOptions, automatic bool) (*connectionReader, error) {
	if !validDigest(o.BuildSHA256) || o.FlowBundle == "" {
		return nil, fmt.Errorf("consumer build and exact flow bundle required")
	}
	if o.Sources.Progress == nil {
		o.Sources.Progress = func(string) {}
	}
	o.Sources.Progress("verifying completed receipt publication and exact backing inputs")
	raw, err := manifestBytes(o.Manifest)
	if err != nil {
		return nil, err
	}
	v, err := decodeCycleCompletion(raw, o.ManifestSHA256)
	if err != nil {
		return nil, err
	}
	bundle, bundleSHA, err := flow.LoadBundleMetadata(o.Sources.StorageRoot, o.FlowBundle)
	if err != nil {
		return nil, err
	}
	if err := compatibleConnectionBundle(v, bundle); err != nil {
		return nil, err
	}
	o.Sources.Progress("proving reference content equivalence with both archive histories preserved")
	references, err := flow.LoadReferenceContext(ctx, o.Sources.StorageRoot, o.FlowBundle, o.Sources.Committees, o.Sources.Linkages)
	if err != nil {
		return nil, err
	}
	master, linkage, proofs, err := references.References(bundle, bundleSHA)
	if err != nil {
		return nil, err
	}
	if (Reference{master.FactSetID, master.ManifestSHA256}) != v.Inputs.Committees || (Reference{linkage.FactSetID, linkage.ManifestSHA256}) != v.Inputs.Linkages || master.SourceReleaseID != v.Inputs.SourceRelease || linkage.SourceReleaseID != v.Inputs.SourceRelease {
		return nil, fmt.Errorf("verified reference context differs from receipt publication")
	}
	o.Sources.Progress(fmt.Sprintf("reference content proofs verified: receipt master=%d rows, flow master=%d rows, linkage=%d rows", proofs[0].Rows, proofs[1].Rows, proofs[2].Rows))
	l, cl, err := openCompletedCycle(ctx, o.Sources, raw, v)
	if err != nil {
		return nil, err
	}
	o.Sources.Progress("opening completed committee-flow graph once for all connection checks")
	freader, err := flowevidence.OpenReader(ctx, flowevidence.Options{StorageRoot: o.Sources.StorageRoot, Bundle: o.FlowBundle, Cycle: v.Inputs.Cycle, Endpoint: o.Sources.Endpoint, Username: o.Sources.Username, Password: o.Sources.Password})
	if err != nil {
		return nil, err
	}
	if view := freader.View(); view.BundleID != bundle.BundleID || view.BundleSHA != bundleSHA {
		return nil, fmt.Errorf("flow bundle changed after connection preflight")
	}
	var selection CandidateSelection
	if automatic {
		recipients, err := freader.Recipients(flowevidence.ScheduleA)
		if err != nil {
			return nil, err
		}
		selection, err = selectGateCandidate(l.links, recipients)
		if err != nil {
			return nil, err
		}
		o.Candidate = selection.Candidate
	}
	u, witnesses, err := upstream.RunWithReferenceWitnesses(ctx, upstream.Options{StorageRoot: o.Sources.StorageRoot, Bundle: o.FlowBundle, Linkages: o.Sources.Linkages, Cycle: v.Inputs.Cycle, Candidate: o.Candidate, Progress: o.Sources.Progress}, references)
	if err != nil {
		return nil, err
	}
	if err = compatibleUpstream(v, u); err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(u.Inputs.ReferenceEquivalence, proofs) {
		return nil, fmt.Errorf("upstream reference proof changed")
	}
	view := freader.View()
	if view.BundleID != u.Inputs.BundleID || view.BundleSHA != u.Inputs.BundleSHA256 || !reflect.DeepEqual(view.Inputs, u.Inputs.Sources) {
		return nil, fmt.Errorf("live committee graph differs from upstream ancestry")
	}
	return &connectionReader{o, v, raw, l, cl, freader, u, witnesses, selection}, nil
}

// row/decision must come from the complete verified source-disposition join.
// Recheck the full source row and every selected graph value on each invocation.
func (reader *connectionReader) inspect(ctx context.Context, row p.Row, decision *c.Decision) (Connection, error) {
	o, v, l, cl, u := reader.o, reader.v, reader.l, reader.cl, reader.u
	source, err := l.inspector.Inspect(ctx, uint64(row.Ordinal))
	if err != nil {
		return Connection{}, err
	}
	if !reflect.DeepEqual(row, source.Participant) {
		return Connection{}, fmt.Errorf("connection source changed")
	}
	out := Connection{SchemaVersion: ConnectionVersion, BuildSHA256: o.BuildSHA256, Cycle: v.Inputs.Cycle, Candidate: o.Candidate,
		Projection: Reference{v.Key, o.ManifestSHA256}, Inputs: v.Inputs, UpstreamInputs: u.Inputs, UpstreamCalculation: u.CalculationID, Source: source,
		CommitteePath: []json.RawMessage{}, Entities: []json.RawMessage{},
		Limitations: []string{"one_source_occurrence_not_complete_integrated_generation", "selected_receiver_cohort_not_all_committee_money", "connectivity_not_chronological_allocation", "conduit_association_adds_no_money", "contributor_identity_unresolved", "outside_spending_and_sender_paths_not_joined", "terminal_and_allocation_policies_unselected"}}
	var recipient string
	err = func() error {
		a, r, conduit, err := project(v.Inputs.Facts.ID, row, decision)
		if err != nil {
			return err
		}
		out.Appearance, err = verifyConnectionDocument(ctx, cl, appearances, a.Key, compact(a))
		if err != nil {
			return err
		}
		// A missing edge is checked too; do not silently hide an unexpected link.
		out.Receipt, err = verifyConnectionDocument(ctx, cl, receipts, a.Key, r)
		if err != nil {
			return err
		}
		out.Conduit, err = verifyConnectionDocument(ctx, cl, conduits, a.Key, conduit)
		if err != nil {
			return err
		}
		if r != nil {
			recipient = *source.Participant.Recipient
		}
		if conduit != nil {
			id := *conduit.Decision.ConduitID
			b, err := verifyConnectionDocument(ctx, cl, entities, id, l.entity(id))
			if err != nil {
				return err
			}
			out.Entities = append(out.Entities, b)
		}
		return nil
	}()
	if err != nil {
		return Connection{}, err
	}
	path, root, state, err := connectionPath(u, reader.witnesses, recipient)
	if err != nil {
		return Connection{}, err
	}
	out.State = state
	freader := reader.freader
	view := freader.View()
	out.FlowProjection = view.ProjectionID
	for _, observation := range path {
		b, err := freader.ObservationAt(ctx, flowevidence.ScheduleA, observation.Ordinal)
		if err != nil {
			return Connection{}, err
		}
		out.CommitteePath = append(out.CommitteePath, b)
	}
	ids := []string{}
	if recipient != "" {
		ids = append(ids, recipient)
	}
	for _, e := range path {
		ids = append(ids, e.Recipient)
	}
	if root != "" {
		ids = append(ids, o.Candidate)
		var authorization *authorization
		for i := range l.links {
			a := &l.links[i]
			if a.From == entities+"/"+root && a.To == entities+"/"+o.Candidate && a.State == "authorized" {
				authorization = a
				break
			}
		}
		if authorization == nil {
			return Connection{}, fmt.Errorf("upstream root lacks matching receipt-graph authorization")
		}
		out.Authorization, err = verifyConnectionDocument(ctx, cl, authorizations, authorization.Key, authorization)
		if err != nil {
			return Connection{}, err
		}
	}
	for _, id := range ids {
		b, err := verifyConnectionDocument(ctx, cl, entities, id, l.entity(id))
		if err != nil {
			return Connection{}, err
		}
		out.Entities = append(out.Entities, b)
	}
	b, err := json.Marshal(out)
	if err != nil {
		return Connection{}, err
	}
	out.ConnectionID = digest(b)
	return out, nil
}

func compatibleUpstream(v completion, u upstream.Result) error {
	in := v.Inputs
	a, master, linkage := u.Inputs.Sources.A, u.Inputs.CommitteeMaster, u.Inputs.Linkages
	// RunWithWitnesses revalidates the coordinated release's exact source bytes.
	// That release may be newer than these shared immutable facts; do not impose
	// label equality or relabel old publications to manufacture it.
	if u.Cycle != in.Cycle || u.SchemaVersion != upstream.Version || u.Policy != upstream.Policy || u.Ledger != "schedule_a" || u.TerminalEligible ||
		a.SourceReleaseID != in.SourceRelease || master.SourceReleaseID != in.SourceRelease || linkage.SourceReleaseID != in.SourceRelease ||
		(Reference{a.FactSetID, a.ManifestSHA256}) != in.Facts || a.Facts != v.SourceRows ||
		(Reference{master.FactSetID, master.ManifestSHA256}) != in.Committees || (Reference{linkage.FactSetID, linkage.ManifestSHA256}) != in.Linkages {
		return fmt.Errorf("receipt and upstream cycle, source, master or authorization ancestry mismatch")
	}
	return nil
}

// Metadata-only rejection avoids reading the complete backing or contacting
// Arango for inputs that cannot meet the accepted exact-reference contract.
// Passing this check never replaces either full source/graph verification.
func compatibleConnectionBundle(v completion, b flow.Bundle) error {
	if b.Cycle != v.Inputs.Cycle {
		return fmt.Errorf("connection not ready: graph cycles differ")
	}
	a := b.Input.A
	if (Reference{a.FactSetID, a.ManifestSHA256}) != v.Inputs.Facts || a.Facts != v.SourceRows || a.SourceReleaseID != v.Inputs.SourceRelease {
		return fmt.Errorf("connection not ready: exact shared Schedule A ancestry differs")
	}
	// Reference equality is not decided by metadata. The mandatory independent
	// CM/CCL content proofs below must pass even when manifest IDs happen to match.
	return nil
}

// The existing full-cohort traversal selects deterministic shortest-hop
// witnesses. Follow decreasing distance, not an arbitrary graph-depth cutoff.
func connectionPath(u upstream.Result, witnesses []flow.Observation, recipient string) ([]flow.Observation, string, string, error) {
	path := []flow.Observation{}
	if recipient == "" {
		return path, "", "unresolved_reported_recipient", nil
	}
	nodes := make(map[string]upstream.Node, len(u.Nodes))
	for _, n := range u.Nodes {
		if _, exists := nodes[n.CommitteeID]; exists {
			return nil, "", "", fmt.Errorf("repeated upstream node")
		}
		nodes[n.CommitteeID] = n
	}
	node, ok := nodes[recipient]
	if !ok {
		return path, "", "no_path_in_selected_receiver_cohort_not_terminal", nil
	}
	byOrdinal := make(map[uint64]flow.Observation, len(witnesses))
	for _, e := range witnesses {
		if _, exists := byOrdinal[e.Ordinal]; exists {
			return nil, "", "", fmt.Errorf("repeated upstream witness")
		}
		byOrdinal[e.Ordinal] = e
	}
	for !node.Authorized {
		if node.Hops <= 0 || node.Hops >= len(nodes) || node.WitnessOrdinal == nil {
			return nil, "", "", fmt.Errorf("invalid upstream distance or witness")
		}
		e, ok := byOrdinal[*node.WitnessOrdinal]
		next, found := nodes[e.Recipient]
		if !ok || !found || e.Sender != node.CommitteeID || next.Hops != node.Hops-1 {
			return nil, "", "", fmt.Errorf("broken upstream witness path")
		}
		path = append(path, e)
		node = next
	}
	if node.Hops != 0 {
		return nil, "", "", fmt.Errorf("authorized root has nonzero distance")
	}
	return path, node.CommitteeID, "connected_observation_path_not_attributed_money", nil
}
