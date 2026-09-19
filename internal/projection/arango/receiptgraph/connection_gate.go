package receiptgraph

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"

	upstream "github.com/vedantadhobley/legal-tender/internal/calculation/fec/candidateupstream"
	c "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptconduits"
	p "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
)

const ConnectionGateVersion = "legal-tender.receipt-candidate-connection-gate.v1"

type ConnectionGateOptions struct {
	ConnectionOptions
	ExpectedGateID string
}

type ConnectionCase struct {
	Kind         string  `json:"kind"`
	Population   uint64  `json:"matching_occurrences"`
	State        string  `json:"state"`
	Ordinal      *uint64 `json:"witness_source_row_ordinal"`
	ConnectionID *string `json:"connection_id"`
}

type ConnectionGate struct {
	SchemaVersion        string             `json:"schema_version"`
	GateID               string             `json:"gate_id"`
	Policy               string             `json:"policy"`
	State                string             `json:"state"`
	Scope                string             `json:"scope"`
	BuildSHA256          string             `json:"executable_sha256"`
	Cycle                string             `json:"cycle"`
	Projection           Reference          `json:"receipt_projection"`
	Inputs               Inputs             `json:"receipt_inputs"`
	FlowProjection       string             `json:"committee_flow_projection_id"`
	UpstreamInputs       upstream.Inputs    `json:"upstream_inputs"`
	UpstreamCalculation  string             `json:"upstream_calculation_id"`
	Selection            CandidateSelection `json:"candidate_selection"`
	Rows                 uint64             `json:"source_occurrences_verified"`
	Cases                []ConnectionCase   `json:"cases"`
	Connections          []Connection       `json:"connections"`
	Checks               []string           `json:"checks"`
	FinancialEligibility bool               `json:"financial_eligibility"`
	TerminalEligible     bool               `json:"terminal_attribution_eligible"`
}

// ValidateConnections chooses its own candidate and exact occurrences, checks
// complete source-index membership once, and reads every selected connection
// twice. Success needs no operator interpretation of sampled output.
func ValidateConnections(ctx context.Context, o ConnectionGateOptions) (ConnectionGate, error) {
	if o.Candidate != "" || o.Ordinal != 0 || o.ExpectedGateID != "" && !validDigest(o.ExpectedGateID) {
		return ConnectionGate{}, fmt.Errorf("connection gate selects candidate and occurrence scope automatically; optional replay ID must be a SHA256")
	}
	r, err := openConnection(ctx, o.ConnectionOptions, true)
	if err != nil {
		return ConnectionGate{}, err
	}
	s, err := newConnectionSelector(r.u.Nodes, r.l.masters)
	if err != nil {
		return ConnectionGate{}, err
	}
	selected := r.o.Sources
	selected.First, selected.Rows = 1, r.v.SourceRows
	r.o.Sources.Progress("selecting connection cases from the complete participant/conduit stream")
	err = streamWith(ctx, selected, r.l, func(f p.File, visit func(p.Row) error) error {
		_, err := p.ReadShard(ctx, participantDir(selected), f, visit)
		if err == nil {
			r.o.Sources.Progress(fmt.Sprintf("connection census verified through %d/%d source occurrences", f.Last, r.v.SourceRows))
		}
		return err
	}, s.observe)
	if err != nil {
		return ConnectionGate{}, err
	}
	if err := s.validate(r.v.SourceRows); err != nil {
		return ConnectionGate{}, err
	}
	out, err := runConnectionCases(ctx, s, r.inspect)
	if err != nil {
		return ConnectionGate{}, err
	}
	r.o.Sources.Progress("rechecking completed graph metadata after source-backed connection replay")
	if err := verifyCycleCompletion(ctx, r.cl, r.raw, r.v); err != nil {
		return ConnectionGate{}, err
	}
	if err := r.freader.VerifyCompletion(ctx); err != nil {
		return ConnectionGate{}, err
	}
	out.SchemaVersion, out.Policy, out.State = ConnectionGateVersion, ConnectionGatePolicy, "passed"
	out.Scope = "complete_cycle_participant_census_and_selected_cross_graph_witnesses"
	out.BuildSHA256, out.Cycle = r.o.BuildSHA256, r.v.Inputs.Cycle
	out.Projection, out.Inputs = Reference{r.v.Key, r.o.ManifestSHA256}, r.v.Inputs
	out.FlowProjection = r.freader.View().ProjectionID
	out.UpstreamInputs, out.UpstreamCalculation, out.Selection = r.u.Inputs, r.u.CalculationID, r.selection
	out.Checks = []string{"exact_completed_graph_ancestry", "reference_content_equivalence", "full_participant_and_conduit_membership", "conserving_candidate_connectivity_census", "deterministic_data_selected_cases", "all_selected_graph_fields", "full_selected_source_rows", "exact_connection_replay", "unchanged_completion_metadata", "no_financial_or_identity_promotion"}
	b, err := json.Marshal(out)
	if err != nil {
		return ConnectionGate{}, err
	}
	out.GateID = digest(b)
	if o.ExpectedGateID != "" && out.GateID != o.ExpectedGateID {
		return ConnectionGate{}, fmt.Errorf("connection gate differs from expected replay identity")
	}
	return out, nil
}

// The callback seam tests corruption, cancellation and replay without mocking
// source interpretation. Production supplies the same reader as the single
// connection command. Reused witnesses are checked once per replay round.
func runConnectionCases(ctx context.Context, s *connectionSelector, inspect func(context.Context, p.Row, *c.Decision) (Connection, error)) (ConnectionGate, error) {
	out := ConnectionGate{Rows: s.rows, Cases: []ConnectionCase{}, Connections: []Connection{}}
	unique := map[uint64]*selectedReceipt{}
	for _, pick := range s.first {
		if pick != nil {
			unique[uint64(pick.Row.Ordinal)] = pick
		}
	}
	ordinals := make([]uint64, 0, len(unique))
	for ordinal := range unique {
		ordinals = append(ordinals, ordinal)
	}
	sort.Slice(ordinals, func(i, j int) bool { return ordinals[i] < ordinals[j] })
	byOrdinal := map[uint64]Connection{}
	for round := 0; round < 2; round++ {
		for _, ordinal := range ordinals {
			if err := ctx.Err(); err != nil {
				return ConnectionGate{}, err
			}
			pick := unique[ordinal]
			connection, err := inspect(ctx, pick.Row, pick.Decision)
			if err != nil {
				return ConnectionGate{}, err
			}
			if connection.FinancialEligibility || connection.TerminalEligible || connection.Source.FinancialEligibility || connection.Source.IdentityResolved || !reflect.DeepEqual(connection.Source.Participant, pick.Row) {
				return ConnectionGate{}, fmt.Errorf("connection replay changed source or financial boundary")
			}
			if err := s.checkConnection(pick.Row, connection); err != nil {
				return ConnectionGate{}, err
			}
			if round == 0 {
				byOrdinal[ordinal] = connection
			} else if !reflect.DeepEqual(byOrdinal[ordinal], connection) {
				return ConnectionGate{}, fmt.Errorf("connection replay changed for source ordinal %d", ordinal)
			}
		}
	}
	for i, name := range connectionCaseNames {
		v := ConnectionCase{Kind: name, Population: s.counts[i], State: "not_present_in_complete_candidate_cycle_scope"}
		if pick := s.first[i]; pick != nil {
			n := uint64(pick.Row.Ordinal)
			connection := byOrdinal[n]
			v.State, v.Ordinal, v.ConnectionID = "verified_and_replayed", &n, &connection.ConnectionID
		}
		out.Cases = append(out.Cases, v)
	}
	for _, ordinal := range ordinals {
		out.Connections = append(out.Connections, byOrdinal[ordinal])
	}
	return out, nil
}

// Selection and rendering must agree, not merely reproduce the same mistake
// twice. A missing route cannot satisfy a direct or upstream witness category.
func (s *connectionSelector) checkConnection(row p.Row, v Connection) error {
	state, hops := "unresolved_reported_recipient", 0
	connected := false
	if row.Recipient != nil && committeePattern.MatchString(*row.Recipient) {
		state = "no_path_in_selected_receiver_cohort_not_terminal"
		if n, ok := s.nodes[*row.Recipient]; ok {
			state, hops, connected = "connected_observation_path_not_attributed_money", n.Hops, true
		}
	}
	if v.State != state || len(v.CommitteePath) != hops || (len(v.Authorization) > 0) != connected {
		return fmt.Errorf("connection witness disagrees with complete connectivity census")
	}
	return nil
}
