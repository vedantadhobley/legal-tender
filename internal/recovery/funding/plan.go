package funding

import (
	"context"
	"fmt"
	"slices"
	"sort"
)

const PlanVersion = "legal-tender.funding-recovery-plan.v1"
const planAdapterVersion = "fec/fact-start-dependency-plan@1.0.0"

const (
	executionRole  = "execution_input"
	comparisonRole = "comparison_evidence"
	historyRole    = "historical_provenance"
)

type PlannedNode struct {
	NodeID string   `json:"node_id"`
	Roles  []string `json:"roles"`
}
type Binding struct {
	Role   string `json:"role"`
	NodeID string `json:"node_id"`
	Mode   string `json:"mode"`
	StepID string `json:"step_id,omitempty"`
}
type RecoveryStep struct {
	ID                 string    `json:"step_id"`
	Operation          string    `json:"operation"`
	Implementation     string    `json:"implementation"`
	OutputSchema       string    `json:"output_schema"`
	Cycle              string    `json:"cycle"`
	ExpectedNode       string    `json:"expected_metadata_node"`
	ExpectedProjection string    `json:"expected_projection_id,omitempty"`
	Inputs             []Binding `json:"inputs"`
	ExpectedFiles      []string  `json:"expected_file_nodes"`
	PolicyEvidence     string    `json:"policy_metadata_sha256"`
	Comparison         string    `json:"comparison_status"`
	Completion         string    `json:"required_completion"`
}
type PlanBlocker struct {
	Owner  string `json:"owner"`
	Code   string `json:"code"`
	Detail string `json:"detail"`
}
type PlanCounts struct {
	ExecutionFiles  uint64 `json:"execution_files"`
	ExecutionBytes  uint64 `json:"execution_logical_bytes"`
	ComparisonFiles uint64 `json:"comparison_files"`
	ComparisonBytes uint64 `json:"comparison_logical_bytes"`
	UnknownSizes    uint64 `json:"unknown_size_files"`
}
type RecoveryPlan struct {
	Version                string         `json:"schema_version"`
	ID                     string         `json:"plan_id"`
	Adapter                string         `json:"adapter_version"`
	StartLayer             string         `json:"starting_layer"`
	State                  string         `json:"state"`
	DependencyPlanComplete bool           `json:"dependency_plan_complete"`
	ExecutionReady         bool           `json:"execution_ready"`
	InputSHA256            string         `json:"input_sha256"`
	BuildSHA256            string         `json:"planner_executable_sha256"`
	InventorySHA256        string         `json:"inventory_canonical_json_sha256"`
	Inventory              Result         `json:"provenance_inventory"`
	Nodes                  []PlannedNode  `json:"node_roles"`
	Steps                  []RecoveryStep `json:"steps"`
	Blockers               []PlanBlocker  `json:"execution_blockers"`
	Counts                 PlanCounts     `json:"file_counts"`
	Limitations            []string       `json:"limitations"`
}

type planner struct {
	ctx      context.Context
	metadata map[string][]byte
	nodes    map[string]Node
	edges    map[string][]Edge
	roles    map[string]map[string]bool
	steps    map[string]RecoveryStep
	visiting map[string]bool
	blockers map[PlanBlocker]bool
	out      RecoveryPlan
}

// Plan derives a fact-start dependency recipe, not an executable authorization.
// It uses the unchanged full provenance inventory and never hashes data bodies.
func Plan(ctx context.Context, in Inputs, inputSHA string, o Options) (RecoveryPlan, error) {
	if o.HashBlobs {
		return RecoveryPlan{}, fmt.Errorf("metadata-only planning forbids blob hashing")
	}
	metadata := map[string][]byte{}
	inv, err := inspect(ctx, in, inputSHA, o, metadata)
	if err != nil {
		return RecoveryPlan{}, err
	}
	return planInventory(ctx, inv, metadata)
}

// Internal only: callers cannot supply an unverified saved inventory to Plan.
func planInventory(ctx context.Context, inv Result, metadata map[string][]byte) (RecoveryPlan, error) {
	p := planner{ctx: ctx, metadata: metadata, nodes: map[string]Node{}, edges: map[string][]Edge{}, roles: map[string]map[string]bool{}, steps: map[string]RecoveryStep{}, visiting: map[string]bool{}, blockers: map[PlanBlocker]bool{}}
	p.out = RecoveryPlan{Version: PlanVersion, Adapter: planAdapterVersion, StartLayer: "normalized_facts_with_required_validator_sources", State: "blocked_not_executable", DependencyPlanComplete: true, InputSHA256: inv.InputSHA256, BuildSHA256: inv.BuildSHA256, InventorySHA256: identity(inv), Inventory: inv, Nodes: []PlannedNode{}, Steps: []RecoveryStep{}, Blockers: []PlanBlocker{}, Limitations: []string{
		"metadata_only_no_blob_hashing_domain_replay_or_live_graph_access",
		"all_historical_inventory_states_preserved_no_missing_stage_waiver",
		"file_bytes_are_path_deduplicated_logical_sizes_not_disk_or_peak_workspace_cost",
		"producer_policy_metadata_pinned_but_full_semantics_require_normal_readers",
		"execution_mounts_runtime_pins_retention_and_comparison_adapters_not_accepted",
	}}
	for _, n := range inv.Nodes {
		if _, duplicate := p.nodes[n.Key]; duplicate {
			return RecoveryPlan{}, fmt.Errorf("duplicate inventory node")
		}
		p.nodes[n.Key] = n
		p.roles[n.Key] = map[string]bool{historyRole: true}
	}
	for _, e := range inv.Edges {
		if _, ok := p.nodes[e.From]; !ok {
			return RecoveryPlan{}, fmt.Errorf("unknown dependency parent")
		}
		if _, ok := p.nodes[e.To]; !ok {
			return RecoveryPlan{}, fmt.Errorf("unknown dependency child")
		}
		p.edges[e.From] = append(p.edges[e.From], e)
	}
	for key := range p.edges {
		sort.Slice(p.edges[key], func(i, j int) bool {
			a, b := p.edges[key][i], p.edges[key][j]
			if a.Role != b.Role {
				return a.Role < b.Role
			}
			return a.To < b.To
		})
	}
	if dependencyCycle(inv.Nodes, inv.Edges) {
		p.fail(inv.Root, "dependency_cycle", "cyclic provenance cannot supply a recovery order")
	} else {
		p.derive(inv.Root)
	}
	if err := ctx.Err(); err != nil {
		return RecoveryPlan{}, err
	}
	for _, q := range inv.Requirements {
		// Keep provenance-only requirements in the embedded inventory. Producer
		// executables and contracts on selected inputs/outputs need replay closure.
		if p.hasRole(q.Parent, executionRole) || p.hasRole(q.Parent, comparisonRole) {
			p.block(q.Parent, "runtime_or_contract_unverified", q.Kind+":"+q.Identity)
		}
	}
	for _, item := range []PlanBlocker{
		{inv.Root, "replay_build_unbound", "retain accepted producer/comparator executable, source, modules and compiler image"},
		{inv.Root, "arango_runtime_unbound", "pin and retain the isolated server image and explicit internal memory settings"},
		{inv.Root, "retention_unenforced", "protect execution/comparison/runtime closure from cleanup; test fail-closed retention"},
		{inv.Root, "workspace_unadmitted", "bind fresh targets, read-only input mounts, output growth, memory, heap, CPU and live disk limits"},
		{inv.Root, "isolated_executor_unimplemented", "prove no historical calculations, graph data or current pointers can seed execution"},
		{inv.Root, "recovery_comparators_unimplemented", "complete per-schema field comparison, derived-ID mapping and corruption tests required"},
	} {
		p.block(item.Owner, item.Code, item.Detail)
	}
	for _, n := range inv.Nodes {
		roles := []string{}
		for role := range p.roles[n.Key] {
			roles = append(roles, role)
		}
		sort.Strings(roles)
		p.out.Nodes = append(p.out.Nodes, PlannedNode{n.Key, roles})
		if !p.hasRole(n.Key, executionRole) && !p.hasRole(n.Key, comparisonRole) {
			continue
		}
		if n.Verification != "sha256_verified" && n.Reference.Kind == "file" {
			p.block(n.Key, "body_verification_required", n.Verification)
		}
		if !n.Expanded {
			p.fail(n.Key, "required_file_unavailable", n.Verification)
		}
		size := n.Reference.Bytes
		if size == nil {
			size = n.ObservedBytes
		}
		if size == nil {
			p.out.Counts.UnknownSizes++
			continue
		}
		if p.hasRole(n.Key, executionRole) {
			p.out.Counts.ExecutionFiles++
			if *size > ^uint64(0)-p.out.Counts.ExecutionBytes {
				return RecoveryPlan{}, fmt.Errorf("execution byte total overflow")
			}
			p.out.Counts.ExecutionBytes += *size
		}
		if p.hasRole(n.Key, comparisonRole) {
			p.out.Counts.ComparisonFiles++
			if *size > ^uint64(0)-p.out.Counts.ComparisonBytes {
				return RecoveryPlan{}, fmt.Errorf("comparison byte total overflow")
			}
			p.out.Counts.ComparisonBytes += *size
		}
	}
	for b := range p.blockers {
		p.out.Blockers = append(p.out.Blockers, b)
	}
	sort.Slice(p.out.Nodes, func(i, j int) bool { return p.out.Nodes[i].NodeID < p.out.Nodes[j].NodeID })
	sort.Slice(p.out.Blockers, func(i, j int) bool {
		a, b := p.out.Blockers[i], p.out.Blockers[j]
		if a.Owner != b.Owner {
			return a.Owner < b.Owner
		}
		if a.Code != b.Code {
			return a.Code < b.Code
		}
		return a.Detail < b.Detail
	})
	p.out.ID = identity(p.out)
	return p.out, nil
}

func (p *planner) block(owner, code, detail string) {
	p.blockers[PlanBlocker{owner, code, detail}] = true
}
func (p *planner) fail(owner, code, detail string) {
	p.out.DependencyPlanComplete = false
	p.block(owner, code, detail)
}
func (p *planner) hasRole(key, role string) bool { return p.roles[key][role] }
func (p *planner) mark(key, role string) bool {
	n, ok := p.nodes[key]
	if !ok {
		p.fail(key, "unlocated_dependency", "dependency is absent from typed inventory")
		return false
	}
	p.roles[key][role] = true
	if !validDigest(n.Reference.SHA256) || !relative(n.Reference.Path) {
		p.fail(key, "missing_exact_pin", "required input needs an exact digest and immutable path")
		return false
	}
	if n.Reference.Kind != "file" && (n.Verification != "sha256_verified" || !n.Expanded || digest(p.metadata[key]) != n.Reference.SHA256) {
		p.fail(key, "unverified_metadata", n.Verification)
		return false
	}
	return true
}

func (p *planner) derive(key string) string {
	if p.ctx.Err() != nil {
		return ""
	}
	if s, ok := p.steps[key]; ok {
		return s.ID
	}
	if p.visiting[key] {
		p.fail(key, "dependency_cycle", "recursive producer dependency")
		return ""
	}
	if !p.mark(key, comparisonRole) {
		return ""
	}
	n := p.nodes[key]
	spec, ok := producer(n.Reference.Kind, n.Schema)
	if !ok {
		p.fail(key, "unsupported_producer", n.Reference.Kind+":"+n.Schema)
		return ""
	}
	p.visiting[key] = true
	defer delete(p.visiting, key)
	p.checkProducerVersions(key)
	s := RecoveryStep{ID: key, Operation: spec.operation, Implementation: spec.implementation, OutputSchema: n.Schema, Cycle: p.cycle(key), ExpectedNode: key, PolicyEvidence: n.Reference.SHA256, Inputs: []Binding{}, ExpectedFiles: []string{}, Comparison: "not_implemented", Completion: "normal_publisher_integrity_then_complete_equivalence_and_source_drilldown"}
	counts := map[string]int{}
	for _, e := range p.edges[key] {
		rule, ok := spec.dependencies[e.Role]
		child := p.nodes[e.To]
		if !ok || rule.kind != child.Reference.Kind {
			p.fail(key, "unclassified_dependency", e.Role+":"+child.Reference.Kind)
			continue
		}
		counts[e.Role]++
		if rule.output {
			p.mark(e.To, comparisonRole)
			s.ExpectedFiles = append(s.ExpectedFiles, e.To)
			continue
		}
		b := Binding{Role: e.Role, NodeID: e.To, Mode: "retained_input"}
		if _, derived := producer(child.Reference.Kind, child.Schema); derived {
			b.Mode, b.StepID = "rebuilt_output", p.derive(e.To)
		} else {
			p.retain(e.To)
		}
		s.Inputs = append(s.Inputs, b)
	}
	for role, rule := range spec.dependencies {
		if counts[role] < rule.min || (rule.max != 0 && counts[role] > rule.max) {
			p.fail(key, "ambiguous_dependency_membership", role)
		}
	}
	if n.Reference.Kind == "generation" {
		p.generationExtras(key, &s)
	}
	sort.Slice(s.Inputs, func(i, j int) bool {
		a, b := s.Inputs[i], s.Inputs[j]
		if a.Role != b.Role {
			return a.Role < b.Role
		}
		return a.NodeID < b.NodeID
	})
	sort.Strings(s.ExpectedFiles)
	s.ExpectedFiles = slices.Compact(s.ExpectedFiles)
	p.steps[key] = s
	p.out.Steps = append(p.out.Steps, s) // deterministic DFS: dependencies first
	return s.ID
}
