package funding

import (
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"testing"

	flow "github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
	ie "github.com/vedantadhobley/legal-tender/internal/calculation/fec/independentexpenditures"
	refs "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptreferences"
	fg "github.com/vedantadhobley/legal-tender/internal/projection/arango/fundinggeneration"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	rel "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
)

// These are metadata planner fixtures, not accepted source publications or a
// substitute for the later isolated publisher/database integration gate.
type planFixture struct {
	inv      Result
	metadata map[string][]byte
	keys     map[string]string
}

func newPlanFixture() *planFixture {
	return &planFixture{inv: Result{Version: Version, InputSHA256: digest([]byte("input")), BuildSHA256: digest([]byte("build")), Mode: "manifest_sha256_and_blob_presence_size", Nodes: []Node{}, Edges: []Edge{}, Requirements: []Requirement{}}, metadata: map[string][]byte{}, keys: map[string]string{}}
}
func (f *planFixture) add(name, kind string, v any) string {
	b, _ := json.Marshal(v)
	id := digest([]byte(name))
	if kind == "release" {
		id = "fec-" + id
	}
	r := Reference{Kind: kind, ID: id, SHA256: digest(b), Path: "metadata/" + name + ".json"}
	key := digest([]byte(refKey(r)))
	var h struct {
		Schema string `json:"schema_version"`
	}
	json.Unmarshal(b, &h)
	size := uint64(len(b))
	f.inv.Nodes = append(f.inv.Nodes, Node{Key: key, Reference: r, Schema: h.Schema, Verification: "sha256_verified", ObservedSHA256: r.SHA256, ObservedBytes: &size, Expanded: true})
	f.metadata[key] = b
	f.keys[name] = key
	return key
}
func (f *planFixture) file(name string, size uint64) string {
	r := Reference{Kind: "file", ID: digest([]byte(name)), SHA256: digest([]byte(name)), Path: "data/" + name, Bytes: &size}
	key := digest([]byte("file:" + r.Path))
	f.inv.Nodes = append(f.inv.Nodes, Node{Key: key, Reference: r, Verification: "size_verified_not_hashed", ObservedBytes: &size, Expanded: true})
	f.keys[name] = key
	return key
}
func (f *planFixture) edge(parent, child, role string) {
	f.inv.Edges = append(f.inv.Edges, Edge{parent, child, role})
}
func flowPlanFixture(cycle string) *planFixture {
	f := newPlanFixture()
	release := f.add("release", "release", rel.ReleaseManifest{SchemaVersion: rel.ManifestSchemaVersion})
	stage := f.add("lost-stage", "stage", rel.StageResult{SchemaVersion: rel.StageSchemaVersion})
	f.inv.Nodes[len(f.inv.Nodes)-1].Verification = "missing"
	f.inv.Nodes[len(f.inv.Nodes)-1].Expanded = false
	delete(f.metadata, stage)
	f.edge(release, stage, "stage")
	facts := map[string]string{}
	for _, side := range []string{"a", "b"} {
		facts[side] = f.add(side, ""+side+"_facts", map[string]any{"cycle": cycle, "schema_version": "source-fixture"})
		f.edge(facts[side], f.file(side+".parquet", 123), "parquet_shard")
		f.edge(facts[side], release, "source_release")
	}
	f.edge(facts["b"], f.file("historical-raw", 999), "raw_source")
	calc := f.add("flow", "flow_calculation", flow.Result{SchemaVersion: flow.Version, Cycle: cycle, Policy: flow.Policy{Matcher: flow.MatchPolicy}})
	f.edge(calc, facts["a"], "receiver_facts")
	f.edge(calc, facts["b"], "sender_facts")
	f.edge(calc, release, "coordinated_release")
	for _, role := range []string{"receiver_observations", "sender_observations", "reconciliation_assertions"} {
		f.edge(calc, f.file(role, 42), role)
	}
	master := f.add("master", "classic_facts", map[string]any{"cycle": cycle, "schema_version": "source-fixture"})
	f.edge(master, f.file("master-facts", 10), "facts")
	f.edge(master, release, "source_release")
	bundle := f.add("bundle", "flow_bundle", flow.Bundle{SchemaVersion: flow.BundleVersion, Cycle: cycle})
	f.edge(bundle, calc, "flow_calculation")
	f.edge(bundle, facts["a"], "receiver_facts")
	f.edge(bundle, facts["b"], "sender_facts")
	f.edge(bundle, release, "coordinated_release")
	f.edge(bundle, master, "committee_master")
	f.inv.Root = bundle
	return f
}
func hasBlocker(p RecoveryPlan, code string) bool {
	for _, b := range p.Blockers {
		if b.Code == code {
			return true
		}
	}
	return false
}
func rolesFor(p RecoveryPlan, key string) []string {
	for _, n := range p.Nodes {
		if n.NodeID == key {
			return n.Roles
		}
	}
	return nil
}

func TestRecoveryPlanDependencyOrderAndHistoricalGap(t *testing.T) {
	for _, cycle := range []string{"2022", "2024"} {
		t.Run(cycle, func(t *testing.T) {
			f := flowPlanFixture(cycle)
			before, _ := json.Marshal(f.inv)
			p, err := planInventory(context.Background(), f.inv, f.metadata)
			if err != nil || !p.DependencyPlanComplete || p.ExecutionReady || p.Inventory.Complete || p.Inventory.RecoveryReady || len(p.Steps) != 2 {
				t.Fatalf("unexpected plan: %+v %v", p, err)
			}
			if p.Steps[0].Operation != "committee_flow_reconciliation" || p.Steps[1].Operation != "committee_flow_readiness" {
				t.Fatal("not dependency ordered")
			}
			for _, s := range p.Steps {
				if s.Cycle != cycle || s.PolicyEvidence == "" {
					t.Fatal("cycle or policy pins lost")
				}
			}
			for _, name := range []string{"lost-stage", "historical-raw"} {
				if !reflect.DeepEqual(rolesFor(p, f.keys[name]), []string{historyRole}) {
					t.Fatal("historical-only file promoted", name)
				}
			}
			if slices.Contains(rolesFor(p, f.keys["receiver_observations"]), executionRole) {
				t.Fatal("old output became input")
			}
			if !slices.Contains(rolesFor(p, f.keys["a.parquet"]), executionRole) {
				t.Fatal("fact shard not retained")
			}
			for _, code := range []string{"workspace_unadmitted", "replay_build_unbound", "arango_runtime_unbound", "retention_unenforced", "body_verification_required", "isolated_executor_unimplemented", "recovery_comparators_unimplemented"} {
				if !hasBlocker(p, code) {
					t.Fatal("lost blocker", code)
				}
			}
			after, _ := json.Marshal(p.Inventory)
			if string(before) != string(after) {
				t.Fatal("provenance was rewritten")
			}
			again, err := planInventory(context.Background(), f.inv, f.metadata)
			if err != nil || !reflect.DeepEqual(p, again) {
				t.Fatal("replay differs", err)
			}
			// Inventory order is evidence; the dependency order does not depend on it.
			slices.Reverse(f.inv.Edges)
			again, err = planInventory(context.Background(), f.inv, f.metadata)
			if err != nil || !reflect.DeepEqual(p.Steps, again.Steps) {
				t.Fatal("unstable step order", err)
			}
		})
	}
}

func TestRecoveryPlanRejectsIncompleteAndAmbiguousDependencies(t *testing.T) {
	for _, tc := range []struct {
		name, code string
		change     func(*planFixture)
	}{
		{"missing-pin", "missing_exact_pin", func(f *planFixture) {
			for i := range f.inv.Nodes {
				if f.inv.Nodes[i].Key == f.keys["a"] {
					f.inv.Nodes[i].Reference.SHA256 = ""
				}
			}
		}},
		{"unavailable", "required_file_unavailable", func(f *planFixture) {
			for i := range f.inv.Nodes {
				if f.inv.Nodes[i].Key == f.keys["a.parquet"] {
					f.inv.Nodes[i].Expanded = false
					f.inv.Nodes[i].Verification = "missing"
				}
			}
		}},
		{"unknown-role", "unclassified_dependency", func(f *planFixture) { f.edge(f.keys["flow"], f.keys["a"], "new-input") }},
		{"missing-role", "ambiguous_dependency_membership", func(f *planFixture) {
			f.inv.Edges = slices.DeleteFunc(f.inv.Edges, func(e Edge) bool { return e.From == f.keys["flow"] && e.Role == "receiver_facts" })
		}},
		{"duplicate-role", "ambiguous_dependency_membership", func(f *planFixture) { f.edge(f.keys["flow"], f.keys["a"], "receiver_facts") }},
		{"unknown-source-role", "unclassified_source_dependency", func(f *planFixture) { f.edge(f.keys["a"], f.keys["historical-raw"], "new-backing") }},
		{"cycle", "dependency_cycle", func(f *planFixture) { f.edge(f.keys["flow"], f.keys["bundle"], "back-edge") }},
		{"unknown-producer", "unsupported_producer", func(f *planFixture) {
			for i := range f.inv.Nodes {
				if f.inv.Nodes[i].Key == f.inv.Root {
					f.inv.Nodes[i].Schema = "future"
				}
			}
		}},
		{"traversal", "missing_exact_pin", func(f *planFixture) {
			for i := range f.inv.Nodes {
				if f.inv.Nodes[i].Key == f.keys["a"] {
					f.inv.Nodes[i].Reference.Path = "../escape"
				}
			}
		}},
		{"current", "missing_exact_pin", func(f *planFixture) {
			for i := range f.inv.Nodes {
				if f.inv.Nodes[i].Key == f.keys["a"] {
					f.inv.Nodes[i].Reference.Path = "facts/current.json"
				}
			}
		}},
		{"changed-metadata", "unverified_metadata", func(f *planFixture) { f.metadata[f.keys["flow"]] = []byte("changed") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := flowPlanFixture("2024")
			tc.change(f)
			p, err := planInventory(context.Background(), f.inv, f.metadata)
			if err != nil || p.DependencyPlanComplete || !hasBlocker(p, tc.code) {
				t.Fatalf("failure not explicit: %+v %v", p, err)
			}
		})
	}
}

func TestRecoveryPlanFutureProducerVersion(t *testing.T) {
	f := newPlanFixture()
	f.inv.Root = f.add("effective", "effective", ie.Manifest{SchemaVersion: ie.ManifestSchemaVersion, Cycle: "2024", PublisherVersion: "future", CalculationVersion: ie.ContractVersion})
	p, err := planInventory(context.Background(), f.inv, f.metadata)
	if err != nil || !hasBlocker(p, "unsupported_producer_contract") || p.DependencyPlanComplete {
		t.Fatal("future producer accepted", err)
	}
}

func TestRecoveryPlanDoesNotHashOrReadSourceBodies(t *testing.T) {
	root := t.TempDir()
	start, locators := releaseFixture(t, root)
	// A valid outer generation may have unavailable downstream evidence. Its
	// partial plan must still preserve the inventory and must not open blobs.
	id := digest([]byte("missing-child"))
	g := fg.SharedGeneration{SchemaVersion: "legal-tender.funding-evidence-generation.shared-conduits.v1", Base: fg.Result{GenerationID: id, Cycle: "2024"}, BaseSHA256: id}
	g.Extension.Projection.ID, g.Extension.Projection.SHA256 = id, id
	g.Extension.Calculation.ID, g.Extension.Calculation.SHA256 = id, id
	g.GenerationID = identity(g)
	r := fixture(t, root, "generation.json", g)
	r.Kind = "shared_generation"
	r.ID = g.GenerationID
	in := Inputs{Version: InputsVersion, Generation: r, Locators: append(locators, start)}
	sha := identity(in)
	p, err := Plan(context.Background(), in, sha, Options{StorageRoot: root, BuildSHA256: id})
	if err != nil || p.ExecutionReady || p.DependencyPlanComplete || len(p.Blockers) == 0 {
		t.Fatal("missing dependency accepted", err)
	}
	if _, err = Plan(context.Background(), in, sha, Options{StorageRoot: root, BuildSHA256: id, HashBlobs: true}); err == nil {
		t.Fatal("blob scan allowed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = Plan(ctx, in, sha, Options{StorageRoot: root, BuildSHA256: id}); err != context.Canceled {
		t.Fatal("cancellation", err)
	}
}

func TestRecoveryReferenceProofIncludesOnlySelectedBodies(t *testing.T) {
	f := newPlanFixture()
	archive := f.file("cm.zip", 50)
	member := f.file("cm.txt.zst", 25)
	unrelated := f.file("other-cycle.zip", 500)
	byKey := func(key string) Node {
		for _, n := range f.inv.Nodes {
			if n.Key == key {
				return n
			}
		}
		panic("absent node")
	}
	a, m := byKey(archive), byKey(member)
	memberSHA := digest([]byte("member"))
	r := rel.ReleaseManifest{SchemaVersion: rel.ManifestSchemaVersion, Artifacts: []rel.PublishedArtifact{{SelectedSource: rel.SelectedSource{SourceID: "fec:cm:2024"}, StorageKey: a.Reference.Path, SHA256: a.Reference.SHA256, ByteCount: 50}}, StagedOutputs: []rel.StagedOutput{{SourceID: "fec:cm:2024", Period: "2024", SelectionKind: "member", Selection: "cm.txt", StorageKey: m.Reference.Path, CompressedSHA256: m.Reference.SHA256, CompressedByteCount: 25, UncompressedSHA256: memberSHA, UncompressedByteCount: 40, SourceArtifactSHA256: a.Reference.SHA256}}}
	release := f.add("release", "release", r)
	f.edge(release, archive, "raw_source")
	f.edge(release, member, "staged_source")
	f.edge(release, unrelated, "raw_source")
	owner := f.add("generation", "generation", fg.Result{SchemaVersion: fg.Version, Cycle: "2024"})
	releaseNode := byKey(release)
	proof := occ.ClassicReferenceProof{Policy: occ.ClassicReferencePolicy, Dataset: "committee-master", Cycle: "2024", SourceRelease: occ.ReferenceIdentity{ID: releaseNode.Reference.ID, SHA256: releaseNode.Reference.SHA256}, TargetRelease: occ.ReferenceIdentity{ID: releaseNode.Reference.ID, SHA256: releaseNode.Reference.SHA256}, SourceArchive: occ.ReferenceArchive{SHA256: a.Reference.SHA256, Bytes: 50}, TargetArchive: occ.ReferenceArchive{SHA256: a.Reference.SHA256, Bytes: 50}, Member: "cm.txt", MemberSHA256: memberSHA, MemberBytes: 40}
	newPlanner := func() *planner {
		p := &planner{metadata: f.metadata, nodes: map[string]Node{}, roles: map[string]map[string]bool{}, blockers: map[PlanBlocker]bool{}, out: RecoveryPlan{DependencyPlanComplete: true}}
		for _, n := range f.inv.Nodes {
			p.nodes[n.Key] = n
			p.roles[n.Key] = map[string]bool{historyRole: true}
		}
		return p
	}
	p := newPlanner()
	bindings := p.proofInputs(owner, proof)
	if len(bindings) != 4 || len(p.blockers) != 0 || !p.hasRole(archive, executionRole) || !p.hasRole(member, executionRole) || p.hasRole(unrelated, executionRole) {
		t.Fatalf("wrong selected backing: %+v %+v", bindings, p.blockers)
	}
	proof.TargetArchive.SHA256 = digest([]byte("wrong"))
	p = newPlanner()
	p.proofInputs(owner, proof)
	if p.out.DependencyPlanComplete {
		t.Fatal("wrong archive pin accepted")
	}
	proof.TargetArchive = proof.SourceArchive
	r.StagedOutputs = append(r.StagedOutputs, r.StagedOutputs[0])
	b, _ := json.Marshal(r)
	f.metadata[release] = b
	for i := range f.inv.Nodes {
		if f.inv.Nodes[i].Key == release {
			f.inv.Nodes[i].Reference.SHA256 = digest(b)
		}
	}
	proof.SourceRelease.SHA256, proof.TargetRelease.SHA256 = digest(b), digest(b)
	p = newPlanner()
	p.proofInputs(owner, proof)
	if p.out.DependencyPlanComplete {
		t.Fatal("ambiguous selected member accepted")
	}
}

func TestRecoveryProducerRegistry(t *testing.T) {
	for _, kind := range []string{"generation", "shared_generation", "receipt_projection", "shared_projection", "participants", "references", "topology", "conduits", "flow_calculation", "flow_bundle", "effective", "resolution", "resolved_aggregate", "resolved_bundle", "future"} {
		if _, ok := producer(kind, "future"); ok {
			t.Fatal("unknown schema accepted", kind)
		}
	}
	s, ok := producer("topology", refs.TopologyVersion)
	if !ok || s.dependencies["endpoints"].output != true || s.dependencies["reference_join"].output {
		t.Fatal("topology input/output confusion")
	}
}
