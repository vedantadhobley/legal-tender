package funding

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"

	rel "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
)

func fixture(t *testing.T, root, name string, value any) Reference {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	writeFixture(t, root, name, b)
	return Reference{ID: digest(b), SHA256: digest(b), Path: name}
}
func writeFixture(t *testing.T, root, name string, b []byte) {
	t.Helper()
	file := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(file), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, b, 0600); err != nil {
		t.Fatal(err)
	}
}
func walkFixture(t *testing.T, root string, start Reference, locators []Reference, hash bool) (Result, error) {
	t.Helper()
	r, err := os.OpenRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	w := walker{ctx: context.Background(), root: r, options: Options{HashBlobs: hash}, locators: map[string]Reference{}, indices: map[string]int{}, paths: map[string]Reference{}, edges: map[Edge]bool{}, requirements: map[Requirement]bool{}, out: Result{Counts: map[string]uint64{}}}
	for _, l := range locators {
		w.locators[refKey(l)] = l
	}
	w.out.Root, err = w.add("", "", start)
	if err != nil {
		return Result{}, err
	}
	for i := 0; i < len(w.pending); i++ {
		if err = w.visit(w.pending[i]); err != nil {
			return Result{}, err
		}
	}
	return w.finish(), nil
}
func releaseFixture(t *testing.T, root string) (Reference, []Reference) {
	t.Helper()
	plan := fixture(t, root, "operational/plan.json", rel.ReleasePlan{SchemaVersion: rel.PlanSchemaVersion})
	plan.Kind = "plan"
	blob := []byte("source bytes")
	writeFixture(t, root, "raw/source", blob)
	acq := fixture(t, root, "operational/acquisition.json", rel.AcquisitionResult{SchemaVersion: rel.AcquisitionSchemaVersion, PlanSHA256: plan.SHA256})
	acq.Kind = "acquisition"
	stage := fixture(t, root, "operational/stage.json", rel.StageResult{SchemaVersion: rel.StageSchemaVersion, PlanSHA256: plan.SHA256, AcquisitionSHA256: acq.SHA256})
	stage.Kind = "stage"
	id := "fec-" + digest([]byte("release"))
	manifest := fixture(t, root, "release.json", rel.ReleaseManifest{SchemaVersion: rel.ManifestSchemaVersion, ReleaseID: id, PlanSHA256: plan.SHA256, AcquisitionSHA256: acq.SHA256, StageSHA256: stage.SHA256, Artifacts: []rel.PublishedArtifact{{StorageKey: "raw/source", SHA256: digest(blob), ByteCount: int64(len(blob))}}, Checks: []rel.ReleaseCheck{{ID: "prior-check", Passed: true}}})
	manifest.ID = id
	manifest.Kind = "release"
	return manifest, []Reference{plan, acq, stage}
}

func TestTypedClosureAndVerificationModes(t *testing.T) {
	root := t.TempDir()
	start, locators := releaseFixture(t, root)
	first, err := walkFixture(t, root, start, locators, false)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Complete || first.AllFilesHashed || first.RecoveryReady || len(first.Nodes) != 5 || first.Counts["sha256_verified"] != 4 || first.Counts["size_verified_not_hashed"] != 1 {
		t.Fatalf("unexpected inventory: %+v", first)
	}
	second, err := walkFixture(t, root, start, locators, false)
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatalf("nondeterministic replay: %v", err)
	}
	full, err := walkFixture(t, root, start, locators, true)
	if err != nil || !full.Complete || !full.AllFilesHashed || full.RecoveryReady || full.ID == first.ID {
		t.Fatalf("full hash result: %+v %v", full, err)
	}
	for _, n := range full.Nodes {
		if n.Reference.Kind == "release" && !reflect.DeepEqual(n.PriorChecks, []string{"prior-check"}) {
			t.Fatal("lost prior attestation")
		}
	}
	// Same-length corruption is deliberately invisible to presence/size mode.
	writeFixture(t, root, "raw/source", []byte("broken bytes"))
	presence, err := walkFixture(t, root, start, locators, false)
	if err != nil || !presence.Complete {
		t.Fatalf("presence: %v", err)
	}
	corrupt, err := walkFixture(t, root, start, locators, true)
	if err != nil || corrupt.Complete || corrupt.Counts["sha256_mismatch"] != 1 {
		t.Fatalf("corruption missed: %+v %v", corrupt, err)
	}
}

func TestMissingUnlocatedUnknownAndChanged(t *testing.T) {
	for _, tc := range []struct {
		name, state string
		change      func(*testing.T, string, *Reference, *[]Reference)
	}{
		{"missing", "missing", func(t *testing.T, r string, _ *Reference, _ *[]Reference) {
			if err := os.Remove(filepath.Join(r, "raw/source")); err != nil {
				t.Fatal(err)
			}
		}},
		{"size", "size_mismatch", func(t *testing.T, r string, _ *Reference, _ *[]Reference) { writeFixture(t, r, "raw/source", nil) }},
		{"missing-control-locator", "missing", func(_ *testing.T, _ string, _ *Reference, l *[]Reference) { *l = nil }},
		{"sha", "sha256_mismatch", func(t *testing.T, r string, _ *Reference, _ *[]Reference) {
			writeFixture(t, r, "release.json", []byte("{}"))
		}},
		{"version", "invalid_manifest", func(t *testing.T, r string, s *Reference, _ *[]Reference) {
			b := []byte(`{"schema_version":"new-version"}`)
			writeFixture(t, r, s.Path, b)
			s.SHA256 = digest(b)
		}},
		{"unknown-field", "invalid_manifest", func(t *testing.T, r string, s *Reference, _ *[]Reference) {
			b, err := os.ReadFile(filepath.Join(r, s.Path))
			if err != nil {
				t.Fatal(err)
			}
			b = append(b[:len(b)-1], []byte(`,"new_dependency":"do-not-ignore"}`)...)
			writeFixture(t, r, s.Path, b)
			s.SHA256 = digest(b)
		}},
		{"symlink", "invalid_file_type", func(t *testing.T, r string, _ *Reference, _ *[]Reference) {
			p := filepath.Join(r, "raw/source")
			if err := os.Remove(p); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("/etc/passwd", p); err != nil {
				t.Fatal(err)
			}
		}},
		{"fifo", "invalid_file_type", func(t *testing.T, r string, _ *Reference, _ *[]Reference) {
			p := filepath.Join(r, "raw/source")
			if err := os.Remove(p); err != nil {
				t.Fatal(err)
			}
			if err := syscall.Mkfifo(p, 0600); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			s, l := releaseFixture(t, root)
			tc.change(t, root, &s, &l)
			v, err := walkFixture(t, root, s, l, false)
			if err != nil || v.Complete || v.Counts[tc.state] == 0 {
				t.Fatalf("wrong failure: %+v %v", v, err)
			}
		})
	}
}

func TestPathsAndInputPins(t *testing.T) {
	u, err := walkFixture(t, t.TempDir(), Reference{Kind: "topology", ID: digest([]byte("topology")), SHA256: digest([]byte("bytes"))}, nil, false)
	if err != nil || u.Counts["unlocated"] != 1 || u.Complete {
		t.Fatalf("unlocated publication: %+v %v", u, err)
	}
	for _, p := range []string{"", "/tmp/a", "../a", "a/../b", "a//b", "./a", "a\\b", "a/current/manifest.json", "a/current.json", "."} {
		if relative(p) {
			t.Errorf("unsafe path accepted: %q", p)
		}
	}
	root := t.TempDir()
	in := Inputs{Version: InputsVersion, Generation: Reference{Kind: "generation", ID: digest([]byte("id")), SHA256: digest([]byte("body")), Path: "generation.json"}}
	r := fixture(t, root, "inputs.json", in)
	if _, err := ReadInputs(filepath.Join(root, r.Path), r.SHA256); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadInputs(filepath.Join(root, r.Path), strings.Repeat("0", 64)); err == nil {
		t.Fatal("input substitution accepted")
	}
	in.Locators = []Reference{in.Generation}
	if validateInputs(in) == nil {
		t.Fatal("duplicate locator accepted")
	}
	in.Locators = nil
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Inspect(ctx, in, r.SHA256, Options{StorageRoot: root, BuildSHA256: r.SHA256}); err != context.Canceled {
		t.Fatalf("cancellation: %v", err)
	}
}

func TestConflictingPinsAndCycleDetection(t *testing.T) {
	root := t.TempDir()
	start, locators := releaseFixture(t, root)
	locators[0].SHA256 = digest([]byte("replacement"))
	if _, err := walkFixture(t, root, start, locators, false); err == nil {
		t.Fatal("locator changed parent pin")
	}
	nodes := []Node{{Key: "a"}, {Key: "b"}, {Key: "c"}}
	if dependencyCycle(nodes, []Edge{{From: "a", To: "b"}, {From: "a", To: "c"}, {From: "b", To: "c"}}) {
		t.Fatal("diamond mistaken for cycle")
	}
	if !dependencyCycle(nodes, []Edge{{From: "a", To: "b"}, {From: "b", To: "a"}}) {
		t.Fatal("cycle missed")
	}
}

func TestUnpinnedReferenceLearnsOnlyExactPublishedPin(t *testing.T) {
	r := Reference{Kind: "release", ID: "fec-" + digest([]byte("one"))}
	w := walker{indices: map[string]int{}, locators: map[string]Reference{}, paths: map[string]Reference{}, edges: map[Edge]bool{}}
	key, err := w.add("", "", r)
	if err != nil {
		t.Fatal(err)
	}
	r.SHA256 = digest([]byte("published bytes"))
	second, err := w.add("parent", "dependency", r)
	if err != nil {
		t.Fatal(err)
	}
	if key != second || len(w.out.Nodes) != 1 || w.out.Nodes[0].Reference.SHA256 != r.SHA256 || len(w.pending) != 2 {
		t.Fatal("did not resolve exact pin")
	}
	r.SHA256 = digest([]byte("other bytes"))
	if _, err := w.add("parent", "dependency", r); err == nil {
		t.Fatal("conflicting pins accepted")
	}
}
