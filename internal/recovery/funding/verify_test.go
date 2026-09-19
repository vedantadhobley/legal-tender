package funding

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"syscall"
	"testing"
)

func verificationFixture(t *testing.T, cycle string) (RecoveryPlan, VerifyOptions) {
	t.Helper()
	f := flowPlanFixture(cycle)
	root := t.TempDir()
	for i, n := range f.inv.Nodes {
		if n.Verification == "missing" || n.Key == f.keys["historical-raw"] {
			continue
		}
		b := f.metadata[n.Key]
		if n.Reference.Kind == "file" {
			b = []byte(strings.Repeat("x", int(*n.Reference.Bytes)))
			f.inv.Nodes[i].Reference.SHA256 = digest(b)
		}
		writeFixture(t, root, n.Reference.Path, b)
	}
	p, err := planInventory(context.Background(), f.inv, f.metadata)
	if err != nil || !p.DependencyPlanComplete {
		t.Fatalf("fixture plan: %v", err)
	}
	return p, VerifyOptions{Options: Options{StorageRoot: root, BuildSHA256: p.BuildSHA256}, Workers: 4, MaxBytes: 1 << 20}
}

func TestRecoveryVerificationRolesReplayAndIsolation(t *testing.T) {
	for _, cycle := range []string{"2022", "2024"} {
		t.Run(cycle, func(t *testing.T) {
			p, o := verificationFixture(t, cycle)
			before, _ := json.Marshal(p)
			first, err := verifyPlannedFiles(context.Background(), p, o)
			if err != nil || !first.Complete || first.RecoveryReady || first.HistoryComplete || first.VerifiedBytes != first.TotalBytes {
				t.Fatalf("verification: %+v %v", first, err)
			}
			for _, f := range first.Files {
				if strings.Contains(f.Path, "historical-raw") || strings.Contains(f.Path, "lost-stage") {
					t.Fatal("historical-only body selected")
				}
			}
			for _, workers := range []int{1, 8} {
				o.Workers = workers
				again, e := verifyPlannedFiles(context.Background(), p, o)
				if e != nil || !reflect.DeepEqual(first, again) {
					t.Fatal("worker-varied replay differs", e)
				}
			}
			after, _ := json.Marshal(p)
			if string(before) != string(after) {
				t.Fatal("historical plan changed")
			}
			// Comparison bodies are separately verified, not trusted as old success.
			for _, f := range first.Files {
				if slices.Contains(f.Roles, comparisonRole) && strings.HasPrefix(f.Path, "data/") {
					writeFixture(t, o.StorageRoot, f.Path, []byte(strings.Repeat("y", int(f.ExpectedBytes))))
					break
				}
			}
			bad, e := verifyPlannedFiles(context.Background(), p, o)
			if e != nil || bad.Complete || bad.Counts["sha256_mismatch"] != 1 {
				t.Fatal("corrupt comparison accepted", e)
			}
		})
	}
}

func TestRecoveryVerificationAdmissionAndMissing(t *testing.T) {
	p, o := verificationFixture(t, "2024")
	files, total, err := selectedFiles(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(o.StorageRoot, files[0].Path)); err != nil {
		t.Fatal(err)
	}
	o.MaxBytes = total - 1
	blocked, err := verifyPlannedFiles(context.Background(), p, o)
	if err != nil || blocked.Complete || blocked.State != "byte_ceiling_exceeded" || blocked.Counts["not_checked"] != uint64(len(files)) {
		t.Fatal("hash before byte admission", err)
	}
	o.MaxBytes = total
	missing, err := verifyPlannedFiles(context.Background(), p, o)
	if err != nil || missing.Complete || missing.Counts["missing"] != 1 {
		t.Fatal("missing input accepted", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	v, err := verifyPlannedFiles(ctx, p, o)
	if !errors.Is(err, context.Canceled) || v.Complete || v.State != "cancelled" || v.Counts["not_checked"] != uint64(len(files)) {
		t.Fatal("cancellation accepted", err)
	}
}

func TestRecoveryVerificationSelectedPinGuards(t *testing.T) {
	for _, change := range []func(*RecoveryPlan){
		func(p *RecoveryPlan) { p.Inventory.Nodes[0].Reference.Path = "../escape" },
		func(p *RecoveryPlan) { p.Inventory.Nodes[0].Reference.Path = "facts/current.json" },
		func(p *RecoveryPlan) { p.Inventory.Nodes[0].Reference.SHA256 = "" },
		func(p *RecoveryPlan) { p.Inventory.Nodes[0].ObservedBytes = nil },
	} {
		p, _ := verificationFixture(t, "2024")
		// Put a known selected metadata node first (inventory ordering is hashed
		// evidence, not a semantic selection rule).
		for i, n := range p.Inventory.Nodes {
			if n.Reference.Kind == "release" {
				p.Inventory.Nodes[0], p.Inventory.Nodes[i] = p.Inventory.Nodes[i], p.Inventory.Nodes[0]
				break
			}
		}
		change(&p)
		if _, _, err := selectedFiles(p); err == nil {
			t.Fatal("unusable selected pin accepted")
		}
	}
	p, _ := verificationFixture(t, "2024")
	files, total, _ := selectedFiles(p)
	// Equal digests at distinct paths must not be deduplicated away.
	var count int
	for _, f := range files {
		if f.ExpectedSHA256 == digest([]byte(strings.Repeat("x", 123))) {
			count++
		}
	}
	if count != 2 {
		t.Fatal("distinct copies deduplicated by hash")
	}
	// Repeated path references retain all roles but consume bytes once.
	p.Nodes = append(p.Nodes, p.Nodes[0])
	again, bytes, err := selectedFiles(p)
	if err != nil || total != bytes || !reflect.DeepEqual(files, again) {
		t.Fatal("same path counted twice", err)
	}
}

func TestRecoveryVerificationPublicBoundary(t *testing.T) {
	root := t.TempDir()
	in := Inputs{Version: InputsVersion, Generation: Reference{Kind: "generation", ID: digest([]byte("id")), SHA256: digest([]byte("bytes")), Path: "missing.json"}}
	o := VerifyOptions{Options: Options{StorageRoot: root, BuildSHA256: digest([]byte("build"))}, Workers: 1, MaxBytes: 100}
	v, err := VerifyFiles(context.Background(), in, digest([]byte("input")), o)
	if err != nil || v.Complete || v.State != "dependency_plan_incomplete" || v.HistoryComplete {
		t.Fatal("bad dependency plan accepted", err)
	}
	for _, workers := range []int{0, 9, -1} {
		o.Workers = workers
		if _, err := VerifyFiles(context.Background(), in, digest([]byte("input")), o); err == nil {
			t.Fatal("invalid workers accepted")
		}
	}
	o.Workers = 1
	o.MaxBytes = 0
	if _, err := VerifyFiles(context.Background(), in, digest([]byte("input")), o); err == nil {
		t.Fatal("unbounded verification accepted")
	}
	o.MaxBytes = 100
	o.HashBlobs = true
	if _, err := VerifyFiles(context.Background(), in, digest([]byte("input")), o); err == nil {
		t.Fatal("full-history hashing accepted")
	}
}

func TestRecoveryVerificationFileSafety(t *testing.T) {
	for _, tc := range []struct {
		name, state string
		mutate      func(*testing.T, string)
	}{
		{"same-size-corruption", "sha256_mismatch", func(t *testing.T, r string) { writeFixture(t, r, "data/file", []byte("xxxx")) }},
		{"truncated", "size_mismatch", func(t *testing.T, r string) { writeFixture(t, r, "data/file", []byte("x")) }},
		{"symlink", "invalid_file_type", func(t *testing.T, r string) {
			mustRemove(t, r+"/data/file")
			if err := os.Symlink("/etc/passwd", r+"/data/file"); err != nil {
				t.Fatal(err)
			}
		}},
		{"fifo", "invalid_file_type", func(t *testing.T, r string) {
			mustRemove(t, r+"/data/file")
			if err := syscall.Mkfifo(r+"/data/file", 0600); err != nil {
				t.Fatal(err)
			}
		}},
		{"parent-symlink", "invalid_parent_directory", func(t *testing.T, r string) {
			if err := os.Rename(r+"/data", r+"/alias"); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("alias", r+"/data"); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeFixture(t, dir, "data/file", []byte("data"))
			tc.mutate(t, dir)
			r, err := os.OpenRoot(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			v := verifyFile(context.Background(), r, VerifiedFile{Path: "data/file", ExpectedSHA256: digest([]byte("data")), ExpectedBytes: 4}, make([]byte, 128<<10))
			if v.State != tc.state {
				t.Fatalf("%+v", v)
			}
		})
	}
}

func mustRemove(t *testing.T, file string) {
	t.Helper()
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
}

type verificationContext struct {
	context.Context
	check func() error
}

func (c verificationContext) Err() error { return c.check() }

func TestRecoveryVerificationConcurrentReplacementAndCancel(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		dir := t.TempDir()
		writeFixture(t, dir, "file", []byte("data"))
		root, err := os.OpenRoot(dir)
		if err != nil {
			t.Fatal(err)
		}
		calls := 0
		ctx := verificationContext{Context: context.Background(), check: func() error {
			calls++
			if calls == 3 {
				if cancel {
					return context.Canceled
				}
				writeFixture(t, dir, "new-file", []byte("data"))
				if err := os.Rename(dir+"/new-file", dir+"/file"); err != nil {
					t.Fatal(err)
				}
			}
			return nil
		}}
		v := verifyFile(ctx, root, VerifiedFile{Path: "file", ExpectedSHA256: digest([]byte("data")), ExpectedBytes: 4}, make([]byte, 128<<10))
		root.Close()
		want := "file_changed_during_verification"
		if cancel {
			want = "cancelled"
		}
		if v.State != want {
			t.Fatalf("%+v", v)
		}
	}
}

func TestRecoveryVerificationGrowingFileReadIsBounded(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir, "file", []byte("data"))
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	calls := 0
	ctx := verificationContext{Context: context.Background(), check: func() error {
		calls++
		if calls == 2 {
			writeFixture(t, dir, "file", []byte(strings.Repeat("x", 1<<20)))
		}
		return nil
	}}
	v := verifyFile(ctx, root, VerifiedFile{Path: "file", ExpectedSHA256: digest([]byte("data")), ExpectedBytes: 4}, make([]byte, 128<<10))
	if v.State != "file_changed_during_verification" || v.ObservedBytes != 5 {
		t.Fatalf("unbounded growth read: %+v", v)
	}
}
