package flowreconciliation

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

func publicationFixture(t *testing.T) (Options, Inputs, occurrence.ScheduleAColumnarManifest, occurrence.ScheduleBColumnarManifest) {
	t.Helper()
	root := t.TempDir()
	o := Options{StorageRoot: root, OutputRoot: filepath.Join(root, PublicationBase), Cycle: "2024", Workers: 1, Progress: func(string) {}}
	f := FactReference{FactSetID: strings.Repeat("a", 64), ManifestSHA256: strings.Repeat("b", 64), SourceReleaseID: "fec-" + strings.Repeat("c", 64), SourceArtifactSHA256: strings.Repeat("d", 64)}
	i := Inputs{ReleaseID: f.SourceReleaseID, ReleaseSHA256: strings.Repeat("e", 64), A: f, B: f}
	i.B.Facts = 6
	s := writeBShard(t, root, 1)
	b := occurrence.ScheduleBColumnarManifest{Cycle: o.Cycle, Shards: []occurrence.ScheduleBColumnarShard{{StorageKey: s.key, FirstSourceRowOrdinal: s.first, LastSourceRowOrdinal: s.last, Facts: s.rows, Bytes: s.bytes}}}
	return o, i, occurrence.ScheduleAColumnarManifest{Cycle: o.Cycle}, b
}

func TestPublishLoadedAndNoScanReuse(t *testing.T) {
	ctx := context.Background()
	o, inputs, a, b := publicationFixture(t)
	r, err := publishLoaded(ctx, o, inputs, a, b)
	if err != nil {
		t.Fatal(err)
	}
	if r.A.Total.Rows != 0 || r.B.Total.Rows != 6 || r.B.Selected.Rows != 3 || r.Assertions.RecordCount != 3 || r.GraphEligible {
		t.Fatal("lost empty/one-sided grain", r)
	}
	current := filepath.Join(o.OutputRoot, "current", o.Cycle+".json")
	before, err := os.ReadFile(current)
	if err != nil {
		t.Fatal(err)
	}
	loaded, digest, err := readPublication(ctx, o.StorageRoot, current)
	if err != nil || !reflect.DeepEqual(loaded, r) || digest != hashBytes(before) {
		t.Fatal("load differs", err)
	}
	// This seam receives already verified inputs. An unusable scan manifest
	// proves reuse never enters the Parquet row reader; the public boundary still
	// rehashes source backing before entering this seam.
	b.Shards[0].StorageKey = "must-not-open.parquet"
	o.Workers = 4
	var progress []string
	o.Progress = func(s string) { progress = append(progress, s) }
	reused, err := publishLoaded(ctx, o, inputs, a, b)
	if err != nil || !reflect.DeepEqual(r, reused) {
		t.Fatal("reuse scanned or changed result", err)
	}
	if len(progress) != 1 || !strings.Contains(progress[0], "no source-row scan") {
		t.Fatal(progress)
	}
	after, _ := os.ReadFile(current)
	if !bytes.Equal(before, after) {
		t.Fatal("worker-dependent publication bytes")
	}
	// A missing source ancestry blocks the exported consumer despite intact
	// compact artifacts and the internal reuse seam succeeding.
	if _, _, err := LoadPublished(ctx, o.StorageRoot, current); err == nil {
		t.Fatal("missing source backing accepted")
	}
	for _, change := range []func(*Inputs){func(i *Inputs) { i.B.ManifestSHA256 = strings.Repeat("f", 64) }, func(i *Inputs) { i.ReleaseSHA256 = strings.Repeat("f", 64) }} {
		changed := inputs
		change(&changed)
		if _, err := publishLoaded(ctx, o, changed, a, b); err == nil {
			t.Fatal("changed input incorrectly reused")
		}
		after, _ = os.ReadFile(current)
		if !bytes.Equal(before, after) {
			t.Fatal("failed new calculation moved pointer")
		}
	}
	policy := currentPolicy()
	policy.Matcher += ".changed"
	if calculationIdentity(o.Cycle, inputs, policy) == r.CalculationSetID {
		t.Fatal("policy omitted from identity")
	}
	if err := os.WriteFile(filepath.Join(o.OutputRoot, r.B.Observations.StorageKey), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := publishLoaded(ctx, o, inputs, a, b); err == nil {
		t.Fatal("corrupt evidence reused")
	}
	after, _ = os.ReadFile(current)
	if !bytes.Equal(before, after) {
		t.Fatal("corruption moved pointer")
	}
}

func TestPublicationRecoveryConcurrencyAndCancellation(t *testing.T) {
	o, inputs, a, b := publicationFixture(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	errors := make(chan error, 4)
	for range 4 {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := publishLoaded(ctx, o, inputs, a, b); errors <- err }()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	current := filepath.Join(o.OutputRoot, "current", o.Cycle+".json")
	before, _ := os.ReadFile(current)
	// Simulate interruption between immutable publication and pointer update.
	if err := os.Remove(current); err != nil {
		t.Fatal(err)
	}
	b.Shards[0].StorageKey = "must-not-open.parquet"
	if _, err := publishLoaded(ctx, o, inputs, a, b); err != nil {
		t.Fatal("failed recovery", err)
	}
	after, _ := os.ReadFile(current)
	if !bytes.Equal(before, after) {
		t.Fatal("recovery changed immutable result")
	}
	unlock, err := publicationLock(ctx, o.StorageRoot, PublicationBase, o.Cycle)
	if err != nil {
		t.Fatal(err)
	}
	wait, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if _, err := publicationLock(wait, o.StorageRoot, PublicationBase, o.Cycle); err == nil {
		t.Fatal("ignored lock cancellation")
	}
	unlock()
	if _, err := publishLoaded(wait, o, inputs, a, b); err == nil {
		t.Fatal("ignored cancellation")
	}
	if err := writePublicationJSON(ctx, filepath.Join(o.OutputRoot, "manifests", calculationIdentity(o.Cycle, inputs, currentPolicy())+".json"), []byte("replace"), true); err == nil {
		t.Fatal("overwrote immutable result")
	}
	if err := os.WriteFile(current, append(before, ' '), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readPublication(ctx, o.StorageRoot, current); err == nil {
		t.Fatal("accepted pointer byte drift")
	}
}

func TestPublicationHeaderRejectsUnsafeIdentities(t *testing.T) {
	o, i, a, b := publicationFixture(t)
	r, err := publishLoaded(context.Background(), o, i, a, b)
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*Result){
		func(v *Result) { v.GraphEligible = true },
		func(v *Result) { v.Cycle = "2023" },
		func(v *Result) {
			v.Input.A.FactSetID = "../../outside"
			v.CalculationSetID = calculationIdentity(v.Cycle, v.Input, v.Policy)
		},
		func(v *Result) { v.A.Observations.StorageKey = "../outside" },
		func(v *Result) {
			v.Policy.Matcher = "guess"
			v.CalculationSetID = calculationIdentity(v.Cycle, v.Input, v.Policy)
		},
	} {
		v := r
		change(&v)
		if err := validateResultHeader(v); err == nil {
			t.Fatal("unsafe header accepted")
		}
	}
	if _, err := inside(o.StorageRoot, filepath.Join(o.StorageRoot, "..", "outside")); err == nil {
		t.Fatal("path escape accepted")
	}
	if _, err := Publish(context.Background(), o); err == nil {
		t.Fatal("custom publication root accepted")
	}
}
