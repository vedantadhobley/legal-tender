package fundingwindow_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/app/cli"
	c "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptconduits"
	p "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
	refs "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptreferences"
	gen "github.com/vedantadhobley/legal-tender/internal/projection/arango/fundinggeneration"
	receipt "github.com/vedantadhobley/legal-tender/internal/projection/arango/receiptgraph"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulea"
)

func sharedBody(t *testing.T, cycle string) []byte {
	out := scheduleBody(t, "a", cycle)
	names := []string{}
	for _, col := range schedulea.Columns() {
		names = append(names, col.Name)
	}
	for i := 0; i < 3; i++ {
		v := map[string]string{"sub_id": strconv.Itoa(501 + i), "two_year_transaction_period": cycle, "filing_form": "F3X", "cmte_id": "C00000003", "file_num": "999", "schedule_type": "SA", "line_num": "11AI", "receipt_tp": "15E", "entity_tp": "IND", "tran_id": "SHARED-" + strconv.Itoa(i), "back_ref_tran_id": "SHARED-2", "back_ref_sched_nm": "SA", "contb_receipt_amt": "-5.00"}
		if i == 0 {
			v["contb_receipt_dt"] = cycle + "-12-31 00:00:00"
			if cycle == "2024" {
				v["contb_receipt_dt"] = "2023-01-01 00:00:00"
			}
		}
		if i == 1 {
			v["contb_receipt_amt"] = `\N`
		}
		if i == 2 {
			// A memo date cannot supply the original's observation date.
			v["contb_receipt_dt"] = "2021-06-01 00:00:00"
			v["entity_tp"], v["memo_cd"], v["contbr_id"], v["clean_contbr_id"] = "PAC", "X", "C00000002", "C00000002"
			v["contb_receipt_amt"] = "99.00"
			delete(v, "back_ref_tran_id")
			delete(v, "back_ref_sched_nm")
		}
		out = append(out, copyRow(t, names, v)...)
	}
	return out
}

func TestSharedConduitGeneration(t *testing.T) {
	if os.Getenv("LT_WINDOW_INTEGRATION") != "1" {
		t.Skip("disposable integration stack required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	root := t.TempDir()
	build := executableSHA(t)
	s := makeSourcesWithA(t, root, "shared-conduit-source-fixture-v1", sharedBody)
	base := publishCycle(t, ctx, root, "2024", s)
	dir := filepath.Dir(base.input.Conduits)
	baseline, err := c.Load(base.input.Conduits, base.generation.Receipts.Inputs.Conduits.ID)
	check(t, err)
	topPath := filepath.Join(filepath.Dir(dir), "topology", "manifest.json")
	top, _, err := refs.LoadTopology(topPath, baseline.TopologyID)
	check(t, err)
	calculationDir := filepath.Join(filepath.Dir(dir), "shared")
	updated, err := c.Run(ctx, c.Options{Participants: base.input.Participants, ParticipantID: baseline.ParticipantID, Topology: topPath, TopologyID: top.CalculationID, GroupBaseline: base.input.Conduits, GroupBaselineID: baseline.CalculationID, OutputDirectory: calculationDir, BuildSHA256: build, Workers: 2, RunRows: 2, FanIn: 2, MaxWorkspaceBytes: 16 << 20})
	check(t, err)
	if updated.Groups.ChangedRows != 2 {
		t.Fatal("fixture group did not qualify", updated.Groups)
	}
	path := filepath.Join(calculationDir, "manifest.json")
	op := gen.ReadOptions{Generation: base.input.Generation, GenerationSHA256: base.input.GenerationSHA256, StorageRoot: root, GraphManifest: base.input.GraphManifest, Participants: base.input.Participants, Conduits: base.input.Conduits, Endpoint: fixtureEndpoint, Username: "fixture", Password: "fixture", BuildSHA256: build}
	r, err := gen.OpenReader(ctx, op)
	check(t, err)
	o := receipt.SharedOptions{Calculation: path, CalculationID: updated.CalculationID, BuildSHA256: build, PublicationDirectory: filepath.Join(root, "shared-graph"), LockDirectory: filepath.Join(root, "locks"), ArangoDataDirectory: "/arango-data", Workers: 2, BatchSize: 1, ReserveFreeBytes: 1, MaxFilesystemGrowthBytes: 256 << 20, MaxEncodedBytes: 16 << 20}
	out, err := r.PublishShared(ctx, o)
	check(t, err)
	if out.Publication.Reused || out.Generation.Extension.Added != 2 || out.Generation.AdditionalAmount != "0" || out.Generation.FinancialEligibility || out.Generation.TerminalEligible || !reflect.DeepEqual(out.Generation.Base, base.generation) {
		t.Fatal("shared generation changed base or monetary semantics")
	}
	if out.Generation.Extension.Counts["contributor_appearances"] != 2 || out.Generation.Extension.Counts["reported_conduit_associations"] != 2 || out.Generation.Extension.Counts["reported_receipts"] != 0 {
		t.Fatal("duplicated money graph")
	}
	before, err := os.ReadFile(out.Publication.Manifest)
	check(t, err)
	r, err = gen.OpenReader(ctx, op)
	check(t, err)
	o.Workers, o.BatchSize = 4, 3
	again, err := r.PublishShared(ctx, o)
	check(t, err)
	if !again.Publication.Reused || !reflect.DeepEqual(again.Generation, out.Generation) {
		t.Fatal("fresh varied-layout shared replay differs")
	}
	after, err := os.ReadFile(out.Publication.Manifest)
	check(t, err)
	if !bytes.Equal(before, after) {
		t.Fatal("immutable shared manifest changed")
	}
	t.Setenv("LT_SHARED_FIXTURE_PASSWORD", "fixture")
	args := []string{"pipeline", "fec", "publish-shared-conduit-generation", "--generation", op.Generation, "--expected-generation-sha256", op.GenerationSHA256, "--storage-root", root, "--graph-manifest", op.GraphManifest, "--participants", op.Participants, "--conduits", op.Conduits, "--endpoint", fixtureEndpoint, "--username", "fixture", "--password-env", "LT_SHARED_FIXTURE_PASSWORD", "--shared-conduits", path, "--expected-shared-conduit-id", updated.CalculationID, "--publication-dir", o.PublicationDirectory, "--lock-dir", o.LockDirectory, "--arango-data-dir", o.ArangoDataDirectory, "--reserve-free-bytes", "1", "--max-filesystem-growth-bytes", strconv.FormatUint(o.MaxFilesystemGrowthBytes, 10), "--max-encoded-bytes", strconv.FormatUint(o.MaxEncodedBytes, 10)}
	var stdout, stderr bytes.Buffer
	if cli.Run(args, &stdout, &stderr) != 0 {
		t.Fatal(stderr.String())
	}
	var actual gen.SharedGeneration
	check(t, json.Unmarshal(stdout.Bytes(), &actual))
	if !reflect.DeepEqual(actual, out.Generation) {
		t.Fatal("CLI diverged")
	}
	testSharedQueries(t, ctx, op, out, path)
	testSharedWindow(t, ctx, root, s, base, out, path)
	// A mismatched parent and a damaged completed edge must fail, not be repaired.
	bad := o
	bad.CalculationID = digest([]byte("wrong"))
	if _, err = r.PublishShared(ctx, bad); err == nil {
		t.Fatal("wrong calculation accepted")
	}
	var first c.Decision
	check(t, c.ReadAdditions(ctx, base.input.Conduits, baseline, path, updated, func(d c.Decision) error {
		if first.Ordinal == 0 {
			first = d
		}
		return nil
	}))
	key, err := p.AppearanceID(updated.FactSetID, first.Ordinal)
	check(t, err)
	api := "/_db/" + out.Generation.Extension.Database + "/_api/document/reported_conduit_associations/" + key
	original := request(t, ctx, http.MethodGet, api, nil)
	request(t, ctx, http.MethodPatch, api, []byte(`{"additional_amount_minor_units":"1"}`))
	if _, err = r.PublishShared(ctx, o); err == nil {
		t.Fatal("damaged completed edge silently repaired")
	}
	var damaged struct {
		Amount string `json:"additional_amount_minor_units"`
	}
	check(t, json.Unmarshal(request(t, ctx, http.MethodGet, api, nil), &damaged))
	if damaged.Amount != "1" {
		t.Fatal("read-only rejection changed damaged edge")
	}
	request(t, ctx, http.MethodPut, api, original)
	_, err = r.PublishShared(ctx, o)
	check(t, err)
}
