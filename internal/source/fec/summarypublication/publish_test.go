package summarypublication

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/committeesummary"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	"github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

func fixture(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile("../../../../contracts/sources/fec/committee-summary/v1/fixtures/sample.csv")
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestPublishReleaseBoundSummaryAndReplay(t *testing.T) {
	options := fixtureRelease(t, fixture(t))
	m, err := Publish(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	if m.Verification.Rows != 8 || m.Facts.RecordCount != 8 || !m.ReadbackVerified || m.TerminalAttributionEligible {
		t.Fatal(m)
	}
	if m.Verification.RowsWithIssues != 2 || m.Verification.Multiplicity.RepeatedCommitteeIDs != 1 {
		t.Fatal(m.Verification)
	}
	path := ManifestPath(options.StorageRoot, m.FactSetID)
	raw, _ := os.ReadFile(path)
	options.RunID = "different-replay-run"
	again, err := Publish(context.Background(), options)
	if err != nil || !reflect.DeepEqual(m, again) {
		t.Fatal("nonidentical replay", err)
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(raw, after) {
		t.Fatal("immutable manifest overwritten")
	}
	if _, err := os.Stat(filepath.Join(options.StorageRoot, basePath, "current")); !os.IsNotExist(err) {
		t.Fatal("created a mutable current pointer")
	}
	// The complete release fixture must survive downstream fact publication.
	var source release.ReleaseManifest
	if _, err := readJSON(options.ReleasePath, &source); err != nil {
		t.Fatal(err)
	}
	if len(source.Artifacts) != 27 || len(source.StagedOutputs) != 25 {
		t.Fatal("incomplete release membership")
	}
	if issues := release.ValidateKnownManifest(source); len(issues) != 0 {
		t.Fatal(issues)
	}
	if out := os.Getenv("LT_SUMMARY_FIXTURE_OUTPUT"); out != "" {
		if err := os.MkdirAll(out, 0o750); err != nil {
			t.Fatal(err)
		}
		for name, value := range map[string]any{"manifest.json": m, "release.json": source} {
			if err := writeImmutable(filepath.Join(out, name), value); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestSummaryPublicationFailsClosed(t *testing.T) {
	for _, kind := range []string{"raw_corrupt", "release_pointer", "old_inventory", "wrong_cycle", "cancelled", "artifact_corrupt", "manifest_tamper", "disk_full"} {
		t.Run(kind, func(t *testing.T) {
			options := fixtureRelease(t, fixture(t))
			ctx := context.Background()
			if kind == "artifact_corrupt" || kind == "manifest_tamper" {
				m, err := Publish(ctx, options)
				if err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(options.StorageRoot, m.Facts.StorageKey)
				if kind == "manifest_tamper" {
					path = ManifestPath(options.StorageRoot, m.FactSetID)
					m.Verification.Rows++
					b, _ := json.Marshal(m)
					if err := os.WriteFile(path, b, 0o600); err != nil {
						t.Fatal(err)
					}
				} else if err := os.WriteFile(path, []byte("corrupt"), 0o600); err != nil {
					t.Fatal(err)
				}
			} else if kind == "raw_corrupt" {
				_, _, source, err := loadSource(options.StorageRoot, options.ReleasePath, options.Cycle)
				if err != nil {
					t.Fatal(err)
				}
				raw := fixture(t)
				raw[len(raw)-2] ^= 1
				if err := os.WriteFile(filepath.Join(options.StorageRoot, source.StorageKey), raw, 0o600); err != nil {
					t.Fatal(err)
				}
			} else if kind == "release_pointer" || kind == "old_inventory" {
				var m release.ReleaseManifest
				_, err := readJSON(options.ReleasePath, &m)
				if err != nil {
					t.Fatal(err)
				}
				if kind == "old_inventory" {
					m.InventoryVersion = release.ActiveInventoryVersion
				} else {
					m.RunID = "other"
				}
				path := filepath.Join(options.StorageRoot, "unbacked-release.json")
				if err := writeImmutable(path, m); err != nil {
					t.Fatal(err)
				}
				options.ReleasePath = path
			} else if kind == "disk_full" {
				options.DiskAvailable = func(string) (uint64, error) { return 0, nil }
			} else if kind == "wrong_cycle" {
				options.Cycle = "2018"
			} else {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if _, err := Publish(ctx, options); err == nil {
				t.Fatal("accepted invalid publication")
			}
			if kind != "artifact_corrupt" && kind != "manifest_tamper" {
				paths, _ := filepath.Glob(filepath.Join(options.StorageRoot, basePath, "manifests", "*.json"))
				if len(paths) != 0 {
					t.Fatal("failed publication wrote manifest")
				}
			}
		})
	}
}

func TestSummaryFactsKeepDuplicateOccurrences(t *testing.T) {
	raw := fixture(t)
	lines := bytes.SplitAfter(raw, []byte("\n"))
	raw = append(append(append([]byte(nil), lines[0]...), lines[1]...), lines[1]...)
	expected := committeesummary.Expected{Cycle: "2024", Bytes: int64(len(raw)), SHA256: digest(raw)}
	root := t.TempDir()
	descriptor, verification, err := buildRows(context.Background(), root, bytes.NewReader(raw), expected)
	if err != nil {
		t.Fatal(err)
	}
	if descriptor.RecordCount != 2 || verification.Multiplicity.ExactDuplicateExtraRows != 1 {
		t.Fatal("duplicate erased")
	}
	r, err := artifact.Open[Fact](context.Background(), root, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Abort()
	a, _, _ := r.Next()
	b, _, _ := r.Next()
	if a.OccurrenceID == b.OccurrenceID || a.FactID == b.FactID || a.Record.RawSHA256 != b.Record.RawSHA256 {
		t.Fatal("invalid duplicate identity")
	}
}

func TestConcurrentSummaryPublication(t *testing.T) {
	options := fixtureRelease(t, fixture(t))
	var wg sync.WaitGroup
	results := make(chan Manifest, 2)
	errors := make(chan error, 2)
	for i := range 2 {
		wg.Go(func() {
			copy := options
			copy.RunID = fmt.Sprintf("concurrent-%d", i)
			m, err := Publish(context.Background(), copy)
			results <- m
			errors <- err
		})
	}
	wg.Wait()
	for range 2 {
		if err := <-errors; err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(<-results, <-results) {
		t.Fatal("concurrent publication returned different manifests")
	}
}

// This fixture drives the real release planner/acquirer/stager/publisher with
// in-memory HTTP responses. Unrelated dump containers and extraction are mocked;
// no synthetic release is ever written into durable project storage.
func fixtureRelease(t *testing.T, summary []byte) Options {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	inventory := release.CommitteeSummaryInventory()
	bodies := map[string][]byte{}
	for _, source := range inventory.Sources {
		body := []byte(source.SourceID)
		if source.ArtifactFormat != "" {
			rows, err := csv.NewReader(bytes.NewReader(summary)).ReadAll()
			if err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			writer := csv.NewWriter(&out)
			for _, row := range rows[1:] {
				for i, field := range rows[0] {
					if field == "FEC_ELECTION_YR" {
						row[i] = source.Periods[0]
					}
				}
			}
			if err := writer.WriteAll(rows); err != nil {
				t.Fatal(err)
			}
			body = out.Bytes()
		}
		bodies[source.RequestURL] = body
	}
	client := fixtureHTTP{bodies}
	discovery, err := release.Discover(ctx, client, inventory, time.Now, release.DiscoverOptions{})
	if err != nil {
		t.Fatal(err)
	}
	plan := release.Plan(inventory, discovery, nil, time.Now())
	if plan.Status != release.PlanUpdateAvailable {
		t.Fatal(plan)
	}
	planSHA := jsonDigest(plan)
	disk := func(string) (release.DiskSpace, error) { return release.DiskSpace{AvailableBytes: 1 << 40}, nil }
	acquisition, err := release.Acquire(ctx, client, inventory, plan, nil, planSHA, "fixture-acquisition", release.AcquisitionOptions{
		StorageRoot: root, Clock: time.Now, DiskUsage: disk,
		ContainerValidator: func(context.Context, release.SourceSpec, string, string) (string, error) {
			return "fixture_container", nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	acquisitionSHA := jsonDigest(acquisition)
	stage, err := release.Stage(ctx, inventory, plan, planSHA, acquisition, acquisitionSHA, nil, "fixture-stage", release.StageOptions{
		StorageRoot: root, Clock: time.Now, DiskUsage: disk,
		MemberExtractor: func(_ context.Context, _, _ string, output io.Writer) error {
			_, err := io.WriteString(output, "fixture\n")
			return err
		},
		RelationExtractor: func(_ context.Context, _, _, _ string, output io.Writer) (uint64, error) {
			_, err := io.WriteString(output, "fixture\n")
			return 1, err
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	m, err := release.Publish(ctx, inventory, plan, planSHA, acquisition, acquisitionSHA, stage, jsonDigest(stage), "fixture-release", release.PublishOptions{StorageRoot: root})
	if err != nil {
		t.Fatal(err)
	}
	return Options{StorageRoot: root, ReleasePath: filepath.Join(root, "releases", "fec", "manifests", m.ReleaseID+".json"), Cycle: "2024", RunID: "fixture-facts", DiskAvailable: func(string) (uint64, error) { return 1 << 40, nil }}
}

type fixtureHTTP struct{ bodies map[string][]byte }

func (f fixtureHTTP) Do(request *http.Request) (*http.Response, error) {
	body, ok := f.bodies[request.URL.String()]
	if !ok {
		return nil, fmt.Errorf("unexpected fixture URL")
	}
	content := body
	if request.Method == http.MethodHead {
		content = nil
	} else if request.Method != http.MethodGet {
		return nil, fmt.Errorf("unexpected fixture method")
	}
	header := http.Header{}
	header.Set("ETag", `"`+digest(body)+`"`)
	header.Set("Content-Length", fmt.Sprint(len(body)))
	header.Set("Accept-Ranges", "bytes")
	return &http.Response{StatusCode: 200, Header: header, Body: io.NopCloser(bytes.NewReader(content)), ContentLength: int64(len(body)), Request: request}, nil
}

func jsonDigest(value any) string { raw, _ := json.Marshal(value); return digest(raw) }

func TestSummaryBuildRejectsMalformedSuffix(t *testing.T) {
	raw := append(fixture(t), []byte("broken,row\n")...)
	root := t.TempDir()
	_, _, err := buildRows(context.Background(), root, bytes.NewReader(raw), committeesummary.Expected{Cycle: "2024", Bytes: int64(len(raw)), SHA256: digest(raw)})
	if err == nil || !strings.Contains(err.Error(), "invalid_csv_or_width") {
		t.Fatal(err)
	}
	paths, _ := filepath.Glob(filepath.Join(root, basePath, "facts", "sha256", "*", "*"))
	if len(paths) != 0 {
		t.Fatal("malformed source finalized facts")
	}
}
