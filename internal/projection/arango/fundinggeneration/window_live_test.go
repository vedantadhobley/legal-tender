package fundinggeneration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	fc "github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
	flow "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
	"github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

// Explicitly opt-in, read-only acceptance over retained publications. The normal
// test suite does not discover credentials, open databases or scan real sources.
func TestWindowLiveGate(t *testing.T) {
	specPath := os.Getenv("LT_WINDOW_INPUTS")
	if specPath == "" {
		t.Skip("retained-input window gate not requested")
	}
	ctx := context.Background()
	spec, err := ReadWindowInputs(specPath, os.Getenv("LT_WINDOW_INPUTS_SHA256"))
	if err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(exe)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	_, hashErr := io.Copy(h, f)
	closeErr := f.Close()
	if hashErr != nil || closeErr != nil {
		t.Fatal(hashErr, closeErr)
	}
	root := os.Getenv("LT_WINDOW_STORAGE_ROOT")
	r, err := OpenWindowReader(ctx, WindowOpenOptions{Inputs: spec.Inputs, StorageRoot: root, Endpoint: os.Getenv("LT_WINDOW_ENDPOINT"), Username: "root", Password: os.Getenv("ARANGO_PASSWORD"), BuildSHA256: hex.EncodeToString(h.Sum(nil)), Progress: func(s string) { t.Log(s) }})
	if err != nil {
		t.Fatal(err)
	}
	type census struct {
		days    map[int32]uint64
		unknown uint64
	}
	counts := map[string]map[flow.Ledger]census{}
	// Independently decode complete compact calculation artifacts, not the new
	// dated-accessor or windowTopology, to establish date-population expectations.
	for _, p := range r.partitions {
		g := p.publication.Generation
		calculation, _, err := fc.LoadPublished(ctx, root, filepath.Join(root, fc.PublicationBase, "manifests", g.CommitteeFlow.CalculationID+".json"))
		if err != nil {
			t.Fatal(err)
		}
		counts[p.publication.GenerationID] = map[flow.Ledger]census{}
		for _, ledger := range []flow.Ledger{flow.ScheduleA, flow.ScheduleB} {
			d := calculation.A.Observations
			if ledger == flow.ScheduleB {
				d = calculation.B.Observations
			}
			reader, err := artifact.Open[fc.Observation](ctx, filepath.Join(root, fc.PublicationBase), d)
			if err != nil {
				t.Fatal(err)
			}
			c := census{days: map[int32]uint64{}}
			for {
				row, ok, err := reader.Next()
				if err != nil {
					reader.Abort()
					t.Fatal(err)
				}
				if !ok {
					break
				}
				if row.Date == nil {
					c.unknown++
				} else {
					c.days[*row.Date]++
				}
			}
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
			counts[p.publication.GenerationID][ledger] = c
		}
	}
	type liveCase struct {
		Kind   string            `json:"kind"`
		Result WindowPathsResult `json:"result"`
	}
	gate := struct {
		Version string     `json:"schema_version"`
		GateID  string     `json:"gate_id"`
		Cases   []liveCase `json:"cases"`
	}{Version: "legal-tender.funding-window-live-gate.v1", Cases: []liveCase{}}
	for _, ledger := range []flow.Ledger{flow.ScheduleA, flow.ScheduleB} {
		var witness *flow.DatedLink
		for _, p := range r.partitions {
			if err := p.source.VisitDatedLinks(ctx, ledger, func(row flow.DatedLink) error {
				if row.Date != nil && row.Link.From != row.Link.To && (witness == nil || row.Link.ID() < witness.Link.ID()) {
					v := row
					witness = &v
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}
		if witness == nil {
			t.Fatal("no dated non-self witness in selected ledger", ledger)
		}
		date := time.Unix(int64(*witness.Date)*86400, 0).UTC()
		day, next := date.Format(time.DateOnly), date.AddDate(0, 0, 1).Format(time.DateOnly)
		windows := []*DateWindow{nil, {Start: day, End: day}, {Start: next, End: next}}
		for index, window := range windows {
			kind := string(ledger) + "_" + []string{"all_supplied_dates", "witness_day", "following_day"}[index]
			t.Log("checking", kind)
			q := WindowPathQuery{From: witness.Link.From, Target: witness.Link.To, Ledger: ledger, Window: window, MaxHops: 1, Limit: 3, Budget: 100000}
			result, err := r.Paths(ctx, q)
			if err != nil {
				t.Fatal(err)
			}
			if index < 2 && len(result.Paths) == 0 {
				t.Fatal("known path absent", kind)
			}
			if result.FinancialEligibility || result.TerminalEligible {
				t.Fatal("invented financial status")
			}
			for _, c := range result.Coverage {
				ref := counts[c.GenerationID][ledger]
				want := WindowCoverage{GenerationID: c.GenerationID, Rows: ref.unknown}
				if window == nil {
					want.Included, want.UndatedIncluded = ref.unknown, ref.unknown
				} else {
					want.UnknownExcluded = ref.unknown
				}
				for d, n := range ref.days {
					want.Rows += n
					text := time.Unix(int64(d)*86400, 0).UTC().Format(time.DateOnly)
					switch {
					case window == nil:
						want.Included += n
					case text < window.Start:
						want.Before += n
					case text > window.End:
						want.After += n
					default:
						want.Included += n
					}
				}
				if !reflect.DeepEqual(c, want) {
					t.Fatal("independent compact-artifact census differs", kind, c, want)
				}
			}
			for _, link := range result.Links {
				var document struct {
					Date *int32 `json:"date_days"`
				}
				if err := json.Unmarshal(link.Evidence.Document, &document); err != nil || !reflect.DeepEqual(link.Date, document.Date) {
					t.Fatal("date differs from source-verified observation", err)
				}
				if window != nil && (link.Date == nil || time.Unix(int64(*link.Date)*86400, 0).UTC().Format(time.DateOnly) != window.Start) {
					t.Fatal("returned link outside requested day")
				}
			}
			gate.Cases = append(gate.Cases, liveCase{kind, result})
		}
	}
	gate.GateID = valueID(gate)
	if expected := os.Getenv("LT_WINDOW_EXPECTED_GATE"); expected != "" && gate.GateID != expected {
		t.Fatal("fresh gate identity differs")
	}
	b, err := json.MarshalIndent(gate, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	out, err := os.OpenFile(os.Getenv("LT_WINDOW_OUTPUT"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := out.Write(append(b, '\n'))
	closeErr = out.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatal(writeErr, closeErr)
	}
}
