package receiptreferences

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/earmarkassociation"
	xsort "github.com/vedantadhobley/legal-tender/internal/storage/externalsort"
)

func readEndpoints(t *testing.T, dir string, f xsort.File, rows uint64) map[uint64]Endpoint {
	t.Helper()
	r, err := xsort.Open(context.Background(), dir, f)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	out := map[uint64]Endpoint{}
	var previous uint64
	for {
		v, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		e, err := DecodeEndpoint(v, rows)
		if err != nil || e.Ordinal <= previous {
			t.Fatal(e, err)
		}
		previous = e.Ordinal
		out[e.Ordinal] = e
	}
	return out
}

func TestTopologyMatchesDirectIncidentOracleAcrossGeometry(t *testing.T) {
	rows := fixture()
	var digest string
	for _, size := range []int{1, 3, 100} {
		for _, filterBytes := range []int{1, 1024} {
			e, decisions, _ := runRows(t, rows, size, filterBytes)
			s, err := xsort.NewWorkspace(filepath.Join(t.TempDir(), "topology"), 64<<20)
			if err != nil {
				t.Fatal(err)
			}
			r, err := calculateTopology(context.Background(), s, e.space.Dir, e.out, TopologyOptions{RunRows: size, FanIn: 2})
			if err != nil {
				t.Fatal(err)
			}
			got := readEndpoints(t, s.Dir, r.Endpoints, uint64(len(rows)))
			want := map[uint64]Endpoint{}
			peers := map[uint64]map[uint64]bool{}
			for _, d := range decisions {
				ord := uint64(d.Source.Ordinal)
				if d.Target == nil {
					x := want[ord]
					x.Ordinal = ord
					x.UnsafeReasons |= InvalidOwnReference
					want[ord] = x
					if !scope(d.Source) || !present(d.Source.BackReference) {
						continue
					}
					for _, row := range rows {
						if reflect.DeepEqual(row.Recipient, d.Source.Recipient) && reflect.DeepEqual(row.File, d.Source.File) && reflect.DeepEqual(row.Transaction, d.Source.BackReference) {
							i := uint64(row.Ordinal)
							x := want[i]
							x.Ordinal = i
							x.UnsafeReasons |= InvalidIncomingReference
							want[i] = x
						}
					}
					continue
				}
				for direction, a := range []uint64{ord, *d.Target} {
					b := *d.Target
					if direction == 1 {
						b = ord
					}
					x := want[a]
					x.Ordinal = a
					if direction == 0 {
						x.Outgoing++
					} else {
						x.Incoming++
					}
					want[a] = x
					if peers[a] == nil {
						peers[a] = map[uint64]bool{}
					}
					peers[a][b] = true
				}
			}
			for ord, p := range peers {
				x := want[ord]
				x.Peers = uint64(len(p))
				if len(p) == 1 {
					for peer := range p {
						x.OnlyPeer = peer
					}
				}
				want[ord] = x
			}
			if !reflect.DeepEqual(want, got) {
				t.Fatalf("want %#v\ngot %#v", want, got)
			}
			if digest != "" && digest != r.Endpoints.ValuesSHA256 {
				t.Fatal("run/filter geometry changed topology")
			}
			digest = r.Endpoints.ValuesSHA256
		}
	}
}

func TestTopologyFeedsSharedAssociationPolicyWithoutRoleGuesses(t *testing.T) {
	base := func() []Row {
		rows := fixture()[:2]
		rows[1].BackReference, rows[1].BackSchedule = nil, nil
		return rows
	}
	for _, tc := range []struct {
		name, state string
		edit        func([]Row) []Row
	}{
		{"forward", "reported_earmark_memo_association", func(r []Row) []Row { return r }},
		{"reverse", "reported_earmark_memo_association", func(r []Row) []Row {
			r[0].BackReference, r[0].BackSchedule = nil, nil
			r[1].BackReference, r[1].BackSchedule = ptr("a"), ptr("SA")
			return r
		}},
		{"reciprocal", "reported_earmark_memo_association", func(r []Row) []Row { r[1].BackReference, r[1].BackSchedule = ptr("a"), ptr("SA"); return r }},
		{"invalid incoming original", "ambiguous_or_incomplete_reference_evidence", func(r []Row) []Row {
			x := r[1]
			x.Ordinal = 3
			x.Transaction = ptr("third")
			x.BackReference = ptr("a")
			x.BackSchedule = ptr("SB")
			return append(r, x)
		}},
		{"missing schedule into memo", "ambiguous_or_incomplete_reference_evidence", func(r []Row) []Row {
			x := r[1]
			x.Ordinal = 3
			x.Transaction = ptr("third")
			x.BackReference = ptr("b")
			return append(r, x)
		}},
		{"missing own transaction into memo", "ambiguous_or_incomplete_reference_evidence", func(r []Row) []Row {
			x := r[1]
			x.Ordinal = 3
			x.Transaction = nil
			x.BackReference = ptr("b")
			x.BackSchedule = ptr("SA")
			return append(r, x)
		}},
		{"shared memo", "shared_related_record_unresolved", func(r []Row) []Row { x := r[0]; x.Ordinal = 3; x.Transaction = ptr("third"); return append(r, x) }},
		{"multiple peers", "multiple_related_records_unresolved", func(r []Row) []Row {
			x := r[0]
			x.Ordinal = 3
			x.Transaction = ptr("third")
			x.BackReference = ptr("a")
			return append(r, x)
		}},
		{"other report invalid reference", "reported_earmark_memo_association", func(r []Row) []Row {
			x := r[0]
			x.Ordinal = 3
			x.File = ptr("99")
			x.BackSchedule = ptr("SB")
			return append(r, x)
		}},
		{"invalid report scope", "reported_earmark_memo_association", func(r []Row) []Row {
			x := r[0]
			x.Ordinal = 3
			x.Recipient = nil
			x.BackSchedule = ptr("SB")
			return append(r, x)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := tc.edit(base())
			e, _, _ := runRows(t, rows, 1, 1)
			s, err := xsort.NewWorkspace(filepath.Join(t.TempDir(), "topology"), 64<<20)
			if err != nil {
				t.Fatal(err)
			}
			r, err := calculateTopology(context.Background(), s, e.space.Dir, e.out, TopologyOptions{RunRows: 1, FanIn: 2})
			if err != nil {
				t.Fatal(err)
			}
			x := readEndpoints(t, s.Dir, r.Endpoints, uint64(len(rows)))
			amount, memoAmount := int64(10001), int64(-9)
			original := earmarkassociation.Evidence{ReceiptType: ptr("15E"), Entity: ptr("IND"), Amount: &amount}
			var other *earmarkassociation.Related
			if x[1].Peers == 1 {
				p := x[x[1].OnlyPeer]
				other = &earmarkassociation.Related{Evidence: earmarkassociation.Evidence{Memo: true, Entity: ptr("PAC"), Contributor: ptr("C00000003"), CleanContributor: ptr("C00000003"), Amount: &memoAmount}, Topology: earmarkassociation.Topology{Peers: p.Peers, Unsafe: p.UnsafeReasons != 0}}
			}
			d, err := earmarkassociation.Decide(original, earmarkassociation.Topology{Peers: x[1].Peers, Unsafe: x[1].UnsafeReasons != 0}, other)
			if err != nil || d.State != tc.state || d.AdditionalAmount != "0" || d.TerminalEligible {
				t.Fatal(d, err)
			}
		})
	}
}

func topologyRunFixture(t *testing.T) TopologyOptions {
	t.Helper()
	e, _, _ := runRows(t, fixture(), 3, 1)
	dir := t.TempDir()
	if err := os.Rename(e.space.Dir, filepath.Join(dir, "data")); err != nil {
		t.Fatal(err)
	}
	r := e.out
	r.State = "complete_cycle_reference_join"
	r.BuildSHA256 = strings.Repeat("1", 64)
	r.FactSetID = strings.Repeat("2", 64)
	r.ManifestSHA256 = strings.Repeat("3", 64)
	r.Cycle = "2024"
	r.CalculationID = logicalID(r)
	if err := saveResult(dir, r); err != nil {
		t.Fatal(err)
	}
	return TopologyOptions{ReferenceManifest: filepath.Join(dir, "manifest.json"), ExpectedReferenceID: r.CalculationID, OutputDirectory: filepath.Join(t.TempDir(), "output"), BuildSHA256: strings.Repeat("4", 64), RunRows: 3, FanIn: 2, MaxWorkspaceBytes: 64 << 20}
}

func TestTopologyPublicationReplayAndNoSourceMutation(t *testing.T) {
	o := topologyRunFixture(t)
	before, err := os.ReadFile(o.ReferenceManifest)
	if err != nil {
		t.Fatal(err)
	}
	r, err := RunTopology(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	if r.CalculationID != topologyID(r) || r.ConduitEligibilityEvaluated || r.FinancialEligibility {
		t.Fatal(r)
	}
	if _, err = RunTopology(context.Background(), o); err == nil {
		t.Fatal("existing output accepted")
	}
	o.OutputDirectory = filepath.Join(t.TempDir(), "replay")
	o.RunRows = 1
	o.FanIn = 3
	again, err := RunTopology(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	if again.CalculationID != r.CalculationID || again.Endpoints.ValuesSHA256 != r.Endpoints.ValuesSHA256 {
		t.Fatal("non-deterministic topology replay")
	}
	after, err := os.ReadFile(o.ReferenceManifest)
	if err != nil || string(before) != string(after) {
		t.Fatal("source mutation", err)
	}
	t.Setenv("LT_TOPOLOGY_MANIFEST", filepath.Join(o.OutputDirectory, "manifest.json"))
	t.Setenv("LT_TOPOLOGY_REFERENCE_MANIFEST", o.ReferenceManifest)
	TestRetainedTopologyCorpus(t)
}

func TestTopologyFailsClosedOnBackingAndIdentityErrors(t *testing.T) {
	for _, target := range []string{"lookup", "decisions", "incidences", "neighbors", "identity", "scope", "cancel", "cap"} {
		t.Run(target, func(t *testing.T) {
			o := topologyRunFixture(t)
			r, _, err := loadReferenceResult(o.ReferenceManifest, o.ExpectedReferenceID)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			switch target {
			case "identity":
				o.ExpectedReferenceID = strings.Repeat("f", 64)
			case "scope":
				r.Scope = "partial"
				r.CalculationID = logicalID(r)
				o.ExpectedReferenceID = r.CalculationID
				b, _ := json.Marshal(r)
				if err = os.WriteFile(o.ReferenceManifest, b, 0600); err != nil {
					t.Fatal(err)
				}
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "cap":
				o.MaxWorkspaceBytes = 1
			default:
				f := map[string]xsort.File{"lookup": r.LookupEvidence, "decisions": r.Decisions, "incidences": r.ExactIncidences, "neighbors": r.Neighbors}[target]
				path := filepath.Join(filepath.Dir(o.ReferenceManifest), "data", f.Name)
				b, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				b[len(b)-1] ^= 1
				if err = os.WriteFile(path, b, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err = RunTopology(ctx, o); err == nil {
				t.Fatal("invalid input published")
			}
			if _, err = os.Stat(filepath.Join(o.OutputDirectory, "manifest.json")); !os.IsNotExist(err) {
				t.Fatal("failure emitted success manifest", err)
			}
		})
	}
}

func TestTopologyEmptyReferencePopulationAndEndpointValidation(t *testing.T) {
	e, _, _ := runRows(t, []Row{{Ordinal: 1}}, 1, 1)
	s, err := xsort.NewWorkspace(filepath.Join(t.TempDir(), "topology"), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	r, err := calculateTopology(context.Background(), s, e.space.Dir, e.out, TopologyOptions{RunRows: 1, FanIn: 2})
	if err != nil || r.Endpoints.Rows != 0 || r.InvalidReferenceRows != 0 {
		t.Fatal(r, err)
	}
	for _, e := range []Endpoint{{Ordinal: 1}, {Ordinal: 1, Peers: 1}, {Ordinal: 1, OnlyPeer: 2, UnsafeReasons: 1}, {Ordinal: 1, UnsafeReasons: 4}, {Ordinal: 1, Peers: 1, OnlyPeer: 1, Outgoing: 1}, {Ordinal: 1, Peers: 1, OnlyPeer: 2, Outgoing: 2}} {
		if _, err := DecodeEndpoint(e.record(), 3); err == nil {
			t.Fatal("invalid endpoint accepted", e)
		}
	}
}
