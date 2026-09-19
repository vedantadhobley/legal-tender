package receiptconduits

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	policy "github.com/vedantadhobley/legal-tender/internal/calculation/fec/earmarkassociation"
	xsort "github.com/vedantadhobley/legal-tender/internal/storage/externalsort"
)

func TestGroupPublicationCompleteAndLayoutReplay(t *testing.T) {
	var prior, priorGroups string
	for _, layout := range [][4]int{{7, 1, 3, 2}, {11, 4, 5, 3}, {1, 8, 100, 8}} {
		rows, eps := fixture()
		rows[4].Amount, rows[5].Amount = nil, ptr(int64(-500))
		rows[4].AmountState = "source_null"
		base, dir := setup(t, context.Background(), rows, eps, layout[0], layout[1], layout[2], layout[3], 128<<20)
		baseline, err := base.calculate(dir)
		if err != nil {
			t.Fatal(err)
		}
		baseline.SourceRows = uint64(len(rows))
		parent := t.TempDir()
		if err = os.Symlink(base.space.Dir, filepath.Join(parent, "data")); err != nil {
			t.Fatal(err)
		}
		w, dir := setup(t, context.Background(), rows, eps, layout[0], layout[1], layout[2], layout[3], 128<<20)
		w.o.GroupBaseline = filepath.Join(parent, "manifest.json")
		w.o.groups = &groupWork{baseline: baseline, groups: make([]xsort.File, len(w.p.Files)), changes: make([]xsort.File, len(w.p.Files))}
		r, err := w.calculate(dir)
		if err != nil {
			t.Fatal(err)
		}
		if r.Decisions.ValuesSHA256 != baseline.Decisions.ValuesSHA256 {
			t.Fatal("old decisions changed before application")
		}
		r, err = w.applyGroups(r)
		if err != nil {
			t.Fatal(err)
		}
		if r.Qualified != baseline.Qualified+2 || r.Groups.ChangedRows != 2 || r.Groups.SharedRows != 2 || r.Groups.UnchangedRows != baseline.EligibleRoleRows-2 || r.Groups.States[policy.SharedAssociation] != 1 || !reflect.DeepEqual(r.Amounts, baseline.Amounts) {
			t.Fatalf("unexpected group result: %+v / %+v", r, r.Groups)
		}
		if prior != "" && (prior != r.Decisions.ValuesSHA256 || priorGroups != r.Groups.Decisions.ValuesSHA256) {
			t.Fatal("layout changed publication")
		}
		prior, priorGroups = r.Decisions.ValuesSHA256, r.Groups.Decisions.ValuesSHA256
		old, err := xsort.Open(w.ctx, base.space.Dir, baseline.Decisions)
		if err != nil {
			t.Fatal(err)
		}
		current, err := xsort.Open(w.ctx, w.space.Dir, r.Decisions)
		if err != nil {
			t.Fatal(err)
		}
		for {
			a, ae := old.Next()
			b, be := current.Next()
			if ae == io.EOF && be == io.EOF {
				break
			}
			if ae != nil || be != nil {
				t.Fatal(ae, be)
			}
			if a.Ordinal != 5 && a.Ordinal != 6 {
				if !reflect.DeepEqual(a, b) {
					t.Fatal("unrelated decision changed")
				}
			} else {
				x, _ := DecodeDecision(a, uint64(len(rows)))
				y, e := DecodeDecision(b, uint64(len(rows)))
				if e != nil || y.State != policy.SharedAssociation || y.ConduitID == nil || *y.ConduitID != "C00000002" || y.AmountComparison != x.AmountComparison || y.Related != x.Related {
					t.Fatal("invalid additive decision", e)
				}
			}
		}
		old.Close()
		current.Close()
		_, live := w.space.Stats()
		if live != r.Decisions.Bytes+r.Groups.Decisions.Bytes {
			t.Fatal("left temporary data")
		}
	}
}

func TestGroupPublicationRejectsPartialUnsafeAndMixed(t *testing.T) {
	for _, kind := range []string{"partial", "unsafe_original", "unsafe_root", "conflict", "unsupported", "nonleaf"} {
		t.Run(kind, func(t *testing.T) {
			rows, eps := fixture()
			switch kind {
			case "partial":
				e := eps[22]
				e.Peers++
				e.Incoming++
				eps[22] = e
			case "unsafe_original":
				e := eps[5]
				e.UnsafeReasons = 1
				eps[5] = e
			case "unsafe_root":
				e := eps[22]
				e.UnsafeReasons = 1
				eps[22] = e
			case "conflict":
				rows[4].ConduitID = ptr("C00000009")
			case "unsupported":
				rows[4].Entity = ptr("ORG")
			case "nonleaf":
				e := eps[5]
				e.Peers = 2
				e.OnlyPeer = 0
				e.Incoming++
				eps[5] = e
			}
			w, dir := setup(t, context.Background(), rows, eps, 7, 4, 3, 2, 128<<20)
			w.o.groups = &groupWork{groups: make([]xsort.File, len(w.p.Files)), changes: make([]xsort.File, len(w.p.Files))}
			if _, err := w.calculate(dir); err != nil {
				t.Fatal(err)
			}
			f, err := w.mergeLevel(w.o.groups.groups)
			if err != nil {
				t.Fatal(err)
			}
			g, err := summarizeGroups(w.ctx, w.space.Dir, f, uint64(len(rows)))
			if err != nil {
				t.Fatal(err)
			}
			if g.ChangedRows != 0 {
				t.Fatal("unsupported group promoted")
			}
			if kind == "unsafe_root" && f.Rows != 0 {
				t.Fatal("unsafe root was not an old shared rejection")
			}
			if (kind == "partial" || kind == "nonleaf") && (g.UncharacterizedPeers != 1 || g.States["incomplete_group_peer_coverage"] != 1) {
				t.Fatal("incomplete coverage hidden")
			}
			for _, f := range w.o.groups.changes {
				if f.Rows != 0 {
					t.Fatal("unexpected changes")
				}
			}
		})
	}
}

func TestGroupManifestVersionAndConservation(t *testing.T) {
	h := strings.Repeat("a", 64)
	base := Result{SchemaVersion: GroupVersion, Policy: GroupPublicationPolicy, State: "complete_cycle_conduit_evidence", AssociationPolicy: policy.Policy, BuildSHA256: h, ParticipantID: h, ParticipantSHA256: h, TopologyID: h, TopologySHA256: h, FactSetID: h, FactManifestSHA256: h, SourceRows: 4, EligibleRoleRows: 3, OtherRows: 1, OtherDisposition: "not_a_non_memo_reviewed_earmark", AdditionalAmount: "0", Workers: 1, Qualified: 2, States: map[string]uint64{Unassessed: 1, policy.SharedAssociation: 2}, Amounts: map[string]uint64{"not_assessed": 1, "different_reported_amount": 2}, Decisions: xsort.File{Name: "decisions.zst", Rows: 3, SHA256: h, ValuesSHA256: h}, Groups: &GroupPublication{Policy: policy.GroupPolicy, BaselineID: h, BaselineSHA256: h, BaselineValues: h, BaselineStates: map[string]uint64{Unassessed: 1, sharedRejection: 2}, SharedRows: 2, ChangedRows: 2, UnchangedRows: 1, Decisions: xsort.File{Name: "groups.zst", Rows: 1, SHA256: h, ValuesSHA256: h}, States: map[string]uint64{policy.SharedAssociation: 1}}}
	for _, kind := range []string{"valid", "v1_disguise", "baseline_count", "baseline_state", "group_count", "wrong_policy", "new_state", "missing_groups"} {
		t.Run(kind, func(t *testing.T) {
			b, _ := json.Marshal(base)
			var r Result
			json.Unmarshal(b, &r)
			switch kind {
			case "v1_disguise":
				r.SchemaVersion, r.Policy = Version, Policy
			case "baseline_count":
				r.Groups.BaselineStates[sharedRejection]++
			case "baseline_state":
				r.Groups.BaselineStates[Unassessed] = 0
			case "group_count":
				r.Groups.Decisions.Rows++
			case "wrong_policy":
				r.Groups.Policy = "other"
			case "new_state":
				r.States["related_role_unresolved"] = 1
			case "missing_groups":
				r.Groups = nil
			}
			r.CalculationID = identity(r)
			b, _ = json.Marshal(r)
			if _, err := DecodeManifest(b, r.CalculationID); (err == nil) != (kind == "valid") {
				t.Fatal(kind, err)
			}
		})
	}
}

func TestGroupReplacementFailsClosed(t *testing.T) {
	for _, kind := range []string{"wrong_peer", "wrong_state", "duplicate", "missing_baseline", "bad_id", "canceled"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s, err := xsort.NewWorkspace(filepath.Join(t.TempDir(), "data"), 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			b, _ := s.Writer(context.Background())
			d := Decision{Ordinal: 2, Related: 4, State: sharedRejection, AmountComparison: "different_reported_amount"}
			if kind == "wrong_state" {
				d.State = "related_role_unresolved"
			}
			r, _ := d.record()
			if err = b.Add(r); err != nil {
				t.Fatal(err)
			}
			baseline, err := b.Finish()
			if err != nil {
				t.Fatal(err)
			}
			c, _ := s.Writer(context.Background())
			v := xsort.Record{Key: key(2), Ordinal: 2, Data: append([]byte(key(4)), []byte("C00000002")...)}
			if kind == "wrong_peer" {
				v.Data[7] = 3
			}
			if kind == "missing_baseline" {
				v.Ordinal = 3
				v.Key = key(3)
			}
			if kind == "bad_id" {
				v.Data[8] = 'X'
			}
			if err = c.Add(v); err != nil {
				t.Fatal(err)
			}
			if kind == "duplicate" {
				if err = c.Add(v); err != nil {
					t.Fatal(err)
				}
			}
			changes, err := c.Finish()
			if err != nil {
				t.Fatal(err)
			}
			if kind == "canceled" {
				cancel()
			}
			if _, err = replaceShared(ctx, s, baseline, changes, 5); err == nil {
				t.Fatal("bad change accepted")
			}
		})
	}
}
