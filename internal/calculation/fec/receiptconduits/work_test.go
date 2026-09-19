package receiptconduits

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"path/filepath"
	"reflect"
	"testing"

	policy "github.com/vedantadhobley/legal-tender/internal/calculation/fec/earmarkassociation"
	participants "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
	refs "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptreferences"
	xsort "github.com/vedantadhobley/legal-tender/internal/storage/externalsort"
)

func ptr[T any](v T) *T { return &v }
func topRecord(e refs.Endpoint) xsort.Record {
	b := []byte{}
	for _, n := range []uint64{e.Peers, e.OnlyPeer, e.Incoming, e.Outgoing} {
		b = binary.BigEndian.AppendUint64(b, n)
	}
	b = append(b, e.UnsafeReasons)
	return xsort.Record{Key: key(e.Ordinal), Ordinal: e.Ordinal, Data: b}
}
func fixture() ([]participants.Row, map[uint64]refs.Endpoint) {
	rows := make([]participants.Row, 40)
	eps := map[uint64]refs.Endpoint{}
	for i := range rows {
		rows[i] = participants.Row{Ordinal: int64(i + 1), Recipient: ptr("C00000001"), Entity: ptr("IND"), ReceiptType: ptr("15E"), Amount: ptr(int64(100)), AmountState: "reported_value", Component: "fixture", SourceRoute: "fixture", ConduitState: "fixture", EarmarkState: "fixture"}
	}
	for i := 19; i < len(rows); i++ {
		rows[i].Memo = true
		rows[i].Entity = ptr("PAC")
		rows[i].Contributor = ptr("C00000002")
		rows[i].CleanContributor = ptr("C00000002")
	}
	for _, pair := range [][2]uint64{{3, 20}, {4, 21}, {5, 22}, {6, 22}, {7, 23}, {8, 24}, {9, 25}, {10, 26}, {11, 27}, {12, 28}, {13, 29}} {
		a, b := pair[0], pair[1]
		eps[a] = refs.Endpoint{Ordinal: a, Peers: 1, OnlyPeer: b, Outgoing: 1}
		e := eps[b]
		e.Ordinal = b
		e.Peers++
		e.Incoming++
		e.OnlyPeer = a
		if e.Peers != 1 {
			e.OnlyPeer = 0
		}
		eps[b] = e
	}
	e := eps[4]
	e.Incoming = 1
	eps[4] = e
	e = eps[21]
	e.Outgoing = 1
	eps[21] = e
	e = eps[23]
	e.UnsafeReasons = 2
	eps[23] = e
	rows[23].ReceiptType = ptr("15")
	rows[24].CleanContributor = ptr("C00000003")
	rows[9].Entity = ptr("ORG")
	rows[10].Contributor = ptr("C00000009")
	rows[11].Amount = nil
	rows[11].AmountState = "source_null"
	rows[12].Amount = ptr(int64(-10))
	rows[28].Amount = ptr(int64(0))
	eps[14] = refs.Endpoint{Ordinal: 14, UnsafeReasons: 1}
	eps[15] = refs.Endpoint{Ordinal: 15, Peers: 2, Incoming: 1, Outgoing: 1}
	rows[17].ReceiptType = ptr("15I")
	rows[18].ReceiptType = ptr("UNKNOWN")
	return rows, eps
}
func census(rows []participants.Row) participants.Census {
	r := participants.Census{Components: map[string]uint64{}, Routes: map[string]uint64{}, Conduits: map[string]uint64{}, Earmarks: map[string]uint64{}}
	for _, v := range rows {
		r.Rows++
		if v.Memo {
			r.MemoRows++
		}
		switch {
		case v.Amount == nil:
			r.UnknownAmounts++
		case *v.Amount > 0:
			r.PositiveAmounts++
		case *v.Amount < 0:
			r.NegativeAmounts++
		default:
			r.ZeroAmounts++
		}
		r.Components[v.Component]++
		r.Routes[v.SourceRoute]++
		r.Conduits[v.ConduitState]++
		r.Earmarks[v.EarmarkState]++
	}
	return r
}
func setup(t *testing.T, ctx context.Context, rows []participants.Row, eps map[uint64]refs.Endpoint, shardSize, workers, runRows, fanIn int, cap uint64) (work, string) {
	t.Helper()
	p := participants.Result{SourceRows: uint64(len(rows)), Census: census(rows)}
	for i := 0; i < len(rows); i += shardSize {
		last := min(i+shardSize, len(rows))
		p.Files = append(p.Files, participants.File{First: uint64(i + 1), Last: uint64(last), Rows: uint64(last - i)})
	}
	topSpace, err := xsort.NewWorkspace(filepath.Join(t.TempDir(), "data"), 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	tw, err := topSpace.Writer(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	tr := refs.TopologyResult{SourceRows: uint64(len(rows))}
	for _, row := range rows {
		if e, ok := eps[uint64(row.Ordinal)]; ok {
			if err = tw.Add(topRecord(e)); err != nil {
				t.Fatal(err)
			}
			if e.Peers > 0 {
				tr.ExactEndpointRows++
			}
			if e.UnsafeReasons != 0 {
				tr.UnsafeEndpointRows++
			}
		}
	}
	tr.Endpoints, err = tw.Finish()
	if err != nil {
		t.Fatal(err)
	}
	s, err := xsort.NewWorkspace(filepath.Join(t.TempDir(), "work"), cap)
	if err != nil {
		t.Fatal(err)
	}
	w := work{ctx: ctx, space: s, p: p, t: tr, o: Options{Workers: workers, RunRows: runRows, FanIn: fanIn}}
	w.read = func(ctx context.Context, i int, visit func(participants.Row) error) (participants.Census, error) {
		f := p.Files[i]
		part := rows[f.First-1 : f.Last]
		for _, v := range part {
			if err := ctx.Err(); err != nil {
				return participants.Census{}, err
			}
			if err := visit(v); err != nil {
				return participants.Census{}, err
			}
		}
		return census(part), nil
	}
	return w, topSpace.Dir
}

func TestCompleteCrossShardPolicyAndLayoutReplay(t *testing.T) {
	rows, eps := fixture()
	var prior string
	for _, layout := range [][4]int{{7, 1, 3, 2}, {11, 4, 5, 3}, {1, 8, 100, 8}} {
		w, dir := setup(t, context.Background(), rows, eps, layout[0], layout[1], layout[2], layout[3], 128<<20)
		r, err := w.calculate(dir)
		if err != nil {
			t.Fatal(err)
		}
		if prior != "" && prior != r.Decisions.ValuesSHA256 {
			t.Fatal("layout changed decisions")
		}
		prior = r.Decisions.ValuesSHA256
		reader, err := xsort.Open(w.ctx, w.space.Dir, r.Decisions)
		if err != nil {
			t.Fatal(err)
		}
		var seen uint64
		for {
			record, err := reader.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			d, err := DecodeDecision(record, uint64(len(rows)))
			if err != nil {
				t.Fatal(err)
			}
			row := rows[d.Ordinal-1]
			e := eps[d.Ordinal]
			want := Decision{Ordinal: d.Ordinal, State: Unassessed, AmountComparison: "not_assessed"}
			if e.Peers > 0 || e.UnsafeReasons != 0 {
				var related *policy.Related
				if e.Peers == 1 {
					peer := rows[e.OnlyPeer-1]
					related = &policy.Related{Evidence: evidence(peer), Topology: topology(eps[e.OnlyPeer])}
				}
				v, err := policy.Decide(evidence(row), topology(e), related)
				if err != nil {
					t.Fatal(err)
				}
				want = Decision{Ordinal: d.Ordinal, Related: e.OnlyPeer, State: v.State, AmountComparison: v.AmountComparison, ConduitID: v.ConduitID}
			}
			if !reflect.DeepEqual(d, want) {
				t.Fatalf("ordinal %d: %+v want %+v", d.Ordinal, d, want)
			}
			seen++
		}
		reader.Close()
		if seen != 17 || r.Qualified != 4 || r.States[Unassessed] != 4 || r.States["shared_related_record_unresolved"] != 2 || r.States["ambiguous_or_incomplete_reference_evidence"] != 2 {
			t.Fatalf("bad fixture census: %+v", r)
		}
		_, live := w.space.Stats()
		if live != r.Decisions.Bytes {
			t.Fatal("unremoved workspace inputs")
		}
	}
}

func TestFailClosedInputsCapsCancellationAndMembership(t *testing.T) {
	rows, eps := fixture()
	for _, kind := range []string{"canceled", "cap", "census", "topology_census", "bad_peer", "duplicate_decision"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			limit := uint64(64 << 20)
			if kind == "cap" {
				limit = 1
			}
			w, dir := setup(t, ctx, rows, eps, 7, 4, 3, 2, limit)
			switch kind {
			case "canceled":
				cancel()
			case "census":
				w.p.Census.Rows++
			case "topology_census":
				w.t.ExactEndpointRows++
			case "bad_peer":
				old := w.read
				w.read = func(ctx context.Context, i int, visit func(participants.Row) error) (participants.Census, error) {
					return old(ctx, i, func(r participants.Row) error {
						if r.Ordinal == 20 {
							r.Recipient = ptr("C00000009")
						}
						return visit(r)
					})
				}
			case "duplicate_decision":
				writer, err := w.space.Writer(ctx)
				if err != nil {
					t.Fatal(err)
				}
				d := Decision{Ordinal: 1, State: Unassessed, AmountComparison: "not_assessed"}
				rec, _ := d.record()
				writer.Add(rec)
				writer.Add(rec)
				f, err := writer.Finish()
				if err != nil {
					t.Fatal(err)
				}
				if _, err = w.verifyMembership(f); err == nil {
					t.Fatal("duplicate membership accepted")
				}
				return
			}
			if _, err := w.calculate(dir); err == nil {
				t.Fatal("invalid publication accepted")
			}
		})
	}
}
func TestRequestAndDecisionCodec(t *testing.T) {
	rows, eps := fixture()
	rows[2].Contributor = ptr("")
	rows[2].Entity = ptr("É中")
	rows[2].Amount = ptr(int64(-9223372036854775807 - 1))
	r, err := request(rows[2], eps[3])
	if err != nil {
		t.Fatal(err)
	}
	v, e, err := decodeRequest(r, 40)
	if err != nil || !reflect.DeepEqual(evidence(v), evidence(rows[2])) || e.OnlyPeer != 20 {
		t.Fatal("request changed evidence", err)
	}
	r.Data = append(r.Data, 0)
	if _, _, err = decodeRequest(r, 40); err == nil {
		t.Fatal("trailing bytes accepted")
	}
	d := Decision{Ordinal: 3, Related: 20, State: "reported_earmark_memo_association", AmountComparison: "different_reported_amount", ConduitID: ptr("C00000002")}
	record, err := d.record()
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeDecision(record, 40)
	if err != nil || !reflect.DeepEqual(got, d) {
		t.Fatal(got, err)
	}
	record.Data[18] = 'x'
	if _, err = DecodeDecision(record, 40); err == nil {
		t.Fatal("bad conduit ID accepted")
	}
	if err := parallel(context.Background(), 8, 200, func(context.Context, int) error { return errors.New("stop") }); err == nil {
		t.Fatal("worker errors lost")
	}
}
