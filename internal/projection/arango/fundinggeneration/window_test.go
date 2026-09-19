package fundinggeneration

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"testing"

	flow "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
)

type windowStub struct {
	rows         []flow.DatedLink
	err          error
	name         string
	evidenceKeys []string
}

func (s *windowStub) VisitDatedLinks(ctx context.Context, _ flow.Ledger, visit func(flow.DatedLink) error) error {
	for _, row := range s.rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(row); err != nil {
			return err
		}
	}
	return s.err
}
func (s *windowStub) PathEvidence(_ context.Context, _ flow.Ledger, key string) (graphread.Item, error) {
	s.evidenceKeys = append(s.evidenceKeys, key)
	for _, row := range s.rows {
		if row.Link.Key == key {
			b, _ := json.Marshal(row)
			return graphread.Item{Key: key, Document: b, EvidenceKind: "fixture", Evidence: b}, s.err
		}
	}
	return graphread.Item{}, errors.New("wrong publication used for source lookup")
}
func (s *windowStub) Facet(_ context.Context, _ string) (graphread.Facet, error) {
	b, _ := json.Marshal(map[string]string{"reported_name": s.name})
	return graphread.Facet{State: "present", Document: b}, s.err
}

// Only the private test constructor supplies stubs. The public opener must
// always execute the existing exact-generation/backing verification boundary.
func windowFixture(t *testing.T, ledger flow.Ledger) *WindowReader {
	t.Helper()
	r := &WindowReader{build: valueID("consumer")}
	for i, p := range probePartitions(t, 2022) {
		g := Result{Cycle: p.Cycle, GenerationID: valueID(p.Cycle)}
		g.CommitteeFlow.Inputs.A.FactSetID = p.FactSet
		g.CommitteeFlow.Inputs.B.FactSetID = valueID("b-" + p.Cycle)
		g.Receipts.Counts = map[string]uint64{"fixture": 1}
		fact, family := g.CommitteeFlow.Inputs.A.FactSetID, "receiver_reported_committee_observation"
		g.CommitteeFlow.A.Rows = uint64(len(p.Rows))
		if ledger == flow.ScheduleB {
			fact, family = g.CommitteeFlow.Inputs.B.FactSetID, "sender_reported_committee_observation"
			g.CommitteeFlow.A.Rows, g.CommitteeFlow.B.Rows = 0, uint64(len(p.Rows))
		}
		s := &windowStub{name: []string{"older assertion", "newer assertion"}[i]}
		for _, row := range p.Rows {
			s.rows = append(s.rows, flow.DatedLink{Link: graphread.Link{Family: family, Key: valueID([]any{fact, row.Ordinal}), From: row.Sender, To: row.Recipient}, FactSetID: fact, Ordinal: row.Ordinal, Date: row.Date})
		}
		r.partitions = append(r.partitions, windowPartition{publication: WindowPublication{GenerationID: g.GenerationID, GenerationSHA256: valueID("bytes-" + p.Cycle), Generation: g}, source: s, verify: func(context.Context) error { return nil }})
	}
	sort.Slice(r.partitions, func(i, j int) bool {
		return r.partitions[i].publication.GenerationID < r.partitions[j].publication.GenerationID
	})
	return r
}

func windowQuery(ledger flow.Ledger) WindowPathQuery {
	return WindowPathQuery{From: "C00000001", Target: "C00000003", Ledger: ledger, MaxHops: 4, Limit: 10, Budget: 100}
}

func TestWindowReaderComposesBothLedgersWithExactSourceRoutingAndReplay(t *testing.T) {
	for _, ledger := range []flow.Ledger{flow.ScheduleA, flow.ScheduleB} {
		r := windowFixture(t, ledger)
		q := windowQuery(ledger)
		q.Window = &DateWindow{"2022-12-30", "2023-01-02"}
		out, err := r.Paths(context.Background(), q)
		if err != nil {
			t.Fatal(err)
		}
		if len(out.Paths) != 2 || len(out.Links) != 3 || len(out.Vertices) != 3 || out.FinancialEligibility || out.TerminalEligible {
			t.Fatal("lost paths/grain or invented money", out)
		}
		var rows, included, unknown uint64
		for _, c := range out.Coverage {
			rows += c.Rows
			included += c.Included
			unknown += c.UnknownExcluded
		}
		if rows != 4 || included != 3 || unknown != 1 {
			t.Fatal(out.Coverage)
		}
		for _, vertex := range out.Vertices {
			if len(vertex.Facets) != 2 || reflect.DeepEqual(vertex.Facets[0].Facet, vertex.Facets[1].Facet) {
				t.Fatal("historical facets merged")
			}
		}
		for _, l := range out.Links {
			if l.Date == nil {
				t.Fatal("undated link included in bounded window")
			}
			for _, p := range r.partitions {
				if l.GenerationID == p.publication.GenerationID && !slices.Contains(p.source.(*windowStub).evidenceKeys, l.Topology.Key) {
					t.Fatal("wrong source routing")
				}
			}
		}
		want, _ := json.Marshal(out)
		// Reorder the source stream and replay; input ordering must not affect paths.
		for _, p := range r.partitions {
			slices.Reverse(p.source.(*windowStub).rows)
		}
		again, err := r.Paths(context.Background(), q)
		got, _ := json.Marshal(again)
		if err != nil || string(got) != string(want) {
			t.Fatal("unstable source-order replay", err)
		}
		out.Inputs[0].Generation.Receipts.Counts["fixture"] = 999
		q.Window.Start = "2023-01-01"
		if again.Inputs[0].Generation.Receipts.Counts["fixture"] != 1 || out.Query.Window.Start != "2022-12-30" || r.partitions[0].publication.Generation.Receipts.Counts["fixture"] != 1 {
			t.Fatal("response aliases query or verified state")
		}
	}
}

func TestWindowDateScopeUnknownsAndSearchBoundsStayExplicit(t *testing.T) {
	r := windowFixture(t, flow.ScheduleA)
	q := windowQuery(flow.ScheduleA)
	q.Target = "C00000004"
	all, err := r.Paths(context.Background(), q)
	if err != nil || len(all.Paths) != 2 {
		t.Fatal(err, all.Paths)
	}
	var undated uint64
	for _, c := range all.Coverage {
		undated += c.UndatedIncluded
	}
	if undated != 1 {
		t.Fatal("unknown time lost")
	}
	q.Window = &DateWindow{"2022-12-30", "2023-01-02"}
	bounded, err := r.Paths(context.Background(), q)
	if err != nil || len(bounded.Paths) != 0 || bounded.TerminalEligible {
		t.Fatal("unknown time promoted or terminal invented", err)
	}
	q.Target = "C00000003"
	q.Window.Start = "2023-01-01"
	narrow, err := r.Paths(context.Background(), q)
	if err != nil || len(narrow.Paths) != 0 {
		t.Fatal(err)
	}
	q.Window = &DateWindow{"2022-12-30", "2022-12-30"}
	oneDay, err := r.Paths(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	var before, after, included uint64
	for _, c := range oneDay.Coverage {
		before += c.Before
		after += c.After
		included += c.Included
	}
	if before != 0 || after != 2 || included != 1 {
		t.Fatal(oneDay.Coverage)
	}
	q.Window = nil
	q.Limit = 1
	limited, err := r.Paths(context.Background(), q)
	if err != nil || limited.Search.State != "truncated_path_limit" || limited.Search.MorePaths != "yes" {
		t.Fatal(limited.Search, err)
	}
	q.Budget = 1
	limited, err = r.Paths(context.Background(), q)
	if err != nil || limited.Search.MorePaths != "unknown" || limited.TerminalEligible {
		t.Fatal(limited.Search, err)
	}
	if all.ResultID == bounded.ResultID || bounded.ResultID == narrow.ResultID {
		t.Fatal("query scope absent from identity")
	}
}

func TestWindowReaderRejectsMissingForeignDuplicateAndFailedEvidence(t *testing.T) {
	for _, mutate := range []func(*WindowReader){
		func(r *WindowReader) { s := r.partitions[0].source.(*windowStub); s.rows = nil },
		func(r *WindowReader) { r.partitions[0].source.(*windowStub).rows[0].FactSetID = valueID("foreign") },
		func(r *WindowReader) {
			r.partitions[0].source.(*windowStub).rows[0].Link.Family = "sender_reported_committee_observation"
		},
		func(r *WindowReader) { s := r.partitions[0].source.(*windowStub); s.rows = append(s.rows, s.rows[0]) },
		func(r *WindowReader) { r.partitions[0].source.(*windowStub).err = errors.New("failed source") },
		func(r *WindowReader) {
			r.partitions[0].verify = func(context.Context) error { return errors.New("changed completion") }
		},
	} {
		r := windowFixture(t, flow.ScheduleA)
		mutate(r)
		out, err := r.Paths(context.Background(), windowQuery(flow.ScheduleA))
		if err == nil || out.ResultID != "" {
			t.Fatal("accepted invalid backing")
		}
	}
	r := windowFixture(t, flow.ScheduleA)
	checks := 0
	r.partitions[0].verify = func(context.Context) error {
		checks++
		if checks == 2 {
			return errors.New("completion changed during query")
		}
		return nil
	}
	if out, err := r.Paths(context.Background(), windowQuery(flow.ScheduleA)); err == nil || out.ResultID != "" {
		t.Fatal("missed final completion change")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := windowFixture(t, flow.ScheduleA).Paths(ctx, windowQuery(flow.ScheduleA)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestWindowPublicationAdmissionRejectsSnapshotOverlapAndResourceExcess(t *testing.T) {
	r := windowFixture(t, flow.ScheduleA)
	inputs := []WindowPublication{r.partitions[0].publication, r.partitions[1].publication}
	if err := validateWindowPublications(inputs); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func([]WindowPublication){
		func(v []WindowPublication) { v[1] = v[0] },
		func(v []WindowPublication) { v[1].Generation.Cycle = v[0].Generation.Cycle },
		func(v []WindowPublication) {
			v[1].Generation.CommitteeFlow.Inputs.A.FactSetID = v[0].Generation.CommitteeFlow.Inputs.A.FactSetID
		},
		func(v []WindowPublication) { v[0].Generation.CommitteeFlow.B.Rows = MaxWindowObservations },
		func(v []WindowPublication) { v[0].Generation.CommitteeFlow.B.Rows = ^uint64(0) },
	} {
		copy := slices.Clone(inputs)
		mutate(copy)
		if validateWindowPublications(copy) == nil {
			t.Fatal("accepted overlapping/excessive input")
		}
	}
	if validateWindowPublications(nil) == nil || validateWindowPublications(make([]WindowPublication, MaxWindowInputs+1)) == nil {
		t.Fatal("input cap ignored")
	}
}

func TestWindowQueryAndUnopenedReaderFailBeforeIO(t *testing.T) {
	valid := windowQuery(flow.ScheduleA)
	for _, mutate := range []func(*WindowPathQuery){
		func(q *WindowPathQuery) { q.From = q.Target },
		func(q *WindowPathQuery) { q.Target = "H0ZZ00001" },
		func(q *WindowPathQuery) { q.Ledger = "both" },
		func(q *WindowPathQuery) { q.MaxHops = 9 },
		func(q *WindowPathQuery) { q.Window = &DateWindow{"2023-02-29", "2023-03-01"} },
		func(q *WindowPathQuery) { q.Window = &DateWindow{"2024-01-02", "2024-01-01"} },
		func(q *WindowPathQuery) { q.Window = &DateWindow{"2024-01-01", ""} },
		func(q *WindowPathQuery) { q.Window = &DateWindow{"2024-01-01T00:00:00Z", "2024-01-02"} },
	} {
		q := valid
		mutate(&q)
		if q.Validate() == nil {
			t.Fatal("bad query accepted", q)
		}
	}
	if lo, hi, err := (DateWindow{"2024-02-29", "2024-02-29"}).bounds(); err != nil || lo != hi {
		t.Fatal("valid leap day rejected", err)
	}
	var r *WindowReader
	if _, err := r.Paths(context.Background(), valid); err == nil {
		t.Fatal("unopened reader accepted")
	}
}

func TestWindowInputSpecPinsBytesAndCannotAuthorizeMissingBacking(t *testing.T) {
	spec := WindowInputSpec{Version: WindowInputsVersion, Inputs: []WindowInput{{Generation: "absent", GenerationSHA256: valueID("sha"), GraphManifest: "g", Participants: "p", Conduits: "c"}}}
	b, _ := json.Marshal(spec)
	path := filepath.Join(t.TempDir(), "inputs.json")
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	got, err := ReadWindowInputs(path, fileSHA(b))
	if err != nil || !reflect.DeepEqual(got, spec) {
		t.Fatal(err)
	}
	if _, err := ReadWindowInputs(path, valueID("changed")); err == nil {
		t.Fatal("wrong digest accepted")
	}
	if _, err := OpenWindowReader(context.Background(), WindowOpenOptions{Inputs: got.Inputs, StorageRoot: t.TempDir(), Endpoint: "http://unused.invalid", BuildSHA256: valueID("build")}); err == nil {
		t.Fatal("serialized request became source approval")
	}
	for _, bad := range [][]byte{[]byte(`{"version":"unknown","inputs":[]}`), []byte(`{"version":"a","version":"b"}`), make([]byte, (64<<10)+1)} {
		if err := os.WriteFile(path, bad, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadWindowInputs(path, fileSHA(bad)); err == nil {
			t.Fatal("invalid specification accepted")
		}
	}
}
