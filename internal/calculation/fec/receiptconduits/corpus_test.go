package receiptconduits

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	policy "github.com/vedantadhobley/legal-tender/internal/calculation/fec/earmarkassociation"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/fundingbasis"
	participants "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
	refs "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptreferences"
	xsort "github.com/vedantadhobley/legal-tender/internal/storage/externalsort"
)

func TestRetainedConduitCorpus(t *testing.T) {
	path := os.Getenv("LT_CONDUIT_MANIFEST")
	if path == "" {
		t.Skip("retained conduit gate is opt-in")
	}
	r, err := Load(path, os.Getenv("LT_CONDUIT_ID"))
	if err != nil {
		t.Fatal(err)
	}
	pPath, tPath := os.Getenv("LT_CONDUIT_PARTICIPANTS"), os.Getenv("LT_CONDUIT_TOPOLOGY")
	b, pSHA, err := readManifest(pPath)
	if err != nil {
		t.Fatal(err)
	}
	p, err := participants.DecodeManifest(b, r.ParticipantID)
	if err != nil {
		t.Fatal(err)
	}
	top, tSHA, err := refs.LoadTopology(tPath, r.TopologyID)
	if err != nil {
		t.Fatal(err)
	}
	if pSHA != r.ParticipantSHA256 || tSHA != r.TopologySHA256 || top.FactSetID != p.FactSetID || r.FactSetID != p.FactSetID || r.SourceRows != p.SourceRows {
		t.Fatal("exact parent identities")
	}
	wants := map[uint64]Decision{}
	for _, spec := range []struct{ name, sha string }{{"report-1730369-final.json", "3db80e7ef709084c6225030d755501c369d9378b8db9b94b6c1063e324d51bd7"}, {"report-1753173.json", "eacdd7a37742883b1c7c1c389a6bfdd3872c65431d0675b58d2cd7797157f485"}} {
		base := os.Getenv("LT_CONDUIT_REPORTS")
		if base == "" {
			t.Fatal("retained report evidence required")
		}
		b, err := os.ReadFile(filepath.Join(base, spec.name))
		if err != nil {
			t.Fatal(err)
		}
		h := sha256.Sum256(b)
		if hex.EncodeToString(h[:]) != spec.sha {
			t.Fatal("report artifact changed")
		}
		var report fundingbasis.ReportEvidence
		if err = json.Unmarshal(b, &report); err != nil {
			t.Fatal(err)
		}
		if report.Input.FactSetID != r.FactSetID || report.Policy != policy.Policy {
			t.Fatal("report identity")
		}
		for _, a := range report.Associations {
			var peer uint64
			if len(a.Related) == 1 {
				peer = a.Related[0]
			}
			wants[a.Ordinal] = Decision{Ordinal: a.Ordinal, Related: peer, State: a.State, AmountComparison: a.AmountComparison, ConduitID: a.ConduitID}
		}
	}
	stream, err := xsort.Open(context.Background(), filepath.Join(filepath.Dir(path), "data"), r.Decisions)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	statesSeen, amountsSeen := map[string]uint64{}, map[string]uint64{}
	samples := map[uint64]Decision{}
	needed := map[uint64]bool{}
	var previous, seen, reportsChecked uint64
	for {
		record, err := stream.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		d, err := DecodeDecision(record, r.SourceRows)
		if err != nil || d.Ordinal <= previous {
			t.Fatal("bad complete disposition stream", err)
		}
		previous = d.Ordinal
		seen++
		if want, ok := wants[d.Ordinal]; ok {
			if !reflect.DeepEqual(d, want) {
				t.Fatalf("old report decision differs at %d", d.Ordinal)
			}
			reportsChecked++
		}
		if statesSeen[d.State] == 0 || seen%1000000 == 0 {
			samples[d.Ordinal] = d
			needed[d.Ordinal] = true
			if d.Related != 0 {
				needed[d.Related] = true
			}
		}
		statesSeen[d.State]++
		amountsSeen[d.AmountComparison]++
	}
	if seen != r.EligibleRoleRows || reportsChecked != uint64(len(wants)) || !reflect.DeepEqual(statesSeen, r.States) || !reflect.DeepEqual(amountsSeen, r.Amounts) {
		t.Fatal("full census/report conservation")
	}
	endpoints := map[uint64]refs.Endpoint{}
	tr, err := xsort.Open(context.Background(), filepath.Join(filepath.Dir(tPath), "data"), top.Endpoints)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	for {
		record, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		e, err := refs.DecodeEndpoint(record, r.SourceRows)
		if err != nil {
			t.Fatal(err)
		}
		if needed[e.Ordinal] {
			endpoints[e.Ordinal] = e
		}
	}
	rows := map[uint64]participants.Row{}
	var mu sync.Mutex
	err = parallel(context.Background(), 8, len(p.Files), func(ctx context.Context, i int) error {
		f := p.Files[i]
		used := false
		for ordinal := range needed {
			if ordinal >= f.First && ordinal <= f.Last {
				used = true
				break
			}
		}
		if !used {
			return nil
		}
		_, err := participants.ReadShard(ctx, filepath.Join(filepath.Dir(pPath), "data"), f, func(v participants.Row) error {
			if needed[uint64(v.Ordinal)] {
				b, err := json.Marshal(v)
				if err != nil {
					return err
				}
				var own participants.Row
				if err = json.Unmarshal(b, &own); err != nil {
					return err
				}
				mu.Lock()
				rows[uint64(v.Ordinal)] = own
				mu.Unlock()
			}
			return nil
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != len(needed) {
		t.Fatal("witness source coverage")
	}
	for ordinal, want := range samples {
		row := rows[ordinal]
		e := endpoints[ordinal]
		var related *policy.Related
		if e.Peers == 1 {
			other := rows[e.OnlyPeer]
			related = &policy.Related{Evidence: evidence(other), Topology: topology(endpoints[e.OnlyPeer])}
		}
		got, err := decide(row, e, related)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatal("independent endpoint/role witness mismatch", ordinal, err)
		}
	}
	t.Logf("verified all %d decisions, %d retained-report expectations and %d data-selected role/endpoint witnesses", seen, reportsChecked, len(samples))
}
