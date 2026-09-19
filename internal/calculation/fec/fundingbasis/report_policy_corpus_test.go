package fundingbasis

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

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/earmarkassociation"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptreferences"
	xsort "github.com/vedantadhobley/legal-tender/internal/storage/externalsort"
)

// Reuse pinned full-source report evidence, not a fresh report-by-report cycle
// scan. The optional topology check connects real source-role fixtures to the
// new complete-cycle endpoint artifact without changing the old report output.
func TestRetainedReportAssociationPolicyReplay(t *testing.T) {
	path := os.Getenv("LT_REPORT_ASSOCIATION_JSON")
	if path == "" {
		t.Skip("retained report policy gate is opt-in")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(b)
	if hex.EncodeToString(h[:]) != os.Getenv("LT_REPORT_ASSOCIATION_SHA256") {
		t.Fatal("report artifact digest mismatch")
	}
	var report ReportEvidence
	if err = json.Unmarshal(b, &report); err != nil {
		t.Fatal(err)
	}
	refs, associations, err := resolveReport(report.Receipts, report.Committee, report.File)
	if err != nil || report.Policy != ReportPolicy || !reflect.DeepEqual(refs, report.References) || !reflect.DeepEqual(associations, report.Associations) {
		t.Fatal("report-policy replay differs", err)
	}
	topologyPath := os.Getenv("LT_REPORT_TOPOLOGY_MANIFEST")
	if topologyPath == "" {
		t.Logf("unchanged reference/association decisions for %d retained rows", len(report.Receipts))
		return
	}
	b, err = os.ReadFile(topologyPath)
	if err != nil {
		t.Fatal(err)
	}
	var topology receiptreferences.TopologyResult
	if err = json.Unmarshal(b, &topology); err != nil {
		t.Fatal(err)
	}
	if topology.FactSetID != report.Input.FactSetID || topology.Cycle != report.Cycle || topology.Scope != report.Scope {
		t.Fatal("report/topology ancestry mismatch")
	}
	inputs := map[uint64]earmarkassociation.Evidence{}
	for _, row := range report.Receipts {
		input, err := decodeEvidence(row)
		if err != nil {
			t.Fatal(err)
		}
		inputs[row.Ordinal] = associationEvidence(input)
	}
	r, err := xsort.Open(context.Background(), filepath.Join(filepath.Dir(topologyPath), "data"), topology.Endpoints)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	endpoints := map[uint64]receiptreferences.Endpoint{}
	for {
		v, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if _, selected := inputs[v.Ordinal]; !selected {
			continue
		}
		x, err := receiptreferences.DecodeEndpoint(v, topology.SourceRows)
		if err != nil {
			t.Fatal(err)
		}
		endpoints[x.Ordinal] = x
	}
	for _, expected := range report.Associations {
		x := endpoints[expected.Ordinal]
		var other *earmarkassociation.Related
		if x.Peers == 1 {
			evidence, ok := inputs[x.OnlyPeer]
			if !ok {
				t.Fatal("peer outside complete retained report")
			}
			peer, ok := endpoints[x.OnlyPeer]
			if !ok {
				t.Fatal("missing peer topology")
			}
			other = &earmarkassociation.Related{Evidence: evidence, Topology: earmarkassociation.Topology{Unsafe: peer.UnsafeReasons != 0, Peers: peer.Peers}}
			if !reflect.DeepEqual(expected.Related, []uint64{x.OnlyPeer}) {
				t.Fatal("related source ordinal differs")
			}
		}
		got, err := earmarkassociation.Decide(inputs[expected.Ordinal], earmarkassociation.Topology{Peers: x.Peers, Unsafe: x.UnsafeReasons != 0}, other)
		if err != nil || got.State != expected.State || !reflect.DeepEqual(got.ConduitID, expected.ConduitID) || got.AmountComparison != expected.AmountComparison || got.AdditionalAmount != expected.AdditionalAmount || got.TerminalEligible != expected.TerminalEligible {
			t.Fatal("cycle topology role decision differs", err)
		}
	}
	t.Logf("unchanged report replay and cycle-topology qualification for %d source rows / %d association decisions", len(report.Receipts), len(report.Associations))
}
