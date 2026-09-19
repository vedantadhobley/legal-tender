package receiptreferences

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	xsort "github.com/vedantadhobley/legal-tender/internal/storage/externalsort"
)

// This read-only gate cross-checks the entire new endpoint stream against the
// retained decision and neighbor streams. It does not reread Schedule A or call
// the topology builder. Invalid-target propagation has a separate direct oracle
// in the fixture gate; this census does not substitute for that proof.
func TestRetainedTopologyCorpus(t *testing.T) {
	path := os.Getenv("LT_TOPOLOGY_MANIFEST")
	if path == "" {
		t.Skip("retained topology gate is opt-in")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out TopologyResult
	if err = json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	referencePath := os.Getenv("LT_TOPOLOGY_REFERENCE_MANIFEST")
	ref, digest, err := loadReferenceResult(referencePath, out.ReferenceCalculationID)
	if err != nil {
		t.Fatal(err)
	}
	if out.CalculationID != topologyID(out) || out.SchemaVersion != TopologyVersion || out.State != "complete_reference_endpoint_topology" || out.Policy != TopologyPolicy || out.ConduitEligibilityEvaluated || out.FinancialEligibility ||
		out.ReferenceManifestSHA256 != digest || out.FactSetID != ref.FactSetID || out.FactManifestSHA256 != ref.ManifestSHA256 || out.Cycle != ref.Cycle || out.Scope != ref.Scope || out.SourceRows != ref.SourceRows {
		t.Fatal("invalid topology identity/scope")
	}
	open := func(dir string, file xsort.File) *xsort.Reader {
		r, err := xsort.Open(context.Background(), dir, file)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(r.Close)
		return r
	}
	sourceDir := filepath.Join(filepath.Dir(referencePath), "data")
	d := open(sourceDir, ref.Decisions)
	n := open(sourceDir, ref.Neighbors)
	e := open(filepath.Join(filepath.Dir(path), "data"), out.Endpoints)
	nextD, de := d.Next()
	nextN, ne := n.Next()
	var total, exact, unsafe, own, target, previous, singleSafe uint64
	for {
		v, err := e.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		x, err := DecodeEndpoint(v, out.SourceRows)
		if err != nil || x.Ordinal <= previous {
			t.Fatal(x, err)
		}
		previous = x.Ordinal
		total++
		for de == nil && nextD.Ordinal < x.Ordinal {
			// Every reference source, including exact references, must be present.
			t.Fatal("reference source missing from endpoint stream", nextD.Ordinal)
		}
		ownInvalid := false
		if de == nil && nextD.Ordinal == x.Ordinal {
			var decision Decision
			if err = json.Unmarshal(nextD.Data, &decision); err != nil {
				t.Fatal(err)
			}
			ownInvalid = decision.Target == nil
			nextD, de = d.Next()
		}
		if de != nil && de != io.EOF {
			t.Fatal(de)
		}
		if (x.UnsafeReasons&InvalidOwnReference != 0) != ownInvalid {
			t.Fatal("invalid-source membership differs", x.Ordinal)
		}
		if ownInvalid {
			own++
		}
		if x.UnsafeReasons&InvalidIncomingReference != 0 {
			target++
		}
		if x.UnsafeReasons != 0 {
			unsafe++
		}
		if x.Peers == 1 && x.UnsafeReasons == 0 {
			singleSafe++
		}
		if x.Peers > 0 {
			if ne != nil {
				t.Fatal("missing neighbor", ne)
			}
			var neighbor Neighbors
			if err = json.Unmarshal(nextN.Data, &neighbor); err != nil {
				t.Fatal(err)
			}
			if neighbor.Ordinal != x.Ordinal || neighbor.Peers != x.Peers || neighbor.Incoming != x.Incoming || neighbor.Outgoing != x.Outgoing {
				t.Fatal("neighbor evidence differs", x.Ordinal)
			}
			exact++
			nextN, ne = n.Next()
		}
		if ne != nil && ne != io.EOF {
			t.Fatal(ne)
		}
	}
	if de != io.EOF || ne != io.EOF {
		t.Fatal("unconsumed reference backing", de, ne)
	}
	if total != out.Endpoints.Rows || exact != out.ExactEndpointRows || exact != ref.Neighbors.Rows || unsafe != out.UnsafeEndpointRows || own != out.InvalidReferenceRows || target != out.InvalidTargetMemberRows {
		t.Fatal("endpoint census mismatch", total, exact, unsafe, own, target)
	}
	t.Logf("verified %d endpoints; %d exact-neighbor endpoints; %d unsafe; %d invalid sources; %d invalid-target members; %d locally safe one-peer endpoints (NOT qualified pairs/conduits)", total, exact, unsafe, own, target, singleSafe)
}
