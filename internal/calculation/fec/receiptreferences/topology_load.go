package receiptreferences

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
)

// LoadTopology pins the manifest bytes and logical identity. Consumers must
// still exhaust the endpoint stream to verify its complete backing and census.
func LoadTopology(path, expected string) (TopologyResult, string, error) {
	if filepath.Base(path) != "manifest.json" || !canonicalDigest(expected) {
		return TopologyResult{}, "", fmt.Errorf("exact topology manifest and identity required")
	}
	f, err := os.Open(path)
	if err != nil {
		return TopologyResult{}, "", err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil || len(b) > 1<<20 {
		return TopologyResult{}, "", fmt.Errorf("topology manifest read/size failed")
	}
	var r TopologyResult
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err = d.Decode(&r); err != nil {
		return r, "", err
	}
	if d.Decode(new(any)) != io.EOF {
		return r, "", fmt.Errorf("trailing topology manifest data")
	}
	if r.SchemaVersion != TopologyVersion || r.Policy != TopologyPolicy || r.State != "complete_reference_endpoint_topology" || r.Scope != "published_schedule_a_cycle_report" || r.CalculationID != expected || topologyID(r) != expected || r.ConduitEligibilityEvaluated || r.FinancialEligibility ||
		!canonicalDigest(r.BuildSHA256) || !canonicalDigest(r.ReferenceCalculationID) || !canonicalDigest(r.ReferenceManifestSHA256) || !canonicalDigest(r.FactSetID) || !canonicalDigest(r.FactManifestSHA256) || r.SourceRows == 0 || r.SourceRows > math.MaxInt64 ||
		r.ExactEndpointRows > r.Endpoints.Rows || r.UnsafeEndpointRows > r.Endpoints.Rows || r.Endpoints.Rows > r.SourceRows || r.Endpoints.Rows > r.ExactEndpointRows+r.UnsafeEndpointRows || r.InvalidReferenceRows > r.SourceRows || r.InvalidTargetMemberRows > r.SourceRows ||
		r.Endpoints.Name == "" || filepath.Base(r.Endpoints.Name) != r.Endpoints.Name || !canonicalDigest(r.Endpoints.SHA256) || !canonicalDigest(r.Endpoints.ValuesSHA256) {
		return r, "", fmt.Errorf("topology identity/scope/conservation mismatch")
	}
	h := sha256.Sum256(b)
	return r, hex.EncodeToString(h[:]), nil
}
