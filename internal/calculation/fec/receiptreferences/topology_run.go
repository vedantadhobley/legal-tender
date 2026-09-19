package receiptreferences

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/reportreference"
	xsort "github.com/vedantadhobley/legal-tender/internal/storage/externalsort"
)

const TopologyVersion = "legal-tender.fec.receipt-reference-topology.v1"
const TopologyPolicy = "fec/receipt-reference-endpoint-safety@1.0.0"

type TopologyOptions struct {
	ReferenceManifest, ExpectedReferenceID, OutputDirectory, BuildSHA256 string
	RunRows, FanIn                                                       int
	MaxWorkspaceBytes                                                    uint64
	Progress                                                             func(string)
}

type TopologyResult struct {
	SchemaVersion               string     `json:"schema_version"`
	State                       string     `json:"state"`
	CalculationID               string     `json:"calculation_id"`
	BuildSHA256                 string     `json:"executable_sha256"`
	ReferenceCalculationID      string     `json:"reference_calculation_id"`
	ReferenceManifestSHA256     string     `json:"reference_manifest_sha256"`
	FactSetID                   string     `json:"fact_set_id"`
	FactManifestSHA256          string     `json:"fact_manifest_sha256"`
	Cycle                       string     `json:"cycle"`
	Scope                       string     `json:"scope"`
	Policy                      string     `json:"topology_policy"`
	SourceRows                  uint64     `json:"source_rows"`
	ExactEndpointRows           uint64     `json:"exact_endpoint_rows"`
	UnsafeEndpointRows          uint64     `json:"unsafe_endpoint_rows"`
	InvalidReferenceRows        uint64     `json:"invalid_reference_rows"`
	InvalidTargetMemberRows     uint64     `json:"invalid_target_member_rows"`
	Endpoints                   xsort.File `json:"endpoints"`
	RunRows                     int        `json:"sort_run_rows"`
	FanIn                       int        `json:"merge_fan_in"`
	PeakWorkspaceBytes          uint64     `json:"peak_workspace_bytes"`
	RetainedBytes               uint64     `json:"retained_data_bytes"`
	ElapsedMS                   int64      `json:"elapsed_ms"`
	PeakRSSBytes                uint64     `json:"peak_rss_bytes"`
	ConduitEligibilityEvaluated bool       `json:"conduit_eligibility_evaluated"`
	FinancialEligibility        bool       `json:"financial_eligibility"`
}

func canonicalDigest(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == 32 && hex.EncodeToString(b) == s
}

// The manifest is pinned by calculation ID, and its exact bytes are retained in
// the consumer identity. All four backing artifacts are consumed and verified
// before a topology manifest can be published. This is not a raw-source rescan.
func loadReferenceResult(path, expectedID string) (Result, string, error) {
	if !canonicalDigest(expectedID) || filepath.Base(path) != "manifest.json" {
		return Result{}, "", fmt.Errorf("exact reference manifest.json and expected calculation ID required")
	}
	f, err := os.Open(path)
	if err != nil {
		return Result{}, "", err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil || len(b) > 1<<20 {
		return Result{}, "", fmt.Errorf("reference manifest read/size failed: %v", err)
	}
	var r Result
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err = d.Decode(&r); err != nil {
		return Result{}, "", err
	}
	if d.Decode(new(any)) != io.EOF {
		return Result{}, "", fmt.Errorf("trailing reference manifest data")
	}
	if r.SchemaVersion != Version || r.State != "complete_cycle_reference_join" || r.Policy != reportreference.Policy || r.Scope != "published_schedule_a_cycle_report" ||
		r.ConduitEligibilityEvaluated || r.FinancialEligibility || r.CalculationID != expectedID || logicalID(r) != expectedID ||
		!canonicalDigest(r.BuildSHA256) || !canonicalDigest(r.FactSetID) || !canonicalDigest(r.ManifestSHA256) || r.SourceRows == 0 || r.SourceRows > math.MaxInt64 ||
		r.ReferenceRows > r.SourceRows || r.Decisions.Rows != r.ReferenceRows || r.Neighbors.Rows > r.SourceRows || r.LookupMemberRows > r.SourceRows {
		return Result{}, "", fmt.Errorf("invalid reference result identity/scope/counts")
	}
	var total uint64
	for _, n := range r.States {
		if n > r.SourceRows-total {
			return Result{}, "", fmt.Errorf("reference state overflow")
		}
		total += n
	}
	if total != r.SourceRows || r.States["no_report_reference"] != r.SourceRows-r.ReferenceRows || r.States["exact_same_report_reference"] > r.ReferenceRows ||
		r.ExactIncidences.Rows != 2*r.States["exact_same_report_reference"] || r.LookupEvidence.Rows < r.LookupMemberRows || r.LookupEvidence.Rows-r.LookupMemberRows > 2*r.ReferenceRows {
		return Result{}, "", fmt.Errorf("reference result conservation")
	}
	names := map[string]bool{}
	for _, desc := range []xsort.File{r.Decisions, r.LookupEvidence, r.ExactIncidences, r.Neighbors} {
		if desc.Name == "" || desc.Name == "." || desc.Name == ".." || filepath.Base(desc.Name) != desc.Name || names[desc.Name] || !canonicalDigest(desc.SHA256) || !canonicalDigest(desc.ValuesSHA256) {
			return Result{}, "", fmt.Errorf("invalid reference backing descriptor")
		}
		names[desc.Name] = true
	}
	h := sha256.Sum256(b)
	return r, hex.EncodeToString(h[:]), nil
}

// Load validates and pins metadata; callers must verify each backing stream
// they consume. It does not imply that every reference artifact was opened.
func Load(path, expectedID string) (Result, string, error) {
	return loadReferenceResult(path, expectedID)
}

func RunTopology(ctx context.Context, o TopologyOptions) (TopologyResult, error) {
	started := time.Now()
	if o.OutputDirectory == "" || !canonicalDigest(o.BuildSHA256) || o.RunRows < 1 || o.RunRows > 100000 || o.FanIn < 2 || o.FanIn > 16 || o.MaxWorkspaceBytes == 0 || o.MaxWorkspaceBytes > 64<<30 {
		return TopologyResult{}, fmt.Errorf("new output, executable digest, bounded runs/fan-in and <=64GiB workspace required")
	}
	if _, err := os.Lstat(o.OutputDirectory); !os.IsNotExist(err) {
		return TopologyResult{}, fmt.Errorf("output directory must be new")
	}
	reference, digest, err := loadReferenceResult(o.ReferenceManifest, o.ExpectedReferenceID)
	if err != nil {
		return TopologyResult{}, err
	}
	if err = ctx.Err(); err != nil {
		return TopologyResult{}, err
	}
	var fs syscall.Statfs_t
	if err = syscall.Statfs(filepath.Dir(o.OutputDirectory), &fs); err != nil {
		return TopologyResult{}, err
	}
	if fs.Bavail*uint64(fs.Bsize) < o.MaxWorkspaceBytes {
		return TopologyResult{}, fmt.Errorf("workspace cap exceeds available disk")
	}
	if err = os.Mkdir(o.OutputDirectory, 0750); err != nil {
		return TopologyResult{}, err
	}
	space, err := xsort.NewWorkspace(filepath.Join(o.OutputDirectory, "data"), o.MaxWorkspaceBytes)
	if err != nil {
		return TopologyResult{}, err
	}
	r, err := calculateTopology(ctx, space, filepath.Join(filepath.Dir(o.ReferenceManifest), "data"), reference, o)
	if err != nil {
		return TopologyResult{}, err
	}
	r.SchemaVersion, r.State, r.Policy = TopologyVersion, "complete_reference_endpoint_topology", TopologyPolicy
	r.BuildSHA256, r.ReferenceCalculationID, r.ReferenceManifestSHA256 = o.BuildSHA256, reference.CalculationID, digest
	r.FactSetID, r.FactManifestSHA256, r.Cycle, r.Scope = reference.FactSetID, reference.ManifestSHA256, reference.Cycle, reference.Scope
	r.SourceRows, r.RunRows, r.FanIn = reference.SourceRows, o.RunRows, o.FanIn
	r.PeakWorkspaceBytes, r.RetainedBytes = space.Stats()
	r.ElapsedMS = time.Since(started).Milliseconds()
	var usage syscall.Rusage
	if err = syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return TopologyResult{}, err
	}
	r.PeakRSSBytes = uint64(usage.Maxrss) * 1024
	r.CalculationID = topologyID(r)
	if err = ctx.Err(); err != nil {
		return TopologyResult{}, err
	}
	if err = saveJSONResult(o.OutputDirectory, r); err != nil {
		return TopologyResult{}, err
	}
	return r, nil
}

func calculateTopology(ctx context.Context, space *xsort.Workspace, dir string, reference Result, o TopologyOptions) (TopologyResult, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	t := topologyWork{ctx: ctx, space: space, dir: dir, reference: reference, runRows: o.RunRows, fanIn: o.FanIn}
	var progressMu sync.Mutex
	log := func(s string) {
		progressMu.Lock()
		defer progressMu.Unlock()
		if o.Progress != nil {
			o.Progress(s)
		}
	}
	var wg sync.WaitGroup
	var exact, unsafe xsort.File
	var exactErr, unsafeErr error
	var r TopologyResult
	// These passes read disjoint immutable inputs and share one disk budget.
	wg.Go(func() {
		log("checking exact incidences against every retained neighbor")
		exact, exactErr = t.exactEndpoints()
		if exactErr != nil {
			cancel()
		} else {
			log("exact endpoint readback complete")
		}
	})
	wg.Go(func() {
		log("propagating invalid references to sources and all matching targets")
		unsafe, r.InvalidReferenceRows, r.InvalidTargetMemberRows, unsafeErr = t.invalidIncidents()
		if unsafeErr != nil {
			cancel()
		} else {
			log("invalid incident propagation complete")
		}
	})
	wg.Wait()
	if exactErr != nil && !errors.Is(exactErr, context.Canceled) {
		return TopologyResult{}, exactErr
	}
	if unsafeErr != nil {
		return TopologyResult{}, unsafeErr
	}
	if exactErr != nil {
		return TopologyResult{}, exactErr
	}
	log("merging and verifying sparse endpoint topology")
	var err error
	r.Endpoints, r.UnsafeEndpointRows, err = t.combine(exact, unsafe)
	if err != nil {
		return TopologyResult{}, err
	}
	r.ExactEndpointRows = exact.Rows
	if r.ExactEndpointRows != reference.Neighbors.Rows || r.Endpoints.Rows > reference.SourceRows || r.UnsafeEndpointRows < r.InvalidReferenceRows || r.Endpoints.Rows < r.ExactEndpointRows || r.Endpoints.Rows < r.UnsafeEndpointRows {
		return TopologyResult{}, fmt.Errorf("endpoint disposition conservation")
	}
	for _, f := range []xsort.File{exact, unsafe} {
		if err = space.Remove(f); err != nil {
			return TopologyResult{}, err
		}
	}
	return r, nil
}

func topologyID(r TopologyResult) string {
	v := struct {
		Version, Policy, Build, Reference, ReferenceManifest, Fact, FactManifest, Cycle, Scope, Values string
		SourceRows, ExactRows, UnsafeRows, InvalidRows, TargetRows, EndpointRows                       uint64
	}{r.SchemaVersion, r.Policy, r.BuildSHA256, r.ReferenceCalculationID, r.ReferenceManifestSHA256, r.FactSetID, r.FactManifestSHA256, r.Cycle, r.Scope, r.Endpoints.ValuesSHA256,
		r.SourceRows, r.ExactEndpointRows, r.UnsafeEndpointRows, r.InvalidReferenceRows, r.InvalidTargetMemberRows, r.Endpoints.Rows}
	h := sha256.Sum256(marshal(v))
	return hex.EncodeToString(h[:])
}
