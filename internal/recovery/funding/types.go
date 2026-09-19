// Package funding inventories explicitly pinned, typed publication dependencies.
// It never opens a database, follows a current pointer or fetches source data.
package funding

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	rel "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

const Version = "legal-tender.funding-recovery-inventory.v1"
const InputsVersion = "legal-tender.funding-recovery-inputs.v1"
const maxManifestBytes = 8 << 20
const maxMetadataBytes = 128 << 20
const maxNodes = 50000

// A locator supplies a physical location, never a replacement identity. Its
// digest must agree with the parent when the parent pins one. Dataset is used
// only by the classic FEC families. Paths are always storage-root relative.
type Reference struct {
	Kind    string  `json:"kind"`
	ID      string  `json:"id"`
	SHA256  string  `json:"sha256"`
	Path    string  `json:"path,omitempty"`
	Dataset string  `json:"dataset,omitempty"`
	Bytes   *uint64 `json:"bytes,omitempty"`
}
type Inputs struct {
	Version    string      `json:"schema_version"`
	Generation Reference   `json:"generation"`
	Locators   []Reference `json:"locators"`
}
type Options struct {
	StorageRoot string
	BuildSHA256 string
	HashBlobs   bool
}
type Node struct {
	Key            string    `json:"node_id"`
	Reference      Reference `json:"reference"`
	Schema         string    `json:"manifest_schema,omitempty"`
	Verification   string    `json:"verification"`
	ObservedSHA256 string    `json:"observed_sha256,omitempty"`
	ObservedBytes  *uint64   `json:"observed_bytes,omitempty"`
	Expanded       bool      `json:"dependencies_enumerated"`
	PriorChecks    []string  `json:"publisher_reported_passing_checks,omitempty"`
	Problem        string    `json:"problem,omitempty"`
}
type Edge struct {
	From string `json:"from"`
	To   string `json:"to"`
	Role string `json:"role"`
}
type Requirement struct {
	Parent   string `json:"introduced_by"`
	Kind     string `json:"kind"`
	Identity string `json:"identity"`
	State    string `json:"state"`
}
type Result struct {
	Version         string            `json:"schema_version"`
	ID              string            `json:"inventory_id"`
	BuildSHA256     string            `json:"executable_sha256"`
	InputSHA256     string            `json:"input_sha256"`
	Mode            string            `json:"file_verification_mode"`
	Root            string            `json:"root_node_id"`
	Complete        bool              `json:"file_dependency_inventory_complete"`
	AllFilesHashed  bool              `json:"all_file_bytes_verified"`
	RecoveryReady   bool              `json:"recovery_ready"`
	DependencyCycle bool              `json:"dependency_cycle_detected"`
	Nodes           []Node            `json:"nodes"`
	Edges           []Edge            `json:"dependencies"`
	Requirements    []Requirement     `json:"unverified_runtime_and_build_requirements"`
	Counts          map[string]uint64 `json:"verification_counts"`
	Limitations     []string          `json:"limitations"`
}

func digest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func validDigest(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 32 && hex.EncodeToString(b) == s
}
func validID(kind, id string) bool {
	if kind == "release" {
		return rel.ValidReleaseID(id)
	}
	return validDigest(id)
}
func identity(v any) string { b, _ := json.Marshal(v); return digest(b) }
func relative(s string) bool {
	if s == "" || path.Clean(s) != s {
		return false
	}
	if path.IsAbs(s) || s == "." || s == ".." || strings.HasPrefix(s, "../") || strings.Contains(s, "\\") {
		return false
	}
	for _, p := range strings.Split(s, "/") {
		if p == "current" || p == "current.json" {
			return false
		}
	}
	return true
}
func refKey(r Reference) string { return r.Kind + ":" + r.Dataset + ":" + r.ID }

func ReadInputs(file, sha string) (Inputs, error) {
	var v Inputs
	if !validDigest(sha) {
		return v, fmt.Errorf("exact input SHA256 required")
	}
	f, err := os.Open(file)
	if err != nil {
		return v, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil {
		return v, err
	}
	if len(b) > 1<<20 || digest(b) != sha {
		return v, fmt.Errorf("input bytes differ or exceed limit")
	}
	if err = strictjson.Decode(b, &v); err != nil {
		return v, err
	}
	return v, validateInputs(v)
}
func validateInputs(v Inputs) error {
	if v.Version != InputsVersion || (v.Generation.Kind != "generation" && v.Generation.Kind != "shared_generation") || len(v.Locators) > 1024 {
		return fmt.Errorf("supported generation input and bounded locators required")
	}
	seen := map[string]bool{}
	for _, r := range append([]Reference{v.Generation}, v.Locators...) {
		if !validID(r.Kind, r.ID) || !validDigest(r.SHA256) || !relative(r.Path) || seen[refKey(r)] {
			return fmt.Errorf("exact unique identities, digests and relative immutable paths required")
		}
		seen[refKey(r)] = true
	}
	return nil
}

func Inspect(ctx context.Context, in Inputs, inputSHA string, o Options) (Result, error) {
	return inspect(ctx, in, inputSHA, o, nil)
}

// metadata optionally retains the exact, bounded bytes already opened by the
// walker. Planning must not reread different bytes or open data bodies.
func inspect(ctx context.Context, in Inputs, inputSHA string, o Options, metadata map[string][]byte) (Result, error) {
	if err := validateInputs(in); err != nil {
		return Result{}, err
	}
	if !validDigest(inputSHA) || !validDigest(o.BuildSHA256) {
		return Result{}, fmt.Errorf("input and executable digests required")
	}
	root, err := os.OpenRoot(o.StorageRoot)
	if err != nil {
		return Result{}, err
	}
	defer root.Close()
	w := walker{ctx: ctx, root: root, options: o, metadata: metadata, locators: map[string]Reference{}, indices: map[string]int{}, paths: map[string]Reference{}, edges: map[Edge]bool{}, requirements: map[Requirement]bool{}}
	for _, r := range append([]Reference{in.Generation}, in.Locators...) {
		w.locators[refKey(r)] = r
	}
	w.out = Result{Version: Version, BuildSHA256: o.BuildSHA256, InputSHA256: inputSHA, Mode: "manifest_sha256_and_blob_presence_size", Counts: map[string]uint64{}, Nodes: []Node{}, Edges: []Edge{}, Requirements: []Requirement{}, Limitations: []string{
		"closed_typed_manifest_bytes_and_dependency_inventory_not_domain_replay",
		"publisher_checks_are_prior_attestations_not_fresh_content_checks",
		"full_coordinated_release_artifacts_and_history_are_retained_as_provenance",
		"no_live_graph_check_runtime_image_retention_or_raw_to_graph_rebuild",
	}}
	if o.HashBlobs {
		w.out.Mode = "sha256_all_declared_files"
	}
	w.out.Root, err = w.add("", "root", in.Generation)
	if err != nil {
		return Result{}, err
	}
	for i := 0; i < len(w.pending); i++ {
		if err = ctx.Err(); err != nil {
			return Result{}, err
		}
		if err = w.visit(w.pending[i]); err != nil {
			return Result{}, err
		}
	}
	return w.finish(), nil
}
