package fundinggeneration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"reflect"
	"sort"

	flow "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
	ie "github.com/vedantadhobley/legal-tender/internal/projection/arango/independentexpenditures"
	receipt "github.com/vedantadhobley/legal-tender/internal/projection/arango/receiptgraph"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

const WindowVersion = "legal-tender.funding-window-paths.v1"
const WindowPolicy = "fec/selected-observation-date-window@1.0.0"
const WindowInputsVersion = "legal-tender.funding-window-inputs.v1"
const WindowSharedInputsVersion = "legal-tender.funding-window-inputs.v2"

// Operational caps, not election-window or retention rules. The first reader
// holds verified selected models in memory; larger input sets need measurement.
const MaxWindowInputs = 8
const MaxWindowObservations uint64 = 2_000_000

type WindowInput struct {
	Generation       string             `json:"generation"`
	GenerationSHA256 string             `json:"generation_sha256"`
	GraphManifest    string             `json:"graph_manifest"`
	Participants     string             `json:"participants"`
	Conduits         string             `json:"conduits"`
	Shared           *SharedReadOptions `json:"shared_conduits,omitempty"`
}

func (i WindowInput) sharedOptions() SharedReadOptions {
	if i.Shared != nil {
		return *i.Shared
	}
	return SharedReadOptions{}
}

type WindowInputSpec struct {
	Version string        `json:"version"`
	Inputs  []WindowInput `json:"inputs"`
}

// ReadWindowInputs checks the request file, not graph/source backing. Secrets
// and connection settings are deliberately absent from this retained request.
func ReadWindowInputs(path, expectedSHA string) (WindowInputSpec, error) {
	var s WindowInputSpec
	if !validDigest(expectedSHA) {
		return s, fmt.Errorf("exact input specification checksum required")
	}
	f, err := os.Open(path)
	if err != nil {
		return s, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, (64<<10)+1))
	if err != nil {
		return s, err
	}
	digest := sha256.Sum256(b)
	if len(b) > 64<<10 || hex.EncodeToString(digest[:]) != expectedSHA {
		return s, fmt.Errorf("input specification bytes differ or exceed limit")
	}
	if err := strictjson.Decode(b, &s); err != nil {
		return s, err
	}
	if (s.Version != WindowInputsVersion && s.Version != WindowSharedInputsVersion) || len(s.Inputs) < 1 || len(s.Inputs) > MaxWindowInputs {
		return s, fmt.Errorf("supported input version and 1..%d publications required", MaxWindowInputs)
	}
	shared := false
	for _, input := range s.Inputs {
		if input.Shared != nil {
			shared = true
			v := input.Shared
			if v.BaseGeneration == "" || v.GraphManifest == "" || v.Conduits == "" {
				return s, fmt.Errorf("complete shared generation locators required")
			}
		}
	}
	if shared != (s.Version == WindowSharedInputsVersion) {
		return s, fmt.Errorf("shared inputs require explicit v2 specification; base-only inputs use v1")
	}
	return s, nil
}

type WindowOpenOptions struct {
	Inputs                                                 []WindowInput
	StorageRoot, Endpoint, Username, Password, BuildSHA256 string
	Progress                                               func(string)
}

type WindowPublication struct {
	GenerationID     string `json:"generation_id"`
	GenerationSHA256 string `json:"generation_sha256"`
	Generation       Result `json:"generation"`
	// Generation remains the unchanged base. This optional envelope binds the
	// outer identity and additional physical projection without relabeling it.
	Shared *SharedGeneration `json:"shared_generation,omitempty"`
}

// Only a successfully opened existing generation supplies a production source.
// This private interface permits failure/conservation tests without opening a DB.
type windowFlowSource interface {
	VisitDatedLinks(context.Context, flow.Ledger, func(flow.DatedLink) error) error
	PathEvidence(context.Context, flow.Ledger, string) (graphread.Item, error)
	Facet(context.Context, string) (graphread.Facet, error)
}

type windowPartition struct {
	publication WindowPublication
	source      windowFlowSource
	verify      func(context.Context) error
	receipts    windowReceiptSource
	outside     windowSpendingSource
	shared      windowSharedSource
}

type windowSharedSource interface {
	DatedPathEntry(context.Context, uint64) (receipt.DatedPathEntry, error)
	Entity(context.Context, string) (graphread.Facet, error)
}

type windowSpendingSource interface {
	VisitDatedMembers(context.Context, func(ie.DatedMember) error) error
	DatedMemberEvidence(context.Context, []ie.DatedMember) (map[string]graphread.Item, error)
	Entity(context.Context, string) (graphread.Facet, error)
}

type windowReceiptSource interface {
	DatedPathEntry(context.Context, string, uint64) (receipt.DatedPathEntry, error)
	AuthorizedLinks() []graphread.Link
	AuthorizationEvidence(context.Context, string) (graphread.Item, error)
	Entity(context.Context, string) (graphread.Facet, error)
}

type WindowReader struct {
	partitions []windowPartition
	build      string
}

// OpenWindowReader preserves each single-cycle opener and all its checks. It
// does not ask an existing reader to accept mismatched inputs or relabel a cycle.
func OpenWindowReader(ctx context.Context, o WindowOpenOptions) (*WindowReader, error) {
	if !validDigest(o.BuildSHA256) || o.StorageRoot == "" || o.Endpoint == "" || len(o.Inputs) < 1 || len(o.Inputs) > MaxWindowInputs {
		return nil, fmt.Errorf("storage, connection, build and bounded explicit inputs required")
	}
	metadata := make([]WindowPublication, len(o.Inputs))
	for i, input := range o.Inputs {
		if input.Shared != nil && (input.Shared.BaseGeneration == "" || input.Shared.GraphManifest == "" || input.Shared.Conduits == "") {
			return nil, fmt.Errorf("complete shared generation locators required")
		}
		if input.GraphManifest == "" || input.Participants == "" || input.Conduits == "" {
			return nil, fmt.Errorf("exact receipt locators required for every input")
		}
		v, shared, err := readQueryGeneration(input.Generation, input.GenerationSHA256, input.sharedOptions())
		if err != nil {
			return nil, err
		}
		id := v.GenerationID
		if shared != nil {
			id = shared.GenerationID
		}
		metadata[i] = WindowPublication{GenerationID: id, GenerationSHA256: input.GenerationSHA256, Generation: v, Shared: shared}
	}
	if err := validateWindowPublications(metadata); err != nil {
		return nil, err
	}
	out := &WindowReader{build: o.BuildSHA256}
	for i, input := range o.Inputs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		r, err := OpenQueryReader(ctx, ReadOptions{Generation: input.Generation, GenerationSHA256: input.GenerationSHA256, StorageRoot: o.StorageRoot, GraphManifest: input.GraphManifest, Participants: input.Participants, Conduits: input.Conduits, Endpoint: o.Endpoint, Username: o.Username, Password: o.Password, BuildSHA256: o.BuildSHA256, Progress: o.Progress}, input.sharedOptions())
		if err != nil {
			return nil, err
		}
		if r.queryGenerationID() != metadata[i].GenerationID || !reflect.DeepEqual(r.generation, metadata[i].Generation) || !reflect.DeepEqual(r.sharedGeneration, metadata[i].Shared) {
			return nil, fmt.Errorf("generation changed while opening window")
		}
		p := windowPartition{publication: metadata[i], source: r.flow, verify: r.VerifyCompletion, receipts: r.receipts, outside: r.outside}
		if r.shared != nil {
			p.shared = r.shared
		}
		out.partitions = append(out.partitions, p)
	}
	sort.Slice(out.partitions, func(i, j int) bool {
		return out.partitions[i].publication.GenerationID < out.partitions[j].publication.GenerationID
	})
	if err := out.verify(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

func validateWindowPublications(inputs []WindowPublication) error {
	if len(inputs) < 1 || len(inputs) > MaxWindowInputs {
		return fmt.Errorf("1..%d publications required", MaxWindowInputs)
	}
	cycles, generations, facts := map[string]bool{}, map[string]bool{}, map[string]bool{}
	var rows uint64
	for _, input := range inputs {
		v := input.Generation
		id := v.GenerationID
		if input.Shared != nil {
			if !reflect.DeepEqual(input.Shared.Base, v) {
				return fmt.Errorf("window extension base differs")
			}
			id = input.Shared.GenerationID
		}
		if !validDigest(input.GenerationID) || !validDigest(input.GenerationSHA256) || id != input.GenerationID || v.Cycle == "" || cycles[v.Cycle] || generations[input.GenerationID] {
			return fmt.Errorf("duplicate, overlapping or invalid generation scope")
		}
		cycles[v.Cycle], generations[input.GenerationID] = true, true
		// One pinned version per cycle avoids mixing amendments/snapshots. This
		// is source-selection safety, not a restriction on the query's dates.
		for _, id := range []string{v.CommitteeFlow.Inputs.A.FactSetID, v.CommitteeFlow.Inputs.B.FactSetID} {
			if !validDigest(id) || facts[id] {
				return fmt.Errorf("overlapping or invalid A/B fact input")
			}
			facts[id] = true
		}
		for _, count := range []uint64{v.CommitteeFlow.A.Rows, v.CommitteeFlow.B.Rows} {
			if count > MaxWindowObservations-rows {
				return fmt.Errorf("selected observations exceed window reader memory guard")
			}
			rows += count
		}
	}
	return nil
}

func (r *WindowReader) verify(ctx context.Context) error {
	if r == nil || !validDigest(r.build) || len(r.partitions) < 1 || len(r.partitions) > MaxWindowInputs {
		return fmt.Errorf("opened window reader required")
	}
	for _, p := range r.partitions {
		if err := ctx.Err(); err != nil {
			return err
		}
		if p.source == nil || p.verify == nil || (p.shared == nil) != (p.publication.Shared == nil) {
			return fmt.Errorf("verified window source required")
		}
		if err := p.verify(ctx); err != nil {
			return err
		}
	}
	return nil
}
