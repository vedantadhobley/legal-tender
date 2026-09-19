package receiptgraph

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
)

type Comparison struct {
	ProjectionID string `json:"projection_id"`
	ResultSHA256 string `json:"result_sha256"`
	Database     string `json:"database"`
	State        string `json:"state"`
	Documents    uint64 `json:"documents_verified"`
	EncodedBytes uint64 `json:"expanded_encoded_bytes"`
}

type baseline struct {
	result Result
	client *client
	sha    string
}

// A comparison is optional extra evidence. It does not become a prerequisite
// for a future full-cycle graph when no whole-cycle expanded graph exists.
func loadBaseline(ctx context.Context, o Options, l loaded) (*baseline, error) {
	if o.CompareResult == "" {
		return nil, nil
	}
	b, err := manifestBytes(o.CompareResult)
	if err != nil {
		return nil, err
	}
	if digest(b) != o.CompareSHA256 {
		return nil, fmt.Errorf("comparison result digest mismatch")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	var r Result
	if err = d.Decode(&r); err != nil {
		return nil, fmt.Errorf("invalid comparison result")
	}
	if d.Decode(new(any)) != io.EOF {
		return nil, fmt.Errorf("trailing comparison result")
	}
	if err = validateBaseline(r, l.definition); err != nil {
		return nil, err
	}
	cl, err := newClient(o, r.Database)
	if err != nil {
		return nil, err
	}
	prior := completion{definition: r.Definition, Counts: r.Counts, Digests: r.PayloadSHA256, Unrouted: r.Unrouted, States: r.ConduitStates}
	raw, err := json.Marshal(prior)
	if err != nil {
		return nil, err
	}
	if err = cl.verifyBatch(ctx, metadata, batch{keys: []string{r.Definition.Key}, data: append(raw, '\n')}); err != nil {
		return nil, fmt.Errorf("comparison completion: %w", err)
	}
	for _, name := range collections {
		want := r.Counts[name]
		if name == metadata {
			want = 1
		}
		n, err := cl.count(ctx, name)
		if err != nil {
			return nil, err
		}
		if n != want {
			return nil, fmt.Errorf("comparison graph count mismatch")
		}
	}
	return &baseline{r, cl, digest(b)}, nil
}

func validateBaseline(r Result, d definition) error {
	v := r.Definition
	key := v.Key
	v.Key = ""
	b, _ := json.Marshal(v)
	if !validDigest(key) || key != digest(b) || r.Definition.Version != Version || r.Definition.State != "verified_bounded_sample_not_complete_cycle" || r.Definition.FinancialEligibility || r.Definition.IdentityResolved || !validDigest(r.Definition.Build) || !reflect.DeepEqual(r.Definition.Inputs, d.Inputs) || r.Definition.First != d.First || r.Definition.Rows != d.Rows || r.Definition.SourceRows != d.SourceRows || r.SourceEvidenceSHA256 != nil || r.Comparison != nil {
		return fmt.Errorf("comparison must be the exact expanded-v1 input and range")
	}
	if r.Database != "lt_receipt_sample_"+d.Inputs.Cycle+"_"+key[:32] {
		return fmt.Errorf("comparison database identity mismatch")
	}
	if r.Counts[appearances] != d.Rows || r.Unrouted > d.Rows || r.Counts[receipts] != d.Rows-r.Unrouted {
		return fmt.Errorf("comparison membership mismatch")
	}
	for _, name := range collections {
		if !validDigest(r.PayloadSHA256[name]) {
			return fmt.Errorf("comparison lacks canonical document hashes")
		}
	}
	return nil
}

func (b *baseline) verify(ctx context.Context, v batch) error {
	data := v.data
	if len(v.evidence) > 0 {
		data = v.evidence
	}
	return b.client.verifyBatch(ctx, v.collection, batch{keys: v.keys, data: data})
}

func (b *baseline) finish(r Result) (*Comparison, error) {
	if !reflect.DeepEqual(r.SourceEvidenceSHA256, b.result.PayloadSHA256) || !reflect.DeepEqual(r.Counts, b.result.Counts) || r.Unrouted != b.result.Unrouted || r.MissingMasters != b.result.MissingMasters || !reflect.DeepEqual(r.ConduitStates, b.result.ConduitStates) {
		return nil, fmt.Errorf("compact graph does not reproduce expanded evidence")
	}
	// Graph paths are physically identical: only appearance document payloads
	// changed. Names/amounts and source inspection are never filled from summaries.
	for _, pair := range [][2]any{{r.Queries, b.result.Queries}, {r.ConduitWitness, b.result.ConduitWitness}, {r.SourceChecks, b.result.SourceChecks}} {
		x, _ := json.Marshal(pair[0])
		y, _ := json.Marshal(pair[1])
		if !equalJSON(x, y) {
			return nil, fmt.Errorf("compact graph path or source witness changed")
		}
	}
	var docs, size uint64
	for _, n := range r.Counts {
		docs += n
	}
	for _, n := range b.result.PayloadBytes {
		size += n
	}
	return &Comparison{b.result.Definition.Key, b.sha, b.result.Database, "complete_live_field_and_membership_equivalence", docs, size}, nil
}
