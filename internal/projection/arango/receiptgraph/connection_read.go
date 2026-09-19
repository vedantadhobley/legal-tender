package receiptgraph

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strconv"
)

// A connection consumer pins the completed publisher's build, not its own
// executable. It never resumes a publication or creates missing graph state.
func decodeCycleCompletion(raw []byte, expectedSHA string) (completion, error) {
	var v completion
	if !validDigest(expectedSHA) || digest(raw) != expectedSHA || len(raw) > 8<<20 {
		return v, fmt.Errorf("exact completed cycle manifest SHA256 required")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&v) != nil || d.Decode(new(any)) != io.EOF {
		return v, fmt.Errorf("invalid cycle completion")
	}
	x := v.definition
	x.Key = ""
	b, _ := json.Marshal(x)
	year, err := strconv.Atoi(v.Inputs.Cycle)
	if err != nil || len(v.Inputs.Cycle) != 4 || year < 1976 || year%2 != 0 ||
		v.Key != digest(b) || !validDigest(v.Build) || v.Version != CycleVersion || v.State != CycleState ||
		v.First != 1 || v.Rows == 0 || v.Rows != v.SourceRows || v.FinancialEligibility || v.IdentityResolved || v.Inputs.SourceRelease == "" {
		return v, fmt.Errorf("complete cycle observation definition required")
	}
	for _, ref := range []Reference{v.Inputs.Participants, v.Inputs.Conduits, v.Inputs.Facts, v.Inputs.Committees, v.Inputs.Candidates, v.Inputs.Linkages} {
		if !validDigest(ref.ID) || !validDigest(ref.SHA256) {
			return v, fmt.Errorf("invalid completed input reference")
		}
	}
	if v.Counts[appearances] != v.Rows || v.Unrouted > v.Rows || v.Counts[receipts] != v.Rows-v.Unrouted || v.Counts[conduits] > v.Counts[receipts] || v.Counts[metadata] != 0 {
		return v, fmt.Errorf("completed occurrence counts do not conserve")
	}
	var total uint64
	for _, n := range v.States {
		if n > v.Rows-total {
			return v, fmt.Errorf("completed conduit states overflow")
		}
		total += n
	}
	if total != v.Rows {
		return v, fmt.Errorf("completed conduit states do not conserve")
	}
	for _, hashes := range []map[string]string{v.Digests, v.SourceEvidenceSHA256} {
		if len(hashes) != len(collections) {
			return v, fmt.Errorf("incomplete collection evidence")
		}
		for _, name := range collections {
			if !validDigest(hashes[name]) {
				return v, fmt.Errorf("invalid collection evidence digest")
			}
		}
		if hashes[metadata] != digest(nil) {
			return v, fmt.Errorf("completion cannot contain its own digest")
		}
	}
	for name := range v.Counts {
		if _, ok := v.Digests[name]; !ok {
			return v, fmt.Errorf("unknown completed collection")
		}
	}
	return v, nil
}

func openCompletedCycle(ctx context.Context, o Options, raw []byte, v completion) (loaded, *client, error) {
	o.FullCycle, o.Layout, o.BuildSHA256 = true, CompactLayout, v.Build
	l, err := load(ctx, o)
	if err != nil {
		return l, nil, err
	}
	if !reflect.DeepEqual(l.definition, v.definition) || l.c.Qualified != v.Counts[conduits] {
		return l, nil, fmt.Errorf("connection inputs differ from completed graph ancestry")
	}
	cl, err := newClient(o, "lt_receipt_cycle_"+v.Inputs.Cycle+"_"+v.Key[:32])
	if err != nil {
		return l, nil, err
	}
	if err = cl.ensureSchema(ctx, false); err != nil {
		return l, nil, err
	}
	if err = verifyCycleCompletion(ctx, cl, raw, v); err != nil {
		return l, nil, err
	}
	return l, cl, nil
}

// This verifies completion and collection membership, not all corpus fields.
// Selected path fields are checked separately against their retained sources.
func verifyCycleCompletion(ctx context.Context, cl *client, raw []byte, v completion) error {
	if err := cl.verifyBatch(ctx, metadata, batch{keys: []string{v.Key}, data: raw}); err != nil {
		return err
	}
	for _, name := range collections {
		want := v.Counts[name]
		if name == metadata {
			want = 1
		}
		n, err := cl.count(ctx, name)
		if err != nil {
			return err
		}
		if n != want {
			return fmt.Errorf("completed %s membership changed", name)
		}
	}
	return nil
}

func verifyConnectionDocument(ctx context.Context, cl *client, collection, key string, want any) (json.RawMessage, error) {
	b, err := json.Marshal(want)
	if err != nil {
		return nil, err
	}
	if err := cl.verifyBatch(ctx, collection, batch{keys: []string{key}, data: b}); err != nil {
		return nil, err
	}
	return b, nil
}
