package receiptreferences

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"slices"

	xsort "github.com/vedantadhobley/legal-tender/internal/storage/externalsort"
)

const (
	InvalidOwnReference      byte = 1
	InvalidIncomingReference byte = 2
)

// Endpoint is reference topology, not conduit qualification. Missing endpoints
// have no exact peers or invalid reference incidents. Their transaction-key
// uniqueness is NOT established by this sparse index.
type Endpoint struct {
	Ordinal       uint64 `json:"source_row_ordinal"`
	Peers         uint64 `json:"distinct_exact_reference_peers"`
	OnlyPeer      uint64 `json:"sole_peer_source_row_ordinal"`
	Incoming      uint64 `json:"incoming_exact_references"`
	Outgoing      uint64 `json:"outgoing_exact_references"`
	UnsafeReasons byte   `json:"unsafe_reference_reason_bits"`
}

func (e Endpoint) record() xsort.Record {
	data := make([]byte, 33)
	for i, n := range []uint64{e.Peers, e.OnlyPeer, e.Incoming, e.Outgoing} {
		binary.BigEndian.PutUint64(data[8*i:], n)
	}
	data[32] = e.UnsafeReasons
	return xsort.Record{Key: ordinalKey(e.Ordinal), Ordinal: e.Ordinal, Data: data}
}

// DecodeEndpoint enforces the compact v1 endpoint representation. Zero is the
// absent sole-peer sentinel; source ordinals are strictly positive.
func DecodeEndpoint(r xsort.Record, sourceRows uint64) (Endpoint, error) {
	if r.Ordinal == 0 || r.Ordinal > sourceRows || r.Key != ordinalKey(r.Ordinal) || r.Tag != 0 || len(r.Data) != 33 {
		return Endpoint{}, fmt.Errorf("invalid endpoint record")
	}
	e := Endpoint{Ordinal: r.Ordinal, Peers: binary.BigEndian.Uint64(r.Data), OnlyPeer: binary.BigEndian.Uint64(r.Data[8:]), Incoming: binary.BigEndian.Uint64(r.Data[16:]), Outgoing: binary.BigEndian.Uint64(r.Data[24:]), UnsafeReasons: r.Data[32]}
	if e.Peers >= sourceRows || e.Incoming > sourceRows || e.Outgoing > 1 || e.Peers > e.Incoming+e.Outgoing ||
		e.Peers < max(e.Incoming, e.Outgoing) || (e.Peers == 1) != (e.OnlyPeer != 0) || e.OnlyPeer > sourceRows || e.OnlyPeer == e.Ordinal ||
		e.UnsafeReasons & ^byte(3) != 0 || (e.Peers == 0 && e.UnsafeReasons == 0) {
		return Endpoint{}, fmt.Errorf("invalid endpoint topology")
	}
	return e, nil
}

type topologyWork struct {
	ctx            context.Context
	space          *xsort.Workspace
	dir            string
	reference      Result
	runRows, fanIn int
}

// invalidIncidents propagates every non-exact reference to its source and all
// matching target occurrences. A missing schedule or duplicated source can
// therefore invalidate an otherwise exact pair. The join never buffers a key
// group, and Bloom false positives cannot introduce unsafe endpoints.
func (t topologyWork) invalidIncidents() (xsort.File, uint64, uint64, error) {
	bad, err := xsort.New(t.ctx, t.space, t.runRows, t.fanIn)
	if err != nil {
		return xsort.File{}, 0, 0, err
	}
	targets, err := xsort.New(t.ctx, t.space, t.runRows, t.fanIn)
	if err != nil {
		return xsort.File{}, 0, 0, err
	}
	r, err := xsort.Open(t.ctx, t.dir, t.reference.Decisions)
	if err != nil {
		return xsort.File{}, 0, 0, err
	}
	defer r.Close()
	states := map[string]uint64{}
	var previous, invalid uint64
	for {
		v, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return xsort.File{}, 0, 0, err
		}
		var d Decision
		if err = json.Unmarshal(v.Data, &d); err != nil {
			return xsort.File{}, 0, 0, err
		}
		if err = t.validateDecision(v, d, previous); err != nil {
			return xsort.File{}, 0, 0, err
		}
		previous = v.Ordinal
		states[d.State]++
		if d.Target != nil {
			continue
		}
		invalid++
		if err = bad.Add(xsort.Record{Key: v.Key, Tag: InvalidOwnReference, Ordinal: v.Ordinal}); err != nil {
			return xsort.File{}, 0, 0, err
		}
		if scope(d.Source) && present(d.Source.BackReference) {
			if err = targets.Add(xsort.Record{Key: key(d.Source.Recipient, d.Source.File, d.Source.BackReference), Ordinal: v.Ordinal}); err != nil {
				return xsort.File{}, 0, 0, err
			}
		}
	}
	for state, n := range t.reference.States {
		if state != "no_report_reference" && states[state] != n {
			return xsort.File{}, 0, 0, fmt.Errorf("reference state conservation")
		}
	}
	if len(states) > len(t.reference.States) {
		return xsort.File{}, 0, 0, fmt.Errorf("unknown reference states")
	}
	r.Close()
	f, err := targets.Finish()
	if err != nil {
		return xsort.File{}, 0, 0, err
	}
	marked, err := t.markTargets(f, bad)
	if err != nil {
		return xsort.File{}, 0, 0, err
	}
	if err = t.space.Remove(f); err != nil {
		return xsort.File{}, 0, 0, err
	}
	f, err = bad.Finish()
	return f, invalid, marked, err
}

func (t topologyWork) validateDecision(v xsort.Record, d Decision, previous uint64) error {
	if v.Tag != 0 || v.Ordinal <= previous || v.Ordinal > t.reference.SourceRows || v.Key != ordinalKey(v.Ordinal) || uint64(d.Source.Ordinal) != v.Ordinal ||
		!slices.Contains([]string{"invalid_report_scope", "incomplete_report_reference", "duplicate_source_transaction_id", "ambiguous_target_transaction_id", "missing_reference_schedule", "target_absent_from_cycle_report", "self_reference", "reference_schedule_mismatch", "exact_same_report_reference"}, d.State) ||
		(!present(d.Source.BackReference) && !present(d.Source.BackSchedule)) || (d.State == "exact_same_report_reference") != (d.Target != nil) {
		return fmt.Errorf("invalid reference decision")
	}
	if d.Target != nil && (*d.Target == 0 || *d.Target > t.reference.SourceRows || *d.Target == v.Ordinal || !scope(d.Source) || d.SourceMultiplicity == nil || *d.SourceMultiplicity != 1 || d.TargetMultiplicity == nil || *d.TargetMultiplicity != 1) {
		return fmt.Errorf("invalid exact reference decision")
	}
	return nil
}

func (t topologyWork) markTargets(targets xsort.File, bad *xsort.Sorter) (uint64, error) {
	requests, err := xsort.Open(t.ctx, t.space.Dir, targets)
	if err != nil {
		return 0, err
	}
	defer requests.Close()
	members, err := xsort.Open(t.ctx, t.dir, t.reference.LookupEvidence)
	if err != nil {
		return 0, err
	}
	defer members.Close()
	next, nextErr := requests.Next()
	var seen, marked, previousMember uint64
	var previousKey string
	for {
		v, err := members.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
		if v.Ordinal == 0 || v.Ordinal > t.reference.SourceRows || v.Tag > 2 {
			return 0, fmt.Errorf("invalid lookup evidence")
		}
		for nextErr == nil && next.Key < v.Key {
			next, nextErr = requests.Next()
		}
		if nextErr != nil && nextErr != io.EOF {
			return 0, nextErr
		}
		if v.Tag != 0 {
			continue
		}
		seen++
		if v.Key == previousKey && v.Ordinal <= previousMember {
			return 0, fmt.Errorf("duplicate lookup occurrence")
		}
		previousKey, previousMember = v.Key, v.Ordinal
		// The existing lookup publication already owns member payloads. Validate
		// them only when used here; the entire backing stream is digest-checked.
		if nextErr == nil && next.Key == v.Key {
			var m member
			if err = json.Unmarshal(v.Data, &m); err != nil {
				return 0, err
			}
			if m.Ordinal != v.Ordinal {
				return 0, fmt.Errorf("lookup member ordinal mismatch")
			}
			if err = bad.Add(xsort.Record{Key: ordinalKey(v.Ordinal), Tag: InvalidIncomingReference, Ordinal: v.Ordinal}); err != nil {
				return 0, err
			}
			marked++
		}
	}
	// Drain requests even when no target exists, so trailing corruption fails.
	for nextErr == nil {
		_, nextErr = requests.Next()
	}
	if nextErr != io.EOF {
		return 0, nextErr
	}
	if seen != t.reference.LookupMemberRows {
		return 0, fmt.Errorf("lookup membership conservation")
	}
	return marked, nil
}

// exactEndpoints re-derives peer counts from directed incidences and compares
// every endpoint with the accepted neighbor artifact. One pair in both reference
// directions is one peer, not two. No adjacency list must fit in memory.
func (t topologyWork) exactEndpoints() (xsort.File, error) {
	r, err := xsort.Open(t.ctx, t.dir, t.reference.ExactIncidences)
	if err != nil {
		return xsort.File{}, err
	}
	defer r.Close()
	n, err := xsort.Open(t.ctx, t.dir, t.reference.Neighbors)
	if err != nil {
		return xsort.File{}, err
	}
	defer n.Close()
	w, err := t.space.Writer(t.ctx)
	if err != nil {
		return xsort.File{}, err
	}
	defer w.Abort()
	var e Endpoint
	var lastPeer, incoming, outgoing uint64
	var peerDirections byte
	emit := func() error {
		if e.Ordinal == 0 {
			return nil
		}
		v, err := n.Next()
		if err != nil {
			return fmt.Errorf("missing neighbor backing: %w", err)
		}
		var want Neighbors
		if err = json.Unmarshal(v.Data, &want); err != nil {
			return err
		}
		if v.Tag != 0 || v.Ordinal != e.Ordinal || v.Key != ordinalKey(e.Ordinal) || want != (Neighbors{Ordinal: e.Ordinal, Peers: e.Peers, Incoming: e.Incoming, Outgoing: e.Outgoing}) {
			return fmt.Errorf("neighbor/incidence mismatch")
		}
		if e.Peers != 1 {
			e.OnlyPeer = 0
		}
		if _, err = DecodeEndpoint(e.record(), t.reference.SourceRows); err != nil {
			return err
		}
		incoming += e.Incoming
		outgoing += e.Outgoing
		return w.Add(e.record())
	}
	for {
		v, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return xsort.File{}, err
		}
		if len(v.Key) != 16 || v.Tag > 1 || len(v.Data) != 0 {
			return xsort.File{}, fmt.Errorf("invalid incidence record")
		}
		ord, peer := binary.BigEndian.Uint64([]byte(v.Key[:8])), binary.BigEndian.Uint64([]byte(v.Key[8:]))
		if ord == 0 || peer == 0 || ord == peer || ord > t.reference.SourceRows || peer > t.reference.SourceRows ||
			(v.Tag == 0 && v.Ordinal != ord) || (v.Tag == 1 && v.Ordinal != peer) {
			return xsort.File{}, fmt.Errorf("invalid incidence endpoint")
		}
		if e.Ordinal != ord {
			if err = emit(); err != nil {
				return xsort.File{}, err
			}
			e = Endpoint{Ordinal: ord}
			lastPeer = 0
		}
		if peer != lastPeer {
			e.Peers++
			e.OnlyPeer = peer
			lastPeer, peerDirections = peer, 0
		}
		if peerDirections&(1<<v.Tag) != 0 {
			return xsort.File{}, fmt.Errorf("duplicate incidence direction")
		}
		peerDirections |= 1 << v.Tag
		if v.Tag == 0 {
			e.Outgoing++
		} else {
			e.Incoming++
		}
	}
	if err = emit(); err != nil {
		return xsort.File{}, err
	}
	if _, err = n.Next(); err != io.EOF {
		return xsort.File{}, fmt.Errorf("extra or corrupt neighbor backing: %v", err)
	}
	if incoming != t.reference.States["exact_same_report_reference"] || incoming != outgoing {
		return xsort.File{}, fmt.Errorf("exact endpoint conservation")
	}
	return w.Finish()
}

func (t topologyWork) combine(exact, unsafe xsort.File) (xsort.File, uint64, error) {
	r, err := xsort.Open(t.ctx, t.space.Dir, exact)
	if err != nil {
		return xsort.File{}, 0, err
	}
	defer r.Close()
	u, err := xsort.Open(t.ctx, t.space.Dir, unsafe)
	if err != nil {
		return xsort.File{}, 0, err
	}
	defer u.Close()
	w, err := t.space.Writer(t.ctx)
	if err != nil {
		return xsort.File{}, 0, err
	}
	defer w.Abort()
	v, re := r.Next()
	bad, ue := u.Next()
	var unsafeRows uint64
	for re == nil || ue == nil {
		if re != nil && re != io.EOF {
			return xsort.File{}, 0, re
		}
		if ue != nil && ue != io.EOF {
			return xsort.File{}, 0, ue
		}
		var e Endpoint
		if re == nil && (ue != nil || v.Ordinal <= bad.Ordinal) {
			e, err = DecodeEndpoint(v, t.reference.SourceRows)
			if err != nil {
				return xsort.File{}, 0, err
			}
			v, re = r.Next()
		} else {
			e.Ordinal = bad.Ordinal
		}
		for ue == nil && bad.Ordinal == e.Ordinal {
			if bad.Key != ordinalKey(bad.Ordinal) || len(bad.Data) != 0 || (bad.Tag != InvalidOwnReference && bad.Tag != InvalidIncomingReference) {
				return xsort.File{}, 0, fmt.Errorf("invalid unsafe incidence")
			}
			e.UnsafeReasons |= bad.Tag
			bad, ue = u.Next()
		}
		if _, err = DecodeEndpoint(e.record(), t.reference.SourceRows); err != nil {
			return xsort.File{}, 0, err
		}
		if e.UnsafeReasons != 0 {
			unsafeRows++
		}
		if err = w.Add(e.record()); err != nil {
			return xsort.File{}, 0, err
		}
	}
	if re != io.EOF || ue != io.EOF {
		return xsort.File{}, 0, fmt.Errorf("endpoint input read failed: %v / %v", re, ue)
	}
	f, err := w.Finish()
	return f, unsafeRows, err
}
