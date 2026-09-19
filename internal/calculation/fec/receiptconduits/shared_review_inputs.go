package receiptconduits

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"slices"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/fundingbasis"
	participants "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
	refs "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptreferences"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/reportreference"
	xsort "github.com/vedantadhobley/legal-tender/internal/storage/externalsort"
)

func reviewIncidences(ctx context.Context, dir string, input refs.Result, classes []SharedGroupClass) (map[uint64]map[uint64]byte, []uint64, error) {
	groups := map[uint64]map[uint64]byte{}
	needed := map[uint64]bool{}
	var bound uint64
	for _, c := range classes {
		w := c.Witness.Related
		if w.Ordinal == 0 || w.Ordinal > input.SourceRows || w.Topology.Peers < 2 || groups[w.Ordinal] != nil || w.Topology.Peers > 4096 {
			return nil, nil, fmt.Errorf("invalid witness group")
		}
		bound += w.Topology.Peers + 1
		if bound > 4096 {
			return nil, nil, fmt.Errorf("selected complete groups exceed 4096-row review bound")
		}
		groups[w.Ordinal] = map[uint64]byte{}
		needed[w.Ordinal] = true
	}
	r, err := xsort.Open(ctx, dir, input.ExactIncidences)
	if err != nil {
		return nil, nil, err
	}
	defer r.Close()
	for {
		v, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		if len(v.Key) != 16 || v.Tag > 1 || len(v.Data) != 0 {
			return nil, nil, fmt.Errorf("invalid exact incidence")
		}
		root, peer := binary.BigEndian.Uint64([]byte(v.Key[:8])), binary.BigEndian.Uint64([]byte(v.Key[8:]))
		if root == 0 || peer == 0 || root > input.SourceRows || peer > input.SourceRows || root == peer || (v.Tag == 0 && v.Ordinal != root) || (v.Tag == 1 && v.Ordinal != peer) {
			return nil, nil, fmt.Errorf("invalid incidence endpoints")
		}
		if g := groups[root]; g != nil {
			bit := byte(1 << v.Tag)
			if g[peer]&bit != 0 {
				return nil, nil, fmt.Errorf("duplicate directional incidence")
			}
			g[peer] |= bit
			needed[peer] = true
			if uint64(len(needed)) > bound {
				return nil, nil, fmt.Errorf("witness incidence population exceeds profile bound")
			}
		}
	}
	for _, c := range classes {
		if uint64(len(groups[c.Witness.Related.Ordinal])) != c.Witness.Related.Topology.Peers {
			return nil, nil, fmt.Errorf("witness peer population mismatch")
		}
	}
	ids := make([]uint64, 0, len(needed))
	for n := range needed {
		ids = append(ids, n)
	}
	slices.Sort(ids)
	return groups, ids, nil
}

func reviewEndpoints(ctx context.Context, dir string, input refs.TopologyResult, ids []uint64) (map[uint64]refs.Endpoint, error) {
	wanted := map[uint64]bool{}
	for _, id := range ids {
		wanted[id] = true
	}
	r, err := xsort.Open(ctx, dir, input.Endpoints)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	out := map[uint64]refs.Endpoint{}
	var previous, exact, unsafe uint64
	for {
		v, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		e, err := refs.DecodeEndpoint(v, input.SourceRows)
		if err != nil {
			return nil, err
		}
		if e.Ordinal <= previous {
			return nil, fmt.Errorf("duplicate or unordered endpoint")
		}
		previous = e.Ordinal
		if e.Peers > 0 {
			exact++
		}
		if e.UnsafeReasons != 0 {
			unsafe++
		}
		if wanted[e.Ordinal] {
			out[e.Ordinal] = e
		}
	}
	if len(out) != len(wanted) || exact != input.ExactEndpointRows || unsafe != input.UnsafeEndpointRows {
		return nil, fmt.Errorf("review endpoint census mismatch")
	}
	return out, nil
}

func participantEvidence(s fundingbasis.SourceEvidenceRow) participants.Row {
	return participants.Row{Ordinal: s.Ordinal, Recipient: s.Recipient, Memo: s.Memo, ReceiptType: s.ReceiptType, Entity: s.Entity, Contributor: s.Contributor, CleanContributor: s.CleanContributor, ConduitID: s.ConduitID, Amount: s.Amount}
}
func sameProfileSource(w ProfileOccurrence, source fundingbasis.SourceEvidenceRow, e refs.Endpoint) bool {
	return reflect.DeepEqual(w, profileOccurrence(participantEvidence(source), e))
}

// Exact incidence membership supplies the established unique source/target
// multiplicities. Recheck literal scope, transaction and schedule fields from
// the original rows as an independent check of those selected references.
func verifyReviewReference(shared, peer fundingbasis.Receipt, directions byte) error {
	if directions == 0 || directions > 3 {
		return fmt.Errorf("invalid reference direction")
	}
	for bit := byte(1); bit <= 2; bit <<= 1 {
		if directions&bit == 0 {
			continue
		}
		source, target := shared, peer
		if bit == 2 {
			source, target = peer, shared
		}
		a, err := fundingbasis.SourceEvidenceFromReceipt(source)
		if err != nil {
			return err
		}
		b, err := fundingbasis.SourceEvidenceFromReceipt(target)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(a.Recipient, b.Recipient) || !reflect.DeepEqual(a.File, b.File) || !reflect.DeepEqual(a.BackReference, b.Transaction) {
			return fmt.Errorf("source reference literal/scope mismatch")
		}
		field := func(name string) (*string, error) {
			v, ok := target.Fields[name]
			if !ok {
				return nil, fmt.Errorf("missing source field %s", name)
			}
			if v == nil {
				return nil, nil
			}
			s, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("invalid source field %s", name)
			}
			return &s, nil
		}
		schedule, err := field("schedule_type")
		if err != nil {
			return err
		}
		line, err := field("line_num")
		if err != nil {
			return err
		}
		state, targetID := reportreference.Decide(reportreference.Input{Ordinal: source.Ordinal, ScopeValid: reportreference.ValidScope(a.Recipient, a.File), Transaction: a.Transaction, BackReference: a.BackReference, BackSchedule: a.BackSchedule, SourceCount: 1, TargetCount: 1, TargetOrdinal: target.Ordinal, TargetSchedule: schedule, TargetLine: line})
		if state != "exact_same_report_reference" || targetID != target.Ordinal {
			return fmt.Errorf("original source no longer supports exact reference")
		}
	}
	return nil
}
