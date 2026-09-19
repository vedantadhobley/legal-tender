package receiptreferences

import (
	"encoding/binary"
	"fmt"
	xsort "github.com/vedantadhobley/legal-tender/internal/storage/externalsort"
	"io"
)

// Exact-reference degree is not conduit eligibility: invalid incident evidence
// remains in decisions/lookup evidence and must be considered by that consumer.
func (e *engine) neighbors() error {
	f, err := e.incidences.Finish()
	if err != nil {
		return err
	}
	e.out.ExactIncidences = f
	if f.Rows != 2*e.out.States["exact_same_report_reference"] {
		return fmt.Errorf("incident conservation")
	}
	r, err := xsort.Open(e.ctx, e.space.Dir, f)
	if err != nil {
		return err
	}
	defer r.Close()
	w, err := e.space.Writer(e.ctx)
	if err != nil {
		return err
	}
	defer w.Abort()
	var n Neighbors
	var lastPeer uint64
	var incoming, outgoing uint64
	emit := func() error {
		if n.Ordinal == 0 {
			return nil
		}
		incoming += n.Incoming
		outgoing += n.Outgoing
		return w.Add(xsort.Record{Key: ordinalKey(n.Ordinal), Ordinal: n.Ordinal, Data: marshal(n)})
	}
	for {
		v, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if len(v.Key) != 16 {
			return fmt.Errorf("invalid incidence key")
		}
		ord, peer := binary.BigEndian.Uint64([]byte(v.Key[:8])), binary.BigEndian.Uint64([]byte(v.Key[8:]))
		if ord == 0 || peer == 0 || ord == peer {
			return fmt.Errorf("invalid incidence endpoint")
		}
		if n.Ordinal != ord {
			if err = emit(); err != nil {
				return err
			}
			n = Neighbors{Ordinal: ord}
			lastPeer = 0
		}
		if peer != lastPeer {
			n.Peers++
			lastPeer = peer
		}
		switch v.Tag {
		case 0:
			n.Outgoing++
		case 1:
			n.Incoming++
		default:
			return fmt.Errorf("invalid incidence direction")
		}
	}
	if err = emit(); err != nil {
		return err
	}
	if incoming != e.out.States["exact_same_report_reference"] || incoming != outgoing {
		return fmt.Errorf("reverse incidence conservation")
	}
	e.out.Neighbors, err = w.Finish()
	return err
}
