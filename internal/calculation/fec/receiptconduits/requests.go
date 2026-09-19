package receiptconduits

import (
	"encoding/binary"
	"fmt"
	participants "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
	refs "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptreferences"
	xsort "github.com/vedantadhobley/legal-tender/internal/storage/externalsort"
)

// A request carries the original's exact role evidence to the sole peer. Names
// and full source rows stay in the parent publication. Null and empty differ.
func request(r participants.Row, e refs.Endpoint) (xsort.Record, error) {
	b := []byte{e.UnsafeReasons}
	if r.Amount == nil {
		b = append(b, 0)
	} else {
		b = append(b, 1)
		b = binary.BigEndian.AppendUint64(b, uint64(*r.Amount))
	}
	for _, v := range []*string{r.Recipient, r.Entity, r.ReceiptType, r.Contributor, r.CleanContributor, r.ConduitID} {
		if v == nil {
			b = binary.BigEndian.AppendUint16(b, 65535)
		} else {
			if len(*v) > 4096 {
				return xsort.Record{}, fmt.Errorf("role field exceeds participant contract")
			}
			b = binary.BigEndian.AppendUint16(b, uint16(len(*v)))
			b = append(b, (*v)...)
		}
	}
	return xsort.Record{Key: key(e.OnlyPeer), Ordinal: uint64(r.Ordinal), Data: b}, nil
}
func decodeRequest(r xsort.Record, rows uint64) (participants.Row, refs.Endpoint, error) {
	v := participants.Row{Ordinal: int64(r.Ordinal)}
	e := refs.Endpoint{Ordinal: r.Ordinal, Peers: 1}
	bad := func() (participants.Row, refs.Endpoint, error) { return v, e, fmt.Errorf("invalid conduit request") }
	if r.Ordinal == 0 || r.Ordinal > rows || len(r.Key) != 8 || r.Tag != 0 || len(r.Data) < 2 {
		return bad()
	}
	e.OnlyPeer = binary.BigEndian.Uint64([]byte(r.Key))
	e.UnsafeReasons = r.Data[0]
	if e.OnlyPeer == 0 || e.OnlyPeer > rows || e.OnlyPeer == r.Ordinal || e.UnsafeReasons > 3 {
		return bad()
	}
	b := r.Data[2:]
	switch r.Data[1] {
	case 0:
	case 1:
		if len(b) < 8 {
			return bad()
		}
		n := int64(binary.BigEndian.Uint64(b))
		v.Amount = &n
		b = b[8:]
	default:
		return bad()
	}
	for _, p := range []**string{&v.Recipient, &v.Entity, &v.ReceiptType, &v.Contributor, &v.CleanContributor, &v.ConduitID} {
		if len(b) < 2 {
			return bad()
		}
		n := int(binary.BigEndian.Uint16(b))
		b = b[2:]
		if n == 65535 {
			continue
		}
		if n > 4096 || n > len(b) {
			return bad()
		}
		s := string(b[:n])
		*p = &s
		b = b[n:]
	}
	if len(b) != 0 {
		return bad()
	}
	return v, e, nil
}
