package receiptconduits

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	policy "github.com/vedantadhobley/legal-tender/internal/calculation/fec/earmarkassociation"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/fundingbasis"
	participants "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
	refs "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptreferences"
	xsort "github.com/vedantadhobley/legal-tender/internal/storage/externalsort"
)

func sourceFixture(r participants.Row, e refs.Endpoint) fundingbasis.Receipt {
	val := func(p *string) any {
		if p == nil {
			return nil
		}
		return *p
	}
	var amount any
	if r.Amount != nil {
		amount = strconv.FormatInt(*r.Amount, 10)
	}
	var back, schedule any
	if e.OnlyPeer != 0 {
		back = "T" + strconv.FormatUint(e.OnlyPeer, 10)
		schedule = "SA11AI"
	}
	s := fundingbasis.SourceEvidenceRow{Ordinal: r.Ordinal, Cycle: 2024, Normalization: "valid", Recipient: r.Recipient, Contributor: r.Contributor, CleanContributor: r.CleanContributor, Memo: r.Memo, ReceiptType: r.ReceiptType, Amount: r.Amount, AmountState: r.AmountState, ConduitID: r.ConduitID, Entity: r.Entity}
	k, _ := s.Classify()
	return fundingbasis.Receipt{Ordinal: uint64(r.Ordinal), Key: k, Fields: map[string]any{
		"cmte_id": val(r.Recipient), "contbr_id": val(r.Contributor), "clean_contbr_id": val(r.CleanContributor), "conduit_cmte_id": val(r.ConduitID), "receipt_tp": val(r.ReceiptType), "entity_tp": val(r.Entity), "is_individual": nil,
		"lt_source_row_ordinal": strconv.FormatInt(r.Ordinal, 10), "lt_two_year_transaction_period": "2024", "lt_normalization_state": "valid", "lt_receipt_amount_minor_units": amount, "lt_receipt_amount_state": r.AmountState, "lt_memoed_subtotal": r.Memo,
		"memo_text": "not a matching input", "conduit_cmte_nm": nil, "file_num": "123", "tran_id": "T" + strconv.FormatInt(r.Ordinal, 10), "back_ref_tran_id": back, "back_ref_sched_nm": schedule, "schedule_type": "SA", "line_num": "11AI",
	}}
}

func TestSourceReviewReconstructsCompleteGroupsAndRejectsWrongSources(t *testing.T) {
	rows, eps := sharedFixture()
	w, dir := setup(t, context.Background(), rows, eps, 5, 4, 3, 2, 64<<20)
	w.o.profile = newSharedProfileCollector()
	b, err := w.calculate(dir)
	if err != nil {
		t.Fatal(err)
	}
	p, err := w.o.profile.finish(b)
	if err != nil {
		t.Fatal(err)
	}
	sources := map[uint64]fundingbasis.Receipt{}
	for _, r := range rows {
		sources[uint64(r.Ordinal)] = sourceFixture(r, eps[uint64(r.Ordinal)])
	}
	for _, c := range p.Classes {
		root := c.Witness.Related.Ordinal
		peers := map[uint64]byte{}
		for id, e := range eps {
			if e.OnlyPeer == root {
				peers[id] = 2
			}
		}
		g, err := reviewGroup(c, peers, eps, sources)
		if err != nil {
			t.Fatal(root, err)
		}
		if len(g.Peers) != len(peers) {
			t.Fatal("incomplete peers")
		}
		if c.Coverage == "complete_safe_earmark_leaf_coverage" && c.Roles == "all_roles_compatible" && g.GroupDecision.State != policy.SharedAssociation {
			t.Fatal(g.GroupDecision)
		}
		for id := range peers {
			old := sources[id].Fields["file_num"]
			sources[id].Fields["file_num"] = "OTHER"
			if _, err := reviewGroup(c, peers, eps, sources); err == nil {
				t.Fatal("wrong report accepted")
			}
			sources[id].Fields["file_num"] = old
			break
		}
	}
}

func TestReviewIncidencesDirectionsCountsAndBounds(t *testing.T) {
	ctx := context.Background()
	w, err := xsort.NewWorkspace(filepath.Join(t.TempDir(), "data"), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	s, err := xsort.New(ctx, w, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	// Two peers at root 3; one reciprocal pair must still count as one peer.
	for _, edge := range [][2]uint64{{1, 3}, {2, 3}, {3, 1}} {
		for direction := uint8(0); direction < 2; direction++ {
			a, b := edge[0], edge[1]
			if direction == 1 {
				a, b = b, a
			}
			if err := s.Add(xsort.Record{Key: key(a) + key(b), Tag: direction, Ordinal: edge[0]}); err != nil {
				t.Fatal(err)
			}
		}
	}
	f, err := s.Finish()
	if err != nil {
		t.Fatal(err)
	}
	r := refs.Result{SourceRows: 3, ExactIncidences: f}
	c := []SharedGroupClass{{Witness: SharedGroupWitness{Related: ProfileOccurrence{Ordinal: 3, Topology: ProfileTopology{Peers: 2}}}}}
	g, ids, err := reviewIncidences(ctx, w.Dir, r, c)
	if err != nil || !reflect.DeepEqual(ids, []uint64{1, 2, 3}) || g[3][1] != 3 || g[3][2] != 2 {
		t.Fatal(g, ids, err)
	}
	c[0].Witness.Related.Topology.Peers = 3
	if _, _, err := reviewIncidences(ctx, w.Dir, r, c); err == nil {
		t.Fatal("false peer count")
	}
	c[0].Witness.Related.Topology.Peers = 4096
	if _, _, err := reviewIncidences(ctx, w.Dir, r, c); err == nil {
		t.Fatal("unbounded review")
	}
	r.ExactIncidences.SHA256 = strings.Repeat("a", 64)
	c[0].Witness.Related.Topology.Peers = 2
	if _, _, err := reviewIncidences(ctx, w.Dir, r, c); err == nil {
		t.Fatal("corrupt input")
	}
}

func TestSharedProfileLoaderRejectsTampering(t *testing.T) {
	h := strings.Repeat("a", 64)
	b := Result{SchemaVersion: Version, State: "complete_cycle_conduit_evidence", BuildSHA256: h, Policy: Policy, AssociationPolicy: policy.Policy, ParticipantID: h, ParticipantSHA256: h, TopologyID: h, TopologySHA256: h, FactSetID: h, FactManifestSHA256: h, SourceRows: 3, EligibleRoleRows: 1, OtherRows: 2, OtherDisposition: "not_a_non_memo_reviewed_earmark", AdditionalAmount: "0", Workers: 1, States: map[string]uint64{Unassessed: 1}, Amounts: map[string]uint64{"not_assessed": 1}, Decisions: xsort.File{Name: "run.zst", Rows: 1, SHA256: h, ValuesSHA256: h}}
	b.CalculationID = identity(b)
	p, err := newSharedProfileCollector().finish(b)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "shared-reference-profile.json")
	write := func(v SharedProfile) {
		buf, _ := json.Marshal(v)
		if err := os.WriteFile(path, buf, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(p)
	if _, _, err := LoadSharedProfile(path, p.ProfileID); err != nil {
		t.Fatal(err)
	}
	p.SharedRows++
	write(p)
	if _, _, err := LoadSharedProfile(path, p.ProfileID); err == nil {
		t.Fatal("changed profile accepted")
	}
}
