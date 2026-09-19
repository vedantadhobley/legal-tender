package fundinggeneration

import (
	"bytes"
	"encoding/json"
	"fmt"

	flow "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleaparquet"
)

func checkConnectionGateResult(out WindowConnectionsResult, counts connectionCounts) error {
	if out.SchemaVersion != WindowConnectionsVersion || out.Policy != WindowConnectionsPolicy {
		return fmt.Errorf("connection evidence version changed")
	}
	return checkConnectionGateCommon(out, counts, nil)
}

// Share structural/source checks without admitting new families to the v1 gate.
func checkConnectionGateCommon(out WindowConnectionsResult, counts connectionCounts, extra func(WindowConnectionLink) error) error {
	if err := out.Query.Validate(); err != nil {
		return err
	}
	if out.FinancialEligibility || out.TerminalEligible || !validDigest(out.BuildSHA256) {
		return fmt.Errorf("connection evidence boundary changed")
	}
	id := out.ResultID
	out.ResultID = ""
	if !validDigest(id) || valueID(out) != id {
		return fmt.Errorf("connection result identity differs")
	}
	inputs := map[string]WindowPublication{}
	for _, p := range out.Inputs {
		if _, ok := inputs[p.GenerationID]; ok {
			return fmt.Errorf("duplicate connection input")
		}
		inputs[p.GenerationID] = p
	}
	if len(inputs) != len(counts) || len(out.Coverage) != len(inputs) {
		return fmt.Errorf("connection input/coverage cardinality differs")
	}
	seenCoverage := map[string]bool{}
	for _, c := range out.Coverage {
		ref, ok := counts[c.GenerationID][out.Query.Ledger]
		if !ok || seenCoverage[c.GenerationID] {
			return fmt.Errorf("foreign or duplicate date census")
		}
		seenCoverage[c.GenerationID] = true
		want := WindowCoverage{GenerationID: c.GenerationID, Rows: ref.unknown}
		if out.Query.Window == nil {
			want.Included, want.UndatedIncluded = ref.unknown, ref.unknown
		} else {
			want.UnknownExcluded = ref.unknown
		}
		for day, n := range ref.days {
			want.Rows += n
			switch independentDateSelection(&day, out.Query.Window) {
			case "included":
				want.Included += n
			case "before_window":
				want.Before += n
			case "after_window":
				want.After += n
			default:
				return fmt.Errorf("unexpected known-date state")
			}
		}
		if c != want {
			return fmt.Errorf("independent complete date census differs: %+v != %+v", c, want)
		}
	}
	if (out.Entry == nil) != (out.Query.ReceiptOrdinal == 0) {
		return fmt.Errorf("receipt entry presence differs")
	}
	if out.Entry != nil {
		entry := out.Entry
		if len(out.Paths) > 0 && (entry.Entry.Link == nil || entry.Entry.Item == nil || entry.Entry.State != "available") {
			return fmt.Errorf("unavailable receipt used by path")
		}
		p, ok := inputs[entry.GenerationID]
		if !ok || entry.GenerationID != out.Query.EntryGeneration {
			return fmt.Errorf("foreign receipt generation")
		}
		var source struct {
			Fact   string `json:"fact_set_id"`
			Source struct {
				Ordinal uint64                     `json:"source_row_ordinal"`
				Fields  map[string]json.RawMessage `json:"fields"`
			} `json:"source"`
		}
		if err := json.Unmarshal(entry.Entry.Source, &source); err != nil {
			return err
		}
		if source.Fact != p.Generation.Receipts.Inputs.Facts.ID || source.Source.Ordinal != out.Query.ReceiptOrdinal {
			return fmt.Errorf("receipt source locator differs")
		}
		b, ok := source.Source.Fields[scheduleaparquet.ColumnReceiptDate]
		if !ok {
			return fmt.Errorf("typed source date absent")
		}
		var day *int32
		if err := json.Unmarshal(b, &day); err != nil {
			return err
		}
		if !sameDate(day, entry.Date) || entry.Selection != independentDateSelection(day, out.Query.Window) {
			return fmt.Errorf("receipt source date or selection differs")
		}
		if entry.Selection != "included" && entry.Selection != "undated_included" && (len(out.Paths) != 0 || out.Search.State != "receipt_entry_excluded_by_date_window") {
			return fmt.Errorf("excluded receipt entered search")
		}
	}
	links := map[string]WindowConnectionLink{}
	for _, link := range out.Links {
		p, ok := inputs[link.GenerationID]
		if _, duplicate := links[link.ID]; !ok || duplicate || link.ID != connectionLinkID(link.GenerationID, link.Topology) || link.Evidence.Key != link.Topology.Key {
			return fmt.Errorf("link source/identity differs")
		}
		links[link.ID] = link
		switch link.Topology.Family {
		case "reported_receipt", "conduit_association":
			entry := out.Entry
			if entry == nil || entry.Entry.Link == nil || entry.GenerationID != link.GenerationID || !sameDate(entry.Date, link.Date) || *entry.Entry.Link != link.Topology || !bytes.Equal(entry.Entry.Source, link.Evidence.Evidence) || link.TemporalBasis != "underlying_receipt_reported_date" {
				return fmt.Errorf("entry link lost source provenance")
			}
			if link.Topology.Family == "conduit_association" {
				var doc struct {
					Amount string `json:"additional_amount_minor_units"`
				}
				if err := json.Unmarshal(link.Evidence.Document, &doc); err != nil {
					return err
				}
				if doc.Amount != "0" {
					return fmt.Errorf("conduit acquired additional money")
				}
			}
		case "candidate_authorization_context":
			var evidence struct {
				Fact    string   `json:"fact_set_id"`
				Members []string `json:"supporting_fact_ids"`
			}
			if err := json.Unmarshal(link.Evidence.Evidence, &evidence); err != nil {
				return err
			}
			if link.Date != nil || link.TemporalBasis != authorizationTimeBasis || evidence.Fact != p.Generation.Receipts.Inputs.Linkages.ID || len(evidence.Members) == 0 {
				return fmt.Errorf("authorization validity or source membership changed")
			}
		case "receiver_reported_committee_observation", "sender_reported_committee_observation":
			var doc struct {
				Fact    string      `json:"fact_set_id"`
				Ordinal uint64      `json:"source_row_ordinal"`
				Date    *int32      `json:"date_days"`
				Ledger  flow.Ledger `json:"ledger"`
			}
			if err := json.Unmarshal(link.Evidence.Document, &doc); err != nil {
				return err
			}
			fact, family := p.Generation.CommitteeFlow.Inputs.A.FactSetID, "receiver_reported_committee_observation"
			if out.Query.Ledger == flow.ScheduleB {
				fact, family = p.Generation.CommitteeFlow.Inputs.B.FactSetID, "sender_reported_committee_observation"
			}
			if doc.Fact != fact || doc.Ordinal == 0 || doc.Ledger != out.Query.Ledger || link.Topology.Family != family || !sameDate(doc.Date, link.Date) || link.TemporalBasis != "selected_committee_observation_reported_date" {
				return fmt.Errorf("committee ledger/source date differs")
			}
		default:
			if extra == nil {
				return fmt.Errorf("unsupported connection family")
			}
			if err := extra(link); err != nil {
				return err
			}
		}
		if link.Topology.Family != "candidate_authorization_context" {
			selection := independentDateSelection(link.Date, out.Query.Window)
			if selection != "included" && selection != "undated_included" {
				return fmt.Errorf("returned observation outside date window")
			}
		}
	}
	used := map[string]bool{}
	for _, path := range out.Paths {
		endpoint := out.Query.From
		if out.Entry != nil {
			endpoint = out.Entry.Entry.Link.From
		}
		if len(path.Links) == 0 {
			return fmt.Errorf("empty connection witness")
		}
		for _, id := range path.Links {
			link, ok := links[id]
			if !ok || link.Topology.From != endpoint {
				return fmt.Errorf("broken connection chain")
			}
			endpoint = link.Topology.To
			used[id] = true
		}
		if endpoint != out.Query.Target {
			return fmt.Errorf("connection missed target")
		}
	}
	if len(used) != len(links) {
		return fmt.Errorf("unused link evidence")
	}
	for _, c := range out.Contexts {
		if _, ok := inputs[c.GenerationID]; !ok || c.TemporalBasis != authorizationTimeBasis {
			return fmt.Errorf("candidate context validity invented")
		}
	}
	for _, v := range out.Vertices {
		if v.Kind == "reported_contributor_appearance" {
			continue
		}
		if len(v.Facets) != len(inputs) {
			return fmt.Errorf("historical facets collapsed")
		}
		seen := map[string]bool{}
		for _, f := range v.Facets {
			if _, ok := inputs[f.GenerationID]; !ok || seen[f.GenerationID] {
				return fmt.Errorf("historical facet source differs")
			}
			seen[f.GenerationID] = true
		}
	}
	return nil
}
