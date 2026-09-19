package receiptgraph

import (
	"encoding/json"
	"fmt"
	"reflect"

	c "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptconduits"
	p "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
)

const CompactVersion = "legal-tender.arango.receipt-participant-sample.v2"
const ExpandedLayout = "expanded-v1"
const CompactLayout = "compact-v2"

// Only the duplicated participant row moves out of the graph. These fields
// retain source membership, queryable dispositions and complete conduit evidence.
// Receipt edges, source-grain keys and financial/identity boundaries do not change.
type compactAppearance struct {
	Key              string      `json:"_key"`
	FactSet          string      `json:"fact_set_id"`
	Ordinal          int64       `json:"source_row_ordinal"`
	Component        string      `json:"inventory_component"`
	SourceRoute      string      `json:"source_route"`
	Conduit          *c.Decision `json:"conduit_decision"`
	ConduitState     string      `json:"conduit_disposition"`
	RecipientState   string      `json:"recipient_connection_state"`
	IdentityResolved bool        `json:"identity_resolved"`
}

func compact(a appearance) compactAppearance {
	return compactAppearance{a.Key, a.FactSet, a.Row.Ordinal, a.Row.Component, a.Row.SourceRoute, a.Conduit, a.ConduitState, a.RecipientState, a.IdentityResolved}
}

// Reconstruct the former document from actual graph fields plus the exact
// verified participant row. Never silently repair a changed graph field.
func expand(a compactAppearance, row p.Row) (appearance, error) {
	want, _, _, err := project(a.FactSet, row, a.Conduit)
	if err != nil {
		return appearance{}, err
	}
	if !reflect.DeepEqual(compact(want), a) {
		return appearance{}, fmt.Errorf("compact appearance differs from exact source membership or disposition")
	}
	return appearance{a.Key, a.FactSet, row, a.Conduit, a.ConduitState, a.RecipientState, a.IdentityResolved}, nil
}

func physicalAppearance(layout string, a appearance) any {
	if layout != CompactLayout {
		return a
	}
	return compact(a)
}

func versionFor(layout string) string {
	if layout == CompactLayout {
		return CompactVersion
	}
	return Version
}

func verifyReconstruction(raw, source []byte) error {
	var got compactAppearance
	var original appearance
	if json.Unmarshal(raw, &got) != nil || json.Unmarshal(source, &original) != nil {
		return fmt.Errorf("invalid compact/source proof document")
	}
	encoded, _ := json.Marshal(got)
	if !equalJSON(raw, encoded) {
		return fmt.Errorf("compact document has extra or omitted fields")
	}
	back, err := expand(got, original.Row)
	if err != nil {
		return err
	}
	b, err := json.Marshal(back)
	if err != nil || !equalJSON(b, source) {
		return fmt.Errorf("reconstructed graph appearance changed source evidence")
	}
	return nil
}
