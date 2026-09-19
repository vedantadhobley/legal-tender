package flowevidence

import (
	"encoding/json"
	"strings"
	"testing"

	flow "github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
)

func fixture() model {
	makeEdge := func(side Ledger, ordinal uint64, amount int64) observation {
		return observation{Key: edgeKey(side, "facts", ordinal), From: "entities/C00000001", To: "entities/C00000002", Ledger: side, FactSetID: "facts", EconomicFlowStatus: "not_established", Observation: flow.Observation{Ordinal: ordinal, SubID: "same-label", Sender: "C00000001", Recipient: "C00000002", Role: "reported_transfer", Type: "24K", Amount: amount}}
	}
	return model{a: []observation{makeEdge(ScheduleA, 1, 9007199254740993), makeEdge(ScheduleA, 2, -3), makeEdge(ScheduleA, 3, 0)}, b: []observation{makeEdge(ScheduleB, 1, 42)}, components: []component{{Key: "x", Assertion: flow.Assertion{ID: "x", State: "ambiguous", A: []uint64{1, 2, 3}, B: []uint64{1}, AAmount: 9007199254740990, BAmount: 42}}}, calculation: flow.Result{A: flow.Side{Selected: flow.Measures{Rows: 3, Known: 3, Amount: 9007199254740990}}, B: flow.Side{Selected: flow.Measures{Rows: 1, Known: 1, Amount: 42}}}}
}
func TestComponentBindingPreservesGrainAndSignedAmounts(t *testing.T) {
	m := fixture()
	if err := bindComponents(&m); err != nil {
		t.Fatal(err)
	}
	if len(m.a) != 3 || m.a[0].Key == m.b[0].Key || m.a[0].Key == m.a[1].Key {
		t.Fatal("collapsed source occurrences")
	}
	for _, e := range append(m.a, m.b...) {
		if e.ComponentID != "x" || e.TerminalEligible || e.EconomicFlowStatus != "not_established" {
			t.Fatal("invented payment status", e)
		}
	}
	if edgeKey(ScheduleA, "other", 1) == m.a[0].Key {
		t.Fatal("fact set omitted from identity")
	}
	if m.components[0].Sender != "C00000001" {
		t.Fatal("component endpoint missing")
	}
}
func TestComponentBindingRejectsLossDuplicationAndConflicts(t *testing.T) {
	for name, change := range map[string]func(*model){
		"duplicate ordinal":    func(m *model) { m.a[1].Ordinal = 1 },
		"duplicate membership": func(m *model) { m.components[0].A = append(m.components[0].A, 1) },
		"missing membership":   func(m *model) { m.components[0].A = []uint64{1, 2} },
		"unknown member":       func(m *model) { m.components[0].A = append(m.components[0].A, 99) },
		"amount":               func(m *model) { m.components[0].AAmount++ },
		"endpoints":            func(m *model) { m.b[0].Recipient = "C00000003" },
		"total":                func(m *model) { m.calculation.B.Selected.Amount++ },
		"duplicate component":  func(m *model) { m.components = append(m.components, m.components[0]) },
	} {
		t.Run(name, func(t *testing.T) {
			m := fixture()
			change(&m)
			if err := bindComponents(&m); err == nil {
				t.Fatal("accepted inconsistent evidence")
			}
		})
	}
}
func TestFullDocumentComparisonIsExactAndRequiresExplicitFalse(t *testing.T) {
	e := fixture().a[0]
	raw, _ := json.Marshal(e)
	if !equalDocument(raw, e) {
		t.Fatal("equal document rejected")
	}
	for _, changed := range []string{
		strings.Replace(string(raw), "9007199254740993", "9007199254740992", 1),
		strings.Replace(string(raw), `"terminal_attribution_eligible":false,`, "", 1),
		strings.Replace(string(raw), `"source_row_ordinal":1`, `"source_row_ordinal":2`, 1),
		strings.Replace(string(raw), `"date_days":null`, `"date_days":0`, 1),
		strings.Replace(string(raw), `"_key":`, `"unexpected":true,"_key":`, 1),
	} {
		if equalDocument([]byte(changed), e) {
			t.Fatal("missed field drift")
		}
	}
}

func TestDatabaseNameFitsTraditionalLimitAndIdentityIncludesInputs(t *testing.T) {
	id := hash(Version, "bundle", "digest")
	name := databaseName("2024", id)
	if len(name) > 64 || !strings.HasSuffix(name, id[:32]) {
		t.Fatal("invalid database name", name)
	}
	if id == hash(Version, "bundle", "changed") || id == hash(Version, "changed", "digest") || id == hash(Version+"changed", "bundle", "digest") {
		t.Fatal("identity omitted input")
	}
}
