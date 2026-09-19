package release

import (
	"fmt"
	"net/url"
	"slices"
)

// initialPeriods is immutable membership of the replayable v1, v2, and v3
// inventories. Advancing the rolling four-cycle view requires a new inventory
// version; never mutate historical inventory membership in place.
var initialPeriods = []string{"2020", "2022", "2024", "2026"}

const allHistoryScope = "all_history"

const (
	RelationMaterializationStagedCopy    = "staged_copy_zstd"
	RelationMaterializationArchiveDirect = "archive_direct"
	ArtifactFormatCommitteeSummaryCSV    = "committee_summary_csv"
)

type cycleFamily struct {
	code           string
	factFamily     string
	sourceContract string
	member         func(string) string
}

var initialCycleFamilies = []cycleFamily{
	{code: "cn", factFamily: "candidate_master", sourceContract: "fec/candidate-master@1.0.0", member: func(string) string { return "cn.txt" }},
	{code: "cm", factFamily: "committee_master", sourceContract: "fec/committee-master@1.0.0", member: func(string) string { return "cm.txt" }},
	{code: "ccl", factFamily: "candidate_committee_linkage", sourceContract: "fec/candidate-committee-linkage@1.0.0", member: func(string) string { return "ccl.txt" }},
	{code: "weball", factFamily: "all_candidates_summary", sourceContract: "fec/all-candidates-summary@1.0.0", member: func(period string) string { return "weball" + period[2:] + ".txt" }},
	{code: "webl", factFamily: "current_campaigns_summary", sourceContract: "fec/current-campaigns-summary@1.0.0", member: func(period string) string { return "webl" + period[2:] + ".txt" }},
}

// InitialInventory returns an independent copy of the v1 21-source inventory.
func InitialInventory() Inventory {
	sources := make([]SourceSpec, 0, 21)
	for _, period := range initialPeriods {
		for _, family := range initialCycleFamilies {
			suffix := period[2:]
			sources = append(sources, SourceSpec{
				SourceID:          fmt.Sprintf("fec:%s:%s", family.code, period),
				FactFamily:        family.factFamily,
				SourceContract:    family.sourceContract,
				RequestURL:        fmt.Sprintf("https://www.fec.gov/files/bulk-downloads/%s/%s%s.zip", period, family.code, suffix),
				Periods:           []string{period},
				SelectedMembers:   []string{family.member(period)},
				SelectedRelations: []string{},
			})
		}
	}
	sources = append(sources, SourceSpec{
		SourceID:        "fec:schedule-a:processed",
		FactFamily:      "processed_schedule_a",
		SourceContract:  "fec/schedule-a@1.0.0",
		RequestURL:      "https://www.fec.gov/files/bulk-downloads/data-dump/schedules/fec_fitem_sched_a.dump",
		Periods:         slices.Clone(initialPeriods),
		SelectedMembers: []string{},
		SelectedRelations: []string{
			"disclosure.fec_fitem_sched_a_2019_2020",
			"disclosure.fec_fitem_sched_a_2021_2022",
			"disclosure.fec_fitem_sched_a_2023_2024",
			"disclosure.fec_fitem_sched_a_2025_2026",
		},
	})
	return Inventory{
		Schema:           "inventory.schema.json",
		SchemaVersion:    InventorySchemaVersionV1,
		InventoryVersion: InitialInventoryVersion,
		Periods:          slices.Clone(initialPeriods),
		Sources:          sources,
	}
}

// ScheduleEInventory returns an independent copy of the replayable v2
// contract. V2 adds Schedule E without mutating the v1 inventory.
func ScheduleEInventory() Inventory {
	inventory := InitialInventory()
	inventory.SchemaVersion = InventorySchemaVersionV2
	inventory.InventoryVersion = ScheduleEInventoryVersion

	scheduleA := &inventory.Sources[len(inventory.Sources)-1]
	scheduleA.RelationSelections = make([]RelationSelection, 0, len(scheduleA.SelectedRelations))
	for index, relation := range scheduleA.SelectedRelations {
		scheduleA.RelationSelections = append(scheduleA.RelationSelections, RelationSelection{
			Name:       relation,
			Scope:      scheduleA.Periods[index],
			FieldCount: 81,
		})
	}
	scheduleA.SelectedRelations = []string{}

	inventory.Sources = append(inventory.Sources, SourceSpec{
		SourceID:          ScheduleESourceID,
		FactFamily:        "processed_schedule_e",
		SourceContract:    "fec/schedule-e@1.0.0",
		RequestURL:        "https://www.fec.gov/files/bulk-downloads/data-dump/schedules/fec_fitem_sched_e.dump",
		Periods:           slices.Clone(initialPeriods),
		SelectedMembers:   []string{},
		SelectedRelations: []string{},
		RelationSelections: []RelationSelection{{
			Name:       "disclosure.fec_fitem_sched_e",
			Scope:      allHistoryScope,
			FieldCount: 80,
		}},
	})
	return inventory
}

// ActiveInventory returns an independent copy of the current release
// contract. V3 adds selected-cycle Schedule B relations without mutating the
// replayable v1 or v2 inventories. Schedule B is read directly from its
// immutable dump during Parquet publication; staging a second full COPY stream
// would add no identity evidence and would consume more than 100 GiB per cycle.
func ActiveInventory() Inventory {
	inventory := ScheduleEInventory()
	inventory.SchemaVersion = InventorySchemaVersionV3
	inventory.InventoryVersion = ActiveInventoryVersion
	inventory.Sources = append(inventory.Sources, SourceSpec{
		SourceID:          ScheduleBSourceID,
		FactFamily:        "processed_schedule_b",
		SourceContract:    "fec/schedule-b@1.0.0",
		RequestURL:        "https://www.fec.gov/files/bulk-downloads/data-dump/schedules/fec_fitem_sched_b.dump",
		Periods:           slices.Clone(initialPeriods),
		SelectedMembers:   []string{},
		SelectedRelations: []string{},
		RelationSelections: []RelationSelection{
			{Name: "disclosure.fec_fitem_sched_b_2019_2020", Scope: "2020", FieldCount: 81, Materialization: RelationMaterializationArchiveDirect},
			{Name: "disclosure.fec_fitem_sched_b_2021_2022", Scope: "2022", FieldCount: 81, Materialization: RelationMaterializationArchiveDirect},
			{Name: "disclosure.fec_fitem_sched_b_2023_2024", Scope: "2024", FieldCount: 81, Materialization: RelationMaterializationArchiveDirect},
			{Name: "disclosure.fec_fitem_sched_b_2025_2026", Scope: "2026", FieldCount: 81, Materialization: RelationMaterializationArchiveDirect},
		},
	})
	return inventory
}

// CommitteeSummaryInventory adds four whole CSV artifacts to v3. It is opt-in
// until a real coordinated v4 release passes; the default stays on v3.
func CommitteeSummaryInventory() Inventory {
	inventory := ActiveInventory()
	inventory.SchemaVersion = InventorySchemaVersionV4
	inventory.InventoryVersion = CommitteeSummaryInventoryVersion
	for _, cycle := range inventory.Periods {
		inventory.Sources = append(inventory.Sources, SourceSpec{
			SourceID:   "fec:committee-summary:" + cycle,
			FactFamily: "committee_summary", SourceContract: "fec/committee-summary@1.0.0",
			RequestURL: fmt.Sprintf("https://www.fec.gov/files/bulk-downloads/%s/committee_summary_%s.csv", cycle, cycle),
			Periods:    []string{cycle}, SelectedMembers: []string{}, SelectedRelations: []string{},
			ArtifactFormat: ArtifactFormatCommitteeSummaryCSV,
		})
	}
	return inventory
}

// InventoryForVersion resolves a committed inventory version. It is used to
// replay historical evidence and migrate release contracts safely.
func InventoryForVersion(version string) (Inventory, bool) {
	switch version {
	case InitialInventoryVersion:
		return InitialInventory(), true
	case ScheduleEInventoryVersion:
		return ScheduleEInventory(), true
	case ActiveInventoryVersion:
		return ActiveInventory(), true
	case CommitteeSummaryInventoryVersion:
		return CommitteeSummaryInventory(), true
	default:
		return Inventory{}, false
	}
}

// ValidateInventory rejects drift in the compiled release membership.
func ValidateInventory(inventory Inventory) []Issue {
	issues := make([]Issue, 0)
	if inventory.Schema != "inventory.schema.json" {
		issues = append(issues, Issue{Code: "inventory_schema", Message: "inventory must reference inventory.schema.json"})
	}
	expected, known := InventoryForVersion(inventory.InventoryVersion)
	if !known {
		issues = append(issues, Issue{Code: "inventory_version", Message: "unknown inventory version"})
		return issues
	}
	if inventory.SchemaVersion != expected.SchemaVersion {
		issues = append(issues, Issue{Code: "inventory_schema_version", Message: "unexpected inventory schema version"})
	}
	if !slices.Equal(inventory.Periods, initialPeriods) {
		issues = append(issues, Issue{Code: "inventory_periods", Message: "inventory periods must be 2020, 2022, 2024, and 2026 in order"})
	}
	if len(inventory.Sources) != len(expected.Sources) {
		issues = append(issues, Issue{Code: "inventory_source_count", Message: fmt.Sprintf("inventory has %d sources; want %d", len(inventory.Sources), len(expected.Sources))})
	}
	if len(inventory.Sources) == len(expected.Sources) {
		for index := range expected.Sources {
			if !sameSourceSpec(inventory.Sources[index], expected.Sources[index]) {
				issues = append(issues, Issue{
					SourceID: inventory.Sources[index].SourceID,
					Code:     "inventory_source_drift",
					Message:  fmt.Sprintf("source at position %d does not match the versioned inventory", index),
				})
			}
		}
	}
	seen := make(map[string]struct{}, len(inventory.Sources))
	for _, source := range inventory.Sources {
		if source.SourceID == "" {
			issues = append(issues, Issue{Code: "inventory_source_id", Message: "source ID is empty"})
			continue
		}
		if _, exists := seen[source.SourceID]; exists {
			issues = append(issues, Issue{SourceID: source.SourceID, Code: "inventory_duplicate_source", Message: "source ID occurs more than once"})
		}
		seen[source.SourceID] = struct{}{}
		parsed, err := url.Parse(source.RequestURL)
		if err != nil || parsed.Scheme != "https" || parsed.Hostname() != "www.fec.gov" {
			issues = append(issues, Issue{SourceID: source.SourceID, Code: "inventory_request_url", Message: "request URL must be an HTTPS www.fec.gov URL"})
		}
		if source.FactFamily == "" || source.SourceContract == "" || len(source.Periods) == 0 {
			issues = append(issues, Issue{SourceID: source.SourceID, Code: "inventory_source_fields", Message: "fact family, source contract, and periods are required"})
		}
		if source.ArtifactFormat != "" && (source.ArtifactFormat != ArtifactFormatCommitteeSummaryCSV || len(source.SelectedMembers)+len(source.SelectedRelations)+len(source.RelationSelections) != 0 || len(source.Periods) != 1) {
			issues = append(issues, Issue{SourceID: source.SourceID, Code: "inventory_artifact_format", Message: "whole CSV selection must be exclusive and cycle-scoped"})
		}
		if source.ArtifactFormat == "" && len(source.SelectedMembers) == 0 && len(source.SelectedRelations) == 0 && len(source.RelationSelections) == 0 {
			issues = append(issues, Issue{SourceID: source.SourceID, Code: "inventory_selection", Message: "source must select at least one archive member or relation"})
		}
		if len(source.SelectedRelations) != 0 && len(source.RelationSelections) != 0 {
			issues = append(issues, Issue{SourceID: source.SourceID, Code: "inventory_relation_selection", Message: "source cannot mix legacy and scoped relation selections"})
		}
		seenRelations := make(map[string]struct{}, len(source.RelationSelections))
		for _, selection := range source.RelationSelections {
			if selection.Name == "" || selection.FieldCount <= 0 || (selection.Scope != allHistoryScope && !slices.Contains(source.Periods, selection.Scope)) {
				issues = append(issues, Issue{SourceID: source.SourceID, Code: "inventory_relation_selection", Message: "scoped relation selection is invalid"})
			}
			if !validRelationMaterialization(selection.Materialization) {
				issues = append(issues, Issue{SourceID: source.SourceID, Code: "inventory_relation_materialization", Message: "relation materialization is invalid"})
			}
			if _, duplicate := seenRelations[selection.Name]; duplicate {
				issues = append(issues, Issue{SourceID: source.SourceID, Code: "inventory_relation_selection", Message: "scoped relation occurs more than once"})
			}
			seenRelations[selection.Name] = struct{}{}
		}
	}
	return issues
}

func relationMaterialization(selection RelationSelection) string {
	if selection.Materialization == "" {
		return RelationMaterializationStagedCopy
	}
	return selection.Materialization
}

func validRelationMaterialization(value string) bool {
	return value == "" || value == RelationMaterializationStagedCopy || value == RelationMaterializationArchiveDirect
}

func relationRequiresStage(selection RelationSelection) bool {
	return relationMaterialization(selection) == RelationMaterializationStagedCopy
}

func sameSourceSpec(left, right SourceSpec) bool {
	return left.SourceID == right.SourceID &&
		left.FactFamily == right.FactFamily &&
		left.SourceContract == right.SourceContract &&
		left.RequestURL == right.RequestURL &&
		left.ArtifactFormat == right.ArtifactFormat &&
		slices.Equal(left.Periods, right.Periods) &&
		slices.Equal(left.SelectedMembers, right.SelectedMembers) &&
		slices.Equal(left.SelectedRelations, right.SelectedRelations) &&
		slices.Equal(left.RelationSelections, right.RelationSelections)
}

func normalizedRelationSelections(source SourceSpec) []RelationSelection {
	if len(source.RelationSelections) != 0 {
		return slices.Clone(source.RelationSelections)
	}
	result := make([]RelationSelection, 0, len(source.SelectedRelations))
	for index, relation := range source.SelectedRelations {
		scope := ""
		if index < len(source.Periods) {
			scope = source.Periods[index]
		}
		result = append(result, RelationSelection{Name: relation, Scope: scope, FieldCount: 81})
	}
	return result
}

func selectedRelationNames(source SourceSpec) []string {
	selections := normalizedRelationSelections(source)
	result := make([]string, 0, len(selections))
	for _, selection := range selections {
		result = append(result, selection.Name)
	}
	return result
}

func sourceSpecsArtifactCompatible(left, right SourceSpec) bool {
	return left.SourceID == right.SourceID &&
		left.FactFamily == right.FactFamily &&
		left.SourceContract == right.SourceContract &&
		left.RequestURL == right.RequestURL &&
		left.ArtifactFormat == right.ArtifactFormat
}

func sourceSpecByID(inventory Inventory) map[string]SourceSpec {
	result := make(map[string]SourceSpec, len(inventory.Sources))
	for _, source := range inventory.Sources {
		result[source.SourceID] = source
	}
	return result
}
