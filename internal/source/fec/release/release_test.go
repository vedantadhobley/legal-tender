package release

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	testObservedAt = time.Date(2026, 8, 31, 8, 0, 0, 0, time.UTC)
	testPlannedAt  = time.Date(2026, 8, 31, 8, 1, 0, 0, time.UTC)
)

func TestInitialInventoryMatchesContract(t *testing.T) {
	t.Parallel()
	path := filepath.Join(repositoryRoot(t), "contracts/releases/fec/v1/inventory.json")
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open inventory contract: %v", err)
	}
	defer func() { _ = file.Close() }()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var contract Inventory
	if err := decoder.Decode(&contract); err != nil {
		t.Fatalf("decode inventory contract: %v", err)
	}
	if issues := ValidateInventory(contract); len(issues) != 0 {
		t.Fatalf("contract inventory is invalid: %+v", issues)
	}
	if compiled := InitialInventory(); !reflect.DeepEqual(compiled, contract) {
		t.Fatalf("compiled inventory does not match %s", path)
	}
}

func TestScheduleEInventoryAddsOneAllHistoryRelation(t *testing.T) {
	t.Parallel()
	inventory := ScheduleEInventory()
	path := filepath.Join(repositoryRoot(t), "contracts/releases/fec/v2/inventory.json")
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open inventory contract: %v", err)
	}
	defer func() { _ = file.Close() }()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var contract Inventory
	if err := decoder.Decode(&contract); err != nil {
		t.Fatalf("decode inventory contract: %v", err)
	}
	if !reflect.DeepEqual(inventory, contract) {
		t.Fatalf("compiled inventory does not match %s", path)
	}
	if issues := ValidateInventory(inventory); len(issues) != 0 {
		t.Fatalf("active inventory is invalid: %+v", issues)
	}
	if inventory.SchemaVersion != InventorySchemaVersionV2 || inventory.InventoryVersion != ScheduleEInventoryVersion {
		t.Fatalf("unexpected v2 inventory identity: %+v", inventory)
	}
	if len(inventory.Sources) != 22 {
		t.Fatalf("sources = %d; want 22", len(inventory.Sources))
	}
	scheduleE := inventory.Sources[len(inventory.Sources)-1]
	if scheduleE.SourceID != ScheduleESourceID || !reflect.DeepEqual(scheduleE.Periods, initialPeriods) {
		t.Fatalf("unexpected Schedule E source: %+v", scheduleE)
	}
	want := []RelationSelection{{Name: "disclosure.fec_fitem_sched_e", Scope: allHistoryScope, FieldCount: 80}}
	if !reflect.DeepEqual(scheduleE.RelationSelections, want) {
		t.Fatalf("Schedule E relations = %+v; want %+v", scheduleE.RelationSelections, want)
	}
	outputs := desiredStageOutputs(inventory)
	if len(outputs) != 25 {
		t.Fatalf("staged outputs = %d; want 25", len(outputs))
	}
	output := outputs[len(outputs)-1]
	if output.SourceID != ScheduleESourceID || output.Period != allHistoryScope || output.ContractedFieldCount == nil || *output.ContractedFieldCount != 80 {
		t.Fatalf("unexpected Schedule E staged output: %+v", output)
	}
}

func TestActiveInventoryAddsArchiveDirectScheduleBRelations(t *testing.T) {
	t.Parallel()
	inventory := ActiveInventory()
	path := filepath.Join(repositoryRoot(t), "contracts/releases/fec/v3/inventory.json")
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open inventory contract: %v", err)
	}
	defer func() { _ = file.Close() }()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var contract Inventory
	if err := decoder.Decode(&contract); err != nil {
		t.Fatalf("decode inventory contract: %v", err)
	}
	if !reflect.DeepEqual(inventory, contract) {
		t.Fatalf("compiled inventory does not match %s", path)
	}
	if inventory.SchemaVersion != InventorySchemaVersionV3 || inventory.InventoryVersion != ActiveInventoryVersion {
		t.Fatalf("unexpected active inventory identity: %+v", inventory)
	}
	if len(inventory.Sources) != 23 {
		t.Fatalf("sources = %d; want 23", len(inventory.Sources))
	}
	scheduleB := inventory.Sources[len(inventory.Sources)-1]
	if scheduleB.SourceID != ScheduleBSourceID || !reflect.DeepEqual(scheduleB.Periods, initialPeriods) {
		t.Fatalf("unexpected Schedule B source: %+v", scheduleB)
	}
	if len(scheduleB.RelationSelections) != 4 {
		t.Fatalf("Schedule B relations = %d; want 4", len(scheduleB.RelationSelections))
	}
	for index, selection := range scheduleB.RelationSelections {
		if selection.Scope != initialPeriods[index] || selection.FieldCount != 81 || selection.Materialization != RelationMaterializationArchiveDirect {
			t.Fatalf("unexpected Schedule B relation %d: %+v", index, selection)
		}
	}
	if outputs := desiredStageOutputs(inventory); len(outputs) != 25 {
		t.Fatalf("staged outputs = %d; want 25 because Schedule B is archive-direct", len(outputs))
	}
}

func TestVersionedInventoryPeriodsAreFrozenAndIndependentlyOwned(t *testing.T) {
	t.Parallel()
	want := []string{"2020", "2022", "2024", "2026"}
	for _, version := range []string{InitialInventoryVersion, ScheduleEInventoryVersion, ActiveInventoryVersion} {
		inventory, ok := InventoryForVersion(version)
		if !ok {
			t.Fatalf("inventory version %s is not registered", version)
		}
		if !reflect.DeepEqual(inventory.Periods, want) {
			t.Fatalf("inventory %s periods = %v; want frozen %v", version, inventory.Periods, want)
		}
		inventory.Periods[0] = "2018"
		replayed, _ := InventoryForVersion(version)
		if !reflect.DeepEqual(replayed.Periods, want) {
			t.Fatalf("inventory %s leaked caller mutation: %v", version, replayed.Periods)
		}
	}
}

func TestPlanMigratesV1ReleaseToV2ByChangingOnlyScheduleE(t *testing.T) {
	t.Parallel()
	v1 := InitialInventory()
	priorDiscovery := availableDiscovery(v1, "same-publisher-version")
	current := publishedManifest(v1, priorDiscovery)

	v2 := ScheduleEInventory()
	discovery := availableDiscovery(v2, "same-publisher-version")
	for index := range v1.Sources {
		discovery.Observations[index] = priorDiscovery.Observations[index]
	}
	plan := Plan(v2, discovery, &current, testPlannedAt)
	assertValidPlan(t, v2, plan)
	if plan.Status != PlanUpdateAvailable {
		t.Fatalf("status = %q; want %q; issues=%+v", plan.Status, PlanUpdateAvailable, plan.Issues)
	}
	if !reflect.DeepEqual(plan.ChangedSourceIDs, []string{ScheduleESourceID}) {
		t.Fatalf("changed sources = %v; want only Schedule E", plan.ChangedSourceIDs)
	}
	if len(plan.ReusedSourceIDs) != 21 {
		t.Fatalf("reused sources = %d; want 21", len(plan.ReusedSourceIDs))
	}
	if plan.CandidateReleaseID == "" || plan.CandidateReleaseID == current.ReleaseID {
		t.Fatalf("v2 candidate release identity was not version-separated: %q", plan.CandidateReleaseID)
	}
}

func TestPlanMigratesV2ReleaseToV3ByChangingOnlyScheduleB(t *testing.T) {
	t.Parallel()
	v2 := ScheduleEInventory()
	priorDiscovery := availableDiscovery(v2, "same-publisher-version")
	current := publishedManifest(v2, priorDiscovery)

	v3 := ActiveInventory()
	discovery := availableDiscovery(v3, "same-publisher-version")
	for index := range v2.Sources {
		discovery.Observations[index] = priorDiscovery.Observations[index]
	}
	plan := Plan(v3, discovery, &current, testPlannedAt)
	assertValidPlan(t, v3, plan)
	if plan.Status != PlanUpdateAvailable {
		t.Fatalf("status = %q; want %q; issues=%+v", plan.Status, PlanUpdateAvailable, plan.Issues)
	}
	if !reflect.DeepEqual(plan.ChangedSourceIDs, []string{ScheduleBSourceID}) {
		t.Fatalf("changed sources = %v; want only Schedule B", plan.ChangedSourceIDs)
	}
	if len(plan.ReusedSourceIDs) != 22 {
		t.Fatalf("reused sources = %d; want 22", len(plan.ReusedSourceIDs))
	}
}

func TestValidateInventoryRejectsVersionedMembershipDrift(t *testing.T) {
	t.Parallel()
	inventory := InitialInventory()
	inventory.Sources[0].RequestURL = "https://www.fec.gov/files/bulk-downloads/2020/cm20.zip"
	issues := ValidateInventory(inventory)
	if !hasIssueCode(issues, "inventory_source_drift") {
		t.Fatalf("membership drift was accepted: %+v", issues)
	}
}

func TestReleasePlanFixtures(t *testing.T) {
	t.Parallel()
	inventory := InitialInventory()
	fixtures := map[string]string{
		"update-available.json": "update_available",
		"no-change.json":        "no_change",
		"source-not-ready.json": "source_not_ready",
		"invalid.json":          "invalid",
	}
	for name, expectedStatus := range fixtures {
		name := name
		expectedStatus := expectedStatus
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(repositoryRoot(t), "contracts/releases/fec/v1/fixtures", name)
			file, err := os.Open(path)
			if err != nil {
				t.Fatalf("open fixture: %v", err)
			}
			defer func() { _ = file.Close() }()
			decoder := json.NewDecoder(file)
			decoder.DisallowUnknownFields()
			var plan ReleasePlan
			if err := decoder.Decode(&plan); err != nil {
				t.Fatalf("decode fixture: %v", err)
			}
			if plan.Status != expectedStatus {
				t.Fatalf("status = %q; want %q", plan.Status, expectedStatus)
			}
			if issues := ValidatePlan(inventory, plan); len(issues) != 0 {
				t.Fatalf("fixture is invalid: %+v", issues)
			}
		})
	}
}

func TestDiscoverUsesOnlyHEADAndDoesNotReadBodies(t *testing.T) {
	t.Parallel()
	inventory := InitialInventory()
	doer := &metadataDoer{}
	clock := &incrementingClock{next: testObservedAt}
	discovery, err := Discover(
		contextWithoutCancellation(),
		doer,
		inventory,
		clock.Now,
		DiscoverOptions{MaxConcurrent: 3},
	)
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(discovery.Observations) != 21 {
		t.Fatalf("observations = %d; want 21", len(discovery.Observations))
	}
	observationTimes := make(map[time.Time]struct{}, len(discovery.Observations))
	for index, observation := range discovery.Observations {
		if observation.SourceID != inventory.Sources[index].SourceID {
			t.Fatalf("observation %d source = %q; want %q", index, observation.SourceID, inventory.Sources[index].SourceID)
		}
		if observation.Status != ObservationAvailable || observation.VersionBasis != "etag" {
			t.Fatalf("observation %s not available by ETag: %+v", observation.SourceID, observation)
		}
		if observation.ObservedAt.Before(discovery.StartedAt) || observation.ObservedAt.After(discovery.CompletedAt) {
			t.Fatalf("observation %s time %s outside discovery window %s..%s", observation.SourceID, observation.ObservedAt, discovery.StartedAt, discovery.CompletedAt)
		}
		observationTimes[observation.ObservedAt] = struct{}{}
	}
	if len(observationTimes) != len(discovery.Observations) {
		t.Fatalf("per-source observation timestamps were collapsed: %d unique for %d observations", len(observationTimes), len(discovery.Observations))
	}
	doer.mu.Lock()
	defer doer.mu.Unlock()
	if len(doer.methods) != 21 {
		t.Fatalf("requests = %d; want 21", len(doer.methods))
	}
	for _, method := range doer.methods {
		if method != http.MethodHead {
			t.Fatalf("request method = %q; want HEAD", method)
		}
	}
	for _, body := range doer.bodies {
		if body.read {
			t.Fatal("metadata discovery read a response body")
		}
		if !body.closed {
			t.Fatal("metadata discovery did not close a response body")
		}
	}
}

func TestPlanUpdateAvailableWithoutCurrentRelease(t *testing.T) {
	t.Parallel()
	inventory := InitialInventory()
	discovery := availableDiscovery(inventory, "v1")
	plan := Plan(inventory, discovery, nil, testPlannedAt)
	assertValidPlan(t, inventory, plan)
	if plan.Status != PlanUpdateAvailable {
		t.Fatalf("status = %q; want %q; issues=%+v", plan.Status, PlanUpdateAvailable, plan.Issues)
	}
	if len(plan.SelectedSources) != 21 || len(plan.ChangedSourceIDs) != 21 || len(plan.ReusedSourceIDs) != 0 {
		t.Fatalf("unexpected bootstrap plan counts: selected=%d changed=%d reused=%d", len(plan.SelectedSources), len(plan.ChangedSourceIDs), len(plan.ReusedSourceIDs))
	}
	if !strings.HasPrefix(plan.CandidateReleaseID, "fec-") || len(plan.CandidateReleaseID) != 68 {
		t.Fatalf("candidate release ID = %q", plan.CandidateReleaseID)
	}

	laterDiscovery := availableDiscovery(inventory, "v1")
	laterDiscovery.StartedAt = testObservedAt.Add(24 * time.Hour)
	laterDiscovery.CompletedAt = laterDiscovery.StartedAt
	for index := range laterDiscovery.Observations {
		laterDiscovery.Observations[index].ObservedAt = laterDiscovery.StartedAt
	}
	later := Plan(inventory, laterDiscovery, nil, testPlannedAt.Add(24*time.Hour))
	if later.CandidateReleaseID != plan.CandidateReleaseID {
		t.Fatalf("candidate ID changed with observation time: %q != %q", later.CandidateReleaseID, plan.CandidateReleaseID)
	}
}

func TestPlanNoChange(t *testing.T) {
	t.Parallel()
	inventory := InitialInventory()
	discovery := availableDiscovery(inventory, "v1")
	current := publishedManifest(inventory, discovery)
	plan := Plan(inventory, discovery, &current, testPlannedAt)
	assertValidPlan(t, inventory, plan)
	if plan.Status != PlanNoChange {
		t.Fatalf("status = %q; want %q; issues=%+v", plan.Status, PlanNoChange, plan.Issues)
	}
	if plan.CandidateReleaseID != "" || len(plan.ChangedSourceIDs) != 0 || len(plan.ReusedSourceIDs) != 21 {
		t.Fatalf("unexpected no-change plan: %+v", plan)
	}
}

func TestPlanUpdateAvailableForOneChangedSource(t *testing.T) {
	t.Parallel()
	inventory := InitialInventory()
	priorDiscovery := availableDiscovery(inventory, "v1")
	current := publishedManifest(inventory, priorDiscovery)
	discovery := availableDiscovery(inventory, "v1")
	changed := &discovery.Observations[13]
	changed.ETag = `"v2-fec:weball:2024"`
	changed.VersionBasis, changed.VersionIdentity = deriveVersionIdentity("", changed.ETag, "", changed.LastModified, changed.ContentLength)

	plan := Plan(inventory, discovery, &current, testPlannedAt)
	assertValidPlan(t, inventory, plan)
	if plan.Status != PlanUpdateAvailable {
		t.Fatalf("status = %q; want %q; issues=%+v", plan.Status, PlanUpdateAvailable, plan.Issues)
	}
	if !reflect.DeepEqual(plan.ChangedSourceIDs, []string{"fec:weball:2024"}) || len(plan.ReusedSourceIDs) != 20 {
		t.Fatalf("unexpected changed/reused sources: changed=%v reused=%d", plan.ChangedSourceIDs, len(plan.ReusedSourceIDs))
	}
}

func TestPlanSourceNotReady(t *testing.T) {
	t.Parallel()
	inventory := InitialInventory()
	discovery := availableDiscovery(inventory, "v1")
	unavailable := &discovery.Observations[20]
	unavailable.Status = ObservationUnavailable
	unavailable.VersionIdentity = ""
	unavailable.VersionBasis = ""
	unavailable.ETag = ""
	unavailable.ProblemCode = "unexpected_http_status"
	unavailable.Problem = "HEAD returned HTTP 503"
	unavailable.HTTPStatus = http.StatusServiceUnavailable

	plan := Plan(inventory, discovery, nil, testPlannedAt)
	assertValidPlan(t, inventory, plan)
	if plan.Status != PlanSourceNotReady {
		t.Fatalf("status = %q; want %q; issues=%+v", plan.Status, PlanSourceNotReady, plan.Issues)
	}
	if len(plan.Issues) != 1 || plan.Issues[0].SourceID != "fec:schedule-a:processed" {
		t.Fatalf("unexpected readiness issues: %+v", plan.Issues)
	}
	if len(plan.SelectedSources) != 0 || plan.CandidateReleaseID != "" {
		t.Fatalf("source-not-ready plan authorized acquisition: %+v", plan)
	}
}

func TestPlanInvalidDiscovery(t *testing.T) {
	t.Parallel()
	inventory := InitialInventory()
	discovery := availableDiscovery(inventory, "v1")
	discovery.Observations = discovery.Observations[:20]
	plan := Plan(inventory, discovery, nil, testPlannedAt)
	assertValidPlan(t, inventory, plan)
	if plan.Status != PlanInvalid {
		t.Fatalf("status = %q; want %q", plan.Status, PlanInvalid)
	}
	if len(plan.Issues) == 0 || len(plan.SelectedSources) != 0 {
		t.Fatalf("invalid plan = %+v", plan)
	}
}

func availableDiscovery(inventory Inventory, version string) Discovery {
	observations := make([]Observation, 0, len(inventory.Sources))
	for index, source := range inventory.Sources {
		contentLength := int64(1000 + index)
		etag := fmt.Sprintf(`"%s-%s"`, version, source.SourceID)
		basis, identity := deriveVersionIdentity("", etag, "", "Mon, 31 Aug 2026 07:00:00 GMT", &contentLength)
		observations = append(observations, Observation{
			SourceID:        source.SourceID,
			RequestMethod:   http.MethodHead,
			RequestURL:      source.RequestURL,
			FinalURL:        source.RequestURL,
			ObservedAt:      testObservedAt,
			Status:          ObservationAvailable,
			HTTPStatus:      http.StatusOK,
			VersionIdentity: identity,
			VersionBasis:    basis,
			ETag:            etag,
			LastModified:    "Mon, 31 Aug 2026 07:00:00 GMT",
			ContentLength:   &contentLength,
			AcceptRanges:    "bytes",
		})
	}
	return Discovery{
		SchemaVersion:    DiscoverySchemaVersion,
		InventoryVersion: inventory.InventoryVersion,
		StartedAt:        testObservedAt,
		CompletedAt:      testObservedAt,
		Observations:     observations,
	}
}

func publishedManifest(inventory Inventory, discovery Discovery) ReleaseManifest {
	artifacts := make([]PublishedArtifact, 0, len(discovery.Observations))
	for _, observation := range discovery.Observations {
		selected := selectObservation(observation)
		artifacts = append(artifacts, PublishedArtifact{
			SelectedSource: selected,
			ByteCount:      *selected.ContentLength,
			SHA256:         strings.Repeat("a", 64),
			StorageKey:     "sha256/aa/" + observation.SourceID,
			AcquiredAt:     testObservedAt.Add(time.Minute),
		})
	}
	outputs := desiredStageOutputs(inventory)
	for index := range outputs {
		outputs[index].Disposition = "staged"
		outputs[index].SourceArtifactSHA256 = strings.Repeat("a", 64)
		outputs[index].UncompressedByteCount = 1
		outputs[index].UncompressedSHA256 = strings.Repeat("b", 64)
		outputs[index].Compression = "zstd"
		outputs[index].CompressionLevel = zstdCompressionLevel
		outputs[index].CompressedByteCount = 1
		outputs[index].CompressedSHA256 = strings.Repeat("c", 64)
		outputs[index].StorageKey = stagedStorageKey(outputs[index].SourceID, strings.Repeat("c", 64))
		outputs[index].DecompressionValidated = true
		outputs[index].StagedAt = testObservedAt.Add(time.Minute)
		if outputs[index].SelectionKind == "relation" {
			rowCount := uint64(1)
			outputs[index].RowCount = &rowCount
		}
	}
	checks := []ReleaseCheck{
		{ID: "input_identity", Passed: true, Severity: "block", Detail: "test input identity"},
		{ID: "source_artifact_membership", Passed: true, Severity: "block", Detail: "test artifact membership"},
		{ID: "selected_output_membership", Passed: true, Severity: "block", Detail: "test output membership"},
		{ID: "output_integrity", Passed: true, Severity: "block", Detail: "test integrity"},
		{ID: "storage_budget", Passed: true, Severity: "block", Detail: "test storage budget"},
	}
	return ReleaseManifest{
		Schema:            "release-manifest.schema.json",
		SchemaVersion:     ManifestSchemaVersion,
		InventoryVersion:  inventory.InventoryVersion,
		ReleaseID:         "fec-prior-release",
		RunID:             "test-publish",
		PlanSHA256:        strings.Repeat("d", 64),
		AcquisitionSHA256: strings.Repeat("e", 64),
		StageSHA256:       strings.Repeat("f", 64),
		State:             "published",
		SelectedAt:        testObservedAt,
		PublishedAt:       testObservedAt.Add(2 * time.Minute),
		Periods:           append([]string(nil), inventory.Periods...),
		Artifacts:         artifacts,
		StagedOutputs:     outputs,
		Checks:            checks,
	}
}

func assertValidPlan(t *testing.T, inventory Inventory, plan ReleasePlan) {
	t.Helper()
	if issues := ValidatePlan(inventory, plan); len(issues) != 0 {
		t.Fatalf("planner emitted an invalid result: %+v\nplan=%+v", issues, plan)
	}
}

func hasIssueCode(issues []Issue, code string) bool {
	for _, issue := range issues {
		if issue.Code == code {
			return true
		}
	}
	return false
}

type metadataDoer struct {
	mu      sync.Mutex
	methods []string
	bodies  []*trackingBody
}

func (doer *metadataDoer) Do(request *http.Request) (*http.Response, error) {
	body := &trackingBody{}
	doer.mu.Lock()
	doer.methods = append(doer.methods, request.Method)
	doer.bodies = append(doer.bodies, body)
	doer.mu.Unlock()
	return &http.Response{
		StatusCode:    http.StatusOK,
		Header:        http.Header{"Etag": []string{`"publisher-version"`}, "Last-Modified": []string{"Mon, 31 Aug 2026 07:00:00 GMT"}},
		Body:          body,
		ContentLength: 123,
		Request:       request,
	}, nil
}

type trackingBody struct {
	read   bool
	closed bool
}

type incrementingClock struct {
	mu   sync.Mutex
	next time.Time
}

func (clock *incrementingClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	current := clock.next
	clock.next = clock.next.Add(time.Second)
	return current
}

func (body *trackingBody) Read([]byte) (int, error) {
	body.read = true
	return 0, io.EOF
}

func (body *trackingBody) Close() error {
	body.closed = true
	return nil
}

func contextWithoutCancellation() context.Context {
	return context.Background()
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(sourceFile), "../../../.."))
}
