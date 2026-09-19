package release

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCopySelectedRelationRequiresOneValidExactSection(t *testing.T) {
	t.Parallel()
	relation := "disclosure.fec_fitem_sched_a_2025_2026"
	fields := make([]string, 81)
	for index := range fields {
		fields[index] = `\N`
	}
	fields[65] = "123"
	fields[66] = "SA11AI"
	fields[69] = "2026"
	row := strings.Join(fields, "\t") + "\n"
	header, err := expectedRelationCOPYHeader(relation)
	if err != nil {
		t.Fatal(err)
	}
	stream := "SET statement_timeout = 0;\n" + string(header) + row + "\\.\n"
	var output bytes.Buffer
	rows, sections, err := copySelectedRelation(context.Background(), strings.NewReader(stream), relation, "2026", &output)
	if err != nil {
		t.Fatalf("copySelectedRelation() error = %v", err)
	}
	if rows != 1 || sections != 1 || output.String() != row {
		t.Fatalf("copy result rows=%d sections=%d output=%q", rows, sections, output.String())
	}
	if _, sections, err := copySelectedRelation(context.Background(), strings.NewReader("SET x;\n"), relation, "2026", io.Discard); err != nil || sections != 0 {
		t.Fatalf("zero-section parser result sections=%d err=%v", sections, err)
	}
}

func TestCopySelectedRelationPreservesInvalidRowsForOccurrenceIssues(t *testing.T) {
	t.Parallel()
	relation := "disclosure.fec_fitem_sched_a_2025_2026"
	invalidRow := "too\tfew\tfields\n"
	header, err := expectedRelationCOPYHeader(relation)
	if err != nil {
		t.Fatal(err)
	}
	stream := string(header) + invalidRow + "\\.\n"
	var output bytes.Buffer
	rows, sections, err := copySelectedRelation(context.Background(), strings.NewReader(stream), relation, "2026", &output)
	if err != nil {
		t.Fatalf("copySelectedRelation() error = %v", err)
	}
	if rows != 1 || sections != 1 || output.String() != invalidRow {
		t.Fatalf("copy result rows=%d sections=%d output=%q", rows, sections, output.String())
	}
}

func TestCopySelectedRelationRejectsReorderedColumns(t *testing.T) {
	t.Parallel()
	for _, relation := range []string{
		"disclosure.fec_fitem_sched_a_2025_2026",
		"disclosure.fec_fitem_sched_e",
	} {
		relation := relation
		t.Run(relation, func(t *testing.T) {
			t.Parallel()
			header, err := expectedRelationCOPYHeader(relation)
			if err != nil {
				t.Fatal(err)
			}
			reordered := strings.Replace(string(header), "cmte_id, cmte_nm", "cmte_nm, cmte_id", 1)
			if reordered == string(header) {
				t.Fatal("test did not reorder the COPY columns")
			}
			if _, _, err := copySelectedRelation(context.Background(), strings.NewReader(reordered+"\\.\n"), relation, "", io.Discard); err == nil {
				t.Fatal("reordered COPY columns were accepted")
			}
		})
	}
}

func TestPgRestoreRelationArgumentsSeparateSchemaAndTable(t *testing.T) {
	t.Parallel()
	arguments, err := pgRestoreRelationArguments("/archive.dump", "disclosure.fec_fitem_sched_a_2023_2024")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"--data-only",
		"--schema=disclosure",
		"--table=fec_fitem_sched_a_2023_2024",
		"--file=-",
		"/archive.dump",
	}
	if strings.Join(arguments, "\n") != strings.Join(want, "\n") {
		t.Fatalf("pg_restore arguments = %q; want %q", arguments, want)
	}
	if _, err := pgRestoreRelationArguments("/archive.dump", "unqualified"); err == nil {
		t.Fatal("unqualified relation was accepted")
	}
}

func TestStageCheckpointsExactOutputsAndRetriesWithoutAcquisition(t *testing.T) {
	t.Parallel()
	fixture := newStageFixture(t)
	memberCalls := 0
	relationCalls := 0
	failRelationCall := 2
	options := fixture.stageOptions()
	options.MemberExtractor = func(_ context.Context, _, member string, destination io.Writer) error {
		memberCalls++
		_, err := io.WriteString(destination, member+"\n")
		return err
	}
	options.RelationExtractor = func(_ context.Context, _, relation, period string, destination io.Writer) (uint64, error) {
		relationCalls++
		if relationCalls == failRelationCall {
			return 0, fmt.Errorf("injected relation failure")
		}
		_, err := io.WriteString(destination, relation+"\t"+period+"\n")
		return 1, err
	}

	failed, err := Stage(context.Background(), fixture.inventory, fixture.plan, fixture.planSHA, fixture.acquisition, fixture.acquisitionSHA, nil, "stage-retry", options)
	if err == nil || failed.Status != StageFailed || len(failed.Outputs) != 21 {
		t.Fatalf("first Stage() = status=%s outputs=%d err=%v", failed.Status, len(failed.Outputs), err)
	}
	if memberCalls != 20 || relationCalls != 2 {
		t.Fatalf("first extraction calls members=%d relations=%d", memberCalls, relationCalls)
	}

	options.RelationExtractor = func(_ context.Context, _, relation, period string, destination io.Writer) (uint64, error) {
		relationCalls++
		_, err := io.WriteString(destination, relation+"\t"+period+"\n")
		return 1, err
	}
	result, err := Stage(context.Background(), fixture.inventory, fixture.plan, fixture.planSHA, fixture.acquisition, fixture.acquisitionSHA, nil, "stage-retry", options)
	if err != nil {
		t.Fatalf("retry Stage() error = %v; result=%+v", err, result)
	}
	if result.Status != StageStaged || len(result.Outputs) != 24 || memberCalls != 20 || relationCalls != 5 {
		t.Fatalf("retry result status=%s outputs=%d calls=%d/%d", result.Status, len(result.Outputs), memberCalls, relationCalls)
	}
	for _, output := range result.Outputs {
		if err := validateStagedOutputFile(context.Background(), fixture.storageRoot, output); err != nil {
			t.Fatalf("invalid staged output %s: %v", output.Selection, err)
		}
	}

	second, err := Stage(context.Background(), fixture.inventory, fixture.plan, fixture.planSHA, fixture.acquisition, fixture.acquisitionSHA, nil, "stage-retry", options)
	if err != nil || second.CompletedAt != result.CompletedAt || memberCalls != 20 || relationCalls != 5 {
		t.Fatalf("idempotent Stage() changed work: result=%+v err=%v calls=%d/%d", second, err, memberCalls, relationCalls)
	}
}

func TestStageStorageGatePrecedesExtraction(t *testing.T) {
	t.Parallel()
	fixture := newStageFixture(t)
	extractCalls := 0
	options := fixture.stageOptions()
	options.DiskUsage = func(string) (DiskSpace, error) {
		return DiskSpace{AvailableBytes: AcquisitionFreeFloorBytes + AcquisitionWorkingMarginBytes - 1}, nil
	}
	options.MemberExtractor = func(context.Context, string, string, io.Writer) error {
		extractCalls++
		return nil
	}
	result, err := Stage(context.Background(), fixture.inventory, fixture.plan, fixture.planSHA, fixture.acquisition, fixture.acquisitionSHA, nil, "storage-blocked", options)
	if err == nil || result.Status != StageBlocked || extractCalls != 0 || !hasIssueCode(result.Issues, "stage_free_floor") {
		t.Fatalf("Stage() status=%s calls=%d issues=%+v err=%v", result.Status, extractCalls, result.Issues, err)
	}
}

func TestPublishWritesImmutableManifestThenAtomicallyAdvancesCurrent(t *testing.T) {
	t.Parallel()
	fixture := newStageFixture(t)
	options := fixture.stageOptions()
	options.MemberExtractor = func(_ context.Context, _, member string, destination io.Writer) error {
		_, err := io.WriteString(destination, member+"\n")
		return err
	}
	options.RelationExtractor = func(_ context.Context, _, relation, period string, destination io.Writer) (uint64, error) {
		_, err := io.WriteString(destination, relation+period+"\n")
		return 1, err
	}
	stage, err := Stage(context.Background(), fixture.inventory, fixture.plan, fixture.planSHA, fixture.acquisition, fixture.acquisitionSHA, nil, "publish-stage", options)
	if err != nil {
		t.Fatal(err)
	}
	stageSHA := digestJSON(t, stage)
	publishedAt := testPlannedAt.Add(4 * time.Hour)
	manifest, err := Publish(context.Background(), fixture.inventory, fixture.plan, fixture.planSHA, fixture.acquisition, fixture.acquisitionSHA, stage, stageSHA, "publish-run", PublishOptions{StorageRoot: fixture.storageRoot, Clock: func() time.Time { return publishedAt }})
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if manifest.ReleaseID != fixture.plan.CandidateReleaseID || manifest.PublishedAt != publishedAt || len(manifest.StagedOutputs) != 24 {
		t.Fatalf("unexpected manifest: %+v", manifest)
	}
	currentPath := filepath.Join(fixture.storageRoot, "releases", "fec", "current.json")
	historyPath := filepath.Join(fixture.storageRoot, "releases", "fec", "manifests", fixture.plan.CandidateReleaseID+".json")
	current, err := os.ReadFile(currentPath)
	if err != nil {
		t.Fatal(err)
	}
	history, err := os.ReadFile(historyPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(current, history) {
		t.Fatal("active pointer content differs from immutable manifest")
	}

	second, err := Publish(context.Background(), fixture.inventory, fixture.plan, fixture.planSHA, fixture.acquisition, fixture.acquisitionSHA, stage, stageSHA, "another-run", PublishOptions{StorageRoot: fixture.storageRoot, Clock: func() time.Time { return publishedAt.Add(time.Hour) }})
	if err != nil || second.PublishedAt != manifest.PublishedAt || second.RunID != manifest.RunID {
		t.Fatalf("idempotent Publish() = (%+v, %v)", second, err)
	}
}

func TestPublishRejectsSelectedOutputFromAnotherSourceArtifact(t *testing.T) {
	t.Parallel()
	fixture := newStageFixture(t)
	options := fixture.stageOptions()
	options.MemberExtractor = func(_ context.Context, _, member string, destination io.Writer) error {
		_, err := io.WriteString(destination, member+"\n")
		return err
	}
	options.RelationExtractor = func(_ context.Context, _, relation, period string, destination io.Writer) (uint64, error) {
		_, err := io.WriteString(destination, relation+period+"\n")
		return 1, err
	}
	stage, err := Stage(context.Background(), fixture.inventory, fixture.plan, fixture.planSHA, fixture.acquisition, fixture.acquisitionSHA, nil, "mismatched-source", options)
	if err != nil {
		t.Fatal(err)
	}
	stage.Outputs[0].SourceArtifactSHA256 = strings.Repeat("9", 64)
	stageSHA := digestJSON(t, stage)
	if _, err := Publish(context.Background(), fixture.inventory, fixture.plan, fixture.planSHA, fixture.acquisition, fixture.acquisitionSHA, stage, stageSHA, "reject-mismatch", PublishOptions{StorageRoot: fixture.storageRoot}); err == nil || !strings.Contains(err.Error(), "does not reference its acquired source artifact") {
		t.Fatalf("Publish() error = %v; want source-artifact rejection", err)
	}
}

func TestFinalizeStagedStreamRejectsCorruptExistingCASObject(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	temporaryDirectory := filepath.Join(storageRoot, "temporary")
	producer := func(destination io.Writer) error {
		_, err := io.WriteString(destination, "immutable selected bytes\n")
		return err
	}
	firstPath, metrics, err := streamToZstd(context.Background(), temporaryDirectory, producer, nil)
	if err != nil {
		t.Fatal(err)
	}
	storageKey, err := finalizeStagedStream(context.Background(), storageRoot, firstPath, "fec:cn:2026", metrics)
	if err != nil {
		t.Fatal(err)
	}
	destination, err := resolveStorageKey(storageRoot, storageKey)
	if err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	content[len(content)/2] ^= 0xff
	if err := os.WriteFile(destination, content, 0o640); err != nil {
		t.Fatal(err)
	}
	secondPath, secondMetrics, err := streamToZstd(context.Background(), temporaryDirectory, producer, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := finalizeStagedStream(context.Background(), storageRoot, secondPath, "fec:cn:2026", secondMetrics); err == nil || !strings.Contains(err.Error(), "decompression validation") {
		t.Fatalf("finalizeStagedStream() error = %v; want corrupt-CAS rejection", err)
	}
}

func TestPublicationPathCannotEscapeStorageRoot(t *testing.T) {
	t.Parallel()
	storageRoot := t.TempDir()
	if err := requirePathWithinRoot(storageRoot, filepath.Join(storageRoot, "releases", "fec", "current.json")); err != nil {
		t.Fatalf("valid publication path rejected: %v", err)
	}
	if err := requirePathWithinRoot(storageRoot, filepath.Join(filepath.Dir(storageRoot), "outside.json")); err == nil {
		t.Fatal("publication path outside storage root was accepted")
	}
}

type stageFixture struct {
	inventory      Inventory
	plan           ReleasePlan
	planSHA        string
	acquisition    AcquisitionResult
	acquisitionSHA string
	storageRoot    string
}

func newStageFixture(t *testing.T) stageFixture {
	t.Helper()
	inventory := InitialInventory()
	discovery, bodies := tinyDiscovery(inventory)
	plan := Plan(inventory, discovery, nil, testPlannedAt)
	planSHA := digestJSON(t, plan)
	storageRoot := t.TempDir()
	acquisition, err := Acquire(
		context.Background(),
		newAcquisitionDoer(plan.SelectedSources, bodies),
		inventory,
		plan,
		nil,
		planSHA,
		"stage-acquisition",
		AcquisitionOptions{
			StorageRoot: storageRoot,
			Clock:       time.Now,
			DiskUsage: func(string) (DiskSpace, error) {
				return DiskSpace{AvailableBytes: 1 << 40}, nil
			},
			ContainerValidator: func(context.Context, SourceSpec, string, string) (string, error) {
				return "test_container", nil
			},
		},
	)
	if err != nil {
		t.Fatalf("fixture acquisition: %v", err)
	}
	return stageFixture{
		inventory:      inventory,
		plan:           plan,
		planSHA:        planSHA,
		acquisition:    acquisition,
		acquisitionSHA: digestJSON(t, acquisition),
		storageRoot:    storageRoot,
	}
}

func (fixture stageFixture) stageOptions() StageOptions {
	return StageOptions{
		StorageRoot: fixture.storageRoot,
		Clock:       time.Now,
		DiskUsage: func(string) (DiskSpace, error) {
			return DiskSpace{AvailableBytes: 1 << 40}, nil
		},
	}
}

func digestJSON(t *testing.T, value any) string {
	t.Helper()
	content, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	content = append(content, '\n')
	return fmt.Sprintf("%x", sha256.Sum256(content))
}
