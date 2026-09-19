package release

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestStageStreamingCapPreservesCheckpointAndRetries(t *testing.T) {
	t.Parallel()
	fixture := newStageFixture(t)
	payload := make([]byte, 16<<10)
	_, _ = rand.New(rand.NewSource(7)).Read(payload)
	producer := func(w io.Writer) error {
		_, err := w.Write(append(bytes.Clone(payload), []byte("2020")...))
		return err
	}
	_, metrics, err := streamToZstd(context.Background(), t.TempDir(), producer, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Simulated competing retained bytes, removed only inside this test root.
	held := filepath.Join(scheduleAStorageRoot(fixture.storageRoot), "external-test-bytes")
	writeStorageTestFile(t, held, int64(3*metrics.CompressedBytes))
	hot, err := directoryBytes(scheduleAStorageRoot(fixture.storageRoot))
	if err != nil {
		t.Fatal(err)
	}
	pointer := filepath.Join(fixture.storageRoot, "active-pointer-sentinel")
	if err := os.WriteFile(pointer, []byte("unchanged-release"), 0o640); err != nil {
		t.Fatal(err)
	}
	options := fixture.stageOptions()
	options.WorkingMarginBytes = 10
	options.ScheduleAHotCapBytes = hot + 10 + 2*metrics.CompressedBytes - 1
	memberCalls, relationCalls := 0, 0
	options.MemberExtractor = func(_ context.Context, _, _ string, destination io.Writer) error {
		memberCalls++
		_, err := io.WriteString(destination, "member\n")
		return err
	}
	options.RelationExtractor = func(_ context.Context, _, _, period string, destination io.Writer) (uint64, error) {
		relationCalls++
		_, err := destination.Write(append(bytes.Clone(payload), []byte(period)...))
		return 1, err
	}
	run := func() (StageResult, error) {
		return Stage(context.Background(), fixture.inventory, fixture.plan, fixture.planSHA, fixture.acquisition, fixture.acquisitionSHA, nil, "stream-cap", options)
	}
	blocked, err := run()
	if err == nil || blocked.Status != StageBlocked || blocked.Storage.Passed || !hasStorageIssue(blocked.Issues, "stage_write_budget") || len(blocked.Outputs) != 21 {
		t.Fatalf("first stage: status=%s outputs=%d issues=%v err=%v", blocked.Status, len(blocked.Outputs), blocked.Issues, err)
	}
	if memberCalls != 20 || relationCalls != 2 {
		t.Fatalf("unexpected calls %d/%d", memberCalls, relationCalls)
	}
	for _, output := range blocked.Outputs {
		if err := validateStagedOutputFile(context.Background(), fixture.storageRoot, output); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(stageTemporaryDirectory(fixture.storageRoot, fixture.plan.CandidateReleaseID, "stream-cap"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("failed temporary not cleaned: %v %v", entries, err)
	}
	if _, err := os.Stat(stageStatePath(fixture.storageRoot, fixture.plan.CandidateReleaseID, "stream-cap")); !os.IsNotExist(err) {
		t.Fatal("blocked stage marked complete")
	}
	if err := os.Remove(held); err != nil {
		t.Fatal(err)
	}
	result, err := run() // same cap, same inputs; completed extracts are not redone
	if err != nil || result.Status != StageStaged || memberCalls != 20 || relationCalls != 5 {
		t.Fatalf("retry: status=%s calls=%d/%d err=%v", result.Status, memberCalls, relationCalls, err)
	}
	if !reflect.DeepEqual(blocked.Outputs, result.Outputs[:len(blocked.Outputs)]) {
		t.Fatal("checkpoint outputs changed")
	}
	again, err := run()
	if err != nil || !reflect.DeepEqual(result, again) || memberCalls != 20 || relationCalls != 5 {
		t.Fatal("completed replay changed work")
	}
	content, err := os.ReadFile(pointer)
	if err != nil || string(content) != "unchanged-release" {
		t.Fatal("active pointer changed")
	}
	if _, err := os.Stat(filepath.Join(fixture.storageRoot, "releases", "fec", "current.json")); !os.IsNotExist(err) {
		t.Fatal("stage published a release")
	}
}

func TestAcquireAndStageShareStorageLockBeforeWork(t *testing.T) {
	t.Parallel()
	fixture := newStageFixture(t)
	lock, err := lockSourceStorage(fixture.storageRoot)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	doer := newAcquisitionDoer(fixture.plan.SelectedSources, nil)
	acquired, err := Acquire(context.Background(), doer, fixture.inventory, fixture.plan, nil, fixture.planSHA, "locked", AcquisitionOptions{StorageRoot: fixture.storageRoot})
	if err == nil || acquired.Status != AcquisitionBlocked || !hasStorageIssue(acquired.Issues, "storage_busy") || doer.getCount != 0 || doer.headCount != 0 {
		t.Fatalf("acquisition lock: %+v err=%v", acquired.Issues, err)
	}
	staged, err := Stage(context.Background(), fixture.inventory, fixture.plan, fixture.planSHA, fixture.acquisition, fixture.acquisitionSHA, nil, "locked", fixture.stageOptions())
	if err == nil || staged.Status != StageBlocked || !hasStorageIssue(staged.Issues, "storage_busy") {
		t.Fatalf("stage lock: %+v err=%v", staged.Issues, err)
	}
}

func TestAcquireChecksPriorSizeScenarioBeforeGET(t *testing.T) {
	t.Parallel()
	inventory := InitialInventory()
	current := publishedManifest(inventory, availableDiscovery(inventory, "old"))
	for i := range current.StagedOutputs {
		if current.StagedOutputs[i].SourceID == ScheduleASourceID {
			current.StagedOutputs[i].CompressedByteCount = ScheduleAHotCapBytes
		}
	}
	discovery, bodies := tinyDiscovery(inventory)
	plan := Plan(inventory, discovery, &current, testPlannedAt)
	doer := newAcquisitionDoer(plan.SelectedSources, bodies)
	options := AcquisitionOptions{StorageRoot: t.TempDir(), DiskUsage: func(string) (DiskSpace, error) { return DiskSpace{AvailableBytes: 8 << 40}, nil }}
	result, err := Acquire(context.Background(), doer, inventory, plan, &current, digestJSON(t, plan), "too-large", options)
	if err == nil || result.Status != AcquisitionBlocked || !hasStorageIssue(result.Issues, "staging_projection") || doer.getCount != 0 {
		t.Fatalf("scenario gate: %+v %v GETs=%d", result.Issues, err, doer.getCount)
	}
	// Even an all-changed plan must bind the prior sizes to the right manifest.
	current.ReleaseID = "fec-wrong-prior"
	result, err = Acquire(context.Background(), doer, inventory, plan, &current, digestJSON(t, plan), "wrong-prior", options)
	if err == nil || result.Status != AcquisitionFailed || doer.getCount != 0 {
		t.Fatal("unvalidated prior scenario accepted")
	}
}

func TestAcquireRuntimeFreeSpaceStopIsBlocked(t *testing.T) {
	t.Parallel()
	inventory := InitialInventory()
	discovery, bodies := tinyDiscovery(inventory)
	plan := Plan(inventory, discovery, nil, testPlannedAt)
	doer := newAcquisitionDoer(plan.SelectedSources, bodies)
	calls := 0
	options := AcquisitionOptions{StorageRoot: t.TempDir(), MaxConcurrent: 1, DiskUsage: func(string) (DiskSpace, error) {
		calls++
		if calls >= 3 {
			return DiskSpace{AvailableBytes: AcquisitionFreeFloorBytes + AcquisitionWorkingMarginBytes - 1}, nil
		}
		return DiskSpace{AvailableBytes: 1 << 40}, nil
	}}
	result, err := Acquire(context.Background(), doer, inventory, plan, nil, digestJSON(t, plan), "runtime-stop", options)
	if err == nil || result.Status != AcquisitionBlocked || !hasStorageIssue(result.Issues, "acquisition_write_budget") || doer.getCount == 0 || doer.headCount != 0 {
		t.Fatalf("runtime gate: %+v %v", result.Issues, err)
	}
	if _, err := os.Stat(acquisitionStatePath(options.StorageRoot, plan.CandidateReleaseID, "runtime-stop")); !os.IsNotExist(err) {
		t.Fatal("blocked acquisition marked complete")
	}
}

func TestCaptureStreamingBudgetPreservesAndResumesPrefix(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	body := bytes.Repeat([]byte("download"), 1<<15)
	length := int64(len(body))
	selected := SelectedSource{SourceID: ScheduleASourceID, RequestURL: "https://www.fec.gov/test", ContentLength: &length, ETag: `"v1"`}
	path := stagingPath(root, "candidate", selected.SourceID)
	budget := storageTestBudget(t, context.Background(), root, 1<<20, 65536)
	doer := newAcquisitionDoer([]SelectedSource{selected}, map[string][]byte{selected.SourceID: body})
	_, err := captureSource(context.Background(), doer, selected, path, time.Now, budget)
	if !errors.Is(err, errStorageBudget) {
		t.Fatalf("budget not enforced: %v", err)
	}
	prefix, err := os.ReadFile(path)
	if err != nil || len(prefix) != 65536 || !bytes.Equal(prefix, body[:len(prefix)]) {
		t.Fatalf("lost partial: len=%d err=%v", len(prefix), err)
	}
	budget = storageTestBudget(t, context.Background(), root, 1<<20, 1<<20)
	resume := &resumeDoer{body: body, expectedOffset: len(prefix), etag: selected.ETag}
	result, err := captureSource(context.Background(), resume, selected, path, time.Now, budget)
	if err != nil || result.byteCount != length || !resume.sawRange {
		t.Fatalf("resume: %+v %v", result, err)
	}
	content, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(content, body) {
		t.Fatal("resumed bytes differ")
	}
}

func TestCaptureRejectsOversizeWithoutAcceptingPrefixOnRetry(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	length := int64(5)
	selected := SelectedSource{SourceID: ScheduleASourceID, RequestURL: "https://www.fec.gov/test", ContentLength: &length}
	path := stagingPath(root, "oversize", selected.SourceID)
	doer := newAcquisitionDoer([]SelectedSource{selected}, map[string][]byte{selected.SourceID: []byte("123456")})
	for range 2 {
		budget := storageTestBudget(t, context.Background(), root, 5, 5)
		_, err := captureSource(context.Background(), doer, selected, path, time.Now, budget)
		if err == nil || !strings.Contains(err.Error(), "exceeds selected content length") {
			t.Fatalf("oversize: %v", err)
		}
		info, err := os.Stat(path)
		if err != nil || info.Size() != 0 {
			t.Fatal("rejected prefix left complete")
		}
	}
	if doer.getCount != 2 {
		t.Fatal("retry silently accepted rejected response")
	}
}

func TestStreamingScenarioTracksTemporaryPeakInStageOrder(t *testing.T) {
	t.Parallel()
	a, other := uint64(10), uint64(50)
	preflight := StoragePreflight{ProjectedScheduleAHotBytes: 100, ScheduleAHotCapBytes: 160, FreeBytesBefore: 1000, WorkingMarginBytes: 10}
	outputs := []StorageOutputEstimate{
		{SourceID: ScheduleASourceID, NewCompressedBytes: &a},
		{SourceID: ScheduleESourceID, NewCompressedBytes: &other},
		{SourceID: ScheduleASourceID, NewCompressedBytes: &a},
	}
	scenario, err := stagingStorageScenario(preflight, true, outputs)
	if err != nil || !scenario.FitsBudget || scenario.HotBytesWithReserve != 160 || scenario.NewScheduleAOutputBytes != 20 {
		t.Fatalf("peak: %+v err=%v", scenario, err)
	}
	preflight.ScheduleAHotCapBytes--
	scenario, err = stagingStorageScenario(preflight, true, outputs)
	if err != nil || scenario.FitsBudget || scenario.HotCapExcessBytes != 1 {
		t.Fatalf("peak over: %+v %v", scenario, err)
	}
}

func TestStreamBudgetPreservesCompressedBytes(t *testing.T) {
	t.Parallel()
	payload := make([]byte, 16<<20)
	_, _ = rand.New(rand.NewSource(19)).Read(payload)
	produce := func(w io.Writer) error { _, err := w.Write(payload); return err }
	_, plain, err := streamToZstd(context.Background(), t.TempDir(), produce, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	budget := storageTestBudget(t, context.Background(), root, 32<<20, 32<<20)
	path, guarded, err := streamToZstd(context.Background(), stageTemporaryDirectory(root, "candidate", "run"), produce, budget)
	if err != nil || guarded != plain {
		t.Fatalf("stream changed: plain=%+v guarded=%+v err=%v", plain, guarded, err)
	}
	hot, err := directoryBytes(scheduleAStorageRoot(root))
	if err != nil || hot != guarded.CompressedBytes {
		t.Fatalf("actual temporary accounting: %d %v", hot, err)
	}
	key, err := finalizeStagedStream(context.Background(), root, path, ScheduleASourceID, guarded)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	final, err := directoryBytes(scheduleAStorageRoot(root))
	if err != nil || final != hot || key == "" {
		t.Fatalf("finalization duplicated bytes: %d -> %d %v", hot, final, err)
	}
}

func BenchmarkStreamingStorageGuard(b *testing.B) {
	payload := make([]byte, 32<<20)
	_, _ = rand.New(rand.NewSource(31)).Read(payload)
	for _, guarded := range []bool{false, true} {
		name := "plain"
		if guarded {
			name = "guarded"
		}
		b.Run(name, func(b *testing.B) {
			root := b.TempDir()
			if err := os.MkdirAll(scheduleAStorageRoot(root), 0o750); err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			for range b.N {
				var budget *writeBudget
				var err error
				if guarded {
					budget, err = newWriteBudget(context.Background(), root, StageStorage{ScheduleAHotCapBytes: 1 << 40, FreeFloorBytes: 1 << 20, WorkingMarginBytes: 1 << 20}, nil)
					if err != nil {
						b.Fatal(err)
					}
				}
				path, _, err := streamToZstd(context.Background(), stageTemporaryDirectory(root, "bench", "run"), func(w io.Writer) error { _, err := w.Write(payload); return err }, budget)
				if err != nil {
					b.Fatal(err)
				}
				if err := os.Remove(path); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
