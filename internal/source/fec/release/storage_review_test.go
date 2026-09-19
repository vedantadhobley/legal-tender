package release

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestStagingScenarioIncludesAllRetainedOutputs(t *testing.T) {
	t.Parallel()
	// Regression from the September 9 read-only v4 audit. Acquisition fits,
	// but same-sized, different-content new extracts do not fit the full reserve.
	preflight := StoragePreflight{
		ProjectedScheduleAHotBytes: 620_395_768_336,
		RemainingDownloadBytes:     129_566_279_592,
		ScheduleAHotCapBytes:       ScheduleAHotCapBytes,
		FreeBytesBefore:            1_404_862_169_088,
		FreeFloorBytes:             AcquisitionFreeFloorBytes,
		WorkingMarginBytes:         AcquisitionWorkingMarginBytes,
		LargestExtractWorkingBytes: LargestScheduleAExtractBytes,
	}
	outputs := []StorageOutputEstimate{}
	for _, bytes := range []uint64{22_484_687_018, 10_222_594_425, 14_765_199_882, 10_495_817_050} {
		outputs = append(outputs, StorageOutputEstimate{SourceID: ScheduleASourceID, NewCompressedBytes: &bytes})
	}
	otherBytes := uint64(38_582_069)
	outputs = append(outputs, StorageOutputEstimate{SourceID: ScheduleESourceID, NewCompressedBytes: &otherBytes})
	got, err := stagingStorageScenario(preflight, true, outputs)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Complete || got.FitsBudget || got.NewScheduleAOutputBytes != 57_968_298_375 || got.NewOutputBytes != 58_006_880_444 || got.HotBytesWithReserve != 678_364_066_711 || got.HotCapExcessBytes != 34_118_972_311 || got.FreeSpaceShortfallBytes != 0 {
		t.Fatalf("scenario=%+v", got)
	}
}

func TestStagingScenarioBoundariesUnknownAndOverflow(t *testing.T) {
	t.Parallel()
	base := StoragePreflight{ProjectedScheduleAHotBytes: 10, ScheduleAHotCapBytes: 50, RemainingDownloadBytes: 5, FreeBytesBefore: 75, FreeFloorBytes: 20, LargestExtractWorkingBytes: 30, WorkingMarginBytes: 10}
	for _, tc := range []struct {
		name    string
		alter   func(*StoragePreflight)
		unknown bool
		fits    bool
	}{
		{"exact boundary without A change", func(*StoragePreflight) {}, false, true},
		{"hot one byte over", func(p *StoragePreflight) { p.ScheduleAHotCapBytes-- }, false, false},
		{"free one byte short", func(p *StoragePreflight) { p.FreeBytesBefore = 64 }, false, false},
		{"unknown is not zero", func(*StoragePreflight) {}, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := base
			tc.alter(&p)
			outputs := []StorageOutputEstimate{}
			if tc.unknown {
				outputs = append(outputs, StorageOutputEstimate{Basis: "unknown"})
			}
			got, err := stagingStorageScenario(p, false, outputs)
			if err != nil || got.FitsBudget != tc.fits || got.Complete == tc.unknown {
				t.Fatalf("scenario=%+v err=%v", got, err)
			}
		})
	}
	for _, values := range [][]uint64{{^uint64(0), 1}, {1, ^uint64(0)}} {
		if _, err := sumStorageBytes(values...); err == nil {
			t.Fatal("overflow accepted")
		}
	}
	for _, mutate := range []func(*StoragePreflight){
		func(p *StoragePreflight) { p.LargestExtractWorkingBytes = ^uint64(0) },
		func(p *StoragePreflight) { p.ProjectedScheduleAHotBytes = ^uint64(0) },
		func(p *StoragePreflight) { p.RemainingDownloadBytes = ^uint64(0) },
	} {
		p := base
		mutate(&p)
		if _, err := stagingStorageScenario(p, false, nil); err == nil {
			t.Fatal("scenario overflow accepted")
		}
	}
}

func TestStageEstimatesFollowInventoryAndDoNotInventReuse(t *testing.T) {
	t.Parallel()
	priorInventory := ActiveInventory()
	current := publishedManifest(priorInventory, availableDiscovery(priorInventory, "old"))
	inventory := CommitteeSummaryInventory()
	discovery := availableDiscovery(inventory, "new")
	// Only one unchanged master; all other sources are new publisher versions.
	discovery.Observations[0] = availableDiscovery(inventory, "old").Observations[0]
	plan := Plan(inventory, discovery, &current, testPlannedAt)
	if plan.Status != PlanUpdateAvailable {
		t.Fatalf("plan=%+v", plan)
	}
	outputs := estimateStageOutputs(inventory, plan, &current)
	if len(outputs) != 25 {
		t.Fatalf("outputs=%d", len(outputs))
	}
	var reused, assumed, a, e int
	for _, output := range outputs {
		if output.NewCompressedBytes == nil {
			t.Fatalf("missing estimate: %+v", output)
		}
		switch output.Basis {
		case "unchanged_source":
			reused++
			if *output.NewCompressedBytes != 0 {
				t.Fatal("reused output adds bytes")
			}
		case "prior_size_new_content":
			assumed++
			if *output.NewCompressedBytes != 1 {
				t.Fatal("changed source got content reuse credit")
			}
		default:
			t.Fatalf("unexpected basis %s", output.Basis)
		}
		if output.SourceID == ScheduleASourceID {
			a++
		}
		if output.SourceID == ScheduleESourceID {
			e++
		}
		if output.SourceID == ScheduleBSourceID {
			t.Fatal("archive-direct B got a staged output")
		}
	}
	if reused != 1 || assumed != 24 || a != 4 || e != 1 {
		t.Fatalf("reuse=%d assumed=%d A=%d E=%d", reused, assumed, a, e)
	}
	for _, output := range estimateStageOutputs(inventory, plan, nil) {
		if output.Basis != "unknown" || output.NewCompressedBytes != nil {
			t.Fatal("seed size invented")
		}
	}
}

func TestReviewStorageReadOnlyAndValidated(t *testing.T) {
	t.Parallel()
	inventory := ActiveInventory()
	current := publishedManifest(inventory, availableDiscovery(inventory, "old"))
	plan := Plan(inventory, availableDiscovery(inventory, "new"), &current, testPlannedAt)
	root := t.TempDir()
	if err := os.MkdirAll(scheduleAStorageRoot(root), 0o750); err != nil {
		t.Fatal(err)
	}
	writeStorageTestFile(t, filepath.Join(scheduleAStorageRoot(root), "retained"), 100)
	disk := func(string) (DiskSpace, error) { return DiskSpace{AvailableBytes: 2 << 40}, nil }
	before := storagePaths(t, root)
	result, err := ReviewStorage(inventory, plan, &current, root, disk)
	if err != nil || !result.Acquisition.Passed || !result.Scenario.FitsBudget {
		t.Fatalf("review=%+v err=%v", result, err)
	}
	if !reflect.DeepEqual(before, storagePaths(t, root)) {
		t.Fatal("review mutated storage")
	}
	if _, err := ReviewStorage(inventory, plan, nil, root, disk); err == nil {
		t.Fatal("missing prior accepted")
	}
	current.ReleaseID = "fec-another-prior"
	if _, err := ReviewStorage(inventory, plan, &current, root, disk); err == nil {
		t.Fatal("wrong prior accepted")
	}
	if _, err := ReviewStorage(inventory, plan, nil, filepath.Join(root, "missing"), disk); err == nil {
		t.Fatal("missing storage accepted")
	}
}

func TestPreflightPartialCreditAndOversizedPartial(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	plan := ReleasePlan{CandidateReleaseID: "fixture", ChangedSourceIDs: []string{ScheduleASourceID}}
	path := stagingPath(root, plan.CandidateReleaseID, ScheduleASourceID)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	length := int64(100)
	selected := map[string]SelectedSource{ScheduleASourceID: {ContentLength: &length}}
	disk := func(string) (DiskSpace, error) { return DiskSpace{AvailableBytes: 2 << 40}, nil }
	for _, tc := range []struct{ partial, remaining int64 }{{30, 70}, {100, 0}, {101, 100}} {
		writeStorageTestFile(t, path, tc.partial)
		result, issues, err := preflightStorage(root, scheduleAStorageRoot(root), plan, selected, disk)
		if err != nil || len(issues) != 0 || result.RemainingDownloadBytes != uint64(tc.remaining) || result.ScheduleAHotBytesBefore != uint64(tc.partial) {
			t.Fatalf("preflight=%+v issues=%v err=%v", result, issues, err)
		}
	}
}

func storagePaths(t *testing.T, root string) []string {
	t.Helper()
	paths := []string{}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		paths = append(paths, path)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return paths
}
