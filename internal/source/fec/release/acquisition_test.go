package release

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestFinalizeArtifactRejectsCorruptExistingCASObject(t *testing.T) {
	t.Parallel()
	content := []byte("verified source bytes")
	digest := fmt.Sprintf("%x", sha256.Sum256(content))
	directory := t.TempDir()
	stagingPath := filepath.Join(directory, "source.partial")
	destination := filepath.Join(directory, digest)
	if err := os.WriteFile(stagingPath, content, 0o640); err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), content...)
	corrupt[0] ^= 0xff
	if err := os.WriteFile(destination, corrupt, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := finalizeArtifact(context.Background(), stagingPath, destination, int64(len(content)), digest); err == nil || !strings.Contains(err.Error(), "invalid SHA-256") {
		t.Fatalf("finalizeArtifact() error = %v; want corrupt-CAS rejection", err)
	}
}

func TestAcquireCapturesRechecksAndPersistsExactPlan(t *testing.T) {
	t.Parallel()
	inventory := InitialInventory()
	discovery, bodies := tinyDiscovery(inventory)
	plan := Plan(inventory, discovery, nil, testPlannedAt)
	doer := newAcquisitionDoer(plan.SelectedSources, bodies)
	storageRoot := t.TempDir()
	clock := &incrementingClock{next: testPlannedAt.Add(time.Hour)}

	result, err := Acquire(
		context.Background(),
		doer,
		inventory,
		plan,
		nil,
		strings.Repeat("b", 64),
		"dagster-run-1",
		AcquisitionOptions{
			StorageRoot: storageRoot,
			Clock:       clock.Now,
			DiskUsage: func(string) (DiskSpace, error) {
				return DiskSpace{AvailableBytes: 1 << 40}, nil
			},
			ContainerValidator: func(_ context.Context, _ SourceSpec, _ string, _ string) (string, error) {
				return "test_container", nil
			},
		},
	)
	if err != nil {
		t.Fatalf("Acquire() error = %v; result=%+v", err, result)
	}
	if issues := ValidateAcquisitionResult(inventory, result); len(issues) != 0 {
		t.Fatalf("invalid acquisition result: %+v", issues)
	}
	if result.Status != AcquisitionAcquired || len(result.Artifacts) != 21 || len(result.PostCapture.Versions) != 21 {
		t.Fatalf("unexpected acquisition result: %+v", result)
	}
	if doer.getCount != 21 || doer.headCount != 21 {
		t.Fatalf("requests: GET=%d HEAD=%d; want 21 each", doer.getCount, doer.headCount)
	}
	for _, artifact := range result.Artifacts {
		path, pathErr := resolveStorageKey(storageRoot, artifact.StorageKey)
		if pathErr != nil {
			t.Fatalf("resolve artifact path: %v", pathErr)
		}
		if info, statErr := os.Stat(path); statErr != nil || info.Size() != artifact.ByteCount {
			t.Fatalf("artifact %s was not finalized: info=%v err=%v", artifact.SourceID, info, statErr)
		}
	}
	requestCount := doer.getCount + doer.headCount
	second, err := Acquire(
		context.Background(),
		doer,
		inventory,
		plan,
		nil,
		strings.Repeat("b", 64),
		"dagster-run-1",
		AcquisitionOptions{StorageRoot: storageRoot, Clock: clock.Now},
	)
	if err != nil || second.Status != AcquisitionAcquired {
		t.Fatalf("idempotent Acquire() = (%+v, %v)", second, err)
	}
	if doer.getCount+doer.headCount != requestCount {
		t.Fatal("completed acquisition performed network requests on retry")
	}
	missingPath, err := resolveStorageKey(storageRoot, second.Artifacts[0].StorageKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(missingPath); err != nil {
		t.Fatal(err)
	}
	if _, err := Acquire(
		context.Background(),
		doer,
		inventory,
		plan,
		nil,
		strings.Repeat("b", 64),
		"dagster-run-1",
		AcquisitionOptions{StorageRoot: storageRoot, Clock: clock.Now},
	); err == nil {
		t.Fatal("completed acquisition state ignored a missing immutable artifact")
	}
	if doer.getCount+doer.headCount != requestCount {
		t.Fatal("invalid completed state performed publisher requests")
	}
}

func TestAcquireStorageGatePrecedesBodyRequests(t *testing.T) {
	t.Parallel()
	inventory := InitialInventory()
	discovery, bodies := tinyDiscovery(inventory)
	plan := Plan(inventory, discovery, nil, testPlannedAt)
	doer := newAcquisitionDoer(plan.SelectedSources, bodies)
	storageRoot := t.TempDir()
	remaining := uint64(0)
	for _, selected := range plan.SelectedSources {
		remaining += uint64(*selected.ContentLength)
	}

	result, err := Acquire(
		context.Background(),
		doer,
		inventory,
		plan,
		nil,
		strings.Repeat("c", 64),
		"storage-blocked",
		AcquisitionOptions{
			StorageRoot: storageRoot,
			Clock:       time.Now,
			DiskUsage: func(string) (DiskSpace, error) {
				return DiskSpace{AvailableBytes: remaining + AcquisitionWorkingMarginBytes + AcquisitionFreeFloorBytes - 1}, nil
			},
		},
	)
	if err == nil || result.Status != AcquisitionBlocked || result.Storage.Passed {
		t.Fatalf("Acquire() = (%+v, %v); want storage block", result, err)
	}
	if !hasIssueCode(result.Issues, "storage_free_floor") {
		t.Fatalf("issues = %+v; want storage_free_floor", result.Issues)
	}
	if doer.getCount != 0 || doer.headCount != 0 {
		t.Fatalf("storage-blocked acquisition made requests: GET=%d HEAD=%d", doer.getCount, doer.headCount)
	}
}

func TestAcquireReusesUnchangedPublishedArtifacts(t *testing.T) {
	t.Parallel()
	inventory := InitialInventory()
	priorDiscovery, bodies := tinyDiscovery(inventory)
	current := publishedManifest(inventory, priorDiscovery)
	storageRoot := t.TempDir()
	for index := range current.Artifacts {
		digest := fmt.Sprintf("%064x", index+1)
		current.Artifacts[index].SHA256 = digest
		current.Artifacts[index].StorageKey = contentStorageKey(current.Artifacts[index].SourceID, digest)
		path, err := resolveStorageKey(storageRoot, current.Artifacts[index].StorageKey)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, bodies[current.Artifacts[index].SourceID], 0o640); err != nil {
			t.Fatal(err)
		}
	}
	changedDiscovery, _ := tinyDiscovery(inventory)
	changed := &changedDiscovery.Observations[0]
	changed.ETag = `"changed-version"`
	changed.VersionBasis, changed.VersionIdentity = deriveVersionIdentity("", changed.ETag, "", changed.LastModified, changed.ContentLength)
	plan := Plan(inventory, changedDiscovery, &current, testPlannedAt)
	doer := newAcquisitionDoer(plan.SelectedSources, bodies)
	containerCalls := 0

	result, err := Acquire(
		context.Background(),
		doer,
		inventory,
		plan,
		&current,
		strings.Repeat("e", 64),
		"reuse-prior",
		AcquisitionOptions{
			StorageRoot: storageRoot,
			Clock:       time.Now,
			DiskUsage: func(string) (DiskSpace, error) {
				return DiskSpace{AvailableBytes: 1 << 40}, nil
			},
			ContainerValidator: func(_ context.Context, _ SourceSpec, _ string, _ string) (string, error) {
				containerCalls++
				return "fixture", nil
			},
		},
	)
	if err != nil {
		t.Fatalf("Acquire() error = %v; result=%+v", err, result)
	}
	if doer.getCount != 1 || doer.headCount != 21 || containerCalls != 1 {
		t.Fatalf("requests GET=%d HEAD=%d containers=%d; want 1, 21, 1", doer.getCount, doer.headCount, containerCalls)
	}
	reused := 0
	for _, artifact := range result.Artifacts {
		if artifact.Disposition == "reused" {
			reused++
		}
	}
	if reused != 20 {
		t.Fatalf("reused artifact count = %d; want 20", reused)
	}
}

func TestCaptureSourceResumesExactPartial(t *testing.T) {
	t.Parallel()
	body := []byte("0123456789abcdefghij")
	length := int64(len(body))
	selected := SelectedSource{
		SourceID:        "fec:cn:2026",
		RequestURL:      "https://www.fec.gov/test.zip",
		FinalURL:        "https://www.fec.gov/test.zip",
		VersionIdentity: `etag:"v1"`,
		VersionBasis:    "etag",
		ETag:            `"v1"`,
		ContentLength:   &length,
	}
	path := filepath.Join(t.TempDir(), "candidate.partial")
	if err := os.WriteFile(path, body[:7], 0o640); err != nil {
		t.Fatal(err)
	}
	doer := &resumeDoer{body: body, expectedOffset: 7, etag: selected.ETag}
	result, err := captureSource(context.Background(), doer, selected, path, time.Now, nil)
	if err != nil {
		t.Fatalf("captureSource() error = %v", err)
	}
	if result.byteCount != length || !doer.sawRange || !doer.sawIfMatch {
		t.Fatalf("resume result=%+v doer=%+v", result, doer)
	}
	content, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(content, body) {
		t.Fatalf("resumed content = %q, %v", content, err)
	}
}

func TestAcquireRejectsPublisherChangeBeforeContainerValidation(t *testing.T) {
	t.Parallel()
	inventory := InitialInventory()
	discovery, bodies := tinyDiscovery(inventory)
	plan := Plan(inventory, discovery, nil, testPlannedAt)
	doer := newAcquisitionDoer(plan.SelectedSources, bodies)
	doer.changedHeadSource = inventory.Sources[0].SourceID
	containerCalls := 0

	result, err := Acquire(
		context.Background(),
		doer,
		inventory,
		plan,
		nil,
		strings.Repeat("d", 64),
		"publisher-race",
		AcquisitionOptions{
			StorageRoot: t.TempDir(),
			Clock:       time.Now,
			DiskUsage: func(string) (DiskSpace, error) {
				return DiskSpace{AvailableBytes: 1 << 40}, nil
			},
			ContainerValidator: func(_ context.Context, _ SourceSpec, _ string, _ string) (string, error) {
				containerCalls++
				return "test", nil
			},
		},
	)
	if err == nil || result.Status != AcquisitionFailed || !hasIssueCode(result.Issues, "publisher_changed_during_capture") {
		t.Fatalf("Acquire() = (%+v, %v); want publisher-change failure", result, err)
	}
	if containerCalls != 0 {
		t.Fatalf("container validator called %d times before metadata stability passed", containerCalls)
	}
}

func tinyDiscovery(inventory Inventory) (Discovery, map[string][]byte) {
	observations := make([]Observation, 0, len(inventory.Sources))
	bodies := make(map[string][]byte, len(inventory.Sources))
	for index, source := range inventory.Sources {
		body := []byte(fmt.Sprintf("tiny-body-%02d-%s", index, source.SourceID))
		bodies[source.SourceID] = body
		length := int64(len(body))
		etag := fmt.Sprintf(`"tiny-%02d"`, index)
		basis, identity := deriveVersionIdentity("", etag, "", "Mon, 31 Aug 2026 07:00:00 GMT", &length)
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
			ContentLength:   &length,
			AcceptRanges:    "bytes",
		})
	}
	return Discovery{
		SchemaVersion:    DiscoverySchemaVersion,
		InventoryVersion: inventory.InventoryVersion,
		StartedAt:        testObservedAt,
		CompletedAt:      testObservedAt,
		Observations:     observations,
	}, bodies
}

type acquisitionDoer struct {
	mu                sync.Mutex
	selected          map[string]SelectedSource
	bodies            map[string][]byte
	getCount          int
	headCount         int
	changedHeadSource string
}

func newAcquisitionDoer(selected []SelectedSource, bodies map[string][]byte) *acquisitionDoer {
	byURL := make(map[string]SelectedSource, len(selected))
	for _, source := range selected {
		byURL[source.RequestURL] = source
	}
	return &acquisitionDoer{selected: byURL, bodies: bodies}
}

func (doer *acquisitionDoer) Do(request *http.Request) (*http.Response, error) {
	doer.mu.Lock()
	defer doer.mu.Unlock()
	selected, exists := doer.selected[request.URL.String()]
	if !exists {
		return nil, fmt.Errorf("unknown request URL %s", request.URL)
	}
	switch request.Method {
	case http.MethodGet:
		doer.getCount++
		body := doer.bodies[selected.SourceID]
		header := make(http.Header)
		header.Set("ETag", selected.ETag)
		return &http.Response{
			StatusCode:    http.StatusOK,
			Header:        header,
			Body:          io.NopCloser(bytes.NewReader(body)),
			ContentLength: int64(len(body)),
			Request:       request,
		}, nil
	case http.MethodHead:
		doer.headCount++
		etag := selected.ETag
		if selected.SourceID == doer.changedHeadSource {
			etag = `"changed-during-capture"`
		}
		header := make(http.Header)
		header.Set("ETag", etag)
		header.Set("Last-Modified", selected.LastModified)
		header.Set("Accept-Ranges", "bytes")
		return &http.Response{
			StatusCode:    http.StatusOK,
			Header:        header,
			Body:          io.NopCloser(bytes.NewReader(nil)),
			ContentLength: *selected.ContentLength,
			Request:       request,
		}, nil
	default:
		return nil, fmt.Errorf("unexpected method %s", request.Method)
	}
}

type resumeDoer struct {
	body           []byte
	expectedOffset int
	etag           string
	sawRange       bool
	sawIfMatch     bool
}

func (doer *resumeDoer) Do(request *http.Request) (*http.Response, error) {
	doer.sawRange = request.Header.Get("Range") == fmt.Sprintf("bytes=%d-", doer.expectedOffset)
	doer.sawIfMatch = request.Header.Get("If-Match") == doer.etag
	remaining := doer.body[doer.expectedOffset:]
	header := make(http.Header)
	header.Set("ETag", doer.etag)
	header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", doer.expectedOffset, len(doer.body)-1, len(doer.body)))
	return &http.Response{
		StatusCode:    http.StatusPartialContent,
		Header:        header,
		Body:          io.NopCloser(bytes.NewReader(remaining)),
		ContentLength: int64(len(remaining)),
		Request:       request,
	}, nil
}
