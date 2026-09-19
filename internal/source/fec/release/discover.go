package release

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

const defaultDiscoveryConcurrency = 4

// HTTPDoer is the narrow transport boundary used by metadata discovery.
type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

// DiscoverOptions controls bounded publisher concurrency.
type DiscoverOptions struct {
	MaxConcurrent int
}

// Discover sends only HEAD requests and returns one observation for every
// inventory member, including publisher and transport failures.
func Discover(
	ctx context.Context,
	client HTTPDoer,
	inventory Inventory,
	clock func() time.Time,
	options DiscoverOptions,
) (Discovery, error) {
	if client == nil {
		return Discovery{}, fmt.Errorf("HTTP client is required")
	}
	if issues := ValidateInventory(inventory); len(issues) != 0 {
		return Discovery{}, fmt.Errorf("invalid inventory: %s", issues[0].Message)
	}
	if clock == nil {
		return Discovery{}, fmt.Errorf("clock is required")
	}
	startedAt := clock().UTC()
	if startedAt.IsZero() {
		return Discovery{}, fmt.Errorf("discovery start time is required")
	}
	maxConcurrent := options.MaxConcurrent
	if maxConcurrent <= 0 {
		maxConcurrent = defaultDiscoveryConcurrency
	}
	if maxConcurrent > len(inventory.Sources) {
		maxConcurrent = len(inventory.Sources)
	}

	observations := make([]Observation, len(inventory.Sources))
	indexes := make(chan int)
	var workers sync.WaitGroup
	for range maxConcurrent {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range indexes {
				observations[index] = discoverSource(ctx, client, inventory.Sources[index], clock)
			}
		}()
	}
	for index := range inventory.Sources {
		indexes <- index
	}
	close(indexes)
	workers.Wait()
	completedAt := clock().UTC()
	if completedAt.IsZero() || completedAt.Before(startedAt) {
		return Discovery{}, fmt.Errorf("discovery completion time precedes its start")
	}

	return Discovery{
		SchemaVersion:    DiscoverySchemaVersion,
		InventoryVersion: inventory.InventoryVersion,
		StartedAt:        startedAt,
		CompletedAt:      completedAt,
		Observations:     observations,
	}, nil
}

func discoverSource(ctx context.Context, client HTTPDoer, source SourceSpec, clock func() time.Time) (observation Observation) {
	observation = Observation{
		SourceID:      source.SourceID,
		RequestMethod: http.MethodHead,
		RequestURL:    source.RequestURL,
		Status:        ObservationUnavailable,
	}
	defer func() {
		observation.ObservedAt = clock().UTC()
	}()
	request, err := http.NewRequestWithContext(ctx, http.MethodHead, source.RequestURL, nil)
	if err != nil {
		observation.ProblemCode = "request_failed"
		observation.Problem = err.Error()
		return observation
	}
	request.Header.Set("User-Agent", "legal-tender-fec-discovery/1")
	response, err := client.Do(request)
	if err != nil {
		observation.ProblemCode = "request_failed"
		observation.Problem = err.Error()
		return observation
	}
	if response == nil {
		observation.ProblemCode = "request_failed"
		observation.Problem = "HTTP client returned no response"
		return observation
	}
	if response.Body != nil {
		defer func() { _ = response.Body.Close() }()
	}

	observation.HTTPStatus = response.StatusCode
	if response.Request != nil && response.Request.URL != nil {
		observation.FinalURL = response.Request.URL.String()
	} else {
		observation.FinalURL = request.URL.String()
	}
	observation.VersionID = firstHeader(response.Header, "X-Amz-Version-Id", "X-Goog-Generation", "X-Ms-Version-Id")
	if strings.EqualFold(observation.VersionID, "null") {
		observation.VersionID = ""
	}
	observation.ETag = strings.TrimSpace(response.Header.Get("ETag"))
	observation.LastModified = strings.TrimSpace(response.Header.Get("Last-Modified"))
	observation.Digest = firstHeader(response.Header, "Content-Digest", "Digest")
	observation.AcceptRanges = strings.TrimSpace(response.Header.Get("Accept-Ranges"))
	if response.ContentLength >= 0 {
		contentLength := response.ContentLength
		observation.ContentLength = &contentLength
	}

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		observation.ProblemCode = "unexpected_http_status"
		observation.Problem = fmt.Sprintf("HEAD returned HTTP %d", response.StatusCode)
		return observation
	}
	observation.VersionBasis, observation.VersionIdentity = deriveVersionIdentity(
		observation.VersionID,
		observation.ETag,
		observation.Digest,
		observation.LastModified,
		observation.ContentLength,
	)
	if observation.VersionIdentity == "" {
		observation.ProblemCode = "version_identity_missing"
		observation.Problem = "successful HEAD response had no usable publisher version metadata"
		return observation
	}
	observation.Status = ObservationAvailable
	return observation
}

func firstHeader(header http.Header, names ...string) string {
	for _, name := range names {
		if value := strings.TrimSpace(header.Get(name)); value != "" {
			return value
		}
	}
	return ""
}

func deriveVersionIdentity(versionID, etag, digest, lastModified string, contentLength *int64) (string, string) {
	if versionID != "" {
		return "version_id", "version_id:" + versionID
	}
	if etag != "" {
		return "etag", "etag:" + etag
	}
	if digest != "" {
		return "digest", "digest:" + digest
	}
	if lastModified != "" && contentLength != nil && *contentLength >= 0 {
		return "last_modified_content_length", fmt.Sprintf("last_modified_content_length:%s|%d", lastModified, *contentLength)
	}
	return "", ""
}
