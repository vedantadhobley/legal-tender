package release

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type capturedDownload struct {
	path       string
	byteCount  int64
	digest     string
	acquiredAt time.Time
}

func captureSource(
	ctx context.Context,
	client HTTPDoer,
	selected SelectedSource,
	path string,
	clock func() time.Time,
	budget *writeBudget,
) (capturedDownload, error) {
	if selected.ContentLength == nil || *selected.ContentLength <= 0 {
		return capturedDownload{}, fmt.Errorf("selected source has no positive content length")
	}
	expected := *selected.ContentLength
	if err := os.MkdirAll(filepathDir(path), 0o750); err != nil {
		return capturedDownload{}, err
	}

	file, offset, digestHash, err := openPartial(path, expected)
	if err != nil {
		return capturedDownload{}, err
	}
	defer func() { _ = file.Close() }()
	if offset == expected {
		return capturedDownload{path: path, byteCount: expected, digest: hex.EncodeToString(digestHash.Sum(nil)), acquiredAt: clock().UTC()}, nil
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, selected.RequestURL, nil)
	if err != nil {
		return capturedDownload{}, err
	}
	request.Header.Set("User-Agent", "legal-tender-fec-acquisition/1")
	if offset > 0 {
		request.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
		if selected.ETag != "" {
			request.Header.Set("If-Match", selected.ETag)
			request.Header.Set("If-Range", selected.ETag)
		} else if selected.LastModified != "" {
			request.Header.Set("If-Range", selected.LastModified)
		}
	}
	response, err := client.Do(request)
	if err != nil {
		return capturedDownload{}, err
	}
	if response == nil {
		return capturedDownload{}, fmt.Errorf("HTTP client returned no response")
	}
	defer func() {
		if response.Body != nil {
			_ = response.Body.Close()
		}
	}()
	if response.Body == nil {
		return capturedDownload{}, fmt.Errorf("GET returned no response body")
	}
	if selected.ETag != "" && response.Header.Get("ETag") != "" && response.Header.Get("ETag") != selected.ETag {
		return capturedDownload{}, fmt.Errorf("GET ETag changed from selected version")
	}

	switch {
	case offset > 0 && response.StatusCode == http.StatusPartialContent:
		if err := validateContentRange(response.Header.Get("Content-Range"), offset, expected); err != nil {
			return capturedDownload{}, err
		}
	case offset > 0 && response.StatusCode == http.StatusOK:
		if err := file.Truncate(0); err != nil {
			return capturedDownload{}, err
		}
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return capturedDownload{}, err
		}
		offset = 0
		digestHash = sha256.New()
	case offset == 0 && response.StatusCode == http.StatusOK:
	default:
		return capturedDownload{}, fmt.Errorf("GET returned HTTP %d", response.StatusCode)
	}

	remaining := expected - offset
	var destination io.Writer = file
	if budget != nil {
		destination, err = budget.writer(file, selected.SourceID == ScheduleASourceID)
		if err != nil {
			return capturedDownload{}, err
		}
	}
	written, copyErr := io.Copy(io.MultiWriter(destination, digestHash), io.LimitReader(response.Body, remaining))
	if copyErr == nil && written == remaining {
		var extra [1]byte
		n, extraErr := io.ReadFull(response.Body, extra[:])
		if n != 0 {
			copyErr = fmt.Errorf("GET exceeds selected content length")
		} else if extraErr != io.EOF {
			copyErr = extraErr
		}
		if copyErr != nil {
			// A rejected complete-length prefix must not look complete on retry.
			// Keep only the prefix that existed before this response.
			if err := file.Truncate(offset); err != nil {
				return capturedDownload{}, fmt.Errorf("invalidate rejected response: %w", err)
			}
		}
	}
	syncErr := file.Sync()
	if copyErr != nil {
		return capturedDownload{}, copyErr
	}
	if syncErr != nil {
		return capturedDownload{}, syncErr
	}
	if written != remaining {
		return capturedDownload{}, fmt.Errorf("GET wrote %d bytes; want %d remaining bytes", written, remaining)
	}
	return capturedDownload{
		path:       path,
		byteCount:  expected,
		digest:     hex.EncodeToString(digestHash.Sum(nil)),
		acquiredAt: clock().UTC(),
	}, nil
}

func openPartial(path string, expected int64) (*os.File, int64, hash.Hash, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o640)
	if err != nil {
		return nil, 0, nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, 0, nil, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || !ok || stat.Nlink != 1 {
		_ = file.Close()
		return nil, 0, nil, fmt.Errorf("partial is not an unshared regular file")
	}
	if info.Size() > expected {
		if err := file.Truncate(0); err != nil {
			_ = file.Close()
			return nil, 0, nil, err
		}
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, 0, nil, err
	}
	digestHash := sha256.New()
	offset, err := io.Copy(digestHash, file)
	if err != nil {
		_ = file.Close()
		return nil, 0, nil, err
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		_ = file.Close()
		return nil, 0, nil, err
	}
	return file, offset, digestHash, nil
}

func validateContentRange(value string, expectedStart, expectedTotal int64) error {
	if !strings.HasPrefix(value, "bytes ") {
		return fmt.Errorf("partial response has invalid Content-Range %q", value)
	}
	parts := strings.Split(strings.TrimPrefix(value, "bytes "), "/")
	if len(parts) != 2 {
		return fmt.Errorf("partial response has invalid Content-Range %q", value)
	}
	rangeParts := strings.Split(parts[0], "-")
	if len(rangeParts) != 2 {
		return fmt.Errorf("partial response has invalid Content-Range %q", value)
	}
	start, startErr := strconv.ParseInt(rangeParts[0], 10, 64)
	end, endErr := strconv.ParseInt(rangeParts[1], 10, 64)
	total, totalErr := strconv.ParseInt(parts[1], 10, 64)
	if startErr != nil || endErr != nil || totalErr != nil || start != expectedStart || end != expectedTotal-1 || total != expectedTotal {
		return fmt.Errorf("partial response Content-Range %q does not match bytes %d-%d/%d", value, expectedStart, expectedTotal-1, expectedTotal)
	}
	return nil
}

func filepathDir(path string) string {
	index := strings.LastIndexAny(path, "/\\")
	if index < 0 {
		return "."
	}
	return path[:index]
}
