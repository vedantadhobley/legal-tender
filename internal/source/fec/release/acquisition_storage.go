package release

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

func defaultDiskUsage(path string) (DiskSpace, error) {
	var statistics syscall.Statfs_t
	if err := syscall.Statfs(path, &statistics); err != nil {
		return DiskSpace{}, err
	}
	if statistics.Bsize <= 0 || statistics.Bavail > ^uint64(0)/uint64(statistics.Bsize) {
		return DiskSpace{}, fmt.Errorf("invalid or overflowing filesystem byte count")
	}
	return DiskSpace{AvailableBytes: statistics.Bavail * uint64(statistics.Bsize)}, nil
}

// directoryBytes counts logical bytes once per device/inode within the tree.
// Separate copies still count separately, including sparse files. Symlinks are
// not followed. This is a conservative file-size budget, not allocated blocks.
func directoryBytes(root string) (uint64, error) {
	var total uint64
	type fileIdentity struct{ device, inode uint64 }
	seen := make(map[fileIdentity]int64)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return fmt.Errorf("storage file identity unavailable at %s", path)
		}
		identity := fileIdentity{uint64(stat.Dev), stat.Ino}
		if size, exists := seen[identity]; exists {
			if size != info.Size() {
				return fmt.Errorf("storage file changed during inspection at %s", path)
			}
			return nil
		}
		if info.Size() < 0 || total > ^uint64(0)-uint64(info.Size()) {
			return fmt.Errorf("storage byte count overflow at %s", path)
		}
		total += uint64(info.Size())
		seen[identity] = info.Size()
		return nil
	})
	return total, err
}

func safeSourceName(sourceID string) string {
	replacer := strings.NewReplacer(":", "-", "/", "-", "\\", "-")
	return replacer.Replace(sourceID)
}

func stagingPath(storageRoot, candidateReleaseID, sourceID string) string {
	base := filepath.Join(storageRoot, "raw", "fec")
	if family := processedScheduleFamily(sourceID); family != "" {
		base = filepath.Join(base, family)
	}
	return filepath.Join(base, "staging", candidateReleaseID, safeSourceName(sourceID)+".partial")
}

func contentStorageKey(sourceID, digest string) string {
	parts := []string{"raw", "fec"}
	if family := processedScheduleFamily(sourceID); family != "" {
		parts = append(parts, family)
	}
	parts = append(parts, "artifacts", "sha256", digest[:2], digest)
	return filepath.ToSlash(filepath.Join(parts...))
}

func processedScheduleFamily(sourceID string) string {
	switch sourceID {
	case ScheduleASourceID:
		return "schedule-a"
	case ScheduleBSourceID:
		return "schedule-b"
	case ScheduleESourceID:
		return "schedule-e"
	default:
		return ""
	}
}

func resolveStorageKey(storageRoot, storageKey string) (string, error) {
	if storageKey == "" || filepath.IsAbs(storageKey) {
		return "", fmt.Errorf("storage key must be a relative path")
	}
	clean := filepath.Clean(storageKey)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("storage key escapes the storage root")
	}
	return filepath.Join(storageRoot, clean), nil
}

func acquisitionStatePath(storageRoot, candidateReleaseID, runID string) string {
	return filepath.Join(storageRoot, "raw", "fec", "acquisitions", candidateReleaseID, runID+".json")
}

func readCompletedAcquisition(path, planSHA256 string) (*AcquisitionResult, error) {
	content, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var result AcquisitionResult
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return nil, fmt.Errorf("decode prior acquisition state: %w", err)
	}
	if result.PlanSHA256 != planSHA256 || result.Status != AcquisitionAcquired {
		return nil, fmt.Errorf("acquisition state exists for a different or incomplete plan")
	}
	return &result, nil
}

func writeAcquisitionState(path string, result AcquisitionResult) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	content, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	content = append(content, '\n')
	temporary, err := os.CreateTemp(filepath.Dir(path), ".pending-*.json")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o640); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(content); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}
