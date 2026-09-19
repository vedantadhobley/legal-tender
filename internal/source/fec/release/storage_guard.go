package release

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

var errStorageBudget = errors.New("streaming storage budget exhausted")
var errStorageBusy = errors.New("another FEC acquisition or stage owns storage")

// One local writer operation owns the hot-lane budget. The file stays in place:
// unlinking a held flock file would allow a second lock on a different inode.
func lockSourceStorage(root string) (*os.File, error) {
	path := filepath.Join(root, "raw", "fec", ".storage.lock")
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o640)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, errStorageBusy
		}
		return nil, err
	}
	return file, nil
}

// New temporary extracts live inside the counted hot lane, including failed
// process remnants. Older workspaces cannot be silently excluded from the cap.
func checkLegacyWorkspace(root string) error {
	legacy := filepath.Join(root, "raw", "fec", "staging")
	return filepath.WalkDir(legacy, func(path string, entry os.DirEntry, err error) error {
		if path == legacy && os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		matched, err := filepath.Match(".selected-*.zst", entry.Name())
		if err != nil {
			return err
		}
		if matched {
			return fmt.Errorf("legacy selected temporary files require storage review before writing")
		}
		return nil
	})
}

const storageWriteChunk = 1 << 20
const storageHotRefreshBytes = 8 << 20

// writeBudget bounds actual file growth, shared by concurrent download workers.
// The global free floor plus margin is checked before each <=1 MiB write. Its
// monotonically decreasing allowances also bound own growth independently of
// filesystem allocation timing. Other applications do not share this lock;
// the margin is a safety buffer, not a filesystem-wide space reservation.
type writeBudget struct {
	mu            sync.Mutex
	ctx           context.Context
	root          string
	diskUsage     func(string) (DiskSpace, error)
	limits        StageStorage
	device        uint64
	hotRemaining  uint64
	freeRemaining uint64
	refreshBytes  uint64
	err           error
}

func newWriteBudget(ctx context.Context, root string, limits StageStorage, diskUsage func(string) (DiskSpace, error)) (*writeBudget, error) {
	if diskUsage == nil {
		diskUsage = defaultDiskUsage
	}
	if err := checkLegacyWorkspace(root); err != nil {
		return nil, err
	}
	info, err := os.Stat(root)
	if err != nil {
		return nil, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, fmt.Errorf("storage filesystem identity unavailable")
	}
	budget := &writeBudget{ctx: ctx, root: root, limits: limits, diskUsage: diskUsage, device: uint64(stat.Dev), hotRemaining: ^uint64(0), freeRemaining: ^uint64(0)}
	if err := budget.refreshHot(); err != nil {
		return nil, err
	}
	if err := budget.refreshFree(); err != nil {
		return nil, err
	}
	return budget, nil
}

func (budget *writeBudget) refreshHot() error {
	hot, err := directoryBytes(scheduleAStorageRoot(budget.root))
	if err != nil {
		return err
	}
	working, err := sumStorageBytes(budget.limits.WorkingMarginBytes, budget.limits.LargestExtractWorkingBytes)
	if err != nil {
		return err
	}
	if hot > budget.limits.ScheduleAHotCapBytes || working > budget.limits.ScheduleAHotCapBytes-hot {
		return fmt.Errorf("%w: Schedule A hot cap", errStorageBudget)
	}
	budget.hotRemaining = min(budget.hotRemaining, budget.limits.ScheduleAHotCapBytes-hot-working)
	budget.refreshBytes = 0
	return nil
}

func (budget *writeBudget) refreshFree() error {
	disk, err := budget.diskUsage(budget.root)
	if err != nil {
		return err
	}
	reserve, err := sumStorageBytes(budget.limits.FreeFloorBytes, budget.limits.WorkingMarginBytes, budget.limits.LargestExtractWorkingBytes)
	if err != nil {
		return err
	}
	if disk.AvailableBytes < reserve {
		return fmt.Errorf("%w: filesystem free floor", errStorageBudget)
	}
	budget.freeRemaining = min(budget.freeRemaining, disk.AvailableBytes-reserve)
	return nil
}

func (budget *writeBudget) writer(file *os.File, chargeHot bool) (io.Writer, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || uint64(stat.Dev) != budget.device || stat.Nlink != 1 {
		return nil, fmt.Errorf("output must be an unshared regular file on the storage filesystem")
	}
	return &budgetWriter{budget: budget, file: file, chargeHot: chargeHot}, nil
}

type budgetWriter struct {
	budget    *writeBudget
	file      *os.File
	chargeHot bool
}

func (writer *budgetWriter) Write(content []byte) (int, error) {
	budget := writer.budget
	budget.mu.Lock()
	defer budget.mu.Unlock()
	var written int
	for len(content) > 0 {
		if budget.err != nil {
			return written, budget.err
		}
		if err := budget.ctx.Err(); err != nil {
			budget.err = err
			return written, err
		}
		if budget.refreshBytes >= storageHotRefreshBytes {
			if err := budget.refreshHot(); err != nil {
				budget.err = err
				return written, err
			}
		}
		if err := budget.refreshFree(); err != nil {
			budget.err = err
			return written, err
		}
		chunk := content[:min(len(content), storageWriteChunk)]
		if uint64(len(chunk)) > budget.freeRemaining || (writer.chargeHot && uint64(len(chunk)) > budget.hotRemaining) {
			budget.err = errStorageBudget
			return written, budget.err
		}
		n, err := writer.file.Write(chunk)
		budget.freeRemaining -= uint64(n)
		if writer.chargeHot {
			budget.hotRemaining -= uint64(n)
		}
		budget.refreshBytes += uint64(n)
		written += n
		content = content[n:]
		if err == nil && n != len(chunk) {
			err = io.ErrShortWrite
		}
		if err != nil {
			budget.err = err
			return written, err
		}
	}
	return written, nil
}
