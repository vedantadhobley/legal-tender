package flowreconciliation

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	artifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

func validDigest(s string) bool {
	return len(s) == 64 && strings.IndexFunc(s, func(c rune) bool { return !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') }) < 0
}

func validCycle(s string) bool {
	n, err := strconv.Atoi(s)
	return err == nil && len(s) == 4 && n >= 1976 && n%2 == 0
}

func inside(root, path string) (string, error) {
	if root == "" || path == "" {
		return "", fmt.Errorf("storage root and path required")
	}
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return "", err
	}
	return artifact.Resolve(root, rel)
}

func publicationLock(ctx context.Context, root, base, cycle string) (func(), error) {
	if !validCycle(cycle) {
		return nil, fmt.Errorf("invalid publication cycle")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path := filepath.Join(root, base, ".publish-"+cycle+".lock")
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		return nil, err
	}
	for {
		if err := ctx.Err(); err != nil {
			f.Close()
			return nil, err
		}
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); _ = f.Close() }, nil
		}
		if err != syscall.EWOULDBLOCK && err != syscall.EAGAIN {
			f.Close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			f.Close()
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func jsonBytes(v any) ([]byte, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	return append(b, '\n'), err
}

// Immutable files are linked without replacement. Only current pointers use
// rename. Callers hold the cycle lock and validate existing pointers first.
func writePublicationJSON(ctx context.Context, path string, content []byte, immutable bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".pending-*.json")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if err := f.Chmod(0640); err != nil {
		return err
	}
	if _, err := f.Write(content); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if immutable {
		if err := os.Link(f.Name(), path); err != nil {
			if !os.IsExist(err) {
				return err
			}
			prior, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if !bytes.Equal(prior, content) {
				return fmt.Errorf("immutable publication collision")
			}
		}
	} else if err := os.Rename(f.Name(), path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func verifyPinnedBytes(root, base, id string, content []byte) error {
	if !validDigest(id) {
		return fmt.Errorf("invalid publication identity")
	}
	backing, err := os.ReadFile(filepath.Join(root, base, "manifests", id+".json"))
	if err != nil {
		return fmt.Errorf("immutable publication backing: %w", err)
	}
	if !bytes.Equal(content, backing) {
		return fmt.Errorf("publication pointer differs from immutable bytes")
	}
	return nil
}
