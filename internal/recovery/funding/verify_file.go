package funding

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"strings"
	"syscall"
)

func verifyFile(ctx context.Context, root *os.Root, out VerifiedFile, buffer []byte) VerifiedFile {
	fail := func(state string) VerifiedFile { out.State = state; return out }
	if ctx.Err() != nil {
		return fail("cancelled")
	}
	// Reject aliases even inside Root. Root also confines concurrent traversal.
	parts := strings.Split(out.Path, "/")
	for i := 1; i < len(parts); i++ {
		info, err := root.Lstat(strings.Join(parts[:i], "/"))
		if err != nil || !info.IsDir() {
			return fail("invalid_parent_directory")
		}
	}
	before, err := root.Lstat(out.Path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fail("missing")
		}
		return fail("unreadable")
	}
	if !before.Mode().IsRegular() {
		return fail("invalid_file_type")
	}
	out.ObservedBytes = uint64(before.Size())
	if out.ObservedBytes != out.ExpectedBytes {
		return fail("size_mismatch")
	}
	f, err := root.OpenFile(out.Path, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return fail("unreadable")
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !opened.Mode().IsRegular() || !sameFileState(before, opened) {
		return fail("file_changed_during_verification")
	}
	h := sha256.New()
	// Read no more than expected+1, including a concurrently growing file.
	r := io.LimitReader(f, int64(out.ExpectedBytes)+1)
	out.ObservedBytes = 0
	for {
		if ctx.Err() != nil {
			return fail("cancelled")
		}
		n, e := r.Read(buffer)
		if n > 0 {
			h.Write(buffer[:n])
			out.ObservedBytes += uint64(n)
		}
		if e == io.EOF {
			break
		}
		if e != nil {
			return fail("read_failed")
		}
	}
	out.ObservedSHA256 = hex.EncodeToString(h.Sum(nil))
	after, err := f.Stat()
	named, namedErr := root.Lstat(out.Path)
	if err != nil || namedErr != nil || !sameFileState(opened, after) || !sameFileState(opened, named) {
		return fail("file_changed_during_verification")
	}
	if out.ObservedBytes != out.ExpectedBytes {
		return fail("size_mismatch")
	}
	if out.ObservedSHA256 != out.ExpectedSHA256 {
		return fail("sha256_mismatch")
	}
	return fail("sha256_verified")
}

func sameFileState(a, b os.FileInfo) bool {
	return os.SameFile(a, b) && a.Mode() == b.Mode() && a.Size() == b.Size() && a.ModTime().Equal(b.ModTime())
}
