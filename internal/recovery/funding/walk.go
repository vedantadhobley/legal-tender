package funding

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"syscall"
)

type walker struct {
	ctx           context.Context
	root          *os.Root
	options       Options
	out           Result
	locators      map[string]Reference
	indices       map[string]int
	paths         map[string]Reference
	pending       []int
	edges         map[Edge]bool
	requirements  map[Requirement]bool
	metadataBytes int64
	metadata      map[string][]byte
}

func (w *walker) add(parent, role string, r Reference) (string, error) {
	if !validID(r.Kind, r.ID) || r.Kind == "" {
		return "", fmt.Errorf("invalid typed dependency identity")
	}
	if loc, ok := w.locators[refKey(r)]; ok {
		if r.SHA256 != "" && r.SHA256 != loc.SHA256 {
			return "", fmt.Errorf("locator cannot replace the parent's pinned digest")
		}
		if r.Path != "" && r.Path != loc.Path {
			return "", fmt.Errorf("locator cannot replace a declared physical path")
		}
		r.Path, r.SHA256 = loc.Path, loc.SHA256
	}
	if r.Path == "" {
		r.Path = defaultPath(r)
	}
	if r.Path != "" && !relative(r.Path) {
		return "", fmt.Errorf("unsafe dependency path")
	}
	if r.SHA256 != "" && !validDigest(r.SHA256) {
		return "", fmt.Errorf("invalid dependency byte digest")
	}
	key := digest([]byte(refKey(r)))
	if r.Kind == "file" {
		key = digest([]byte("file:" + r.Path))
	}
	if old, ok := w.paths[r.Path]; r.Path != "" && ok && conflicts(old, r) {
		return "", fmt.Errorf("conflicting physical dependency identities")
	}
	if r.Path != "" {
		w.paths[r.Path] = merge(w.paths[r.Path], r)
	}
	if i, ok := w.indices[key]; !ok {
		if len(w.out.Nodes) >= maxNodes {
			return "", fmt.Errorf("dependency node budget exceeded")
		}
		w.indices[key] = len(w.out.Nodes)
		w.pending = append(w.pending, len(w.out.Nodes))
		w.out.Nodes = append(w.out.Nodes, Node{Key: key, Reference: r})
	} else {
		old := w.out.Nodes[i].Reference
		if conflicts(old, r) || (old.Path != "" && r.Path != "" && old.Path != r.Path) {
			return "", fmt.Errorf("conflicting publication identity")
		}
		updated := merge(old, r)
		if identity(updated) != identity(old) {
			w.out.Nodes[i] = Node{Key: key, Reference: updated}
			w.pending = append(w.pending, i)
		}
	}
	if parent != "" {
		if len(w.edges) >= maxNodes*16 {
			return "", fmt.Errorf("dependency edge budget exceeded")
		}
		w.edges[Edge{parent, key, role}] = true
	}
	return key, nil
}

func conflicts(a, b Reference) bool {
	return (a.SHA256 != "" && b.SHA256 != "" && a.SHA256 != b.SHA256) || (a.Bytes != nil && b.Bytes != nil && *a.Bytes != *b.Bytes)
}
func merge(a, b Reference) Reference {
	if a.Kind == "" {
		return b
	}
	if a.SHA256 == "" {
		a.SHA256 = b.SHA256
	}
	if a.Path == "" {
		a.Path = b.Path
	}
	if a.Bytes == nil {
		a.Bytes = b.Bytes
	}
	return a
}

func (w *walker) visit(i int) error {
	n := w.out.Nodes[i] // add may reallocate Nodes; never retain a slice pointer.
	if n.Expanded {
		return nil
	}
	r := n.Reference
	if r.Path == "" {
		n.Verification = "unlocated"
		n.Problem = "explicit_locator_required"
		w.out.Nodes[i] = n
		return nil
	}
	info, err := w.root.Lstat(r.Path)
	if err != nil {
		n.Verification = "unreadable"
		if errors.Is(err, os.ErrNotExist) {
			n.Verification = "missing"
		}
		n.Problem = "dependency_open_failed"
		w.out.Nodes[i] = n
		return nil
	}
	if !info.Mode().IsRegular() {
		n.Verification = "invalid_file_type"
		w.out.Nodes[i] = n
		return nil
	}
	size := uint64(info.Size())
	n.ObservedBytes = &size
	if r.Bytes != nil && *r.Bytes != size {
		n.Verification = "size_mismatch"
		w.out.Nodes[i] = n
		return nil
	}
	if r.Kind == "file" && !w.options.HashBlobs {
		n.Verification = "present_unhashed"
		if r.Bytes != nil {
			n.Verification = "size_verified_not_hashed"
		}
		n.Expanded = true
		w.out.Nodes[i] = n
		return nil
	}
	if r.SHA256 == "" {
		n.Verification = "missing_expected_sha256"
		w.out.Nodes[i] = n
		return nil
	}
	// Nonblocking open also protects against a file replaced by a FIFO after
	// Lstat. Root prevents symlink traversal outside the storage directory.
	f, err := w.root.OpenFile(r.Path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		n.Verification = "unreadable"
		n.Problem = "dependency_open_failed"
		w.out.Nodes[i] = n
		return nil
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil {
		return err
	}
	if !opened.Mode().IsRegular() || !os.SameFile(info, opened) || opened.Size() != info.Size() {
		n.Verification = "file_changed_during_inspection"
		w.out.Nodes[i] = n
		return nil
	}
	if r.Kind == "file" {
		h := sha256.New()
		buf := make([]byte, 128<<10)
		for {
			if err := w.ctx.Err(); err != nil {
				return err
			}
			k, e := f.Read(buf)
			if k > 0 {
				h.Write(buf[:k])
			}
			if e == io.EOF {
				break
			}
			if e != nil {
				return e
			}
		}
		n.ObservedSHA256 = hex.EncodeToString(h.Sum(nil))
		n.Expanded = true
	} else {
		if size > maxManifestBytes || w.metadataBytes+int64(size) > maxMetadataBytes {
			return fmt.Errorf("manifest byte budget exceeded")
		}
		b, e := io.ReadAll(io.LimitReader(f, maxManifestBytes+1))
		if e != nil {
			return e
		}
		w.metadataBytes += int64(len(b))
		if len(b) > maxManifestBytes {
			return fmt.Errorf("manifest grew beyond byte budget")
		}
		n.ObservedSHA256 = digest(b)
		if n.ObservedSHA256 == r.SHA256 {
			deps, reqs, checks, schema, e := expand(r, b)
			n.Schema = schema
			n.PriorChecks = checks
			if e != nil {
				n.Problem = e.Error()
				n.Verification = "invalid_manifest"
				w.out.Nodes[i] = n
				return nil
			}
			if w.metadata != nil {
				w.metadata[n.Key] = b
			}
			for _, d := range deps {
				if _, e = w.add(n.Key, d.Role, d.Ref); e != nil {
					return e
				}
			}
			for _, q := range reqs {
				q.Parent = n.Key
				w.requirements[q] = true
			}
			n.Expanded = true
		}
	}
	after, err := f.Stat()
	if err != nil {
		return err
	}
	if after.Size() != opened.Size() || !after.ModTime().Equal(opened.ModTime()) {
		n.Verification = "file_changed_during_inspection"
		n.Expanded = false
		w.out.Nodes[i] = n
		return nil
	}
	n.Verification = "sha256_verified"
	if n.ObservedSHA256 != r.SHA256 {
		n.Verification = "sha256_mismatch"
		n.Expanded = false
	}
	w.out.Nodes[i] = n
	return nil
}

func (w *walker) finish() Result {
	w.out.Complete = true
	w.out.AllFilesHashed = true
	for _, n := range w.out.Nodes {
		w.out.Counts[n.Verification]++
		if !n.Expanded {
			w.out.Complete = false
		}
		if n.Verification != "sha256_verified" {
			w.out.AllFilesHashed = false
		}
	}
	for e := range w.edges {
		w.out.Edges = append(w.out.Edges, e)
	}
	w.out.DependencyCycle = dependencyCycle(w.out.Nodes, w.out.Edges)
	if w.out.DependencyCycle {
		w.out.Complete = false
	}
	for q := range w.requirements {
		w.out.Requirements = append(w.out.Requirements, q)
	}
	sort.Slice(w.out.Nodes, func(i, j int) bool { return w.out.Nodes[i].Key < w.out.Nodes[j].Key })
	sort.Slice(w.out.Edges, func(i, j int) bool {
		a, b := w.out.Edges[i], w.out.Edges[j]
		if a.From != b.From {
			return a.From < b.From
		}
		if a.To != b.To {
			return a.To < b.To
		}
		return a.Role < b.Role
	})
	sort.Slice(w.out.Requirements, func(i, j int) bool {
		a, b := w.out.Requirements[i], w.out.Requirements[j]
		if a.Parent != b.Parent {
			return a.Parent < b.Parent
		}
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		return a.Identity < b.Identity
	})
	w.out.ID = identity(w.out)
	return w.out
}

func dependencyCycle(nodes []Node, edges []Edge) bool {
	degree := map[string]int{}
	children := map[string][]string{}
	for _, n := range nodes {
		degree[n.Key] = 0
	}
	for _, e := range edges {
		degree[e.To]++
		children[e.From] = append(children[e.From], e.To)
	}
	var queue []string
	for k, d := range degree {
		if d == 0 {
			queue = append(queue, k)
		}
	}
	for i := 0; i < len(queue); i++ {
		for _, c := range children[queue[i]] {
			degree[c]--
			if degree[c] == 0 {
				queue = append(queue, c)
			}
		}
	}
	return len(queue) != len(nodes)
}
