// Package externalsort implements bounded, verified sort runs for exact joins.
package externalsort

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
)

const MaxRecordBytes = 65536

type Record struct {
	Key     string
	Tag     byte
	Ordinal uint64
	Data    []byte
}

func Compare(a, b Record) int {
	if c := strings.Compare(a.Key, b.Key); c != 0 {
		return c
	}
	if c := cmp.Compare(a.Tag, b.Tag); c != 0 {
		return c
	}
	if c := cmp.Compare(a.Ordinal, b.Ordinal); c != 0 {
		return c
	}
	return bytes.Compare(a.Data, b.Data)
}

type File struct {
	Name         string `json:"name"`
	Rows         uint64 `json:"rows"`
	Bytes        uint64 `json:"bytes"`
	SHA256       string `json:"sha256"`
	ValuesSHA256 string `json:"values_sha256"`
}
type Workspace struct {
	Dir               string
	Limit, Live, Peak uint64
	mu                sync.Mutex
	next              uint64
	owned             map[string]File
}

func NewWorkspace(dir string, limit uint64) (*Workspace, error) {
	if limit == 0 {
		return nil, fmt.Errorf("positive workspace cap required")
	}
	if err := os.Mkdir(dir, 0750); err != nil {
		return nil, err
	}
	return &Workspace{Dir: dir, Limit: limit, owned: map[string]File{}}, nil
}

type capWriter struct {
	ctx   context.Context
	w     io.Writer
	space *Workspace
}

func (w capWriter) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	// Serialize physical writes and accounting, not sorting/compression. A
	// shared cap must include every concurrent writer's actual partial bytes.
	w.space.mu.Lock()
	defer w.space.mu.Unlock()
	if uint64(len(p)) > w.space.Limit-w.space.Live {
		return 0, fmt.Errorf("sort workspace byte cap exceeded")
	}
	n, err := w.w.Write(p)
	w.space.Live += uint64(n)
	w.space.Peak = max(w.space.Peak, w.space.Live)
	return n, err
}
func (s *Workspace) Remove(f File) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	owned, ok := s.owned[f.Name]
	if !ok || owned != f {
		return fmt.Errorf("refuse removal of unowned sort file")
	}
	if err := os.Remove(filepath.Join(s.Dir, f.Name)); err != nil {
		return err
	}
	s.Live -= f.Bytes
	delete(s.owned, f.Name)
	return nil
}

func (s *Workspace) Stats() (peak, live uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Peak, s.Live
}

func (s *Workspace) owns(f File) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.owned[f.Name] == f && f.Name != ""
}

type Writer struct {
	ctx              context.Context
	space            *Workspace
	file             *os.File
	encoder          *zstd.Encoder
	buffer           *bufio.Writer
	physical, values hash.Hash
	descriptor       File
	previous         Record
	hasPrevious      bool
	closed           bool
}

func (s *Workspace) Writer(ctx context.Context) (*Writer, error) {
	s.mu.Lock()
	name := fmt.Sprintf("run-%08d.zst", s.next)
	s.next++
	s.mu.Unlock()
	f, err := os.OpenFile(filepath.Join(s.Dir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0640)
	if err != nil {
		return nil, err
	}
	h := sha256.New()
	z, err := zstd.NewWriter(capWriter{ctx, io.MultiWriter(f, h), s}, zstd.WithEncoderConcurrency(1))
	if err != nil {
		f.Close()
		return nil, err
	}
	v := sha256.New()
	return &Writer{ctx: ctx, space: s, file: f, encoder: z, buffer: bufio.NewWriterSize(io.MultiWriter(z, v), 65536), physical: h, values: v, descriptor: File{Name: name}}, nil
}
func (w *Writer) Add(r Record) error {
	if w.closed {
		return fmt.Errorf("closed run writer")
	}
	if err := w.ctx.Err(); err != nil {
		return err
	}
	if len(r.Key)+len(r.Data)+17 > MaxRecordBytes {
		return fmt.Errorf("sort record exceeds %d bytes", MaxRecordBytes)
	}
	if w.hasPrevious && Compare(w.previous, r) > 0 {
		return fmt.Errorf("unordered sort run")
	}
	var head [17]byte
	binary.BigEndian.PutUint32(head[:4], uint32(len(r.Key)))
	binary.BigEndian.PutUint32(head[4:8], uint32(len(r.Data)))
	head[8] = r.Tag
	binary.BigEndian.PutUint64(head[9:], r.Ordinal)
	if _, err := w.buffer.Write(head[:]); err != nil {
		return err
	}
	if _, err := w.buffer.WriteString(r.Key); err != nil {
		return err
	}
	if _, err := w.buffer.Write(r.Data); err != nil {
		return err
	}
	w.previous = r
	w.hasPrevious = true
	w.descriptor.Rows++
	return nil
}
func (w *Writer) Abort() {
	if !w.closed {
		w.closed = true
		w.file.Close()
	}
} // retain partial attempt; never flush on failure
func (w *Writer) Finish() (File, error) {
	if w.closed {
		return File{}, fmt.Errorf("closed run writer")
	}
	defer w.Abort()
	if err := w.buffer.Flush(); err != nil {
		return File{}, err
	}
	if err := w.encoder.Close(); err != nil {
		return File{}, err
	}
	if err := w.file.Sync(); err != nil {
		return File{}, err
	}
	info, err := w.file.Stat()
	if err != nil {
		return File{}, err
	}
	if err = w.file.Close(); err != nil {
		return File{}, err
	}
	w.closed = true
	d := w.descriptor
	d.Bytes = uint64(info.Size())
	d.SHA256 = hex.EncodeToString(w.physical.Sum(nil))
	d.ValuesSHA256 = hex.EncodeToString(w.values.Sum(nil))
	if err := Verify(w.ctx, w.space.Dir, d); err != nil {
		return File{}, err
	}
	w.space.mu.Lock()
	w.space.owned[d.Name] = d
	w.space.mu.Unlock()
	return d, nil
}

type Reader struct {
	ctx              context.Context
	file             *os.File
	decoder          *zstd.Decoder
	reader           *bufio.Reader
	physical, values hash.Hash
	expected         File
	rows             uint64
	previous         Record
	done             bool
}

func Open(ctx context.Context, dir string, d File) (*Reader, error) {
	if filepath.Base(d.Name) != d.Name {
		return nil, fmt.Errorf("invalid run name")
	}
	f, err := os.Open(filepath.Join(dir, d.Name))
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || uint64(info.Size()) != d.Bytes {
		f.Close()
		return nil, fmt.Errorf("sort file size/type mismatch")
	}
	h, v := sha256.New(), sha256.New()
	z, err := zstd.NewReader(io.TeeReader(f, h), zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(32<<20))
	if err != nil {
		f.Close()
		return nil, err
	}
	return &Reader{ctx: ctx, file: f, decoder: z, reader: bufio.NewReaderSize(io.TeeReader(z, v), 65536), physical: h, values: v, expected: d}, nil
}
func (r *Reader) Close() { r.decoder.Close(); r.file.Close() }
func (r *Reader) Next() (Record, error) {
	if err := r.ctx.Err(); err != nil {
		return Record{}, err
	}
	if r.done {
		return Record{}, io.EOF
	}
	var head [17]byte
	n, err := io.ReadFull(r.reader, head[:])
	if err == io.EOF && n == 0 {
		if r.rows != r.expected.Rows || hex.EncodeToString(r.physical.Sum(nil)) != r.expected.SHA256 || hex.EncodeToString(r.values.Sum(nil)) != r.expected.ValuesSHA256 {
			return Record{}, fmt.Errorf("sort run readback mismatch")
		}
		r.done = true
		return Record{}, io.EOF
	}
	if err != nil {
		return Record{}, err
	}
	keyN, dataN := uint64(binary.BigEndian.Uint32(head[:4])), uint64(binary.BigEndian.Uint32(head[4:8]))
	if keyN+dataN+17 > MaxRecordBytes {
		return Record{}, fmt.Errorf("oversized sort record")
	}
	b := make([]byte, keyN+dataN)
	if _, err := io.ReadFull(r.reader, b); err != nil {
		return Record{}, err
	}
	x := Record{Key: string(b[:keyN]), Tag: head[8], Ordinal: binary.BigEndian.Uint64(head[9:]), Data: b[keyN:]}
	if r.rows > 0 && Compare(r.previous, x) > 0 {
		return Record{}, fmt.Errorf("unordered readback")
	}
	r.previous = x
	r.rows++
	return x, nil
}
func Verify(ctx context.Context, dir string, d File) error {
	r, err := Open(ctx, dir, d)
	if err != nil {
		return err
	}
	defer r.Close()
	for {
		_, err := r.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
