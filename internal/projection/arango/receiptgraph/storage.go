package receiptgraph

import (
	"fmt"
	"os"
	"syscall"
)

type FilesystemSample struct {
	Device    uint64 `json:"device"`
	Total     uint64 `json:"total_bytes"`
	Available uint64 `json:"available_bytes"`
}

// Growth is net loss of available bytes on the shared filesystem, not a claim
// of exact per-database allocation. Other writers can exhaust this envelope.
type StorageEnvelope struct {
	Initial          FilesystemSample `json:"initial_filesystem"`
	ReserveFreeBytes uint64           `json:"reserve_free_bytes"`
	MaxGrowthBytes   uint64           `json:"max_net_filesystem_growth_bytes"`
	MaxEncodedBytes  uint64           `json:"max_encoded_document_bytes"`
}

type storageGuard struct {
	directory string
	envelope  StorageEnvelope
	sample    func(string) (FilesystemSample, error)
}

func filesystemSample(path string) (FilesystemSample, error) {
	var s syscall.Statfs_t
	if err := syscall.Statfs(path, &s); err != nil {
		return FilesystemSample{}, fmt.Errorf("cannot inspect Arango filesystem")
	}
	info, err := os.Stat(path)
	if err != nil {
		return FilesystemSample{}, fmt.Errorf("cannot identify Arango filesystem")
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || s.Bsize <= 0 || s.Blocks > ^uint64(0)/uint64(s.Bsize) {
		return FilesystemSample{}, fmt.Errorf("invalid Arango filesystem measurements")
	}
	return FilesystemSample{Device: uint64(st.Dev), Total: s.Blocks * uint64(s.Bsize), Available: s.Bavail * uint64(s.Bsize)}, nil
}

func (g *storageGuard) identity(s FilesystemSample) error {
	if s.Device != g.envelope.Initial.Device || s.Total != g.envelope.Initial.Total || s.Available > s.Total {
		return fmt.Errorf("Arango filesystem identity or capacity changed")
	}
	return nil
}

func (g *storageGuard) check() error {
	s, err := g.sample(g.directory)
	if err != nil {
		return err
	}
	if err = g.identity(s); err != nil {
		return err
	}
	if s.Available < g.envelope.ReserveFreeBytes {
		return fmt.Errorf("Arango filesystem free-space reserve reached")
	}
	if s.Available < g.envelope.Initial.Available && g.envelope.Initial.Available-s.Available > g.envelope.MaxGrowthBytes {
		return fmt.Errorf("Arango filesystem growth envelope reached")
	}
	return nil
}
