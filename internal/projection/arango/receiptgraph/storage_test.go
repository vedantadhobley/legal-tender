package receiptgraph

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestStorageGuardReserveGrowthIdentityAndErrors(t *testing.T) {
	initial := FilesystemSample{Device: 1, Total: 1000, Available: 800}
	for _, test := range []struct {
		name   string
		sample FilesystemSample
		err    error
		ok     bool
	}{
		{"initial", initial, nil, true},
		{"growth boundary", FilesystemSample{1, 1000, 400}, nil, true},
		{"growth exceeded", FilesystemSample{1, 1000, 399}, nil, false},
		{"reserve breached", FilesystemSample{1, 1000, 99}, nil, false},
		{"other writer frees space", FilesystemSample{1, 1000, 900}, nil, true},
		{"different device", FilesystemSample{2, 1000, 800}, nil, false},
		{"changed capacity", FilesystemSample{1, 2000, 800}, nil, false},
		{"impossible free space", FilesystemSample{1, 1000, 1001}, nil, false},
		{"stat failure", FilesystemSample{}, errors.New("unavailable"), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			g := storageGuard{envelope: StorageEnvelope{Initial: initial, ReserveFreeBytes: 100, MaxGrowthBytes: 400}, sample: func(string) (FilesystemSample, error) { return test.sample, test.err }}
			if err := g.check(); (err == nil) != test.ok {
				t.Fatalf("guard: %v", err)
			}
		})
	}
}

func TestStorageAdmissionDoesNotCreatePublication(t *testing.T) {
	o, d, files := cycleFixture(t)
	o.MaxFilesystemGrowthBytes = ^uint64(0) - o.ReserveFreeBytes
	if _, err := openCycle(o, d, files); err == nil {
		t.Fatal("admitted impossible budget")
	}
	if _, err := os.Stat(filepath.Join(o.PublicationDirectory, d.Key)); !os.IsNotExist(err) {
		t.Fatal("failed admission created publication")
	}
}
