package receiptgraph

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	p "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
)

const CycleVersion = "legal-tender.arango.receipt-participant-cycle.v1"
const CycleState = "verified_complete_cycle_receipt_observations"

// Operational paths and resource limits do not change source/graph identity.
type Publication struct {
	Manifest   string          `json:"manifest"`
	Checkpoint string          `json:"checkpoint"`
	ResumeRows uint64          `json:"checkpoint_rows_at_start"`
	Storage    StorageEnvelope `json:"storage_envelope"`
}

type checkpoint struct {
	Version    string            `json:"schema_version"`
	Definition definition        `json:"definition"`
	Last       uint64            `json:"verified_through_source_row_ordinal"`
	Evidence   completion        `json:"verified_prefix"`
	Bytes      map[string]uint64 `json:"encoded_document_bytes"`
	Storage    StorageEnvelope   `json:"storage_envelope"`
	SHA256     string            `json:"checkpoint_sha256"`
}

type cycleRun struct {
	dir        string
	prior      checkpoint
	resumeRows uint64
	guard      *storageGuard
	encoded    uint64
	published  []byte
}

func validateCycleOptions(o Options) error {
	if o.First != 0 || o.Rows != 0 || o.Layout != CompactLayout || o.CompareResult != "" || o.PublicationDirectory == "" || o.ArangoDataDirectory == "" || o.ReserveFreeBytes == 0 || o.MaxFilesystemGrowthBytes == 0 || o.MaxEncodedBytes == 0 {
		return fmt.Errorf("complete-cycle publication requires compact layout, no sample/comparison scope, publication directory and explicit storage limits")
	}
	if o.MaxFilesystemGrowthBytes > ^uint64(0)-o.ReserveFreeBytes {
		return fmt.Errorf("storage admission overflow")
	}
	return nil
}

func openCycle(o Options, d definition, files []p.File) (*cycleRun, error) {
	dir := filepath.Join(o.PublicationDirectory, d.Key)
	v := &cycleRun{dir: dir}
	info, err := os.Stat(filepath.Join(o.ArangoDataDirectory, "ENGINE"))
	if err != nil || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("Arango data directory must expose the actual server ENGINE file read-only")
	}
	sample, err := filesystemSample(o.ArangoDataDirectory)
	if err != nil {
		return nil, err
	}
	b, err := manifestBytes(filepath.Join(dir, "progress.json"))
	fresh := os.IsNotExist(err)
	if err == nil {
		if err = decodeCheckpoint(b, d, files, &v.prior); err != nil {
			return nil, err
		}
		v.resumeRows = v.prior.Last
		if v.prior.Storage.ReserveFreeBytes != o.ReserveFreeBytes || v.prior.Storage.MaxGrowthBytes != o.MaxFilesystemGrowthBytes || v.prior.Storage.MaxEncodedBytes != o.MaxEncodedBytes {
			return nil, fmt.Errorf("resume storage envelope differs; use the original explicit limits")
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	} else {
		if sample.Available < o.ReserveFreeBytes+o.MaxFilesystemGrowthBytes {
			return nil, fmt.Errorf("storage admission requires reserve plus complete filesystem-growth budget")
		}
		v.prior = checkpoint{Version: CycleVersion, Definition: d, Storage: StorageEnvelope{Initial: sample, ReserveFreeBytes: o.ReserveFreeBytes, MaxGrowthBytes: o.MaxFilesystemGrowthBytes, MaxEncodedBytes: o.MaxEncodedBytes}}
	}
	v.guard = &storageGuard{directory: o.ArangoDataDirectory, envelope: v.prior.Storage, sample: filesystemSample}
	if err = v.guard.identity(sample); err != nil {
		return nil, err
	}
	if err = os.MkdirAll(dir, 0750); err != nil {
		return nil, err
	}
	if fresh {
		if err = v.store(); err != nil {
			return nil, err
		}
	}
	v.published, err = manifestBytes(filepath.Join(dir, "manifest.json"))
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	return v, nil
}

func decodeCheckpoint(b []byte, d definition, files []p.File, out *checkpoint) error {
	if len(b) > 8<<20 {
		return fmt.Errorf("oversized cycle checkpoint")
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if dec.Decode(out) != nil || dec.Decode(new(any)) != io.EOF {
		return fmt.Errorf("invalid cycle checkpoint")
	}
	want := out.SHA256
	out.SHA256 = ""
	raw, _ := json.Marshal(out)
	out.SHA256 = want
	x, _ := json.Marshal(out.Definition)
	y, _ := json.Marshal(d)
	if out.Version != CycleVersion || !validDigest(want) || digest(raw) != want || !equalJSON(x, y) {
		return fmt.Errorf("checkpoint integrity or ancestry mismatch")
	}
	valid := out.Last == 0
	for _, f := range files {
		valid = valid || f.Last == out.Last
	}
	if !valid || out.Last > d.Rows {
		return fmt.Errorf("checkpoint is not a complete source-shard boundary")
	}
	if out.Last > 0 {
		x, _ = json.Marshal(out.Evidence.definition)
		if !equalJSON(x, y) || out.Evidence.Counts[appearances] != out.Last || out.Evidence.Counts[receipts]+out.Evidence.Unrouted != out.Last {
			return fmt.Errorf("checkpoint prefix membership mismatch")
		}
	}
	return nil
}

func (v *cycleRun) store() error {
	v.prior.SHA256 = ""
	b, _ := json.Marshal(v.prior)
	v.prior.SHA256 = digest(b)
	b, err := json.Marshal(v.prior)
	if err != nil {
		return err
	}
	return writePublicationFile(filepath.Join(v.dir, "progress.json"), b, false)
}

func (v *cycleRun) admit(b batch) error {
	if v == nil {
		return nil
	}
	n := uint64(len(b.data))
	if n > v.prior.Storage.MaxEncodedBytes-v.encoded {
		return fmt.Errorf("cycle encoded-document budget exceeded")
	}
	v.encoded += n
	return nil
}

func (v *cycleRun) beforeWrite() error {
	if v == nil {
		return nil
	}
	return v.guard.check()
}

// Called only after shard EOF, all buffers flushed, and every worker readback
// acknowledged. Replayed prefixes are compared, never silently repaired.
func (v *cycleRun) mark(last uint64, r Result, b *batches, reused bool) error {
	proof := completion{definition: r.Definition, Counts: b.counts, Digests: b.digests(), Unrouted: r.Unrouted, States: r.ConduitStates, SourceEvidenceSHA256: b.sourceDigests()}
	if last == v.resumeRows {
		x, _ := json.Marshal(proof)
		y, _ := json.Marshal(v.prior.Evidence)
		xb, _ := json.Marshal(b.bytes)
		yb, _ := json.Marshal(v.prior.Bytes)
		if !equalJSON(x, y) || !equalJSON(xb, yb) {
			return fmt.Errorf("replayed checkpoint evidence changed")
		}
	}
	if last <= v.resumeRows || reused {
		return nil
	}
	if err := v.beforeWrite(); err != nil {
		return err
	}
	v.prior.Last, v.prior.Evidence, v.prior.Bytes = last, proof, b.bytes
	return v.store()
}

func (v *cycleRun) publish(raw []byte) (*Publication, error) {
	if v == nil {
		return nil, nil
	}
	path := filepath.Join(v.dir, "manifest.json")
	if err := writePublicationFile(path, raw, true); err != nil {
		return nil, err
	}
	return &Publication{Manifest: path, Checkpoint: filepath.Join(v.dir, "progress.json"), ResumeRows: v.resumeRows, Storage: v.prior.Storage}, nil
}

// Checkpoints replace only this publication's progress. A completed manifest is
// immutable; an existing equivalent file is reused, never replaced.
func writePublicationFile(path string, b []byte, immutable bool) error {
	if immutable {
		old, err := os.ReadFile(path)
		if err == nil {
			if !equalJSON(old, b) {
				return fmt.Errorf("immutable publication differs")
			}
			return nil
		}
		if !os.IsNotExist(err) {
			return err
		}
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".receipt-publication-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if _, err = f.Write(b); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if immutable {
		err = os.Link(f.Name(), path)
	} else {
		err = os.Rename(f.Name(), path)
	}
	if err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	if err = dir.Sync(); err != nil {
		return err
	}
	got, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if !bytes.Equal(got, b) {
		return fmt.Errorf("publication file readback differs")
	}
	return nil
}
