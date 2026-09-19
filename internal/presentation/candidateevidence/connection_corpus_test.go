package candidateevidence

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/parquet-go/parquet-go"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

// Independent physical-row readback: no runtime report/source-reader helper is
// used here. This uses the pinned Parquet library, not an independent decoder.
func TestConnectionCorpusFullPhysicalRowReadback(t *testing.T) {
	root, audit := os.Getenv("LT_CONNECTION_STORAGE"), os.Getenv("LT_CONNECTION_OUTPUT")
	if root == "" || audit == "" {
		t.Skip("requires retained connection outputs and read-only source storage")
	}
	paths, err := filepath.Glob(filepath.Join(audit, "S6*-*.json"))
	if err != nil || len(paths) < 2 {
		t.Fatal("two retained connection cases required", err)
	}
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var c Connection
		if err := json.Unmarshal(raw, &c); err != nil {
			t.Fatal(err)
		}
		manifest, err := os.ReadFile(filepath.Join(root, "facts/fec/schedule-a/columnar/manifests", c.Source.FactSetID+".json"))
		if err != nil {
			t.Fatal(err)
		}
		h := sha256.Sum256(manifest)
		if hex.EncodeToString(h[:]) != c.Inputs.A.ManifestSHA256 {
			t.Fatal("manifest digest mismatch")
		}
		var m occ.ScheduleAColumnarManifest
		if err := json.Unmarshal(manifest, &m); err != nil {
			t.Fatal(err)
		}
		found := 0
		for _, shard := range m.Shards {
			if c.Source.Ordinal < shard.FirstSourceRowOrdinal || c.Source.Ordinal > shard.LastSourceRowOrdinal {
				continue
			}
			found++
			f, err := os.Open(filepath.Join(root, shard.StorageKey))
			if err != nil {
				t.Fatal(err)
			}
			hash := sha256.New()
			n, err := io.Copy(hash, f)
			if err != nil || uint64(n) != shard.Bytes || hex.EncodeToString(hash.Sum(nil)) != shard.SHA256 || c.Source.ShardSHA256 != shard.SHA256 {
				f.Close()
				t.Fatal("physical shard mismatch", err)
			}
			p, err := parquet.OpenFile(f, n)
			if err != nil {
				f.Close()
				t.Fatal(err)
			}
			reader := parquet.NewReader(p)
			if err := reader.SeekToRow(int64(c.Source.Ordinal - shard.FirstSourceRowOrdinal)); err != nil {
				t.Fatal(err)
			}
			rows := make([]parquet.Row, 1)
			count, err := reader.ReadRows(rows)
			if count != 1 || err != nil && err != io.EOF {
				t.Fatal(count, err)
			}
			columns := p.Schema().Columns()
			fields := map[string]any{}
			for _, v := range rows[0] {
				key := columns[v.Column()][0]
				switch {
				case v.IsNull():
					fields[key] = nil
				case v.Kind() == parquet.ByteArray:
					fields[key] = string(v.ByteArray())
				case v.Kind() == parquet.Boolean:
					fields[key] = v.Boolean()
				case v.Kind() == parquet.Int32:
					fields[key] = v.Int32()
				case v.Kind() == parquet.Int64:
					fields[key] = strconv.FormatInt(v.Int64(), 10)
				default:
					t.Fatal("unexpected physical type")
				}
			}
			reader.Close()
			f.Close()
			actual, err := json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			expected, err := json.Marshal(c.Source.Fields)
			if err != nil || len(fields) != 99 || !bytes.Equal(actual, expected) {
				t.Fatal("full physical row differs", err)
			}
		}
		if found != 1 {
			t.Fatal("source ordinal not in exactly one shard")
		}
	}
}
