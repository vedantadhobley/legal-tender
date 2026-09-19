package summarypublication

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/committeesummary"
	"github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

func TestCommitteeSummaryCorpusArtifacts(t *testing.T) {
	sourceRoot := os.Getenv("LT_COMMITTEE_SUMMARY_REVIEW")
	if sourceRoot == "" {
		t.Skip("requires retained committee-summary review CSVs")
	}
	var review struct {
		Profiles []struct {
			Cycle  int    `json:"cycle"`
			File   string `json:"file"`
			Bytes  int64  `json:"size_bytes"`
			SHA256 string `json:"sha256"`
			Rows   uint64 `json:"rows"`
		} `json:"profiles"`
	}
	raw, err := os.ReadFile("../../../../contracts/sources/fec/committee-summary/v1/review.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &review); err != nil {
		t.Fatal(err)
	}
	for _, profile := range review.Profiles {
		t.Run(fmt.Sprint(profile.Cycle), func(t *testing.T) {
			root := t.TempDir()
			if out := os.Getenv("LT_SUMMARY_CORPUS_OUTPUT"); out != "" {
				root = filepath.Join(out, fmt.Sprint(profile.Cycle))
				if err := os.Mkdir(root, 0o750); err != nil {
					t.Fatal(err)
				}
			}
			input, err := os.Open(filepath.Join(sourceRoot, profile.File))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = input.Close() }()
			expected := committeesummary.Expected{Cycle: fmt.Sprint(profile.Cycle), Bytes: profile.Bytes, SHA256: profile.SHA256}
			facts, verification, err := buildRows(context.Background(), root, input, expected)
			if err != nil {
				t.Fatal(err)
			}
			if facts.RecordCount != profile.Rows {
				t.Fatal("row conservation")
			}
			if _, err := input.Seek(0, 0); err != nil {
				t.Fatal(err)
			}
			replay, second, err := buildRows(context.Background(), root, input, expected)
			if err != nil || !reflect.DeepEqual(facts, replay) || !reflect.DeepEqual(verification, second) {
				t.Fatal("nonidentical corpus replay", err)
			}
			result := struct {
				Scope           string                        `json:"scope"`
				Facts           artifact.Descriptor           `json:"facts"`
				Verification    committeesummary.Verification `json:"verification"`
				ReplayIdentical bool                          `json:"replay_identical"`
			}{"unpublished_artifact_gate_not_a_coordinated_release", facts, verification, true}
			if err := writeImmutable(filepath.Join(root, "artifact-gate.json"), result); err != nil {
				t.Fatal(err)
			}
			t.Logf("cycle=%d records=%d compressed_bytes=%d uncompressed_bytes=%d replay=identical", profile.Cycle, facts.RecordCount, facts.CompressedBytes, facts.UncompressedBytes)
		})
	}
}
