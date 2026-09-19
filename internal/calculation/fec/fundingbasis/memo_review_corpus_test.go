package fundingbasis

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// This opt-in audit selects witnesses from the retained complete v2 profile.
// It uses older immutable facts without relabeling their source ancestry.
// The independent test compares both representations before drawing conclusions.
func TestMemoReviewPublishedCorpus(t *testing.T) {
	out := os.Getenv("LT_MEMO_REVIEW_DIR")
	if out == "" {
		t.Skip("requires selected profile, accepted inventory, and audit output")
	}
	body, err := os.ReadFile(filepath.Join(out, "selected-profile.json"))
	if err != nil {
		t.Fatal(err)
	}
	var cohort struct {
		Reports []struct{ Committee, File string } `json:"reports"`
	}
	if err := json.Unmarshal(body, &cohort); err != nil || len(cohort.Reports) != 11 {
		t.Fatal("unexpected bounded audit cohort", err)
	}
	ctx := context.Background()
	r, err := Open(ctx, os.Getenv("LT_MEMO_REVIEW_STORAGE"), os.Getenv("LT_MEMO_REVIEW_BASIS"))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range cohort.Reports {
		t.Run(tc.File, func(t *testing.T) {
			report, err := r.ReviewReport(ctx, tc.Committee, tc.File, func(s string) { t.Log(s) })
			if err != nil {
				t.Fatal(err)
			}
			lines, err := assembleReportLines(ctx, report, ReceiptSource{r.manifest.SourceReleaseID, r.manifest.SourceReleaseManifestSHA256})
			if err != nil {
				t.Fatal(err)
			}
			for suffix, value := range map[string]any{"report": report, "lines": lines} {
				body, err := json.MarshalIndent(value, "", "  ")
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(out, tc.File+"-"+suffix+".json"), body, 0o640); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
