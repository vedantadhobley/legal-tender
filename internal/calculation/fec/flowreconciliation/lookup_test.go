package flowreconciliation

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestSourceLookupRejectsForeignAndUnselectedLocators(t *testing.T) {
	o, inputs, a, b := publicationFixture(t)
	r, err := publishLoaded(context.Background(), o, inputs, a, b)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(o.OutputRoot, "current", o.Cycle+".json")
	for _, refs := range [][]SourceLocator{nil, {{"both", r.Input.B.FactSetID, 1}}, {{"schedule_b", "foreign", 1}}, {{"schedule_b", r.Input.B.FactSetID, 0}}, {{"schedule_b", r.Input.B.FactSetID, 99999}}, {{"schedule_b", r.Input.B.FactSetID, 1}, {"schedule_b", r.Input.B.FactSetID, 1}}} {
		_, _, err := LookupSources(context.Background(), o.StorageRoot, path, refs)
		if err == nil || strings.Contains(err.Error(), "no such file") {
			t.Fatal("bad locator passed selection guard", err)
		}
	}
}
