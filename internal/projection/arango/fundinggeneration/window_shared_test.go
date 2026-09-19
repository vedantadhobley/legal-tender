package fundinggeneration

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	flow "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
)

func TestSharedWindowSpecificationIsExplicit(t *testing.T) {
	input := WindowInput{Generation: "extended", GenerationSHA256: valueID("bytes"), GraphManifest: "base-graph", Participants: "participants", Conduits: "old", Shared: &SharedReadOptions{BaseGeneration: "base", GraphManifest: "shared-graph", Conduits: "new"}}
	spec := WindowInputSpec{Version: WindowSharedInputsVersion, Inputs: []WindowInput{input}}
	path := filepath.Join(t.TempDir(), "inputs.json")
	check := func(s WindowInputSpec, accept bool) {
		t.Helper()
		b, err := json.Marshal(s)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(path, b, 0600); err != nil {
			t.Fatal(err)
		}
		_, err = ReadWindowInputs(path, fileSHA(b))
		if (err == nil) != accept {
			t.Fatal("unexpected shared spec admission", err)
		}
	}
	check(spec, true)
	spec.Version = WindowInputsVersion
	check(spec, false)
	spec.Version = WindowSharedInputsVersion
	spec.Inputs[0].Shared = nil
	check(spec, false)
	spec.Inputs[0].Shared = &SharedReadOptions{}
	check(spec, false)
	_, err := OpenWindowReader(context.Background(), WindowOpenOptions{Inputs: spec.Inputs, StorageRoot: "unused", Endpoint: "http://unused.invalid", BuildSHA256: valueID("build")})
	if err == nil || err.Error() != "complete shared generation locators required" {
		t.Fatal("programmatic opener ignored empty extension", err)
	}
}

func TestSharedWindowAdmissionUsesBasePopulationAndOuterIdentity(t *testing.T) {
	r := windowFixture(t, flow.ScheduleA)
	base := r.partitions[0].publication
	outer := base
	outer.Shared = &SharedGeneration{GenerationID: valueID("extended"), Base: base.Generation}
	outer.GenerationID = outer.Shared.GenerationID
	if err := validateWindowPublications([]WindowPublication{outer, r.partitions[1].publication}); err != nil {
		t.Fatal(err)
	}
	if validateWindowPublications([]WindowPublication{base, outer}) == nil {
		t.Fatal("base and extension counted twice")
	}
	outer.GenerationID = base.GenerationID
	if validateWindowPublications([]WindowPublication{outer}) == nil {
		t.Fatal("base ID aliased to outer identity")
	}
}
