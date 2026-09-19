package personaffiliation

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"

	screen "github.com/vedantadhobley/legal-tender/internal/calculation/personaffiliation"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportscope"
	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

type AppearanceSelection struct {
	Policy                    string              `json:"policy"`
	CorpusSHA256              string              `json:"corpus_sha256"`
	FilingSHA256              string              `json:"filing_sha256"`
	PopulationScanned         bool                `json:"population_scanned"`
	EligibleOccurrences       int                 `json:"eligible_occurrences"`
	OutsideProfileOccurrences int                 `json:"outside_profile_occurrences"`
	ExcludedReviewedInputs    int                 `json:"excluded_reviewed_name_employer_inputs"`
	DistinctInputs            int                 `json:"distinct_name_employer_inputs"`
	Appearances               []screen.Appearance `json:"appearances"`
}

// SelectAppearances reads only source metadata and FEC rows from the corpus.
// No reviewed company claims, role labels or expected results are loaded. A
// positive sample size selects additional exact name/employer pairs by SHA256,
// excluding the reviewed pairs. This is a reproducible diagnostic sample, not
// a population estimate, person deduplication or receipt-counting decision.
func SelectAppearances(ctx context.Context, directory, pin string, sampleSize int) (AppearanceSelection, error) {
	if sampleSize < 0 || sampleSize > 5 {
		return AppearanceSelection{}, fmt.Errorf("sample size must be 0..5 (discovery request budget)")
	}
	raw, err := read(directory, Artifact{Path: "corpus.json", SHA256: pin}, 1<<20)
	if err != nil {
		return AppearanceSelection{}, err
	}
	var c Corpus
	if err := strictjson.Decode(raw, &c); err != nil {
		return AppearanceSelection{}, err
	}
	if c.Version != Version || c.Layout.SHA256 != layoutPin || len(c.Cases) == 0 || len(c.Cases) > wikimedia.MaxQueries {
		return AppearanceSelection{}, fmt.Errorf("unsupported selection corpus or layout")
	}
	if err := validateSource(c.Filing); err != nil {
		return AppearanceSelection{}, err
	}
	layout, err := read(directory, c.Layout, maxArtifact)
	if err != nil {
		return AppearanceSelection{}, err
	}
	var spec struct {
		Fields []struct {
			Name     string
			Sequence int
		}
	}
	if err := json.Unmarshal(layout, &spec); err != nil {
		return AppearanceSelection{}, err
	}
	positions := map[string]int{}
	for _, f := range spec.Fields {
		positions[f.Name] = f.Sequence - 1
	}
	if _, err := read(directory, c.Filing.Body, maxArtifact); err != nil {
		return AppearanceSelection{}, err
	}
	if _, err := read(directory, c.Headers, 128<<10); err != nil {
		return AppearanceSelection{}, err
	}
	filing, err := reportscope.AssessElectronic(ctx, reportscope.Request{SourceURL: c.Filing.URL,
		BodyPath: filepath.Join(directory, c.Filing.Body.Path), BodySHA256: c.Filing.Body.SHA256,
		HeadersPath: filepath.Join(directory, c.Headers.Path), HeadersSHA256: c.Headers.SHA256})
	if err != nil {
		return AppearanceSelection{}, err
	}
	if filing.Disposition != "electronic_cover_parsed" || filing.CaptureExtent != "complete_response" {
		return AppearanceSelection{}, fmt.Errorf("selection requires a complete supported filing")
	}
	r := AppearanceSelection{Policy: "reviewed_fec_occurrences.v1", CorpusSHA256: pin, FilingSHA256: c.Filing.Body.SHA256, Appearances: []screen.Appearance{}}
	reviewed, ordinals := map[string]bool{}, map[int]bool{}
	for _, tc := range c.Cases {
		if tc.Ordinal < 3 || tc.Ordinal > len(filing.Records) || ordinals[tc.Ordinal] {
			return AppearanceSelection{}, fmt.Errorf("invalid or duplicate reviewed ordinal")
		}
		ordinals[tc.Ordinal] = true
		rec := filing.Records[tc.Ordinal-1]
		fields, err := selectedFields(rec, positions, filing.Cover)
		if err != nil {
			return AppearanceSelection{}, err
		}
		a := appearance(fields, rec, c.Filing.Body.SHA256)
		reviewed[searchInputKey(a)] = true
		r.Appearances = append(r.Appearances, a)
	}
	if sampleSize == 0 {
		return r, nil
	}
	r.Policy = "sha256_name_employer_excluding_reviewed.v1"
	r.PopulationScanned = true
	r.Appearances = []screen.Appearance{}
	r.ExcludedReviewedInputs = len(reviewed)
	unique := map[string]screen.Appearance{}
	for _, rec := range filing.Records[2:] {
		if err := ctx.Err(); err != nil {
			return AppearanceSelection{}, err
		}
		fields, err := selectedFields(rec, positions, filing.Cover)
		if err != nil {
			r.OutsideProfileOccurrences++
			continue
		}
		a := appearance(fields, rec, c.Filing.Body.SHA256)
		r.EligibleOccurrences++
		key := searchInputKey(a)
		if reviewed[key] {
			continue
		}
		if _, exists := unique[key]; !exists {
			unique[key] = a
		}
	}
	r.DistinctInputs = len(unique)
	keys := make([]string, 0, len(unique))
	for key := range unique {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := wikimedia.Hash([]byte(keys[i])), wikimedia.Hash([]byte(keys[j]))
		if a != b {
			return a < b
		}
		return keys[i] < keys[j]
	})
	if len(keys) < sampleSize {
		return AppearanceSelection{}, fmt.Errorf("sample exceeds additional distinct source inputs")
	}
	for _, key := range keys[:sampleSize] {
		r.Appearances = append(r.Appearances, unique[key])
	}
	return r, nil
}

func searchInputKey(a screen.Appearance) string {
	b, _ := json.Marshal([2]*string{a.Receipt.Name, a.Receipt.Employer})
	return string(b)
}

func (s AppearanceSelection) DiscoveryPlan(build, policy string) (wikimedia.DiscoveryPlan, error) {
	inputs := make([]wikimedia.DiscoveryAppearance, 0, len(s.Appearances))
	for _, a := range s.Appearances {
		inputs = append(inputs, wikimedia.DiscoveryAppearance{SHA256: a.Source.SHA256, Locator: a.Source.Locator, Name: a.Receipt.Name, Employer: a.Receipt.Employer})
	}
	return wikimedia.PlanDiscoveryWithPolicy(inputs, fmt.Sprintf("%s; corpus=%s; appearances=%d", s.Policy, s.CorpusSHA256, len(s.Appearances)), build, policy)
}
