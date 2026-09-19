package organizationresolution

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	resolver "github.com/vedantadhobley/legal-tender/internal/calculation/organizationresolution"
	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

func jsonFile(t *testing.T, dir, name string, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, name), b, 0600); err != nil {
		t.Fatal(err)
	}
	return wikimedia.Hash(b)
}

func fixture(t *testing.T) (Options, Corpus) {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "capture")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	build := wikimedia.Hash([]byte("test build"))
	queries := wikimedia.Queries{Version: "organization-queries.v1", Selection: "synthetic evaluation boundary", BuildSHA256: build}
	m := wikimedia.Manifest{Contract: wikimedia.Contract, BuildSHA256: build, UserAgent: "LegalTender/test (test@local)"}
	for i := 0; i < 8; i++ {
		text := fmt.Sprintf("Example %d", i)
		id := fmt.Sprintf("Q%d", 100+i)
		queries.Queries = append(queries.Queries, wikimedia.Query{Text: text, References: []wikimedia.Reference{{FactSetID: "facts", ManifestSHA256: wikimedia.Hash([]byte("facts")), Field: "CONNECTED_ORG_NM", FactID: fmt.Sprint(i)}}})
		label := text
		if i == 2 || i == 3 || i == 7 {
			label = "Other organization"
		}
		p := wikimedia.Page{ID: 1, Index: 1, Title: label, Props: map[string]string{"wikibase_item": id}, Revisions: []wikimedia.Revision{{ID: 1, Parent: 0, Timestamp: "2026-09-15T00:00:00Z"}}}
		search := map[string]any{"batchcomplete": true, "query": map[string]any{"pages": []wikimedia.Page{p}}}
		e := wikimedia.Entity{ID: id, Type: "item", PageID: 1, Title: id, LastRevision: 1, Modified: "2026-09-15T00:00:00Z", Labels: map[string]wikimedia.Term{"en": {Language: "en", Value: label}}}
		if i == 6 {
			e = wikimedia.Entity{ID: id, Missing: json.RawMessage(`true`)}
		}
		// Keep explicit ns=0: Entity's output type omits zero namespace.
		b, _ := json.Marshal(e)
		var em map[string]any
		_ = json.Unmarshal(b, &em)
		if i != 6 {
			em["ns"] = 0
		}
		entities := map[string]any{"success": 1, "entities": map[string]any{id: em}}
		response := func(url string, v any) wikimedia.Response {
			b, err := json.Marshal(v)
			if err != nil {
				t.Fatal(err)
			}
			sha := wikimedia.Hash(b)
			if err = os.WriteFile(filepath.Join(dir, sha+".body"), b, 0600); err != nil {
				t.Fatal(err)
			}
			return wikimedia.Response{URL: url, ObservedAt: "2026-09-15T00:00:00Z", Status: 200, Headers: map[string]string{"Content-Type": "application/json"}, Bytes: len(b), SHA256: sha, Body: sha + ".body"}
		}
		entry := wikimedia.Entry{Query: i, Search: response(wikimedia.SearchURL(text), search)}
		if i == 5 {
			entry.Search.Status = 429
			entry.Search.Failure = "http_status_not_ok"
		} else {
			r := response(wikimedia.EntityURL([]string{id}), entities)
			entry.Entities = &r
		}
		m.Entries = append(m.Entries, entry)
	}
	m.QueriesSHA256 = jsonFile(t, dir, "queries.json", queries)
	sha := jsonFile(t, dir, "capture.json", m)
	source := []byte("A primary-source fixture states the organization identity.")
	if err := os.WriteFile(filepath.Join(root, "primary.txt"), source, 0600); err != nil {
		t.Fatal(err)
	}
	c := Corpus{Version: Version, CaptureSHA256: sha, ReviewedOn: "2026-09-15", ReviewBasis: "Synthetic boundary tests; not real identity authority", Scope: "Synthetic evaluation", Sources: []Source{{ID: "primary", Kind: "official_registry", URL: "https://registry.example.org/organizations", ObservedOn: "2026-09-15", Path: "primary.txt", SHA256: wikimedia.Hash(source), Bytes: len(source), Excerpt: "states the organization identity"}}}
	for _, i := range []int{0, 1, 2, 3, 5, 6, 7} {
		id := fmt.Sprintf("Q%d", 100+i)
		rel := "same_entity"
		if i == 1 || i == 7 {
			rel = "different_entity"
		}
		if i == 3 {
			id = "Q999"
		}
		c.Labels = append(c.Labels, Label{Query: i, Text: queries.Queries[i].Text, QID: id, Relation: rel, Reason: "Synthetic reviewed identity assertion", Sources: []string{"primary"}})
	}
	corpusSHA := jsonFile(t, root, "corpus.json", c)
	return Options{CaptureDirectory: dir, CaptureSHA256: sha, CorpusPath: filepath.Join(root, "corpus.json"), CorpusSHA256: corpusSHA, BuildSHA256: build}, c
}

func TestEvaluationSeparatesEveryDenominator(t *testing.T) {
	o, _ := fixture(t)
	r, err := Run(o)
	if err != nil {
		t.Fatal(err)
	}
	want := Counts{Queries: 8, SourceUsable: 7, SourceUnusable: 1, ReviewedQueries: 7, UnreviewedQueries: 1, ObservedCandidatePairs: 7, ReviewedObservedPairs: 5, UnreviewedObservedPairs: 2, SupportedProposals: 1, ContradictedProposals: 1, UnreviewedProposals: 1, Abstentions: 4, PositiveLabels: 5, PositiveProposed: 1, PositiveRetrievedNotProposed: 1, PositiveNotRetrieved: 1, PositiveEntityMissing: 1, PositiveUnassessed: 1, NegativeLabels: 2, NegativeProposed: 1, NegativeNotProposed: 1}
	if r.Counts != want {
		t.Fatalf("counts: %+v; want %+v", r.Counts, want)
	}
	if r.IdentityPublicationApproved {
		t.Fatal("evaluation approved identities")
	}
	again, err := Run(o)
	if err != nil || !reflect.DeepEqual(r, again) {
		t.Fatal("offline replay differs", err)
	}
	for i, state := range []string{"supported_proposal", "contradicted_proposal", "abstained", "abstained", "unreviewed_proposal", "source_unusable", "abstained", "abstained"} {
		if r.Cases[i].ProposalReview != state {
			t.Fatal(i, r.Cases[i])
		}
	}
}

func TestLabelsCannotFeedResolver(t *testing.T) {
	for _, policy := range []string{resolver.Policy, resolver.ExpandedPolicy} {
		t.Run(policy, func(t *testing.T) {
			o, c := fixture(t)
			o.ProposalPolicy = policy
			r, err := wikimedia.Read(o.CaptureDirectory, o.CaptureSHA256)
			if err != nil {
				t.Fatal(err)
			}
			before, err := resolver.ResolveWithPolicy(r, o.BuildSHA256, policy)
			if err != nil {
				t.Fatal(err)
			}
			c.Labels[0].Relation = "different_entity"
			o.CorpusSHA256 = jsonFile(t, filepath.Dir(o.CorpusPath), "corpus.json", c)
			result, err := Run(o)
			if err != nil {
				t.Fatal(err)
			}
			after, err := resolver.ResolveWithPolicy(r, o.BuildSHA256, policy)
			if err != nil || result.Counts.ContradictedProposals != 2 || !reflect.DeepEqual(before, after) || result.Cases[0].ProposedQID != before.Decisions[0].ProposedQID {
				t.Fatal("review labels changed resolver")
			}
		})
	}
}

func TestUnknownProposalPolicyRejected(t *testing.T) {
	if _, err := Run(Options{ProposalPolicy: "latest"}); err == nil || !strings.Contains(err.Error(), "policy") {
		t.Fatal("unknown policy was not rejected before reading inputs", err)
	}
}

func TestInvalidEvaluationEvidenceFailsClosed(t *testing.T) {
	for name, mutate := range map[string]func(*Corpus){
		"wrong_capture":      func(c *Corpus) { c.CaptureSHA256 = wikimedia.Hash([]byte("other")) },
		"conflicting_label":  func(c *Corpus) { l := c.Labels[0]; l.Relation = "different_entity"; c.Labels = append(c.Labels, l) },
		"wrong_text":         func(c *Corpus) { c.Labels[0].Text = "invented input" },
		"missing_source":     func(c *Corpus) { c.Labels[0].Sources = []string{"missing"} },
		"self_corroboration": func(c *Corpus) { c.Sources[0].URL = "https://www.wikidata.org/wiki/Q100" },
		"source_traversal":   func(c *Corpus) { c.Sources[0].Path = "../outside" },
		"quote_absent":       func(c *Corpus) { c.Sources[0].Excerpt = "not in the source" },
		"source_digest":      func(c *Corpus) { c.Sources[0].SHA256 = wikimedia.Hash([]byte("changed")) },
		"future_evidence":    func(c *Corpus) { c.Sources[0].ObservedOn = "2026-09-16" },
		"invented_relation":  func(c *Corpus) { c.Labels[0].Relation = "probably_the_same" },
	} {
		t.Run(name, func(t *testing.T) {
			o, c := fixture(t)
			mutate(&c)
			o.CorpusSHA256 = jsonFile(t, filepath.Dir(o.CorpusPath), "corpus.json", c)
			if _, err := Run(o); err == nil {
				t.Fatal("bad review accepted")
			}
		})
	}
	o, _ := fixture(t)
	o.CorpusSHA256 = wikimedia.Hash([]byte("wrong"))
	if _, err := Run(o); err == nil {
		t.Fatal("wrong corpus digest accepted")
	}
}

func TestSourceSymlinkAndInvalidJSON(t *testing.T) {
	o, c := fixture(t)
	root := filepath.Dir(o.CorpusPath)
	if err := os.Symlink("primary.txt", filepath.Join(root, "alias.txt")); err != nil {
		t.Fatal(err)
	}
	c.Sources[0].Path = "alias.txt"
	o.CorpusSHA256 = jsonFile(t, root, "corpus.json", c)
	if _, err := Run(o); err == nil {
		t.Fatal("source symlink accepted")
	}
	b := []byte(`{"schema_version":"organization-evaluation.v1","schema_version":"bad"}`)
	if err := os.WriteFile(o.CorpusPath, b, 0600); err != nil {
		t.Fatal(err)
	}
	o.CorpusSHA256 = wikimedia.Hash(b)
	if _, err := Run(o); err == nil {
		t.Fatal("duplicate JSON accepted")
	}
}

func TestFirstQueryIndexCannotBeImplicit(t *testing.T) {
	for _, replacement := range []string{"", `"query_index":null,`} {
		o, _ := fixture(t)
		b, err := os.ReadFile(o.CorpusPath)
		if err != nil {
			t.Fatal(err)
		}
		b = []byte(strings.Replace(string(b), `"query_index":0,`, replacement, 1))
		if err = os.WriteFile(o.CorpusPath, b, 0600); err != nil {
			t.Fatal(err)
		}
		o.CorpusSHA256 = wikimedia.Hash(b)
		if _, err = Run(o); err == nil {
			t.Fatal("absent/null index silently bound to first query")
		}
	}
}
