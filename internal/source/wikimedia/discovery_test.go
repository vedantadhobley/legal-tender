package wikimedia

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func discoveryInput(locator string) DiscoveryAppearance {
	name, employer := "Example Person", "Example Corp."
	return DiscoveryAppearance{SHA256: Hash([]byte("source")), Locator: locator, Name: &name, Employer: &employer}
}

func TestDiscoveryPlanPreservesAppearances(t *testing.T) {
	inputs := []DiscoveryAppearance{discoveryInput("row:2"), discoveryInput("row:1")}
	p, err := PlanDiscovery(inputs, "fixture", Hash([]byte("build")))
	if err != nil || len(p.Searches) != 3 || len(p.Appearances) != 2 {
		t.Fatal("plan", err)
	}
	if inputs[0].Locator != "row:2" || *inputs[0].Employer != "Example Corp." {
		t.Fatal("source mutated")
	}
	for _, s := range p.Searches {
		if !slices.Equal(s.Appearances, []int{0, 1}) {
			t.Fatal("query sharing lost occurrences")
		}
	}
	if p.Searches[0].Kind != "person_name" || p.Searches[0].Text != `"Example" "Person"` || p.Searches[2].Text != `"Example" "Corp"` {
		t.Fatal("unexpected query rendering")
	}
	slices.Reverse(inputs)
	again, err := PlanDiscovery(inputs, "fixture", p.BuildSHA256)
	if err != nil || !reflect.DeepEqual(p, again) {
		t.Fatal("input-order dependence")
	}
	b, _ := json.Marshal(p)
	if _, _, err := decodeDiscovery(b); err != nil {
		t.Fatal(err)
	}
	p.Searches[0].Text = "injected target"
	b, _ = json.Marshal(p)
	if _, _, err := decodeDiscovery(b); err == nil {
		t.Fatal("accepted query not derived from inputs")
	}
}

func TestDiscoveryNullsAndBoundaries(t *testing.T) {
	a := discoveryInput("row:1")
	blank := "   "
	a.Name, a.Employer = nil, &blank
	p, err := PlanDiscovery([]DiscoveryAppearance{a}, "fixture", Hash([]byte("build")))
	if err != nil || len(p.Searches) != 0 || p.InputStates[0] != "no_searchable_text" || p.Appearances[0].Name != nil || *p.Appearances[0].Employer != blank {
		t.Fatal("null/blank collapsed", err)
	}
	for _, raw := range []string{"", "[]", `{"policy":"future"}`} {
		if _, _, err := decodeDiscovery([]byte(raw)); err == nil {
			t.Fatal("invalid plan accepted")
		}
	}
	for _, change := range []func(*DiscoveryAppearance){
		func(a *DiscoveryAppearance) { a.SHA256 = "bad" },
		func(a *DiscoveryAppearance) { a.Locator = "" },
		func(a *DiscoveryAppearance) { s := "x\ny"; a.Name = &s },
		func(a *DiscoveryAppearance) { s := strings.Repeat("x", 501); a.Employer = &s },
		func(a *DiscoveryAppearance) { s := "\xff"; a.Name = &s },
	} {
		a := discoveryInput("row:1")
		change(&a)
		if _, err := PlanDiscovery([]DiscoveryAppearance{a}, "fixture", Hash([]byte("build"))); err == nil {
			t.Fatal("invalid source accepted")
		}
	}
	a = discoveryInput("row:1")
	if _, err := PlanDiscovery([]DiscoveryAppearance{a, a}, "fixture", Hash([]byte("build"))); err == nil {
		t.Fatal("duplicate source accepted")
	}
	s := "Doe, Jane OR intitle:Other +Corp"
	if got := discoveryTerms(&s); got != `"Doe" "Jane" "OR" "intitle" "Other" "Corp"` {
		t.Fatal("query operators or name reordering", got)
	}
	inputs := []DiscoveryAppearance{}
	for i := 0; i < 8; i++ {
		a := discoveryInput(string(rune('a' + i)))
		s := string(rune('a' + i))
		a.Name, a.Employer = &s, &s
		inputs = append(inputs, a)
	}
	if _, err := PlanDiscovery(inputs, "fixture", Hash([]byte("build"))); err == nil {
		t.Fatal("unbounded query fanout")
	}
}

func discoveryBodies(t *testing.T) ([]byte, []byte) {
	t.Helper()
	pages := []Page{}
	for i, qid := range []string{"Q1", "Q2", "Q3", "", "Q4"} {
		p := Page{ID: int64(i + 1), Namespace: 0, Title: "Same-looking result", Index: i + 1, Props: map[string]string{}, Revisions: []Revision{{ID: 1, Timestamp: "2026-09-15T00:00:00Z"}}}
		if qid != "" {
			p.Props["wikibase_item"] = qid
		}
		if qid == "Q4" {
			p.Props["disambiguation"] = ""
		}
		pages = append(pages, p)
	}
	search, _ := json.Marshal(map[string]any{"batchcomplete": true, "continue": map[string]any{"gsroffset": 5, "continue": "gsroffset||"}, "query": map[string]any{"pages": pages}})
	b := roleBody(map[string]any{"P169": []any{roleRow("Q1", "P169", "Q2")}})
	var root map[string]any
	if err := json.Unmarshal(b, &root); err != nil {
		t.Fatal(err)
	}
	entities := root["entities"].(map[string]any)
	entities["Q3"] = map[string]any{"id": "Q3", "missing": true}
	entities["Q4"] = map[string]any{"id": "Q4", "title": "Q4", "type": "item", "pageid": 4, "ns": 0, "lastrevid": 1, "modified": "2026-09-15T00:00:00Z", "claims": map[string]any{}}
	entitiesBody, _ := json.Marshal(root)
	return search, entitiesBody
}

func TestDiscoveryCaptureReplayAndGaps(t *testing.T) {
	p, err := PlanDiscovery([]DiscoveryAppearance{discoveryInput("row:1")}, "fixture", Hash([]byte("build")))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(p)
	search, entities := discoveryBodies(t)
	for _, fail := range []string{"", "search", "entity"} {
		t.Run(fail, func(t *testing.T) {
			calls := 0
			client := &http.Client{Transport: roundTrip(func(req *http.Request) (*http.Response, error) {
				calls++
				if req.URL.Query().Get("maxlag") != "5" || req.Header.Get("User-Agent") != "LegalTender/test (test@local)" {
					t.Fatal("background policy changed")
				}
				body := search
				if req.URL.Host == "www.wikidata.org" {
					if req.URL.String() != EntityURL([]string{"Q1", "Q2", "Q3", "Q4"}) {
						t.Fatal("IDs were not derived from pages")
					}
					body = entities
				} else if calls%2 != 1 || req.URL.String() != SearchURL(p.Searches[(calls-1)/2].Text) {
					t.Fatal("unexpected search")
				}
				if (fail == "search" && calls == 1) || (fail == "entity" && calls == 2) {
					body = []byte(`{"error":{"code":"maxlag"}}`)
				}
				return &http.Response{StatusCode: 200, ContentLength: int64(len(body)), Header: http.Header{"Content-Type": []string{"application/json"}, "Set-Cookie": []string{"private"}, "X-Client-IP": []string{"private"}}, Body: io.NopCloser(strings.NewReader(string(body)))}, nil
			})}
			dir := filepath.Join(t.TempDir(), "capture")
			pin, err := captureDiscovery(context.Background(), raw, CaptureOptions{Directory: dir, UserAgent: "LegalTender/test (test@local)", BuildSHA256: p.BuildSHA256}, client, 0)
			if err != nil {
				t.Fatal(err)
			}
			r, err := ReadDiscovery(dir, pin)
			if err != nil {
				t.Fatal(err)
			}
			again, err := ReadDiscovery(dir, pin)
			if err != nil || !reflect.DeepEqual(r, again) {
				t.Fatal("offline replay changed", err)
			}
			if r.IdentityApproved || r.GraphPublicationApproved || r.FinancialAttribution || r.DiscoveryComplete || len(r.Observations) != 3 {
				t.Fatal("approval or population loss")
			}
			manifest, _ := os.ReadFile(filepath.Join(dir, "capture.json"))
			if strings.Contains(string(manifest), "private") {
				t.Fatal("private header captured")
			}
			if fail != "" {
				want := 1
				if fail == "entity" {
					want = 2
				}
				if calls != want || r.CaptureUsable || r.Observations[0].State != "source_unusable_or_unattempted" || r.Observations[2].State != "source_unusable_or_unattempted" {
					t.Fatal("continued after failure or manufactured absence")
				}
			} else {
				if calls != 6 || !r.CaptureUsable {
					t.Fatal("capture", calls)
				}
				for _, o := range r.Observations {
					if len(o.Candidates) != 5 || o.Roles == nil || len(o.Roles.Entities) != 4 || !o.Candidates[1].HumanStatementObserved || o.Candidates[0].HumanStatementObserved || o.Candidates[2].State != "source_entity_missing" || o.Candidates[3].State != "page_has_no_qid" || o.Candidates[4].State != "disambiguation_page" {
						t.Fatal("candidate gaps/types or role extraction lost")
					}
				}
			}
			if _, err := Read(dir, pin); err == nil {
				t.Fatal("affiliation capture accepted as organization capture")
			}
			if _, err := ReadDiscovery(dir, Hash([]byte("wrong"))); err == nil {
				t.Fatal("wrong pin accepted")
			}
			bodyPath := filepath.Join(dir, r.Manifest.Entries[0].Search.Body)
			if err := os.WriteFile(bodyPath, []byte("altered"), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := ReadDiscovery(dir, pin); err == nil {
				t.Fatal("corrupt body accepted")
			}
		})
	}
}
