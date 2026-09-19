package wikimedia

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestDiscoveryVariantsCaptureReplay(t *testing.T) {
	p, err := PlanDiscoveryWithPolicy([]DiscoveryAppearance{discoveryInput("row:1")}, "fixture", Hash([]byte("build")), DiscoveryVariantsPolicy)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(p)
	search, entities := discoveryBodies(t)
	calls := 0
	client := &http.Client{Transport: roundTrip(func(req *http.Request) (*http.Response, error) {
		calls++
		body := search
		if req.URL.Query().Get("maxlag") != "5" {
			t.Fatal("background policy changed")
		}
		if req.URL.Host == "www.wikidata.org" {
			body = entities
		}
		return &http.Response{StatusCode: 200, ContentLength: int64(len(body)), Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(string(body)))}, nil
	})}
	dir := filepath.Join(t.TempDir(), "capture")
	pin, err := captureDiscovery(context.Background(), raw, CaptureOptions{Directory: dir, UserAgent: "LegalTender/test (test@local)", BuildSHA256: p.BuildSHA256}, client, 0)
	if err != nil {
		t.Fatal(err)
	}
	r, err := ReadDiscovery(dir, pin)
	if err != nil || !r.CaptureUsable || !reflect.DeepEqual(p, r.Plan) || calls != len(p.Searches)*2 {
		t.Fatal("v2 capture/replay", err)
	}
}

func TestDiscoveryVariantsExplicitAndBounded(t *testing.T) {
	a := discoveryInput("row:1")
	name, employer := "Person7, Example", "Route7B Corp."
	a.Name, a.Employer = &name, &employer
	b := a
	b.Locator = "row:2"
	base, err := PlanDiscovery([]DiscoveryAppearance{a, b}, "fixture", Hash([]byte("build")))
	if err != nil {
		t.Fatal(err)
	}
	p, err := PlanDiscoveryWithPolicy([]DiscoveryAppearance{b, a}, "fixture", base.BuildSHA256, DiscoveryVariantsPolicy)
	if err != nil || len(p.Searches) != 9 {
		t.Fatal("variant count", len(p.Searches), err)
	}
	for _, s := range p.Searches {
		if !slices.Equal(s.Appearances, []int{0, 1}) || len(s.Derivations) != 2 {
			t.Fatal("lost occurrence or rule", s)
		}
		if s.Kind == "person_name" && s.Text != `"Person7" "Example"` {
			t.Fatal("person name expanded")
		}
	}
	for _, s := range base.Searches {
		if !slices.ContainsFunc(p.Searches, func(v DiscoverySearch) bool { return v.Kind == s.Kind && v.Text == s.Text }) {
			t.Fatal("baseline lost")
		}
	}
	if !reflect.DeepEqual(base.Appearances, p.Appearances) {
		t.Fatal("source mutated")
	}
	raw, _ := json.Marshal(p)
	if _, _, err := decodeDiscovery(raw); err != nil {
		t.Fatal(err)
	}
	p.Searches[0].Derivations[0].Rule = "invented"
	raw, _ = json.Marshal(p)
	if _, _, err := decodeDiscovery(raw); err == nil {
		t.Fatal("unchecked derivation")
	}
	if _, err := PlanDiscoveryWithPolicy([]DiscoveryAppearance{a}, "fixture", base.BuildSHA256, "future"); err == nil {
		t.Fatal("unknown policy")
	}
	for _, tc := range []struct {
		raw  string
		want []searchVariant
	}{
		{"Example Holdings", []searchVariant{}},
		{"123 Inc", []searchVariant{}},
		{"LLC", []searchVariant{}},
		{"Example inc.", []searchVariant{{`"Example"`, "employer_legal_suffix_omission"}}},
		{"Example L.L.C.", []searchVariant{}},
		{"École2", []searchVariant{{`"École" "2"`, "employer_digit_letter_boundaries"}}},
	} {
		if got := employerSearchVariants(tc.raw); !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("%s: %#v", tc.raw, got)
		}
	}
	inputs := []DiscoveryAppearance{}
	for i := range 3 {
		v := discoveryInput(string(rune('a' + i)))
		n := string(rune('a' + i))
		e := n + "7 Corp"
		v.Name, v.Employer = &n, &e
		inputs = append(inputs, v)
	}
	if _, err := PlanDiscoveryWithPolicy(inputs, "fixture", base.BuildSHA256, DiscoveryVariantsPolicy); err == nil {
		t.Fatal("silently exceeded/truncated search budget")
	}
}

func TestDiscoveryVariantsSharedRequestKeepsDifferentDerivations(t *testing.T) {
	a, b := discoveryInput("a"), discoveryInput("b")
	n := "Same Name"
	a.Name, b.Name = &n, &n
	one, two := "Example Inc", "Example"
	a.Employer, b.Employer = &one, &two
	p, err := PlanDiscoveryWithPolicy([]DiscoveryAppearance{a, b}, "fixture", Hash([]byte("build")), DiscoveryVariantsPolicy)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range p.Searches {
		if s.Kind == "reported_employer" && s.Text == `"Example"` {
			if !reflect.DeepEqual(s.Derivations, []SearchDerivation{{0, "employer_legal_suffix_omission"}, {1, "reported_tokens"}}) || !slices.Equal(s.Appearances, []int{0, 1}) {
				t.Fatal("derivation lost", s)
			}
			return
		}
	}
	t.Fatal("shared query missing")
}
