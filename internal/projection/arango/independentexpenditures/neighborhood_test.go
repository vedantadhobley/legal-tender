package independentexpenditures

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestResolvedNeighborhoodStanceOrderingAndReadback(t *testing.T) {
	for _, mode := range []string{"valid", "missing", "extra", "amount", "duplicate_field"} {
		t.Run(mode, func(t *testing.T) {
			c, _ := newArangoClient("http://example.test", "reader", "example")
			edge := func(key, stance string) resolvedExpenditureEdge {
				return resolvedExpenditureEdge{Key: "ie_" + key[:40], ResultID: key, From: entitiesCollection + "/committee_C00000001", To: entitiesCollection + "/candidate_H0ZZ00001", SupportOppose: stance, AmountMinorUnits: "9007199254740993"}
			}
			first, second, opposition := edge(strings.Repeat("1", 64), "S"), edge(strings.Repeat("2", 64), "S"), edge(strings.Repeat("3", 64), "O")
			r := ResolvedReader{c: c, m: resolvedProjection{Database: "lt_ie_probe_test", Edges: []resolvedExpenditureEdge{second, opposition, first}}}
			c.httpClient.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Method != "POST" || !strings.HasSuffix(req.URL.Path, "/_api/cursor") {
					t.Fatal("mutation")
				}
				var q struct {
					Query string            `json:"query"`
					Bind  map[string]string `json:"bindVars"`
				}
				_ = json.NewDecoder(req.Body).Decode(&q)
				if !strings.Contains(q.Query, "LIMIT 2") || q.Bind["@collection"] != edgesCollection {
					t.Fatal("unbounded or foreign read")
				}
				v := first
				for _, x := range r.m.Edges {
					if x.Key == q.Bind["key"] {
						v = x
					}
				}
				if mode == "amount" {
					v.AmountMinorUnits = "9007199254740992"
				}
				raw, _ := json.Marshal(v)
				if mode == "duplicate_field" {
					raw = append([]byte(`{"support_oppose":"O",`), raw[1:]...)
				}
				rows := []json.RawMessage{raw}
				if mode == "missing" {
					rows = nil
				}
				if mode == "extra" {
					rows = append(rows, raw)
				}
				b, _ := json.Marshal(map[string]any{"result": rows, "hasMore": false})
				return testResponse(200, string(b)), nil
			})
			p, e := r.Page(context.Background(), "independent_support", "H0ZZ00001", "", 1)
			if mode != "valid" {
				if e == nil {
					t.Fatal("accepted corrupt result")
				}
				return
			}
			if e != nil || len(p.Items) != 1 || p.Items[0].Key != first.ResultID || !p.HasMore || p.NextAfter != first.ResultID || p.Items[0].EvidenceKind != "verified_resolved_calculation_group" {
				t.Fatal(p, e)
			}
			next, e := r.Page(context.Background(), "independent_support", "C00000001", p.NextAfter, 1)
			if e != nil || len(next.Items) != 1 || next.Items[0].Key != second.ResultID || next.HasMore {
				t.Fatal(next, e)
			}
			opp, e := r.Page(context.Background(), "independent_opposition", "H0ZZ00001", "", 1)
			if e != nil || len(opp.Items) != 1 || opp.Items[0].Key != opposition.ResultID {
				t.Fatal(opp, e)
			}
		})
	}
}
