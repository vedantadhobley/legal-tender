package independentexpenditures

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

func TestResolvedReadbackBatchesAndRejectsChangedFields(t *testing.T) {
	for _, mode := range []string{"valid", "changed_stance", "changed_amount", "changed_source", "unknown_field", "missing", "duplicate", "extra", "pagination"} {
		t.Run(mode, func(t *testing.T) {
			want := make([]resolvedExpenditureEdge, 1001)
			byKey := map[string]resolvedExpenditureEdge{}
			for i := range want {
				want[i] = resolvedExpenditureEdge{Key: fmt.Sprintf("e%04d", i), SupportOppose: "S", AmountMinorUnits: "9007199254740993", SourceReleaseID: "source-one"}
				byKey[want[i].Key] = want[i]
			}
			batches := 0
			c, err := newArangoClient("http://example.test", "reader", "example-only")
			if err != nil {
				t.Fatal(err)
			}
			c.httpClient.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Method != "POST" || !strings.HasSuffix(req.URL.Path, "/_api/cursor") {
					t.Fatalf("write or unexpected API %s %s", req.Method, req.URL.Path)
				}
				var q struct {
					Query string                     `json:"query"`
					Bind  map[string]json.RawMessage `json:"bindVars"`
				}
				if err := json.NewDecoder(req.Body).Decode(&q); err != nil {
					t.Fatal(err)
				}
				var rows []any
				if q.Query == "RETURN LENGTH(@@collection)" {
					n := len(want)
					if mode == "extra" {
						n++
					}
					rows = []any{n}
				} else {
					var keys []string
					_ = json.Unmarshal(q.Bind["keys"], &keys)
					if len(keys) > 1000 {
						t.Fatal("unbounded batch")
					}
					batches++
					for _, k := range keys {
						v := byKey[k]
						if k == want[0].Key {
							switch mode {
							case "changed_stance":
								v.SupportOppose = "O"
							case "changed_amount":
								v.AmountMinorUnits = "9007199254740992"
							case "changed_source":
								v.SourceReleaseID = "source-two"
							case "missing":
								continue
							case "duplicate":
								rows = append(rows, v)
							case "unknown_field":
								b, _ := json.Marshal(v)
								var m map[string]any
								_ = json.Unmarshal(b, &m)
								m["uncontracted"] = "x"
								rows = append(rows, m)
								continue
							}
						}
						rows = append(rows, v)
					}
				}
				b, _ := json.Marshal(map[string]any{"result": rows, "hasMore": mode == "pagination"})
				return testResponse(200, string(b)), nil
			})
			err = readResolvedDocuments(context.Background(), c, "lt_ie_resolved_fixture", edgesCollection, want, func(v resolvedExpenditureEdge) string { return v.Key })
			if mode == "valid" {
				if err != nil || batches != 2 {
					t.Fatal(err, batches)
				}
			} else if err == nil {
				t.Fatal("accepted", mode)
			}
		})
	}
}

func TestResolvedReaderNeverCreatesMissingGraph(t *testing.T) {
	c, _ := newArangoClient("http://example.test", "reader", "example-only")
	calls := 0
	c.httpClient.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		if req.Method != "GET" || !strings.HasSuffix(req.URL.Path, "/_api/gharial/"+GraphName) {
			t.Fatal("unexpected mutation", req.Method, req.URL.Path)
		}
		return testResponse(404, `{"error":true,"errorNum":1924}`), nil
	})
	if err := verifyResolvedModel(context.Background(), c, resolvedProjection{Database: "lt_ie_resolved_fixture"}); err == nil || calls != 1 {
		t.Fatal(err, calls)
	}
}
