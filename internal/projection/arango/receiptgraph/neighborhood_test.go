package receiptgraph

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestReceiptFacetKeepsMissingMasterDistinctFromAbsence(t *testing.T) {
	for _, mode := range []string{"stub", "absent", "changed"} {
		t.Run(mode, func(t *testing.T) {
			l := loaded{masters: map[string]entity{}}
			l.definition.Inputs.Committees.ID = digest([]byte("master"))
			want := l.entity("C00000001")
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.Method != "POST" || !strings.HasSuffix(req.URL.Path, "/_api/cursor") {
					t.Fatal("unexpected mutation")
				}
				rows := []entity{want}
				if mode == "absent" {
					rows = nil
				}
				if mode == "changed" {
					rows[0].FactSet = "other"
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"result": rows, "hasMore": false})
			}))
			defer s.Close()
			cl, _ := newClient(Options{Endpoint: s.URL, Username: "u", Password: "p"}, "lt_receipt_cycle_test")
			r := CycleReader{cl: cl, loaded: l}
			got, e := r.Entity(context.Background(), "C00000001")
			if mode == "changed" {
				if e == nil {
					t.Fatal("accepted changed master")
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			if mode == "stub" && (got.State != "present" || !strings.Contains(string(got.Document), "missing_same_cycle_master")) {
				t.Fatal(got)
			}
			if mode == "absent" && (got.State != "not_present_in_projection" || got.Document != nil) {
				t.Fatal(got)
			}
		})
	}
}

func TestReceiptPageRejectsBadBackendBeforeSourceLookup(t *testing.T) {
	for _, raw := range []string{`null`, `{"_key":"bad"}`, `{"_key":"` + strings.Repeat("a", 64) + `","extra":true}`} {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{"result": []json.RawMessage{json.RawMessage(raw)}, "hasMore": false})
		}))
		cl, _ := newClient(Options{Endpoint: s.URL, Username: "u", Password: "p"}, "lt_receipt_cycle_test")
		r := CycleReader{cl: cl}
		if _, e := r.Page(context.Background(), "reported_receipt", "C00000001", "", 1); e == nil {
			t.Fatal("accepted malformed page")
		}
		s.Close()
	}
	r := CycleReader{}
	p, e := r.Page(context.Background(), "reported_receipt", "H0ZZ00001", "", 1)
	if e != nil || p.State != "not_applicable" {
		t.Fatal(p, e)
	}
}
