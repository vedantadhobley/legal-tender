package gleif

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

	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

func nameInput(locator, name string) NameInput {
	return NameInput{SHA256: wikimedia.Hash([]byte("source")), Locator: locator, Name: &name}
}

func TestRetainedRegistryNameSchemaProbes(t *testing.T) {
	for _, tc := range []struct {
		file, pin, text string
		records         int
	}{
		{"empty-search.json", "749a0113f8a8d10f497990eb1126c4aec17088d3cc80fc7a4a5d4b4a7ab0ba29", "JC2VENTURES", 0},
		{"name-search.json", "f9a4e8b5cbe42659bde869a0615deae91139fba14b609b47275bcd1dbc65b1de", "RIDGELINE INC", 3},
	} {
		b, err := os.ReadFile("testdata/" + tc.file)
		if err != nil || wikimedia.Hash(b) != tc.pin {
			t.Fatal("fixture pin", err)
		}
		p, err := ParseNames(b, tc.text)
		if err != nil || len(p.Records) != tc.records || !p.WindowComplete {
			t.Fatal("real schema", err)
		}
	}
}

func searchFixture(t *testing.T, text string, total int) []byte {
	t.Helper()
	var single map[string]json.RawMessage
	if err := json.Unmarshal(fixture(t), &single); err != nil {
		t.Fatal(err)
	}
	rows := []json.RawMessage{}
	ids := []string{testLEI, secondLEI, "549300694S8DN13VYU16", "254900Z9WT402Y2GZU77", "549300ZQY5915TFUVQ72"}
	for i := 0; i < min(total, NamePageSize); i++ {
		rows = append(rows, json.RawMessage(strings.ReplaceAll(string(single["data"]), testLEI, ids[i])))
	}
	p := NamePagination{CurrentPage: 1, PerPage: NamePageSize, Total: total, LastPage: 1}
	if total > 0 {
		from, to := 1, len(rows)
		p.From, p.To = &from, &to
		p.LastPage = (total-1)/NamePageSize + 1
	}
	links := map[string]string{"first": NameURL(text, 1), "last": NameURL(text, p.LastPage)}
	if total > NamePageSize {
		links["next"] = NameURL(text, 2)
	}
	var meta map[string]json.RawMessage
	_ = json.Unmarshal(single["meta"], &meta)
	body, _ := json.Marshal(map[string]any{"data": rows, "meta": map[string]any{"goldenCopy": meta["goldenCopy"], "pagination": p}, "links": links})
	return body
}

func TestNamePlanPreservesRawInputsAndSharesOnlyQueries(t *testing.T) {
	inputs := []NameInput{nameInput("b", "Example, Corp."), nameInput("a", "example corp"), nameInput("c", "")}
	inputs[2].Name = nil
	p, err := PlanNames(inputs, "fixture", wikimedia.Hash([]byte("build")))
	if err != nil || len(p.Queries) != 1 || p.Queries[0].Text != "EXAMPLE CORP" || !slices.Equal(p.Queries[0].Inputs, []int{0, 1}) || p.States[2] != "no_searchable_name" {
		t.Fatal("plan", err)
	}
	if *inputs[0].Name != "Example, Corp." || p.Inputs[2].Name != nil {
		t.Fatal("source changed")
	}
	slices.Reverse(inputs)
	again, err := PlanNames(inputs, p.Selection, p.BuildSHA256)
	if err != nil || !reflect.DeepEqual(p, again) || p.Validate() != nil {
		t.Fatal("unstable plan")
	}
	p.Queries[0].Text = "injected"
	if p.Validate() == nil {
		t.Fatal("query tampering")
	}
	for _, bad := range []NameInput{nameInput("", "X"), nameInput("x", "bad\ntext"), nameInput("x", strings.Repeat("a", 501)), nameInput("x", "\xff")} {
		if _, err = PlanNames([]NameInput{bad}, "fixture", again.BuildSHA256); err == nil {
			t.Fatal("invalid source accepted")
		}
	}
	if _, err = PlanNames([]NameInput{inputs[0], inputs[0]}, "fixture", again.BuildSHA256); err == nil {
		t.Fatal("duplicate source accepted")
	}
	if _, err = PlanNames(make([]NameInput, MaxRecords+1), "fixture", again.BuildSHA256); err == nil {
		t.Fatal("budget")
	}
}

func TestNameSearchRecordsPaginationAndStrictShape(t *testing.T) {
	text := "EXAMPLE CORPORATION"
	for _, total := range []int{0, 1, 2, 5, 6, 10, 11} {
		body := searchFixture(t, text, total)
		p, err := ParseNames(body, text)
		if err != nil || len(p.Records) != min(total, 5) || p.WindowComplete != (total <= 5) {
			t.Fatal("pagination", total, err)
		}
		var root struct{ Data []json.RawMessage }
		_ = json.Unmarshal(body, &root)
		for i, r := range p.Records {
			if string(r.Raw) != string(root.Data[i]) || r.PublishDate != p.PublishDate {
				t.Fatal("exact resource lost")
			}
		}
	}
	good := string(searchFixture(t, text, 2))
	for _, bad := range []string{
		strings.Replace(good, `"data":[`, `"data":null,"unexpected":[`, 1),
		strings.Replace(good, `"total":2`, `"total":3`, 1),
		strings.Replace(good, `"currentPage":1`, `"currentPage":2`, 1),
		strings.Replace(good, `"perPage":5`, `"perPage":10`, 1),
		strings.Replace(good, `"to":2`, `"to":null`, 1),
		strings.Replace(good, `"lastPage":1`, `"lastPage":2`, 1),
		strings.Replace(good, `"pagination":`, `"extra":true,"pagination":`, 1),
		strings.ReplaceAll(good, secondLEI, testLEI),
		strings.Replace(good, `"legalName":`, `"unreviewed":`, 1),
		strings.Replace(good, `"links":{"first":`, `"links":{"next":"https://private.invalid/","first":`, 1),
		strings.Replace(good, `api.gleif.org/api/v1/lei-records?`, `other.invalid/api/v1/lei-records?`, 1),
		strings.Replace(good, `"total":2`, `"total":2,"total":2`, 1),
	} {
		if _, err := ParseNames([]byte(bad), text); err == nil {
			t.Fatal("accepted malformed page")
		}
	}
	if _, err := ParseNames([]byte(strings.Replace(string(searchFixture(t, text, 0)), `"total":0`, `"total":null`, 1)), text); err == nil {
		t.Fatal("null total accepted")
	}
}

func TestNameCaptureReplayStopsAndPreservesFailures(t *testing.T) {
	p, err := PlanNames([]NameInput{nameInput("a", "Example Corporation"), nameInput("b", "Other Corp")}, "fixture", wikimedia.Hash([]byte("build")))
	if err != nil {
		t.Fatal(err)
	}
	for _, failure := range []string{"", "http", "schema", "budget", "cancelled"} {
		t.Run(failure, func(t *testing.T) {
			calls := 0
			client := &http.Client{Transport: roundTrip(func(req *http.Request) (*http.Response, error) {
				if req.URL.String() != NameURL(p.Queries[calls].Text, 1) || req.Header.Get("Accept-Encoding") != "identity" {
					t.Fatal("request changed")
				}
				body := searchFixture(t, p.Queries[calls].Text, 2)
				calls++
				status := 200
				switch failure {
				case "http":
					status = 429
					body = []byte(`{"errors":[]}`)
				case "schema":
					body = []byte(`{"unknown":true}`)
				case "budget":
					body = []byte(strings.Repeat("x", MaxBody+1))
				}
				return &http.Response{StatusCode: status, ContentLength: int64(len(body)), Header: http.Header{"Content-Type": []string{"application/vnd.api+json"}, "Set-Cookie": []string{"private"}}, Body: io.NopCloser(strings.NewReader(string(body)))}, nil
			})}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if failure == "cancelled" {
				cancel()
			}
			dir := filepath.Join(t.TempDir(), "capture")
			pin, err := captureNames(ctx, p, Options{Directory: dir, BuildSHA256: p.BuildSHA256, UserAgent: "LegalTender/test (test@local)"}, client, 0)
			if err != nil {
				t.Fatal(err)
			}
			r, err := ReadNames(dir, pin)
			if err != nil {
				t.Fatal(err)
			}
			again, err := ReadNames(dir, pin)
			if err != nil || !reflect.DeepEqual(r, again) {
				t.Fatal("replay", err)
			}
			if len(r.Observations) != 2 || r.CaptureUsable != (failure == "") {
				t.Fatal("observation conservation")
			}
			want := 2
			if failure != "" {
				want = 1
			}
			if failure == "cancelled" {
				want = 0
			}
			if calls != want {
				t.Fatal("network continued", calls)
			}
			if failure != "" && r.Observations[1].Issue != "capture_stopped" {
				t.Fatal("unattempted became negative")
			}
			manifest, _ := os.ReadFile(filepath.Join(dir, "capture.json"))
			if strings.Contains(string(manifest), "private") {
				t.Fatal("private headers retained")
			}
			if _, err := ReadNames(dir, wikimedia.Hash([]byte("wrong"))); err == nil {
				t.Fatal("wrong pin")
			}
			if err := os.WriteFile(filepath.Join(dir, "00.body"), []byte("changed"), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := ReadNames(dir, pin); err == nil {
				t.Fatal("body corruption")
			}
		})
	}
}
