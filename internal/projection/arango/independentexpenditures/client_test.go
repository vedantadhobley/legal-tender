package independentexpenditures

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	feceffective "github.com/vedantadhobley/legal-tender/internal/calculation/fec/independentexpenditures"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func testResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func TestClientCreatesOnlyIndependentExpenditureProbeDatabase(t *testing.T) {
	t.Parallel()
	created := false
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		username, password, ok := request.BasicAuth()
		if !ok || username != "tester" || password != "example-secret" {
			return testResponse(http.StatusUnauthorized, "unauthorized"), nil
		}
		switch request.Method + " " + request.URL.Path {
		case "GET /_db/lt_ie_probe_2024_abcdef/_api/version":
			return testResponse(http.StatusNotFound, `{"error":true,"errorNum":1228,"errorMessage":"database not found"}`), nil
		case "POST /_db/_system/_api/database":
			var body struct {
				Name string `json:"name"`
			}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil || body.Name != "lt_ie_probe_2024_abcdef" {
				return testResponse(http.StatusBadRequest, "bad request"), nil
			}
			created = true
			return testResponse(http.StatusCreated, `{"result":true}`), nil
		default:
			return testResponse(http.StatusNotFound, "not found"), nil
		}
	})

	client, err := newArangoClient("http://example.test", "tester", "example-secret")
	if err != nil {
		t.Fatal(err)
	}
	client.httpClient.Transport = transport
	if err := client.ensureDatabase(context.Background(), "lt_ie_probe_2024_abcdef"); err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("database was not created")
	}
	if err := client.ensureDatabase(context.Background(), "legal_tender"); err == nil || !strings.Contains(err.Error(), "refusing non-independent") {
		t.Fatalf("non-probe database was accepted: %v", err)
	}
}

func TestClientImportsOutsideSpendingEdgesWithReplacement(t *testing.T) {
	t.Parallel()
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodPost || request.URL.Path != "/_db/lt_ie_probe_test/_api/import" {
			return testResponse(http.StatusNotFound, "not found"), nil
		}
		wantQuery := url.Values{
			"collection": {edgesCollection}, "type": {"documents"},
			"onDuplicate": {"replace"}, "complete": {"true"},
		}
		if request.URL.Query().Encode() != wantQuery.Encode() || request.Header.Get("Content-Type") != "application/x-ndjson" {
			return testResponse(http.StatusBadRequest, "bad import contract"), nil
		}
		content, err := io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		if strings.Count(string(content), "\n") != 2 {
			return testResponse(http.StatusBadRequest, fmt.Sprintf("unexpected JSONL %q", content)), nil
		}
		return testResponse(http.StatusCreated, `{"created":2,"errors":0,"empty":0,"updated":0,"ignored":0}`), nil
	})

	client, err := newArangoClient("http://example.test", "tester", "example-secret")
	if err != nil {
		t.Fatal(err)
	}
	client.httpClient.Transport = transport
	response, err := client.importDocuments(context.Background(), "lt_ie_probe_test", edgesCollection, []map[string]string{{"_key": "one"}, {"_key": "two"}})
	if err != nil {
		t.Fatal(err)
	}
	if response.Created != 2 {
		t.Fatalf("unexpected import response: %+v", response)
	}
}

func TestProjectionIdentityResultValidationAndPercentiles(t *testing.T) {
	t.Parallel()
	first := entityDocument{Key: "candidate_H0AA00000", SchemaVersion: EntitySchemaVersion, EntityType: "candidate", EntityID: "H0AA00000", Cycle: "2024"}
	second := first
	if documentDigest(first) != documentDigest(second) {
		t.Fatal("equal documents produced different digests")
	}
	second.Name = "Different"
	if documentDigest(first) == documentDigest(second) {
		t.Fatal("different documents produced the same digest")
	}
	if got := databaseName("2024", strings.Repeat("a", 64)); got != "lt_ie_probe_2024_aaaaaaaaaaaaaaaa" {
		t.Fatalf("unexpected database name %q", got)
	}
	if got := representative(map[string]int{"B": 2, "A": 2, "C": 1}); got != "A" {
		t.Fatalf("unexpected representative %q", got)
	}
	index, err := percentileIndex(10, 95)
	if err != nil || index != 9 {
		t.Fatalf("unexpected p95 index %d: %v", index, err)
	}

	manifest := feceffective.Manifest{CalculationSetID: strings.Repeat("b", 64), Cycle: "2024"}
	result := feceffective.Result{
		SchemaVersion: feceffective.ResultSchemaVersion,
		ResultID:      strings.Repeat("c", 64), CalculationSetID: manifest.CalculationSetID,
		Cycle: "2024", SpenderCommitteeID: "C00000001", CandidateID: "H4AA00001",
		SupportOppose: "S", SignedAmountMinorUnits: "-125",
		ExpenditureCount: 2, PositiveCount: 1, NegativeCount: 1,
	}
	if err := validateCalculationResult(result, manifest); err != nil {
		t.Fatal(err)
	}
	result.SignedAmountMinorUnits = "-0"
	if err := validateCalculationResult(result, manifest); err == nil {
		t.Fatal("noncanonical amount was accepted")
	}
}
