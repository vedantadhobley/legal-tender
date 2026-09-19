package committeeflows

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func TestQueryConsumesEveryCursorBatch(t *testing.T) {
	t.Parallel()
	base, err := url.Parse("http://arango:8529")
	if err != nil {
		t.Fatal(err)
	}
	requests := 0
	client := &arangoClient{
		base: base, username: "root", password: "secret",
		httpClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			requests++
			body := `{"result":[1],"hasMore":true,"id":"cursor-1"}`
			if request.Method == http.MethodPut {
				if request.URL.Path != "/_db/test/_api/cursor/cursor-1" {
					t.Fatalf("unexpected continuation path %s", request.URL.Path)
				}
				body = `{"result":[2],"hasMore":false}`
			}
			return &http.Response{
				StatusCode: http.StatusCreated,
				Body:       io.NopCloser(strings.NewReader(body)),
				Header:     make(http.Header),
			}, nil
		})},
	}
	rows, err := client.query(context.Background(), "test", "RETURN 1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if requests != 2 || len(rows) != 2 || string(rows[0]) != "1" || string(rows[1]) != "2" {
		t.Fatalf("unexpected cursor result: requests=%d rows=%q", requests, rows)
	}
}
