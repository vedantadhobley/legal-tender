package gleif

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

const testLEI = "300300SRCLQKVTFFOM15"
const secondLEI = "LCUAWMT4M5H8DJ8DFH49"

func fixture(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/record.json")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestRegistryRecordBoundary(t *testing.T) {
	raw := fixture(t)
	r, err := Parse(raw, testLEI)
	if err != nil {
		t.Fatal(err)
	}
	if r.LEI != testLEI || r.LegalName.Name != "Example Corporation" || len(r.OtherNames) != 1 || !bytes.Equal(r.Raw, raw) {
		t.Fatal("source grain lost", r)
	}
	for _, lei := range []string{testLEI, secondLEI} {
		if !ValidLEI(lei) {
			t.Fatal("valid identifier rejected", lei)
		}
	}
	for _, lei := range []string{"", strings.ToLower(testLEI), testLEI[:19] + "6", testLEI + " ", "../../private", "00000000000000000000"} {
		if ValidLEI(lei) {
			t.Fatal("invalid identifier accepted", lei)
		}
	}
	for _, bad := range []string{
		strings.Replace(string(raw), `"type":"lei-records"`, `"type":"anything"`, 1),
		strings.Replace(string(raw), `"id":"`+testLEI+`"`, `"id":"`+secondLEI+`"`, 1),
		strings.Replace(string(raw), `"lei":"`+testLEI+`"`, `"lei":"`+secondLEI+`"`, 1),
		strings.Replace(string(raw), `"status":"ACTIVE"`, `"status":"ACTIVE","newField":true`, 1),
		strings.Replace(string(raw), `"otherNames":[`, `"otherNames":null,"duplicate":[`, 1),
		strings.Replace(string(raw), `"type":"lei-records"`, `"type":"lei-records","type":"lei-records"`, 1),
		strings.Replace(string(raw), `"language":"en"`, `"language":null`, 1),
		strings.Replace(string(raw), `"legalName":`, `"legalNameWrong":`, 1),
		strings.Replace(string(raw), `2026-09-15T08:00:00Z`, `not-a-date`, 1),
		strings.Replace(string(raw), RecordURL(testLEI), "https://example.org/other", 1),
		string(raw) + "{}",
	} {
		if _, err := Parse([]byte(bad), testLEI); err == nil {
			t.Fatal("invalid registry record accepted")
		}
	}
	unknown := bytes.Replace(raw, []byte(`"status":"ACTIVE"`), []byte(`"status":"NEW_PUBLISHER_STATE"`), 1)
	if r, err := Parse(unknown, testLEI); err != nil || r.EntityStatus != "NEW_PUBLISHER_STATE" {
		t.Fatal("unknown code lost", err)
	}
	nullRenewal := bytes.Replace(raw, []byte(`"nextRenewalDate":"2027-09-01T00:00:00Z"`), []byte(`"nextRenewalDate":null`), 1)
	if r, err := Parse(nullRenewal, testLEI); err != nil || r.NextRenewal != nil {
		t.Fatal("null renewal lost", err)
	}
}

func saveCapture(t *testing.T, code int, body []byte) (string, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "capture")
	client := &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		if r.URL.String() != RecordURL(testLEI) || r.Header.Get("Authorization") != "" {
			t.Fatal("request scope", r.URL)
		}
		return &http.Response{StatusCode: code, Header: http.Header{"Content-Type": {"application/vnd.api+json"}}, ContentLength: int64(len(body)), Body: io.NopCloser(bytes.NewReader(body))}, nil
	})}
	sha, err := capture(context.Background(), RequestSet{WikimediaCaptureSHA256: wikimedia.Hash([]byte("wm")), SelectionPolicy: "organization-registry-requests.v1", LEIs: []string{testLEI}}, Options{Directory: dir, BuildSHA256: wikimedia.Hash([]byte("build")), UserAgent: "LegalTender/test (test@local)"}, client, 0)
	if err != nil {
		t.Fatal(err)
	}
	return dir, sha
}

func TestRegistryCaptureReadAndCorruption(t *testing.T) {
	dir, sha := saveCapture(t, 200, fixture(t))
	r, err := Read(dir, sha)
	if err != nil {
		t.Fatal(err)
	}
	again, err := Read(dir, sha)
	if err != nil || !reflect.DeepEqual(r, again) || r.Observations[0].Record == nil {
		t.Fatal("replay", err)
	}
	if _, err := Read(dir, wikimedia.Hash([]byte("wrong"))); err == nil {
		t.Fatal("wrong manifest accepted")
	}
	for _, mutation := range []func(*Manifest){
		func(m *Manifest) { m.Entries[0].Response.URL = "https://example.org/other" },
		func(m *Manifest) { m.Entries[0].Response.Body = "../outside" },
		func(m *Manifest) { m.Entries = nil },
		func(m *Manifest) { m.Entries[0].Response.SHA256 = wikimedia.Hash([]byte("different")) },
		func(m *Manifest) { m.Entries[0].Response.Failure = "capture_stopped" },
	} {
		m := r.Manifest
		raw, _ := json.Marshal(m)
		_ = json.Unmarshal(raw, &m)
		mutation(&m)
		raw, _ = json.Marshal(m)
		if err := os.WriteFile(filepath.Join(dir, "capture.json"), raw, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := Read(dir, wikimedia.Hash(raw)); err == nil {
			t.Fatal("corrupt metadata accepted")
		}
	}
	raw, _ := json.Marshal(r.Manifest)
	if err = os.WriteFile(filepath.Join(dir, "capture.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(filepath.Join(dir, "00.body"), filepath.Join(dir, "real.body")); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink("real.body", filepath.Join(dir, "00.body")); err != nil {
		t.Fatal(err)
	}
	if _, err := Read(dir, wikimedia.Hash(raw)); err == nil {
		t.Fatal("symlink accepted")
	}
}

func TestFailuresAreNotAbsentEntitiesAndStopRequests(t *testing.T) {
	for _, code := range []int{404, 429, 503} {
		dir, sha := saveCapture(t, code, []byte(`{"errors":[]}`))
		r, err := Read(dir, sha)
		if err != nil || r.Observations[0].Issue != "http_status_not_ok" || r.Observations[0].Record != nil {
			t.Fatal(code, err)
		}
	}
	for _, body := range [][]byte{[]byte(`{"unexpected":true}`), bytes.Repeat([]byte("x"), MaxBody+1)} {
		calls := 0
		c := &http.Client{Transport: roundTrip(func(*http.Request) (*http.Response, error) {
			calls++
			return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, ContentLength: int64(len(body)), Body: io.NopCloser(bytes.NewReader(body))}, nil
		})}
		dir := filepath.Join(t.TempDir(), "capture")
		sha, err := capture(context.Background(), RequestSet{WikimediaCaptureSHA256: wikimedia.Hash([]byte("wm")), SelectionPolicy: "organization-registry-requests.v1", LEIs: []string{testLEI, secondLEI}}, Options{Directory: dir, BuildSHA256: wikimedia.Hash([]byte("build")), UserAgent: "LegalTender/test (test@local)"}, c, 0)
		if err != nil || calls != 1 {
			t.Fatal("requests not stopped", calls, err)
		}
		r, err := Read(dir, sha)
		if err != nil || r.Observations[0].Issue == "" || r.Observations[1].Issue != "capture_stopped" {
			t.Fatal("failure not preserved", err)
		}
	}
}

func TestCancelledEmptyAndInvalidPlans(t *testing.T) {
	for _, leis := range [][]string{nil, {secondLEI, testLEI}, {testLEI, testLEI}, {"bad"}} {
		if (RequestSet{WikimediaCaptureSHA256: wikimedia.Hash([]byte("wm")), SelectionPolicy: "organization-registry-requests.v1", LEIs: leis}).Validate() == nil {
			t.Fatal("invalid plan accepted")
		}
	}
	for _, cancelled := range []bool{true, false} {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if cancelled {
			cancel()
		}
		ids := []string{}
		if cancelled {
			ids = []string{testLEI}
		}
		dir := filepath.Join(t.TempDir(), "capture")
		c := &http.Client{Transport: roundTrip(func(*http.Request) (*http.Response, error) { t.Fatal("unexpected network call"); return nil, nil })}
		sha, err := capture(ctx, RequestSet{WikimediaCaptureSHA256: wikimedia.Hash([]byte("wm")), SelectionPolicy: "organization-registry-requests.v1", LEIs: ids}, Options{Directory: dir, BuildSHA256: wikimedia.Hash([]byte("build")), UserAgent: "LegalTender/test (test@local)"}, c, 0)
		if err != nil {
			t.Fatal(err)
		}
		r, err := Read(dir, sha)
		if err != nil {
			t.Fatal(err)
		}
		if cancelled && r.Observations[0].Issue != "capture_cancelled" {
			t.Fatal("cancellation lost")
		}
		if _, err = capture(ctx, r.Manifest.Requests, Options{Directory: dir, BuildSHA256: r.Manifest.BuildSHA256, UserAgent: r.Manifest.UserAgent}, c, 0); err == nil {
			t.Fatal("existing directory overwritten")
		}
	}
}
