package flowapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	graph "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
)

// Explicit opt-in only. Source storage must be mounted read-only. The test
// creates no database, indexes, graph records, release pointers, or host ports.
func TestLiveReadOnlyProjectionAPI(t *testing.T) {
	bundle := os.Getenv("LEGAL_TENDER_API_TEST_BUNDLE")
	if bundle == "" {
		t.Skip("real published bundle not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	started := time.Now()
	r, err := graph.OpenReader(ctx, graph.Options{StorageRoot: os.Getenv("LEGAL_TENDER_API_TEST_STORAGE"), Bundle: bundle, Cycle: os.Getenv("LEGAL_TENDER_API_TEST_CYCLE"), Endpoint: os.Getenv("LEGAL_TENDER_API_TEST_ENDPOINT"), Username: os.Getenv("ARANGO_USER"), Password: os.Getenv("ARANGO_PASSWORD")})
	if err != nil {
		t.Fatal(err)
	}
	startup := time.Since(started).Seconds()
	h, err := NewHandler(r)
	if err != nil {
		t.Fatal(err)
	}
	s := httptest.NewServer(h)
	defer s.Close()
	client := s.Client()
	client.Timeout = 20 * time.Second
	prefix := "/v1/committee-flow/" + r.View().ProjectionID
	type sample struct {
		Path     string          `json:"path"`
		Seconds  float64         `json:"seconds"`
		Response json.RawMessage `json:"response"`
	}
	var samples []sample
	fetch := func(path string, code int) json.RawMessage {
		t.Helper()
		before := time.Now()
		resp, err := client.Get(s.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var raw json.RawMessage
		if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != code {
			t.Fatalf("%s: status %d, %s", path, resp.StatusCode, raw)
		}
		samples = append(samples, sample{path, time.Since(before).Seconds(), raw})
		var envelope struct {
			Projection graph.View `json:"projection"`
		}
		if code == 200 && path != "/healthz" {
			if err := json.Unmarshal(raw, &envelope); err != nil || envelope.Projection.ProjectionID != r.View().ProjectionID || envelope.Projection.TerminalEligible {
				t.Fatal("projection/eligibility drift", err)
			}
		}
		return raw
	}
	readPage := func(raw json.RawMessage) page {
		t.Helper()
		var e struct {
			Data page `json:"data"`
		}
		if err := json.Unmarshal(raw, &e); err != nil {
			t.Fatal(err)
		}
		return e.Data
	}
	fetch("/healthz", 200)
	fetch("/v1/committee-flow", 200)
	// Enumerate every referenced identity with stable keyset pagination.
	seen := map[string]bool{}
	entityKeys := []string{}
	next := ""
	entityPages := 0
	for {
		path := prefix + "/queries/entities?limit=100"
		if next != "" {
			path += "&cursor=" + url.QueryEscape(next)
		}
		p := readPage(fetch(path, 200))
		entityPages++
		for _, raw := range p.Items {
			var v struct {
				Key string `json:"_key"`
			}
			_ = json.Unmarshal(raw, &v)
			if seen[v.Key] {
				t.Fatal("repeated identity")
			}
			seen[v.Key] = true
			entityKeys = append(entityKeys, v.Key)
		}
		if !p.HasMore {
			if p.Next != nil {
				t.Fatal("spurious cursor")
			}
			break
		}
		if p.Next == nil || entityPages > 10000 {
			t.Fatal("unbounded identity pagination")
		}
		next = *p.Next
	}
	if len(seen) != r.View().Entities {
		t.Fatal("identity pagination lost records", len(seen), r.View().Entities)
	}
	for _, side := range []string{"schedule_a", "schedule_b"} {
		var edge map[string]json.RawMessage
		var first page
		query := ""
		for i, key := range entityKeys {
			if i >= 50 {
				break
			}
			query = prefix + "/queries/observations?ledger=" + side + "&committee=" + key + "&direction=any&limit=1"
			first = readPage(fetch(query, 200))
			if len(first.Items) > 0 {
				_ = json.Unmarshal(first.Items[0], &edge)
				break
			}
		}
		if edge == nil {
			t.Fatal("no deterministic sample in first bounded identity cohort", side)
		}
		str := func(key string) string {
			var v string
			if err := json.Unmarshal(edge[key], &v); err != nil {
				t.Fatal(err)
			}
			return v
		}
		if first.HasMore {
			second := readPage(fetch(query+"&cursor="+url.QueryEscape(*first.Next), 200))
			if len(second.Items) != 1 || string(first.Items[0]) == string(second.Items[0]) {
				t.Fatal("duplicate observation pagination")
			}
			fetch(strings.Replace(query, side, map[string]string{"schedule_a": "schedule_b", "schedule_b": "schedule_a"}[side], 1)+"&cursor="+url.QueryEscape(*first.Next), 400)
		}
		fetch(prefix+"/entities/"+str("sender_committee_id"), 200)
		fetch(prefix+"/components/"+str("component_id"), 200)
		fetch(prefix+"/queries/members?ledger="+side+"&component="+str("component_id")+"&limit=1", 200)
		paths := prefix + "/queries/paths?ledger=" + side + "&committee=" + str("sender_committee_id") + "&target=" + str("reported_recipient_committee_id") + "&max_depth=1&limit=1"
		p := readPage(fetch(paths, 200))
		if len(p.Items) != 1 {
			t.Fatal("missed known direct path")
		}
		if p.HasMore {
			second := readPage(fetch(paths+"&cursor="+url.QueryEscape(*p.Next), 200))
			if len(second.Items) != 1 || string(second.Items[0]) == string(p.Items[0]) {
				t.Fatal("repeated path")
			}
		}
		fetch(strings.Replace(paths, "/paths?", "/shortest?", 1), 200)
		fetch(prefix+"/queries/neighborhood?ledger="+side+"&committee="+str("sender_committee_id")+"&max_depth=1&limit=2", 200)
		fetch(prefix+"/queries/cycles?ledger="+side+"&committee="+str("sender_committee_id")+"&max_depth=1&limit=2", 200)
		for range 2 {
			fetch(prefix+"/observations/"+side+"/"+str("_key")+"/source", 200)
		}
	}
	fetch(prefix+"/queries/paths?ledger=both&committee=C00000001&target=C00000002&max_depth=1", 400)
	fetch(strings.Replace(prefix, r.View().ProjectionID, strings.Repeat("0", 64), 1)+"/queries/entities", 409)
	var usage syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
	result := struct {
		Status      string     `json:"status"`
		Projection  graph.View `json:"projection"`
		Startup     float64    `json:"startup_seconds"`
		Total       float64    `json:"total_seconds"`
		RSS         uint64     `json:"process_peak_rss_bytes"`
		EntityPages int        `json:"entity_pages"`
		Samples     []sample   `json:"samples"`
	}{"passed", r.View(), startup, time.Since(started).Seconds(), uint64(usage.Maxrss) * 1024, entityPages, samples}
	if output := os.Getenv("LEGAL_TENDER_API_TEST_OUTPUT"); output != "" {
		b, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(output, append(b, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Log(fmt.Sprintf("verified %d identities in %d pages; startup %.3fs, total %.3fs, RSS %d bytes", len(seen), entityPages, startup, result.Total, result.RSS))
}
