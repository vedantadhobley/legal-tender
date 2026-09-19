package reportmetadata

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

const apiOrigin = "https://api.open.fec.gov"
const maxHeaderBytes = 128 << 10

func metadataClient() *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.Proxy = nil // No implicit proxy receives authentication.
	t.DisableCompression = true
	t.DisableKeepAlives = true // Avoid implicit retries on reused connections.
	t.MaxResponseHeaderBytes = maxHeaderBytes
	t.ResponseHeaderTimeout = 20 * time.Second
	t.TLSHandshakeTimeout = 10 * time.Second
	t.MaxConnsPerHost = 1
	return &http.Client{Transport: t, Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

func requestURL(r FetchRequest, page int) string {
	q := url.Values{"page": {strconv.Itoa(page)}, "per_page": {strconv.Itoa(r.Query.PerPage)}}
	if len(r.Query.FileNumbers) > 0 {
		ids := slices.Clone(r.Query.FileNumbers)
		slices.Sort(ids)
		for _, id := range ids {
			q.Add("file_number", strconv.FormatInt(id, 10))
		}
	} else {
		q.Set("committee_id", r.Query.CommitteeID)
		q.Set("cycle", strconv.Itoa(r.Query.Cycle))
	}
	return apiOrigin + r.Endpoint + "?" + q.Encode()
}

// Preserve response header values after Go's HTTP decoding, not original wire
// casing/order. Never retain cookie/authentication header values.
func responseHeaders(resp *http.Response) ([]byte, []string) {
	h := resp.Header.Clone()
	omitted := []string{}
	for k := range h {
		switch strings.ToLower(k) {
		case "authorization", "proxy-authorization", "x-api-key", "cookie", "set-cookie":
			omitted = append(omitted, strings.ToLower(k))
			delete(h, k)
		}
	}
	slices.Sort(omitted)
	var b bytes.Buffer
	b.WriteString(resp.Proto + " " + strconv.Itoa(resp.StatusCode) + " " + http.StatusText(resp.StatusCode) + "\r\n")
	_ = h.Write(&b)
	b.WriteString("\r\n")
	return b.Bytes(), omitted
}

func credentialIn(raw []byte, key string) bool {
	if bytes.Contains(raw, []byte(key)) {
		return true
	}
	if decoded, err := url.QueryUnescape(string(raw)); err == nil && strings.Contains(decoded, key) {
		return true
	}
	// Also catch ordinary JSON escaping of an echoed header token.
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	for {
		token, err := d.Token()
		if err != nil {
			break
		}
		if value, ok := token.(string); ok && strings.Contains(value, key) {
			return true
		}
	}
	return false
}

func retryDelay(value string, now time.Time) time.Duration {
	if value == "" {
		return time.Second
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds < 0 {
			return time.Second
		}
		if seconds > 30 {
			return 31 * time.Second
		}
		return max(time.Second, time.Duration(seconds)*time.Second)
	}
	if at, err := http.ParseTime(value); err == nil {
		return max(time.Second, at.Sub(now))
	}
	return 31 * time.Second // Unknown publisher delay: stop, never guess an early retry.
}

func waitRequest(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func readResponse(resp *http.Response, remaining int64, key string) ([]byte, []byte, int64, bool, string, []string) {
	defer resp.Body.Close()
	headers, omitted := responseHeaders(resp)
	if len(headers) > maxHeaderBytes || int64(len(headers)) >= remaining {
		return nil, nil, 0, false, "header_or_byte_budget", omitted
	}
	// Reserve one byte to detect overflow without crossing the total read budget.
	limit := min(int64(MaxPageBytes), remaining-int64(len(headers))-1)
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	spent := int64(len(headers) + len(body))
	if credentialIn(headers, key) || credentialIn(body, key) {
		return nil, nil, spent, false, "credential_echo_suppressed", omitted
	}
	if int64(len(body)) > limit {
		return headers, body[:limit], spent, false, "body_or_byte_budget", omitted
	}
	if err != nil {
		return headers, body, spent, false, "body_read_error", omitted
	}
	if resp.ContentLength >= 0 && resp.ContentLength != int64(len(body)) {
		return headers, body, spent, false, "content_length_mismatch", omitted
	}
	if encoding := resp.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		return headers, body, spent, false, "unsupported_content_encoding", omitted
	}
	return headers, body, spent, true, "response_received", omitted
}
