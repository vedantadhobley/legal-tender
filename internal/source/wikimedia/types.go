// Package wikimedia captures bounded organization and affiliation searches. It does not resolve
// a FEC identity, verify employment, or publish graph edges.
package wikimedia

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

const Contract = "wikimedia/organization-candidates@1.0.0"
const MaxQueries = 20
const MaxHits = 5
const MaxBody = 4 << 20
const MaxCapture = 64 << 20

var qidPattern = regexp.MustCompile(`^Q[1-9][0-9]*$`)

type Reference struct {
	FactSetID      string `json:"fact_set_id"`
	ManifestSHA256 string `json:"manifest_sha256"`
	FactID         string `json:"fact_id,omitempty"`
	Ordinal        uint64 `json:"source_row_ordinal,omitempty"`
	Field          string `json:"field"`
}

// One query can retain multiple original occurrences, without claiming that
// those occurrences refer to the same real-world organization.
type Query struct {
	Text       string      `json:"reported_text"`
	References []Reference `json:"references"`
}

type Queries struct {
	Version     string  `json:"schema_version"`
	Selection   string  `json:"selection"`
	BuildSHA256 string  `json:"build_sha256"`
	Queries     []Query `json:"queries"`
}

func (q Queries) Validate() error {
	if q.Version != "organization-queries.v1" || q.Selection == "" || !Digest(q.BuildSHA256) || len(q.Queries) == 0 || len(q.Queries) > MaxQueries {
		return fmt.Errorf("invalid query version, provenance or query budget")
	}
	seen := map[string]bool{}
	for _, v := range q.Queries {
		if strings.TrimSpace(v.Text) == "" || !utf8.ValidString(v.Text) || len(v.Text) > 500 || len(v.References) == 0 || len(v.References) > 10000 || seen[v.Text] {
			return fmt.Errorf("invalid or duplicate organization query")
		}
		for _, c := range v.Text {
			if unicode.IsControl(c) {
				return fmt.Errorf("control character in organization query")
			}
		}
		seen[v.Text] = true
		refs := map[Reference]bool{}
		for _, r := range v.References {
			if r.FactSetID == "" || !Digest(r.ManifestSHA256) || refs[r] {
				return fmt.Errorf("invalid or repeated source reference")
			}
			switch r.Field {
			case "CONNECTED_ORG_NM":
				if r.FactID == "" || r.Ordinal != 0 {
					return fmt.Errorf("committee reference requires fact ID")
				}
			case "contbr_employer":
				if r.Ordinal == 0 || r.FactID != "" {
					return fmt.Errorf("receipt reference requires source ordinal")
				}
			default:
				return fmt.Errorf("unreviewed organization field")
			}
			refs[r] = true
		}
	}
	return nil
}

type Response struct {
	URL        string            `json:"url"`
	ObservedAt string            `json:"observed_at"`
	Status     int               `json:"status"`
	Headers    map[string]string `json:"headers"`
	Bytes      int               `json:"bytes"`
	SHA256     string            `json:"sha256"`
	Body       string            `json:"body"`
	Failure    string            `json:"failure,omitempty"`
}

type Entry struct {
	Query    int       `json:"query_index"`
	Search   Response  `json:"search"`
	Entities *Response `json:"entities,omitempty"`
}

type Manifest struct {
	Contract      string  `json:"contract"`
	BuildSHA256   string  `json:"build_sha256"`
	UserAgent     string  `json:"user_agent"`
	QueriesSHA256 string  `json:"queries_sha256"`
	Entries       []Entry `json:"entries"`
}

type Revision struct {
	ID        int64  `json:"revid"`
	Parent    int64  `json:"parentid"`
	Timestamp string `json:"timestamp"`
}

type Page struct {
	ID        int64             `json:"pageid"`
	Namespace int               `json:"ns"`
	Title     string            `json:"title"`
	Index     int               `json:"index"`
	Props     map[string]string `json:"pageprops,omitempty"`
	Revisions []Revision        `json:"revisions"`
}

type Term struct {
	Language string `json:"language"`
	Value    string `json:"value"`
}

// Claims and sitelinks are preserved, not flattened into employment/ownership
// edges. Their qualifiers, references, ranks and directions remain source data.
type Entity struct {
	ID           string                     `json:"id"`
	Type         string                     `json:"type,omitempty"`
	PageID       int64                      `json:"pageid,omitempty"`
	Namespace    int                        `json:"ns,omitempty"`
	Title        string                     `json:"title,omitempty"`
	LastRevision int64                      `json:"lastrevid,omitempty"`
	Modified     string                     `json:"modified,omitempty"`
	Missing      json.RawMessage            `json:"missing,omitempty"`
	Labels       map[string]Term            `json:"labels,omitempty"`
	Descriptions map[string]Term            `json:"descriptions,omitempty"`
	Aliases      map[string][]Term          `json:"aliases,omitempty"`
	Claims       map[string]json.RawMessage `json:"claims,omitempty"`
	Sitelinks    map[string]json.RawMessage `json:"sitelinks,omitempty"`
}

func Hash(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }
func Digest(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 32 && strings.ToLower(s) == s
}
