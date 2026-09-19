package wikimedia

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

type Observation struct {
	rawEntities         []byte
	Query               Query             `json:"query"`
	SearchSHA256        string            `json:"search_sha256"`
	SearchShapeSHA256   string            `json:"search_shape_sha256"`
	EntitiesSHA256      string            `json:"entities_sha256,omitempty"`
	EntitiesShapeSHA256 string            `json:"entities_shape_sha256,omitempty"`
	Pages               []Page            `json:"pages"`
	Entities            map[string]Entity `json:"entities"`
	Issue               string            `json:"issue,omitempty"`
}

type Replay struct {
	CaptureSHA256 string        `json:"capture_sha256"`
	Manifest      Manifest      `json:"manifest"`
	Queries       Queries       `json:"queries"`
	Observations  []Observation `json:"observations"`
}

// Read has no network capability. The caller pins the entire capture manifest;
// each request is reconstructed from the saved query or observed Wikipedia IDs.
func Read(directory, expectedSHA256 string) (Replay, error) {
	var queries Queries
	out, err := readSearches(directory, expectedSHA256, Contract, func(b []byte) ([]Query, error) {
		if err := strictjson.Decode(b, &queries); err != nil {
			return nil, err
		}
		return queries.Queries, queries.Validate()
	})
	out.Queries = queries
	return out, err
}

func readSearches(directory, expectedSHA256, contract string, decode func([]byte) ([]Query, error)) (Replay, error) {
	var out Replay
	if !Digest(expectedSHA256) {
		return out, fmt.Errorf("expected capture SHA-256 required")
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return out, err
	}
	defer root.Close()
	read := func(name string, limit int) ([]byte, error) {
		info, e := root.Lstat(name)
		if e != nil {
			return nil, e
		}
		if !info.Mode().IsRegular() || info.Size() > int64(limit) {
			return nil, fmt.Errorf("capture file type/size")
		}
		f, e := root.Open(name)
		if e != nil {
			return nil, e
		}
		defer f.Close()
		b, e := io.ReadAll(io.LimitReader(f, int64(limit)+1))
		if e != nil {
			return nil, e
		}
		if len(b) > limit {
			return nil, fmt.Errorf("capture read budget")
		}
		return b, nil
	}
	b, err := read("capture.json", MaxBody)
	if err != nil {
		return out, err
	}
	if Hash(b) != expectedSHA256 {
		return out, fmt.Errorf("capture digest mismatch")
	}
	out.CaptureSHA256 = expectedSHA256
	if err = strictjson.Decode(b, &out.Manifest); err != nil {
		return out, err
	}
	m := out.Manifest
	if m.Contract != contract || !Digest(m.BuildSHA256) || len(m.UserAgent) < 10 || len(m.UserAgent) > 256 {
		return out, fmt.Errorf("capture contract/build/agent")
	}
	b, err = read("queries.json", MaxBody)
	if err != nil {
		return out, err
	}
	if Hash(b) != m.QueriesSHA256 {
		return out, fmt.Errorf("queries digest mismatch")
	}
	queries, err := decode(b)
	if err != nil {
		return out, err
	}
	if len(m.Entries) != len(queries) {
		return out, fmt.Errorf("query/attempt conservation")
	}
	used := 0
	body := func(r Response, url string) ([]byte, error) {
		if r.URL != url || !Digest(r.SHA256) || r.Body != r.SHA256+".body" || r.Bytes < 0 || r.Bytes > MaxBody {
			return nil, fmt.Errorf("response scope/locator/budget mismatch")
		}
		if _, e := time.Parse(time.RFC3339Nano, r.ObservedAt); e != nil {
			return nil, fmt.Errorf("response observation time")
		}
		used += r.Bytes
		if used > MaxCapture {
			return nil, fmt.Errorf("capture byte budget")
		}
		b, e := read(r.Body, MaxBody)
		if e != nil {
			return nil, e
		}
		if len(b) != r.Bytes || Hash(b) != r.SHA256 {
			return nil, fmt.Errorf("response bytes/digest mismatch")
		}
		if r.Failure == "" {
			if reason := responseFailure(r); reason != "" {
				return nil, fmt.Errorf("successful response has invalid headers/status")
			}
			if s := r.Headers["Content-Length"]; s != "" {
				n, e := strconv.Atoi(s)
				if e != nil || n != len(b) {
					return nil, fmt.Errorf("response content length")
				}
			}
		}
		return b, nil
	}
	for i, e := range m.Entries {
		if e.Query != i {
			return out, fmt.Errorf("query order mismatch")
		}
		q := queries[i]
		o := Observation{Query: q, SearchSHA256: e.Search.SHA256, Pages: []Page{}, Entities: map[string]Entity{}}
		b, err := body(e.Search, SearchURL(q.Text))
		if err != nil {
			return out, err
		}
		o.SearchShapeSHA256 = ShapeFingerprint(b)
		if e.Search.Failure != "" {
			o.Issue = e.Search.Failure
		} else {
			o.Pages, err = ParseSearch(b)
			if err != nil {
				o.Issue = err.Error()
			}
		}
		if o.Issue != "" {
			if e.Entities != nil {
				return out, fmt.Errorf("entity request after invalid search")
			}
		} else if ids := PageIDs(o.Pages); len(ids) > 0 {
			if e.Entities == nil {
				return out, fmt.Errorf("missing required entity attempt")
			}
			r := *e.Entities
			b, err = body(r, EntityURL(ids))
			if err != nil {
				return out, err
			}
			o.EntitiesSHA256 = r.SHA256
			o.EntitiesShapeSHA256 = ShapeFingerprint(b)
			if r.Failure != "" {
				o.Issue = r.Failure
			} else {
				o.Entities, err = ParseEntities(b, ids)
				if err != nil {
					o.Issue = err.Error()
				} else {
					o.rawEntities = b
				}
			}
		} else if e.Entities != nil {
			return out, fmt.Errorf("unexpected entity request")
		}
		out.Observations = append(out.Observations, o)
	}
	return out, nil
}
