package wikimedia

import (
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

func SearchURL(text string) string {
	p := url.Values{"action": {"query"}, "generator": {"search"}, "gsrsearch": {text}, "gsrnamespace": {"0"}, "gsrlimit": {"5"}, "prop": {"pageprops|revisions"}, "ppprop": {"wikibase_item|disambiguation"}, "rvprop": {"ids|timestamp"}, "format": {"json"}, "formatversion": {"2"}, "maxlag": {"5"}}
	return "https://en.wikipedia.org/w/api.php?" + p.Encode()
}

func EntityURL(ids []string) string {
	p := url.Values{"action": {"wbgetentities"}, "ids": {strings.Join(ids, "|")}, "props": {"info|labels|descriptions|aliases|claims|sitelinks"}, "languages": {"en"}, "sitefilter": {"enwiki"}, "format": {"json"}, "formatversion": {"2"}, "maxlag": {"5"}}
	return "https://www.wikidata.org/w/api.php?" + p.Encode()
}

// ParseSearch accepts one finite search window, not an exhaustive identity search.
// Continuation is retained in the body; it is not followed or called complete.
func ParseSearch(raw []byte) ([]Page, error) {
	var v struct {
		Batch    json.RawMessage `json:"batchcomplete"`
		Continue json.RawMessage `json:"continue"`
		Limits   json.RawMessage `json:"limits"`
		Warnings json.RawMessage `json:"warnings"`
		Error    json.RawMessage `json:"error"`
		Query    *struct {
			Pages []Page `json:"pages"`
		} `json:"query"`
	}
	if err := strictjson.Decode(raw, &v); err != nil {
		return nil, fmt.Errorf("search schema: %w", err)
	}
	if v.Error != nil || v.Warnings != nil {
		return nil, fmt.Errorf("search API error or warning")
	}
	if string(v.Batch) != "true" {
		return nil, fmt.Errorf("search batch not complete")
	}
	var envelope map[string]json.RawMessage
	_ = json.Unmarshal(raw, &envelope) // Strict syntax and duplicate-key gate ran above.
	for _, name := range []string{"continue", "limits"} {
		if b, ok := envelope[name]; ok {
			if _, err := object(b); err != nil {
				return nil, fmt.Errorf("search %s is not an object", name)
			}
		}
	}
	if v.Query == nil {
		if _, present := envelope["query"]; present {
			return nil, fmt.Errorf("null search query")
		}
		return []Page{}, nil
	}
	if v.Query.Pages == nil || len(v.Query.Pages) > MaxHits {
		return nil, fmt.Errorf("search pages missing or over budget")
	}
	seen := map[int64]bool{}
	indexes := map[int]bool{}
	var query struct {
		Pages []json.RawMessage `json:"pages"`
	}
	_ = json.Unmarshal(envelope["query"], &query)
	for i, p := range v.Query.Pages {
		page, err := object(query.Pages[i], "pageid", "ns", "title", "index", "revisions")
		if err != nil {
			return nil, err
		}
		if props, ok := page["pageprops"]; ok {
			fields, err := object(props)
			if err != nil {
				return nil, err
			}
			for _, b := range fields {
				if string(b) == "null" {
					return nil, fmt.Errorf("null page property")
				}
			}
		}
		var revisions []json.RawMessage
		_ = json.Unmarshal(page["revisions"], &revisions)
		for _, rev := range revisions {
			if _, err := object(rev, "revid", "parentid", "timestamp"); err != nil {
				return nil, err
			}
		}
		if p.ID <= 0 || p.Namespace != 0 || p.Title == "" || p.Index < 1 || p.Index > MaxHits || seen[p.ID] || indexes[p.Index] || len(p.Revisions) != 1 {
			return nil, fmt.Errorf("invalid or repeated search page")
		}
		seen[p.ID], indexes[p.Index] = true, true
		for k, val := range p.Props {
			if k != "wikibase_item" && k != "disambiguation" {
				return nil, fmt.Errorf("unknown page property")
			}
			if k == "wikibase_item" && !qidPattern.MatchString(val) {
				return nil, fmt.Errorf("invalid page QID")
			}
		}
		if p.Revisions[0].ID <= 0 || p.Revisions[0].Parent < 0 {
			return nil, fmt.Errorf("missing page revision")
		}
		if _, err := time.Parse(time.RFC3339, p.Revisions[0].Timestamp); err != nil {
			return nil, fmt.Errorf("invalid page revision time")
		}
	}
	sort.Slice(v.Query.Pages, func(i, j int) bool { return v.Query.Pages[i].Index < v.Query.Pages[j].Index })
	return v.Query.Pages, nil
}

func PageIDs(pages []Page) []string {
	set := map[string]bool{}
	for _, p := range pages {
		if id := p.Props["wikibase_item"]; id != "" {
			set[id] = true
		}
	}
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func ParseEntities(raw []byte, ids []string) (map[string]Entity, error) {
	var v struct {
		Entities map[string]Entity `json:"entities"`
		Success  int               `json:"success"`
		Error    json.RawMessage   `json:"error"`
		Warnings json.RawMessage   `json:"warnings"`
	}
	if err := strictjson.Decode(raw, &v); err != nil {
		return nil, fmt.Errorf("entity schema: %w", err)
	}
	if v.Success != 1 || v.Error != nil || v.Warnings != nil || len(v.Entities) != len(ids) {
		return nil, fmt.Errorf("incomplete entity response")
	}
	var envelope struct {
		Entities map[string]json.RawMessage `json:"entities"`
	}
	_ = json.Unmarshal(raw, &envelope)
	for _, id := range ids {
		e, ok := v.Entities[id]
		if !ok || e.ID != id || !qidPattern.MatchString(id) {
			return nil, fmt.Errorf("entity ID mismatch")
		}
		if e.Missing != nil {
			if string(e.Missing) != "true" && string(e.Missing) != `""` {
				return nil, fmt.Errorf("invalid missing-entity flag")
			}
			if e.Type != "" || e.LastRevision != 0 || e.Labels != nil || e.Claims != nil {
				return nil, fmt.Errorf("conflicting missing entity")
			}
			continue
		}
		fields, err := object(envelope.Entities[id], "id", "type", "pageid", "ns", "title", "lastrevid", "modified")
		if err != nil {
			return nil, err
		}
		for _, name := range []string{"labels", "descriptions", "aliases", "claims", "sitelinks"} {
			if b, present := fields[name]; present {
				if _, err := object(b); err != nil {
					return nil, fmt.Errorf("entity %s is not an object", name)
				}
			}
		}
		if e.Type != "item" || e.Title != id || e.PageID <= 0 || e.Namespace != 0 || e.LastRevision <= 0 {
			return nil, fmt.Errorf("invalid entity metadata")
		}
		if _, err := time.Parse(time.RFC3339, e.Modified); err != nil {
			return nil, fmt.Errorf("invalid entity revision time")
		}
		for _, m := range []map[string]Term{e.Labels, e.Descriptions} {
			for lang, t := range m {
				if lang != "en" || t.Language != lang || t.Value == "" {
					return nil, fmt.Errorf("invalid entity term")
				}
			}
		}
		for lang, a := range e.Aliases {
			if lang != "en" || a == nil {
				return nil, fmt.Errorf("invalid alias language/array")
			}
			for _, t := range a {
				if lang != "en" || t.Language != lang || t.Value == "" {
					return nil, fmt.Errorf("invalid entity alias")
				}
			}
		}
		for p, claims := range e.Claims {
			if !propertyID(p) {
				return nil, fmt.Errorf("invalid property ID")
			}
			var statements []map[string]json.RawMessage
			if err := json.Unmarshal(claims, &statements); err != nil || statements == nil {
				return nil, fmt.Errorf("claims are not a statement array")
			}
			for _, statement := range statements {
				if statement == nil {
					return nil, fmt.Errorf("null statement")
				}
			}
		}
		for _, site := range e.Sitelinks {
			if _, err := object(site); err != nil {
				return nil, fmt.Errorf("sitelink is not an object")
			}
		}
	}
	return v.Entities, nil
}

func object(raw []byte, required ...string) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return nil, fmt.Errorf("required object is absent, null or has wrong type")
	}
	for _, name := range required {
		if b, ok := fields[name]; !ok || string(b) == "null" {
			return nil, fmt.Errorf("required field %s absent or null", name)
		}
	}
	return fields, nil
}

func propertyID(s string) bool { return len(s) > 1 && s[0] == 'P' && qidPattern.MatchString("Q"+s[1:]) }

// ShapeFingerprint records all observed JSON paths/types (array indexes are
// wildcarded). It describes this response, not a remotely adopted schema.
func ShapeFingerprint(raw []byte) string {
	if strictjson.Decode(raw, nil) != nil {
		return ""
	}
	var v any
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.UseNumber()
	if d.Decode(&v) != nil {
		return ""
	}
	paths := map[string]bool{}
	var visit func(string, any)
	visit = func(p string, v any) {
		switch x := v.(type) {
		case map[string]any:
			paths[p+":object"] = true
			for k, c := range x {
				visit(p+"/"+strings.ReplaceAll(strings.ReplaceAll(k, "~", "~0"), "/", "~1"), c)
			}
		case []any:
			paths[p+":array"] = true
			for _, c := range x {
				visit(p+"/*", c)
			}
		case string:
			paths[p+":string"] = true
		case json.Number:
			paths[p+":number"] = true
		case bool:
			paths[p+":boolean"] = true
		case nil:
			paths[p+":null"] = true
		}
	}
	visit("", v)
	keys := make([]string, 0, len(paths))
	for k := range paths {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return Hash([]byte(strings.Join(keys, "\n")))
}
