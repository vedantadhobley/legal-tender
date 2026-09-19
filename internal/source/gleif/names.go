package gleif

import (
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/vedantadhobley/legal-tender/internal/nameform"
	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

const NameContract = "gleif/name-search@1.0.0"
const NamePolicy = "reported-organization-name-search.v1"
const NamePageSize = 5

// NameInput is a reported organization field, not a resolved company. The
// source adapter authenticates its bytes; arbitrary caller pins are not proof.
type NameInput struct {
	SHA256  string  `json:"source_sha256"`
	Locator string  `json:"source_locator"`
	Name    *string `json:"reported_name"`
}
type NameQuery struct {
	Text   string `json:"search_text"`
	Inputs []int  `json:"input_indexes"`
}
type NamePlan struct {
	Policy      string      `json:"policy"`
	BuildSHA256 string      `json:"build_sha256"`
	Selection   string      `json:"selection"`
	Inputs      []NameInput `json:"inputs"`
	States      []string    `json:"input_states"`
	Queries     []NameQuery `json:"queries"`
}

func PlanNames(inputs []NameInput, selection, build string) (NamePlan, error) {
	p := NamePlan{Policy: NamePolicy, BuildSHA256: build, Selection: selection, Inputs: slices.Clone(inputs), States: []string{}, Queries: []NameQuery{}}
	if !wikimedia.Digest(build) || strings.TrimSpace(selection) == "" || len(selection) > 2000 || len(inputs) == 0 || len(inputs) > MaxRecords {
		return p, fmt.Errorf("registry name input scope/budget")
	}
	sort.Slice(p.Inputs, func(i, j int) bool {
		a, b := p.Inputs[i], p.Inputs[j]
		if a.SHA256 != b.SHA256 {
			return a.SHA256 < b.SHA256
		}
		return a.Locator < b.Locator
	})
	queries := map[string][]int{}
	valid := func(s string, max int) bool {
		return len(s) <= max && utf8.ValidString(s) && !strings.ContainsFunc(s, unicode.IsControl)
	}
	for i, a := range p.Inputs {
		if !wikimedia.Digest(a.SHA256) || !valid(a.Locator, 1000) || strings.TrimSpace(a.Locator) == "" || (i > 0 && a.SHA256 == p.Inputs[i-1].SHA256 && a.Locator == p.Inputs[i-1].Locator) {
			return p, fmt.Errorf("invalid/repeated registry name source")
		}
		text := ""
		if a.Name != nil {
			if !valid(*a.Name, 500) {
				return p, fmt.Errorf("unsupported reported organization text")
			}
			text = nameform.Normalize(*a.Name)
		}
		state := "no_searchable_name"
		if text != "" {
			queries[text] = append(queries[text], i)
			state = "name_query_planned"
		}
		p.States = append(p.States, state)
	}
	for text, indexes := range queries {
		p.Queries = append(p.Queries, NameQuery{Text: text, Inputs: indexes})
	}
	sort.Slice(p.Queries, func(i, j int) bool { return p.Queries[i].Text < p.Queries[j].Text })
	return p, nil
}

func (p NamePlan) Validate() error {
	want, err := PlanNames(p.Inputs, p.Selection, p.BuildSHA256)
	if err != nil || !reflect.DeepEqual(p, want) {
		return fmt.Errorf("registry name plan is not source-derived")
	}
	return nil
}

func NameURL(text string, page int) string {
	q := url.Values{"filter[entity.names]": {text}, "page[size]": {fmt.Sprint(NamePageSize)}, "page[number]": {fmt.Sprint(page)}}
	return "https://api.gleif.org/api/v1/lei-records?" + q.Encode()
}

type NamePagination struct {
	CurrentPage int  `json:"currentPage"`
	PerPage     int  `json:"perPage"`
	From        *int `json:"from"`
	To          *int `json:"to"`
	Total       int  `json:"total"`
	LastPage    int  `json:"lastPage"`
}
type NamePage struct {
	PublishDate    string            `json:"golden_copy_publish_date"`
	Pagination     NamePagination    `json:"pagination"`
	Links          map[string]string `json:"links"`
	Records        []Record          `json:"records"`
	WindowComplete bool              `json:"publisher_query_window_complete"`
}

// ParseNames reads only the first five-hit window. The upstream search can be
// broader than a literal name match. Empty results never prove company absence.
func ParseNames(raw []byte, text string) (NamePage, error) {
	out := NamePage{Records: []Record{}}
	if len(raw) > MaxBody || text == "" || nameform.Normalize(text) != text {
		return out, fmt.Errorf("name-search input/budget")
	}
	if err := strictjson.Decode(raw, nil); err != nil {
		return out, err
	}
	env, err := object(raw, "data meta links", "")
	if err != nil {
		return out, err
	}
	meta, err := object(env["meta"], "goldenCopy pagination", "")
	if err != nil {
		return out, err
	}
	copy, err := object(meta["goldenCopy"], "publishDate", "")
	if err != nil {
		return out, err
	}
	out.PublishDate = stringValue(copy["publishDate"])
	if _, err = time.Parse(time.RFC3339, out.PublishDate); err != nil {
		return out, fmt.Errorf("registry search snapshot time")
	}
	paginationFields, err := object(meta["pagination"], "currentPage perPage from to total lastPage", "")
	if err != nil {
		return out, err
	}
	for _, key := range []string{"currentPage", "perPage", "total", "lastPage"} {
		if string(paginationFields[key]) == "null" {
			return out, fmt.Errorf("null registry pagination count")
		}
	}
	if err = strictjson.Decode(meta["pagination"], &out.Pagination); err != nil {
		return out, err
	}
	var records []json.RawMessage
	if err = json.Unmarshal(env["data"], &records); err != nil || records == nil || len(records) > NamePageSize {
		return out, fmt.Errorf("registry search records shape/budget")
	}
	p := out.Pagination
	last := 1
	if p.Total > 0 {
		last = (p.Total-1)/NamePageSize + 1
	}
	if p.CurrentPage != 1 || p.PerPage != NamePageSize || p.Total < 0 || p.LastPage != last || len(records) != min(p.Total, NamePageSize) {
		return out, fmt.Errorf("registry pagination conservation")
	}
	if p.Total == 0 {
		if p.From != nil || p.To != nil {
			return out, fmt.Errorf("empty pagination bounds")
		}
	} else if p.From == nil || p.To == nil || *p.From != 1 || *p.To != len(records) {
		return out, fmt.Errorf("pagination bounds")
	}
	if _, err = object(env["links"], "first last", "next prev self"); err != nil {
		return out, err
	}
	if err = strictjson.Decode(env["links"], &out.Links); err != nil {
		return out, err
	}
	if out.Links["first"] != NameURL(text, 1) || out.Links["last"] != NameURL(text, last) {
		return out, fmt.Errorf("registry pagination link identity")
	}
	if self, ok := out.Links["self"]; ok && self != NameURL(text, 1) {
		return out, fmt.Errorf("registry search self link")
	}
	if out.Links["prev"] != "" {
		return out, fmt.Errorf("unexpected previous page")
	}
	if p.Total > NamePageSize {
		if out.Links["next"] != NameURL(text, 2) {
			return out, fmt.Errorf("missing/mismatched next page")
		}
	} else if out.Links["next"] != "" {
		return out, fmt.Errorf("unexpected next page")
	}
	seen := map[string]bool{}
	for _, r := range records {
		var id struct {
			ID string `json:"id"`
		}
		_ = json.Unmarshal(r, &id)
		if seen[id.ID] {
			return out, fmt.Errorf("repeated registry search LEI")
		}
		seen[id.ID] = true
		v, err := parseResource(r, meta["goldenCopy"], id.ID)
		if err != nil {
			return out, err
		}
		out.Records = append(out.Records, v)
	}
	out.WindowComplete = p.Total <= NamePageSize
	return out, nil
}
