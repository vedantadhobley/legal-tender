// Package graphread defines bounded, typed read results shared by graph families.
package graphread

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
	"regexp"
)

const MaxLimit = 10

var committee = regexp.MustCompile(`^C[0-9]{8}$`)
var candidate = regexp.MustCompile(`^[HSP][A-Z0-9]{8}$`)
var digest = regexp.MustCompile(`^[a-f0-9]{64}$`)

func Kind(id string) string {
	if committee.MatchString(id) {
		return "committee"
	}
	if candidate.MatchString(id) {
		return "candidate"
	}
	return ""
}
func ValidPage(id, after string, limit int) error {
	if Kind(id) == "" || limit < 1 || limit > MaxLimit || after != "" && !digest.MatchString(after) {
		return fmt.Errorf("invalid entity, page size or ordering key")
	}
	return nil
}

type Facet struct {
	State    string          `json:"state"`
	Document json.RawMessage `json:"document"`
}
type Item struct {
	Key          string          `json:"key"`
	Document     json.RawMessage `json:"document"`
	EvidenceKind string          `json:"evidence_kind"`
	Evidence     json.RawMessage `json:"evidence"`
}
type Page struct {
	State     string `json:"state"`
	Items     []Item `json:"items"`
	HasMore   bool   `json:"has_more"`
	NextAfter string `json:"-"`
}

func Empty(state string) Page { return Page{State: state, Items: []Item{}} }

// Canonical preserves integer/string/null distinctions while removing backend
// property-order noise. It is not a substitute for source/model verification.
func Canonical(raw json.RawMessage) (json.RawMessage, error) {
	if err := strictjson.Decode(raw, nil); err != nil {
		return nil, err
	}
	var value any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := d.Decode(&value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}
