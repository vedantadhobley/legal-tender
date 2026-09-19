// Package gleif preserves bounded exact-LEI and name-search observations. It does not
// select a legal entity from a name, infer ownership or approve a FEC identity.
package gleif

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

const Contract = "gleif/lei-record@1.0.0"
const MaxRecords = 20
const MaxBody = 2 << 20

type Name struct {
	Name     string `json:"name"`
	Language string `json:"language"`
	Type     string `json:"type,omitempty"`
}

type Record struct {
	LEI                 string          `json:"lei"`
	LegalName           Name            `json:"legal_name"`
	OtherNames          []Name          `json:"other_names"`
	TransliteratedNames []Name          `json:"transliterated_names"`
	EntityStatus        string          `json:"entity_status"`
	RegistrationStatus  string          `json:"registration_status"`
	LastUpdate          string          `json:"last_update"`
	NextRenewal         *string         `json:"next_renewal"`
	PublishDate         string          `json:"golden_copy_publish_date"`
	Raw                 json.RawMessage `json:"raw"`
}

// ValidLEI validates ISO 7064 MOD 97-10 without repairing source identifiers.
// Legacy LEIs need not have 00 in positions five and six.
func ValidLEI(s string) bool {
	if len(s) != 20 {
		return false
	}
	r := 0
	for i, b := range []byte(s) {
		switch {
		case b >= '0' && b <= '9':
			r = (r*10 + int(b-'0')) % 97
		case i < 18 && b >= 'A' && b <= 'Z':
			r = (r*100 + int(b-'A') + 10) % 97
		default:
			return false
		}
	}
	return r == 1
}

func RecordURL(lei string) string { return "https://api.gleif.org/api/v1/lei-records/" + lei }

func Parse(raw []byte, requested string) (Record, error) {
	var out Record
	if !ValidLEI(requested) || len(raw) > MaxBody {
		return out, fmt.Errorf("invalid LEI or record budget")
	}
	if err := strictjson.Decode(raw, nil); err != nil {
		return out, err
	}
	envelope, err := object(raw, "data meta", "")
	if err != nil {
		return out, err
	}
	meta, err := object(envelope["meta"], "goldenCopy", "")
	if err != nil {
		return out, err
	}
	out, err = parseResource(envelope["data"], meta["goldenCopy"], requested)
	if err == nil {
		out.Raw = append(json.RawMessage(nil), raw...)
	}
	return out, err
}

// parseResource shares the exact-LEI record semantics with search pages. Raw is
// the exact resource slice here; Parse retains its historical full-envelope Raw.
func parseResource(raw, goldenCopy []byte, requested string) (Record, error) {
	var out Record
	if !ValidLEI(requested) {
		return out, fmt.Errorf("invalid registry resource LEI")
	}
	data, err := object(raw, "type id attributes links relationships", "")
	if err != nil {
		return out, err
	}
	if stringValue(data["type"]) != "lei-records" || stringValue(data["id"]) != requested {
		return out, fmt.Errorf("registry record identity mismatch")
	}
	attrs, err := object(data["attributes"], "lei entity registration", "bic mic ocid qcc gem spglobal conformityFlag")
	if err != nil {
		return out, err
	}
	if stringValue(attrs["lei"]) != requested {
		return out, fmt.Errorf("registry LEI mismatch")
	}
	e, err := object(attrs["entity"], "legalName otherNames transliteratedOtherNames status", "legalAddress headquartersAddress registeredAt registeredAs jurisdiction category legalForm associatedEntity expiration successorEntity successorEntities creationDate subCategory otherAddresses eventGroups")
	if err != nil {
		return out, err
	}
	reg, err := object(attrs["registration"], "status lastUpdateDate nextRenewalDate", "initialRegistrationDate managingLou corroborationLevel validatedAt validatedAs otherValidationAuthorities")
	if err != nil {
		return out, err
	}
	copy, err := object(goldenCopy, "publishDate", "")
	if err != nil {
		return out, err
	}
	if _, err = object(data["relationships"], "", "managing-lou lei-issuer field-modifications direct-parent ultimate-parent direct-children ultimate-children successor-entity successor-entities fund-manager umbrella-fund master-fund sub-funds feeder-funds"); err != nil {
		return out, err
	}
	links, err := object(data["links"], "self", "")
	if err != nil || stringValue(links["self"]) != RecordURL(requested) {
		return out, fmt.Errorf("registry self link mismatch")
	}
	if err = strictjson.Decode(e["legalName"], &out.LegalName); err != nil {
		return out, err
	}
	if out.LegalName.Name == "" || out.LegalName.Language == "" || out.LegalName.Type != "" {
		return out, fmt.Errorf("invalid registry legal name")
	}
	for _, entry := range []struct {
		raw json.RawMessage
		dst *[]Name
	}{{e["otherNames"], &out.OtherNames}, {e["transliteratedOtherNames"], &out.TransliteratedNames}} {
		if err = strictjson.Decode(entry.raw, entry.dst); err != nil {
			return out, err
		}
		if *entry.dst == nil {
			return out, fmt.Errorf("null registry name list")
		}
		for _, name := range *entry.dst {
			if name.Name == "" || name.Language == "" || name.Type == "" {
				return out, fmt.Errorf("invalid registry alternative name")
			}
		}
	}
	out.LEI = requested
	out.EntityStatus = stringValue(e["status"])
	out.RegistrationStatus = stringValue(reg["status"])
	out.LastUpdate = stringValue(reg["lastUpdateDate"])
	if err = strictjson.Decode(reg["nextRenewalDate"], &out.NextRenewal); err != nil {
		return out, err
	}
	out.PublishDate = stringValue(copy["publishDate"])
	if out.EntityStatus == "" || out.RegistrationStatus == "" {
		return out, fmt.Errorf("invalid registry status")
	}
	dates := []string{out.LastUpdate, out.PublishDate}
	if out.NextRenewal != nil {
		dates = append(dates, *out.NextRenewal)
	}
	for _, date := range dates {
		if _, err = time.Parse(time.RFC3339, date); err != nil {
			return out, fmt.Errorf("invalid registry timestamp")
		}
	}
	out.Raw = append(json.RawMessage(nil), raw...)
	return out, nil
}

// Unconsumed nested values are retained as opaque source JSON by this contract.
// Only the explicitly listed keys are accepted at the interpreted boundaries.
func object(raw []byte, required, optional string) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if len(raw) == 0 || json.Unmarshal(raw, &fields) != nil || fields == nil {
		return nil, fmt.Errorf("registry object required")
	}
	allowed := map[string]bool{}
	for _, key := range strings.Fields(required) {
		if _, ok := fields[key]; !ok {
			return nil, fmt.Errorf("registry required field absent")
		}
		allowed[key] = true
	}
	for _, key := range strings.Fields(optional) {
		allowed[key] = true
	}
	for key := range fields {
		if !allowed[key] {
			return nil, fmt.Errorf("unreviewed registry field")
		}
	}
	return fields, nil
}

func stringValue(raw []byte) string {
	var s string
	if json.Unmarshal(raw, &s) != nil {
		return ""
	}
	return s
}
