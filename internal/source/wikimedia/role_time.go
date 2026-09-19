package wikimedia

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

// RoleTime retains the publisher's precision. Text is a date at that precision,
// never a fabricated validity bound. No receipt date or retrieval date enters here.
type RoleTime struct {
	Property  string `json:"property"`
	Index     int    `json:"index"`
	Raw       []byte `json:"raw_snak_bytes"`
	Time      string `json:"reported_time,omitempty"`
	Precision *int   `json:"precision,omitempty"`
	Timezone  *int   `json:"timezone,omitempty"`
	Before    *int   `json:"before,omitempty"`
	After     *int   `json:"after,omitempty"`
	Calendar  string `json:"calendar,omitempty"`
	Text      string `json:"precision_preserving_text,omitempty"`
	State     string `json:"state"`
}

var roleDate = regexp.MustCompile(`^\+([0-9]{4,11})-([0-9]{2})-([0-9]{2})T00:00:00Z$`)

func extractRoleTime(property string, index int, raw json.RawMessage) RoleTime {
	r := RoleTime{Property: property, Index: index, Raw: append(json.RawMessage(nil), raw...), State: "unsupported_time_shape"}
	s, issue := parseRoleSnak(raw, property)
	if issue != "" {
		return r
	}
	if s.Type != "value" {
		r.State = "source_" + s.Type
		return r
	}
	if s.Datatype != "time" || s.Value.Type != "time" {
		return r
	}
	var v struct {
		Time      string `json:"time"`
		Precision *int   `json:"precision"`
		Timezone  *int   `json:"timezone"`
		Before    *int   `json:"before"`
		After     *int   `json:"after"`
		Calendar  string `json:"calendarmodel"`
	}
	if strictjson.Decode(s.Value.Value, &v) != nil {
		return r
	}
	r.Time, r.Precision, r.Timezone, r.Before, r.After, r.Calendar = v.Time, v.Precision, v.Timezone, v.Before, v.After, v.Calendar
	if v.Precision == nil || v.Timezone == nil || v.Before == nil || v.After == nil || v.Calendar == "" {
		return r
	}
	r.State = "unsupported_time_profile"
	if v.Calendar != "http://www.wikidata.org/entity/Q1985727" || *v.Timezone != 0 || *v.Before != 0 || *v.After != 0 || *v.Precision < 9 || *v.Precision > 11 {
		return r
	}
	parts := roleDate.FindStringSubmatch(v.Time)
	if parts == nil {
		return r
	}
	year, err := strconv.Atoi(parts[1])
	if err != nil || year < 1 || year > 9999 {
		return r
	}
	text := fmt.Sprintf("%04d", year)
	switch *v.Precision {
	case 9:
		r.Text, r.State = text, "year_precision"
	case 10:
		text += "-" + parts[2]
		if _, err := time.Parse("2006-01", text); err == nil {
			r.Text, r.State = text, "month_precision"
		}
	case 11:
		text += "-" + parts[2] + "-" + parts[3]
		if _, err := time.Parse(time.DateOnly, text); err == nil {
			r.Text, r.State = text, "day_precision"
		}
	}
	return r
}
