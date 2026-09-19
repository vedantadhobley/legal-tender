package reportscope

import (
	"context"
	"encoding/json"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
)

func censusRow(tag string, width int) string {
	f := make([]string, width)
	f[0], f[1] = tag, "C12345678"
	return strings.Join(f, "\x1c")
}

func TestScheduleACensus(t *testing.T) {
	for _, tc := range []struct {
		tag   string
		width int
		issue string
	}{
		{"SA11AI", 45, ""}, {"SA11B", 45, ""}, {"SA11C", 45, ""}, {"SA12", 45, ""}, {"SA13", 45, ""},
		{"SA14", 45, ""}, {"SA15", 45, ""}, {"SA16", 45, ""}, {"SA17", 45, ""},
		{"SB21B", 44, ""}, {"SC/10", 38, ""}, {"TEXT", 6, ""},
		// Non-SA tags identify record families only, not a valid financial line.
		{"SB99", 44, ""}, {"SC/99", 38, ""},
		{"SA11B", 44, "record_width_mismatch"}, {"SA11B", 46, "record_width_mismatch"},
		{"SB21B", 43, "record_width_mismatch"}, {"SC/10", 39, "record_width_mismatch"}, {"TEXT", 5, "record_width_mismatch"},
		{"SA11D", 45, "unreviewed_sa_line"}, {"SA19A", 45, "unreviewed_sa_line"}, {"SA11b", 45, "unreviewed_sa_line"},
		{"sa11B", 45, "unreviewed_record_family"}, {"SL11B", 45, "unreviewed_record_family"},
		{"SD10", 32, "unreviewed_record_family"}, {"SC1/10", 36, "unreviewed_record_family"},
		{"SB", 44, "unreviewed_record_family"}, {"SB11b", 44, "unreviewed_record_family"}, {"UNKNOWN", 45, "unreviewed_record_family"},
	} {
		t.Run(tc.tag+"_"+strconv.Itoa(tc.width), func(t *testing.T) {
			rows := append(electronicFixture(false), censusRow(tc.tag, tc.width))
			r, err := InventoryScheduleALines(context.Background(), electronicRequest(t, rows, false))
			if err != nil {
				t.Fatal(err)
			}
			if tc.issue == "" {
				if !r.Complete || len(r.Lines)+len(r.OtherRecords) != 1 {
					t.Fatal(r)
				}
			} else if r.Complete || len(r.Issues) != 1 || r.Issues[0] != (CensusIssue{3, tc.issue}) {
				t.Fatal(r)
			}
		})
	}
}

func TestCensusCompletenessAndIdentity(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func([]string) []string
		prefix bool
	}{
		{"prefix", func(r []string) []string { return r }, true},
		{"wrong_filer", func(r []string) []string { setField(r, 2, 2, "C99999999"); return r }, false},
		{"undecodable", func(r []string) []string { setField(r, 2, 3, "\xff"); return r }, false},
		{"control_byte", func(r []string) []string { setField(r, 2, 3, "\t"); return r }, false},
		{"tag_only", func(r []string) []string { r[2] = "SA11B"; return r }, false},
		{"blank_row", func(r []string) []string { return append(r, "") }, false},
		{"second_header", func(r []string) []string { return append(r, r[0]) }, false},
		{"version", func(r []string) []string { setField(r, 0, 3, "8.5"); return r }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := InventoryScheduleALines(context.Background(), electronicRequest(t, tc.mutate(append(electronicFixture(false), censusRow("SA11B", 45))), tc.prefix))
			if err != nil || r.Complete {
				t.Fatal(r, err)
			}
		})
	}
	rq := electronicRequest(t, electronicFixture(true), false)
	r, err := InventoryScheduleALines(context.Background(), rq)
	if err != nil || !r.Complete || len(r.Lines) != 0 {
		t.Fatal(r, err)
	}
	rq.BodySHA256 = strings.Repeat("0", 64)
	if _, err := InventoryScheduleALines(context.Background(), rq); err == nil {
		t.Fatal("accepted changed original")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := InventoryScheduleALines(ctx, rq); err == nil {
		t.Fatal("ignored cancellation")
	}
}

func TestCensusFormMapAndOrdinals(t *testing.T) {
	body, err := os.ReadFile("../../../../contracts/calculations/fec/receipt-families/v1/contract.json")
	if err != nil {
		t.Fatal(err)
	}
	var c struct {
		Forms map[string]struct {
			Leaves []struct {
				Line     string
				Schedule string `json:"detail_schedule"`
			}
		}
	}
	if err := json.Unmarshal(body, &c); err != nil {
		t.Fatal(err)
	}
	for _, form := range []string{"F3", "F3X"} {
		lines := []string{}
		for _, leaf := range c.Forms[form].Leaves {
			if leaf.Schedule == "SA" {
				lines = append(lines, leaf.Line)
			}
		}
		if !slices.Equal(lines, ScheduleALines(form)) {
			t.Fatal(form, lines, ScheduleALines(form))
		}
		rows := electronicFixture(form == "F3")
		for _, line := range lines {
			rows = append(rows, censusRow("SA"+line, 45))
		}
		rows = append(rows, censusRow("SA11B", 45))
		r, err := InventoryScheduleALines(context.Background(), electronicRequest(t, rows, false))
		if err != nil || !r.Complete {
			t.Fatal(r, err)
		}
		for _, line := range r.Lines {
			for _, ordinal := range line.RecordOrdinals {
				if !strings.HasPrefix(rows[ordinal-1], line.Tag+"\x1c") {
					t.Fatal("lost ordinal", line)
				}
			}
		}
	}
	for _, form := range []string{"F3N", "F3P", "f3", ""} {
		if len(ScheduleALines(form)) != 0 {
			t.Fatal("accepted alias")
		}
	}
}
