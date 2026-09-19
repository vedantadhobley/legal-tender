package candidateevidence

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
)

func ConnectionMarkdown(c Connection) string {
	var b strings.Builder
	names := map[string]EntityName{}
	for _, n := range c.Names {
		names[n.EntityID] = n
	}
	name := func(id string) string {
		n, ok := names[id]
		if !ok {
			n = EntityName{EntityID: id}
		}
		return display(n)
	}
	o := c.Observation
	date := "Unknown in source"
	if o.Date != nil {
		date = time.Unix(int64(*o.Date)*86400, 0).UTC().Format("2006-01-02")
	}
	fmt.Fprintf(&b, "# Source-row drilldown: %s → %s\n\n", name(o.Sender), name(o.Recipient))
	fmt.Fprintf(&b, "This is one receiver-reported Schedule A observation from the selected candidate report, not an independently confirmed payment or terminal-source allocation.\n\n| Reported date | Signed amount | Flow role | Source row | SUB_ID |\n|---|---:|---|---:|---|\n| %s | %s | %s | %d | %s |\n", date, money(fmt.Sprint(o.Amount)), safe(label(o.Role)), o.Ordinal, safe(o.SubID))
	fmt.Fprintln(&b, "\n## Filing and source evidence\n\nThe table below preserves every field in the retained Parquet fact row. Strings are quoted: an empty string is different from null, zero, or false. Fields prefixed `lt_` are typed values and technical source locators; the remaining fields retain the processed source values. No report amendment, memo, identity or cash-availability decision is changed by this lookup.\n\n| Field | Exact retained value |\n|---|---|")
	keys := make([]string, 0, len(c.Source.Fields))
	for key := range c.Source.Fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value, _ := json.Marshal(c.Source.Fields[key]) // SourceReader only emits JSON scalar types.
		fmt.Fprintf(&b, "| %s | %s |\n", safe(key), safe(string(value)))
	}
	fmt.Fprintln(&b, "\n## Verification and boundaries\n\nThe selected observation matches the pinned published membership and the full source row. Its source shard was hashed and read through the same file handle. This lookup verifies that connection; it does not recalculate the whole parent report or independently resolve candidate authorization. Names are inherited display assertions from the content-checked parent report, not new identity matches.\n\nTerminal allocation: unknown, not zero. A connection or matching amount does not prove that the same dollars travelled along a path.")
	fmt.Fprintf(&b, "\nConnection result `%s`; lookup executable SHA-256 `%s`.\n\nParent report `%s`; parent document SHA-256 `%s`; parent evidence `%s`.\n\nCalculation `%s`, manifest SHA-256 `%s`.\n\nSchedule A fact set `%s`; shard SHA-256 `%s`. The JSON retains the full A/B source ancestry and the selected observation. No newer snapshot was substituted.\n", c.ConnectionID, c.ExecutableSHA256, c.ReportID, c.ReportSHA256, c.EvidenceID, c.Calculation.CalculationSetID, c.Calculation.ManifestSHA256, c.Source.FactSetID, c.Source.ShardSHA256)
	return b.String()
}

func WriteConnectionMarkdown(w io.Writer, c Connection) error {
	text := ConnectionMarkdown(c)
	n, err := io.WriteString(w, text)
	if err == nil && n != len(text) {
		return io.ErrShortWrite
	}
	return err
}
