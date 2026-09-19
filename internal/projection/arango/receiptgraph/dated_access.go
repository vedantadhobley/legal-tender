package receiptgraph

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleaparquet"
)

// DatedPathEntry dates the underlying receipt occurrence, not the validity of
// a contributor identity or conduit relationship. The original entry is intact.
type DatedPathEntry struct {
	Entry PathEntry `json:"entry"`
	Date  *int32    `json:"reported_date_days"`
}

func (r *CycleReader) DatedPathEntry(ctx context.Context, family string, ordinal uint64) (DatedPathEntry, error) {
	entry, err := r.PathEntry(ctx, family, ordinal)
	return datedEntry(entry, err)
}

// Shared associations use the original receipt date, never the related memo's
// date, acquisition cycle, or graph publication time.
func (r *SharedReader) DatedPathEntry(ctx context.Context, ordinal uint64) (DatedPathEntry, error) {
	entry, err := r.PathEntry(ctx, ordinal)
	return datedEntry(entry, err)
}

func datedEntry(entry PathEntry, err error) (DatedPathEntry, error) {
	if err != nil {
		return DatedPathEntry{}, err
	}
	date, err := entryDate(entry.Source)
	if err != nil {
		return DatedPathEntry{}, err
	}
	return DatedPathEntry{Entry: entry, Date: date}, nil
}

// Read the typed, nullable date from the already source-verified full fact.
// Do not reparse a raw date or fill it from a filing date or cycle label.
func entryDate(source json.RawMessage) (*int32, error) {
	var v struct {
		Source struct {
			Fields map[string]json.RawMessage `json:"fields"`
		} `json:"source"`
	}
	if err := json.Unmarshal(source, &v); err != nil {
		return nil, err
	}
	b, ok := v.Source.Fields[scheduleaparquet.ColumnReceiptDate]
	if !ok {
		return nil, fmt.Errorf("verified receipt date field absent")
	}
	var date *int32
	if err := json.Unmarshal(b, &date); err != nil {
		return nil, err
	}
	return date, nil
}
