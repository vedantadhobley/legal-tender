package occurrence

import (
	"bufio"
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"sort"
)

const defaultShardCount = 512

type shardEntry struct {
	NaturalKey      string `json:"natural_key"`
	OccurrenceID    string `json:"occurrence_id"`
	RecordVersionID string `json:"record_version_id"`
	SemanticDigest  string `json:"semantic_digest"`
	RowOrdinal      uint64 `json:"row_ordinal"`
	Valid           bool   `json:"valid"`
}

type shardFile struct {
	path    string
	file    *os.File
	buffer  *bufio.Writer
	encoder *json.Encoder
}

type shardSet struct {
	context          context.Context
	dir              string
	shards           []shardFile
	closed           bool
	stateDomain      string
	issueDomain      string
	duplicateMessage string
}

func newShardSet(ctx context.Context, parent string, count int) (*shardSet, error) {
	return newConfiguredShardSet(
		ctx,
		parent,
		count,
		"fec.schedule-a.natural-state.v1",
		"fec.schedule-a.issue.v1",
		"publisher natural key occurs more than once in this snapshot partition",
	)
}

func newConfiguredShardSet(ctx context.Context, parent string, count int, stateDomain, issueDomain, duplicateMessage string) (*shardSet, error) {
	if count <= 0 {
		count = defaultShardCount
	}
	if stateDomain == "" || issueDomain == "" || duplicateMessage == "" {
		return nil, fmt.Errorf("natural-key shard identity configuration is required")
	}
	dir, err := os.MkdirTemp(parent, ".natural-key-shards-*")
	if err != nil {
		return nil, err
	}
	result := &shardSet{
		context: ctx, dir: dir, shards: make([]shardFile, count),
		stateDomain: stateDomain, issueDomain: issueDomain, duplicateMessage: duplicateMessage,
	}
	for index := range result.shards {
		path := filepath.Join(dir, fmt.Sprintf("%04d.rows", index))
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			result.Remove()
			return nil, err
		}
		buffer := bufio.NewWriterSize(file, 64<<10)
		result.shards[index] = shardFile{path: path, file: file, buffer: buffer, encoder: json.NewEncoder(buffer)}
	}
	return result, nil
}

func (set *shardSet) Add(entry shardEntry) error {
	if set.closed {
		return fmt.Errorf("natural-key shards are closed")
	}
	if err := set.context.Err(); err != nil {
		return err
	}
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(entry.NaturalKey))
	return set.shards[int(hasher.Sum32())%len(set.shards)].encoder.Encode(entry)
}

func (set *shardSet) Close() error {
	if set.closed {
		return nil
	}
	set.closed = true
	var first error
	for index := range set.shards {
		if err := set.shards[index].buffer.Flush(); err != nil && first == nil {
			first = err
		}
		if err := set.shards[index].file.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (set *shardSet) Remove() {
	_ = set.Close()
	_ = os.RemoveAll(set.dir)
}

func (set *shardSet) BuildIndex(indexWriter, issueWriter *artifactWriter, counts *Counts) error {
	if err := set.Close(); err != nil {
		return err
	}
	indexPaths := make([]string, len(set.shards))
	for index, shard := range set.shards {
		if err := set.context.Err(); err != nil {
			return err
		}
		entries, err := readShardEntries(shard.path)
		if err != nil {
			return err
		}
		sort.Slice(entries, func(left, right int) bool {
			if entries[left].NaturalKey == entries[right].NaturalKey {
				return entries[left].OccurrenceID < entries[right].OccurrenceID
			}
			return entries[left].NaturalKey < entries[right].NaturalKey
		})
		indexPath := filepath.Join(set.dir, fmt.Sprintf("%04d.index", index))
		if err := set.writeShardIndex(indexPath, entries, issueWriter, counts); err != nil {
			return err
		}
		indexPaths[index] = indexPath
	}
	return mergeShardIndexes(indexPaths, indexWriter)
}

func readShardEntries(path string) ([]shardEntry, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	decoder := json.NewDecoder(bufio.NewReaderSize(file, 256<<10))
	entries := make([]shardEntry, 0)
	for {
		var entry shardEntry
		if err := decoder.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (set *shardSet) writeShardIndex(path string, entries []shardEntry, issueWriter *artifactWriter, counts *Counts) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	buffer := bufio.NewWriterSize(file, 64<<10)
	encoder := json.NewEncoder(buffer)
	failed := true
	defer func() {
		_ = file.Close()
		if failed {
			_ = os.Remove(path)
		}
	}()
	for start := 0; start < len(entries); {
		end := start + 1
		for end < len(entries) && entries[end].NaturalKey == entries[start].NaturalKey {
			end++
		}
		group := entries[start:end]
		state := NaturalIndexEntry{
			NaturalKey:      group[0].NaturalKey,
			OccurrenceCount: uint64(len(group)),
		}
		if len(group) == 1 && group[0].Valid {
			state.State = "unique"
			state.RowOrdinal = group[0].RowOrdinal
			state.OccurrenceID = group[0].OccurrenceID
			state.RecordVersionID = group[0].RecordVersionID
			state.SemanticDigest = group[0].SemanticDigest
			state.StateDigest = digestParts(set.stateDomain, state.NaturalKey, state.State, state.RecordVersionID, state.SemanticDigest)
			counts.UniqueKeys++
		} else if len(group) > 1 {
			state.State = "duplicate"
			parts := []string{set.stateDomain, state.NaturalKey, state.State}
			for _, entry := range group {
				parts = append(parts, entry.RecordVersionID, entry.SemanticDigest, entry.OccurrenceID)
			}
			state.StateDigest = digestParts(parts...)
			counts.DuplicateKeys++
			counts.DuplicateOccurrences += uint64(len(group))
			for _, entry := range group {
				key := entry.NaturalKey
				issue := Issue{
					IssueID:                digestParts(set.issueDomain, entry.OccurrenceID, "duplicate_natural_key"),
					OccurrenceID:           entry.OccurrenceID,
					RowOrdinal:             entry.RowOrdinal,
					NaturalKey:             &key,
					Code:                   "duplicate_natural_key",
					Severity:               "block",
					Message:                set.duplicateMessage,
					RelatedOccurrenceCount: uint64(len(group)),
				}
				if err := issueWriter.WriteJSON(issue); err != nil {
					return err
				}
			}
		} else {
			state.State = "invalid"
			state.StateDigest = digestParts(set.stateDomain, state.NaturalKey, state.State, group[0].RecordVersionID, group[0].OccurrenceID)
			counts.InvalidKeys++
		}
		if err := encoder.Encode(state); err != nil {
			return err
		}
		start = end
	}
	if err := buffer.Flush(); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	failed = false
	return nil
}

type indexCursor struct {
	entry   NaturalIndexEntry
	decoder *json.Decoder
	file    *os.File
	index   int
}

type indexHeap []*indexCursor

func (items indexHeap) Len() int { return len(items) }
func (items indexHeap) Less(left, right int) bool {
	return items[left].entry.NaturalKey < items[right].entry.NaturalKey
}
func (items indexHeap) Swap(left, right int) { items[left], items[right] = items[right], items[left] }
func (items *indexHeap) Push(value any)      { *items = append(*items, value.(*indexCursor)) }
func (items *indexHeap) Pop() any {
	old := *items
	last := old[len(old)-1]
	*items = old[:len(old)-1]
	return last
}

func mergeShardIndexes(paths []string, writer *artifactWriter) error {
	items := make(indexHeap, 0, len(paths))
	files := make([]*os.File, 0, len(paths))
	defer func() {
		for _, file := range files {
			_ = file.Close()
		}
	}()
	for index, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		files = append(files, file)
		cursor := &indexCursor{decoder: json.NewDecoder(bufio.NewReaderSize(file, 64<<10)), file: file, index: index}
		if err := cursor.decoder.Decode(&cursor.entry); err != nil {
			if err == io.EOF {
				continue
			}
			return err
		}
		heap.Push(&items, cursor)
	}
	for items.Len() > 0 {
		cursor := heap.Pop(&items).(*indexCursor)
		if err := writer.WriteJSON(cursor.entry); err != nil {
			return err
		}
		// JSON fields omitted by `omitempty` do not clear an existing decode
		// target. Reset the cursor so duplicate and invalid entries cannot
		// inherit unique-only identity fields from the prior shard record.
		cursor.entry = NaturalIndexEntry{}
		if err := cursor.decoder.Decode(&cursor.entry); err != nil {
			if err == io.EOF {
				continue
			}
			return err
		}
		heap.Push(&items, cursor)
	}
	return nil
}
