package occurrence

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/klauspost/compress/zstd"
)

type artifactJSONReader struct {
	expected     Artifact
	file         *os.File
	compressed   *countingHashReader
	decoder      *zstd.Decoder
	uncompressed *countingHashReader
	jsonDecoder  *json.Decoder
	records      uint64
	lastKey      string
	finished     bool
}

func openArtifactJSON(ctx context.Context, storageRoot string, artifact Artifact) (*artifactJSONReader, error) {
	path, err := resolveStorageKey(storageRoot, artifact.StorageKey)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	compressed := &countingHashReader{reader: &contextReader{ctx: ctx, reader: file}, hash: sha256.New()}
	decoder, err := zstd.NewReader(compressed, zstd.WithDecoderConcurrency(2), zstd.WithDecoderLowmem(true), zstd.WithDecoderMaxMemory(64<<20))
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	uncompressed := &countingHashReader{reader: decoder, hash: sha256.New()}
	return &artifactJSONReader{
		expected:     artifact,
		file:         file,
		compressed:   compressed,
		decoder:      decoder,
		uncompressed: uncompressed,
		jsonDecoder:  json.NewDecoder(bufio.NewReaderSize(uncompressed, 256<<10)),
	}, nil
}

func (reader *artifactJSONReader) Next() (NaturalIndexEntry, bool, error) {
	var entry NaturalIndexEntry
	err := reader.jsonDecoder.Decode(&entry)
	if err == nil {
		if err := validateNaturalIndexEntry(entry); err != nil {
			return entry, false, err
		}
		if reader.lastKey != "" && entry.NaturalKey <= reader.lastKey {
			return entry, false, fmt.Errorf("natural index is not strictly sorted")
		}
		reader.lastKey = entry.NaturalKey
		reader.records++
		return entry, true, nil
	}
	if err != io.EOF {
		return entry, false, err
	}
	if err := reader.finish(); err != nil {
		return entry, false, err
	}
	return entry, false, nil
}

func (reader *artifactJSONReader) finish() error {
	if reader.finished {
		return nil
	}
	reader.finished = true
	if reader.records != reader.expected.RecordCount {
		return fmt.Errorf("natural-index record count mismatch")
	}
	if reader.compressed.bytes != reader.expected.CompressedBytes || hex.EncodeToString(reader.compressed.hash.Sum(nil)) != reader.expected.CompressedSHA256 {
		return fmt.Errorf("natural-index compressed identity mismatch")
	}
	if reader.uncompressed.bytes != reader.expected.UncompressedBytes || hex.EncodeToString(reader.uncompressed.hash.Sum(nil)) != reader.expected.UncompressedSHA256 {
		return fmt.Errorf("natural-index uncompressed identity mismatch")
	}
	return nil
}

func validateNaturalIndexEntry(entry NaturalIndexEntry) error {
	if entry.NaturalKey == "" || entry.OccurrenceCount == 0 || !digestPattern.MatchString(entry.StateDigest) {
		return fmt.Errorf("natural-index entry identity is invalid")
	}
	switch entry.State {
	case "unique":
		if entry.OccurrenceCount != 1 || entry.RowOrdinal == 0 || !digestPattern.MatchString(entry.OccurrenceID) ||
			!digestPattern.MatchString(entry.RecordVersionID) || !digestPattern.MatchString(entry.SemanticDigest) {
			return fmt.Errorf("unique natural-index entry is incomplete")
		}
	case "duplicate":
		if entry.OccurrenceCount < 2 || entry.RowOrdinal != 0 || entry.OccurrenceID != "" || entry.RecordVersionID != "" || entry.SemanticDigest != "" {
			return fmt.Errorf("duplicate natural-index entry is invalid")
		}
	case "invalid":
		if entry.OccurrenceCount != 1 || entry.RowOrdinal != 0 || entry.OccurrenceID != "" || entry.RecordVersionID != "" || entry.SemanticDigest != "" {
			return fmt.Errorf("invalid natural-index entry is invalid")
		}
	default:
		return fmt.Errorf("natural-index entry state is invalid")
	}
	return nil
}

func (reader *artifactJSONReader) Close() error {
	reader.decoder.Close()
	return reader.file.Close()
}

func compareNaturalIndexes(ctx context.Context, storageRoot string, current Artifact, prior *Artifact, writer *artifactWriter) (ChangeCounts, error) {
	currentReader, err := openArtifactJSON(ctx, storageRoot, current)
	if err != nil {
		return ChangeCounts{}, err
	}
	defer func() { _ = currentReader.Close() }()
	var priorReader *artifactJSONReader
	if prior != nil {
		priorReader, err = openArtifactJSON(ctx, storageRoot, *prior)
		if err != nil {
			return ChangeCounts{}, err
		}
		defer func() { _ = priorReader.Close() }()
	}

	currentEntry, hasCurrent, err := currentReader.Next()
	if err != nil {
		return ChangeCounts{}, err
	}
	var priorEntry NaturalIndexEntry
	hasPrior := false
	if priorReader != nil {
		priorEntry, hasPrior, err = priorReader.Next()
		if err != nil {
			return ChangeCounts{}, err
		}
	}

	var counts ChangeCounts
	for hasCurrent || hasPrior {
		if err := ctx.Err(); err != nil {
			return counts, err
		}
		switch {
		case !hasPrior || hasCurrent && currentEntry.NaturalKey < priorEntry.NaturalKey:
			change := changeFor(nil, &currentEntry, &counts)
			if err := writer.WriteJSON(change); err != nil {
				return counts, err
			}
			currentEntry, hasCurrent, err = currentReader.Next()
		case !hasCurrent || priorEntry.NaturalKey < currentEntry.NaturalKey:
			change := changeFor(&priorEntry, nil, &counts)
			if err := writer.WriteJSON(change); err != nil {
				return counts, err
			}
			if priorReader != nil {
				priorEntry, hasPrior, err = priorReader.Next()
			}
		default:
			change := changeFor(&priorEntry, &currentEntry, &counts)
			if change != nil {
				if err := writer.WriteJSON(change); err != nil {
					return counts, err
				}
			}
			currentEntry, hasCurrent, err = currentReader.Next()
			if priorReader != nil {
				priorEntry, hasPrior, err = priorReader.Next()
			}
		}
		if err != nil {
			return counts, err
		}
	}
	return counts, nil
}

func changeFor(prior, current *NaturalIndexEntry, counts *ChangeCounts) *Change {
	change := &Change{PriorState: "missing", CurrentState: "missing"}
	if prior != nil {
		change.NaturalKey = prior.NaturalKey
		change.PriorState = prior.State
		change.PriorOccurrenceID = prior.OccurrenceID
		change.PriorRecordVersionID = prior.RecordVersionID
		change.PriorSemanticDigest = prior.SemanticDigest
	}
	if current != nil {
		change.NaturalKey = current.NaturalKey
		change.CurrentState = current.State
		change.CurrentOccurrenceID = current.OccurrenceID
		change.CurrentRecordVersionID = current.RecordVersionID
		change.CurrentSemanticDigest = current.SemanticDigest
	}
	switch {
	case current == nil:
		change.Change = "absent"
		counts.Absent++
	case current.State != "unique":
		change.Change = "invalid"
		counts.Invalid++
	case prior == nil:
		change.Change = "added"
		counts.Added++
	case prior.State != "unique" || prior.SemanticDigest != current.SemanticDigest:
		change.Change = "changed"
		counts.Changed++
	default:
		counts.Unchanged++
		return nil
	}
	return change
}
