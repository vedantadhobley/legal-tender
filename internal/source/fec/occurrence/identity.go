package occurrence

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"sort"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulea"
)

func occurrenceID(snapshotID, relation, cycle string, rowOrdinal uint64) string {
	return digestParts("fec.schedule-a.occurrence.v1", snapshotID, relation, cycle, fmt.Sprintf("%d", rowOrdinal))
}

func naturalKey(cycle, subID string) string {
	return "fec:schedule-a:" + cycle + ":" + subID
}

func recordVersionID(cycle, publisherReference, rawDigest string) string {
	return digestParts("fec.schedule-a.record-version.v1", "fec", "schedule-a", cycle, publisherReference, rawDigest)
}

func issueID(occurrenceID, code string) string {
	return digestParts("fec.schedule-a.issue.v1", occurrenceID, code)
}

func digestParts(parts ...string) string {
	hasher := sha256.New()
	for _, part := range parts {
		writeLength(hasher, uint64(len(part)))
		_, _ = hasher.Write([]byte(part))
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

func semanticDigest(row *schedulea.Row) (string, error) {
	digest, err := semanticDigestBytes(row)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(digest[:]), nil
}

func semanticDigestBytes(row *schedulea.Row) ([sha256.Size]byte, error) {
	if err := schedulea.Validate(row, ""); err != nil {
		return [sha256.Size]byte{}, err
	}
	hasher := sha256.New()
	_, _ = hasher.Write([]byte(SemanticSchemaVersion))
	columns := schedulea.Columns()
	for index := range columns {
		field, ok := row.Field(index)
		if !ok {
			return [sha256.Size]byte{}, fmt.Errorf("semantic field %d is missing", index+1)
		}
		if field.IsNull() {
			_, _ = hasher.Write([]byte{0})
			continue
		}
		_, _ = hasher.Write([]byte{1})
		writeLength(hasher, uint64(len(field.Bytes())))
		_, _ = hasher.Write(field.Bytes())
	}
	var digest [sha256.Size]byte
	copy(digest[:], hasher.Sum(nil))
	return digest, nil
}

func writeLength(hasher hash.Hash, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = hasher.Write(encoded[:])
}

func validationCodes(err error) []string {
	known := []struct {
		target error
		code   string
	}{
		{schedulea.ErrMissingLineFeed, "missing_line_feed"},
		{schedulea.ErrFieldCount, "field_count"},
		{schedulea.ErrInvalidEscape, "invalid_copy_escape"},
		{schedulea.ErrNullRequired, "required_null"},
		{schedulea.ErrInvalidLexeme, "invalid_lexeme"},
		{schedulea.ErrPeriodMismatch, "period_mismatch"},
	}
	codes := make([]string, 0, 2)
	for _, candidate := range known {
		if errors.Is(err, candidate.target) {
			codes = append(codes, candidate.code)
		}
	}
	if len(codes) == 0 {
		codes = append(codes, "row_validation")
	}
	sort.Strings(codes)
	return codes
}
