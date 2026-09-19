// Package summarypublication publishes lossless committee-summary occurrences
// and normalized facts from an exact coordinated FEC release. It never groups
// committee assertions or establishes a cash denominator.
package summarypublication

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"strconv"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/committeesummary"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	"github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

const (
	SchemaVersion = "legal-tender.fec.committee-summary-fact-set.v1"
	FactType      = "fec.committee_summary.v1"
	basePath      = "facts/fec/committee-summary/v1"
)

// Fact combines the occurrence locator and its normalization in one physical
// record. No publisher event key has been accepted, so record versions are
// explicitly unkeyed. Even identical CSV rows have distinct occurrence IDs.
type Fact struct {
	FactType              string                  `json:"fact_type"`
	FactID                string                  `json:"fact_id"`
	OccurrenceID          string                  `json:"occurrence_id"`
	SourceRecordVersionID string                  `json:"source_record_version_id"`
	PublisherReference    string                  `json:"publisher_reference"`
	OriginSnapshotID      string                  `json:"origin_snapshot_id"`
	Cycle                 string                  `json:"cycle"`
	SourceContract        string                  `json:"source_contract"`
	ParserVersion         string                  `json:"parser_version"`
	Record                committeesummary.Record `json:"record"`
}

type Manifest struct {
	SchemaVersion               string                        `json:"schema_version"`
	FactSetID                   string                        `json:"fact_set_id"`
	FactType                    string                        `json:"fact_type"`
	State                       string                        `json:"state"`
	Cycle                       string                        `json:"cycle"`
	SourceContract              string                        `json:"source_contract"`
	ParserVersion               string                        `json:"parser_version"`
	SourceReleaseID             string                        `json:"source_release_id"`
	SourceReleaseManifestSHA256 string                        `json:"source_release_manifest_sha256"`
	SourceArtifact              release.PublishedArtifact     `json:"source_artifact"`
	RunID                       string                        `json:"run_id"`
	PublishedAt                 time.Time                     `json:"published_at"`
	Verification                committeesummary.Verification `json:"verification"`
	Facts                       artifact.Descriptor           `json:"facts"`
	ReadbackVerified            bool                          `json:"readback_verified"`
	TerminalAttributionEligible bool                          `json:"terminal_attribution_eligible"`
}

func identity(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(part)))
		_, _ = h.Write(size[:])
		_, _ = h.Write([]byte(part))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func digest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func newFact(expected committeesummary.Expected, record *committeesummary.Record) Fact {
	occurrence := identity("fec.committee-summary.occurrence.v1", expected.SHA256, "whole_csv", expected.Cycle, strconv.FormatUint(record.Ordinal, 10))
	reference := "unkeyed:" + occurrence
	version := identity("fec.committee-summary.record-version.v1", expected.Cycle, reference, record.RawSHA256)
	return Fact{
		FactType: FactType, FactID: identity(FactType, version, committeesummary.ParserVersion),
		OccurrenceID: occurrence, SourceRecordVersionID: version, PublisherReference: reference,
		OriginSnapshotID: expected.SHA256, Cycle: expected.Cycle,
		SourceContract: committeesummary.SourceContract, ParserVersion: committeesummary.ParserVersion,
		Record: *record,
	}
}

func setIdentity(releaseSHA, sourceSHA, cycle string) string {
	return identity(SchemaVersion, releaseSHA, sourceSHA, cycle, FactType, committeesummary.SourceContract, committeesummary.ParserVersion)
}
