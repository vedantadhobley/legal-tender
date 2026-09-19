package funding

import (
	"fmt"

	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	rel "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
)

func (a *adapter) sourceRelease(id, sha string) { a.ref("source_release", "release", id, sha, "") }
func (a *adapter) occurrenceChecks(checks []occ.Check) {
	for _, c := range checks {
		if c.Passed {
			a.checks = append(a.checks, c.ID)
		}
	}
}
func (a *adapter) releaseChecks(checks []rel.ReleaseCheck) {
	for _, c := range checks {
		if c.Passed {
			a.checks = append(a.checks, c.ID)
		}
	}
}
func (a *adapter) artifacts(v occ.ArtifactSet) {
	a.occurrenceArtifact("occurrences", v.Occurrences)
	a.occurrenceArtifact("issues", v.Issues)
	a.occurrenceArtifact("natural_key_index", v.NaturalIndex)
	a.occurrenceArtifact("changes", v.Changes)
}
func (a *adapter) staged(v rel.StagedOutput) {
	// Archive-direct relations have no StagedOutput. Every declared output is
	// a physical selected-member or selected-COPY file, not a virtual shard.
	if v.Representation != "selected_member_zstd" && v.Representation != "postgresql_copy_text_data_rows_zstd" {
		a.err = fmt.Errorf("unsupported_staged_representation")
		return
	}
	a.blob("staged_source", v.StorageKey, v.CompressedSHA256, v.CompressedByteCount)
}
func (a *adapter) acquired(file, sha string, size int64) {
	if size < 0 {
		a.err = fmt.Errorf("negative_artifact_size")
		return
	}
	a.blob("raw_source", file, sha, uint64(size))
}
func (a *adapter) byteManifest(kind, sha string) { a.ref(kind, kind, sha, sha, "") }
func (a *adapter) dataset(s string) {
	if s != a.r.Dataset || !classicDataset(s) {
		a.err = fmt.Errorf("classic_dataset_differs_or_unsupported")
	}
}

func (a *adapter) source() bool {
	switch a.r.Kind {
	case "a_facts":
		var v occ.ScheduleAColumnarManifest
		a.decode(&v, occ.ScheduleAColumnarFactSetSchemaVersion)
		a.id(v.FactSetID)
		a.sourceRelease(v.SourceReleaseID, v.SourceReleaseManifestSHA256)
		a.ref("occurrences", "a_occurrences", v.OccurrenceSetID, v.OccurrenceManifestSHA256, "")
		for _, s := range v.Shards {
			a.blob("parquet_shard", s.StorageKey, s.SHA256, s.Bytes)
		}
		a.require("source_contract", v.SourceContract)
		a.require("producer_contract", v.PublisherVersion)
		a.require("physical_schema", v.PhysicalSchemaVersion)
		a.require("normalizer_contract", v.NormalizerVersion)
		a.occurrenceChecks(v.Checks)
	case "b_facts":
		var v occ.ScheduleBColumnarManifest
		a.decode(&v, occ.ScheduleBColumnarFactSetSchemaVersion)
		a.id(v.FactSetID)
		a.sourceRelease(v.SourceReleaseID, v.SourceReleaseManifestSHA256)
		a.acquired(v.SourceArtifactStorageKey, v.SourceArtifactSHA256, v.SourceArtifactByteCount)
		for _, s := range v.Shards {
			a.blob("parquet_shard", s.StorageKey, s.SHA256, s.Bytes)
		}
		a.require("source_contract", v.SourceContract)
		a.require("producer_contract", v.PublisherVersion)
		a.require("physical_schema", v.PhysicalSchemaVersion)
		a.occurrenceChecks(v.Checks)
	case "e_facts":
		var v occ.ScheduleEFactManifest
		a.decode(&v, occ.ScheduleEFactSetSchemaVersion)
		a.id(v.FactSetID)
		a.sourceRelease(v.SourceReleaseID, v.SourceReleaseManifestSHA256)
		a.ref("occurrences", "e_occurrences", v.OccurrenceSetID, v.OccurrenceManifestSHA256, "")
		a.occurrenceArtifact("facts", v.Facts)
		a.require("source_contract", v.SourceContract)
		a.require("normalizer_contract", v.NormalizerVersion)
		a.occurrenceChecks(v.Checks)
	case "classic_facts":
		var v occ.ClassicFactManifest
		a.decode(&v, occ.ClassicFactSetSchemaVersion)
		a.id(v.FactSetID)
		a.dataset(v.Dataset)
		a.sourceRelease(v.SourceReleaseID, v.SourceReleaseManifestSHA256)
		a.ref("occurrences", "classic_occurrences", v.OccurrenceSetID, v.OccurrenceManifestSHA256, v.Dataset)
		a.occurrenceArtifact("facts", v.Facts)
		a.require("source_contract", v.SourceContract)
		a.require("normalizer_contract", v.NormalizerVersion)
		a.occurrenceChecks(v.Checks)
	case "a_occurrences":
		if a.schema == occ.ScheduleACompactManifestSchemaVersion {
			var v occ.ScheduleACompactManifest
			a.decode(&v, occ.ScheduleACompactManifestSchemaVersion)
			a.id(v.OccurrenceSetID)
			a.sourceRelease(v.SourceReleaseID, v.SourceReleaseManifestSHA256)
			a.prior("a_occurrences", v.PriorOccurrenceSetID, "")
			for _, p := range v.IndexPartitions {
				a.blob("compact_index", p.Index.StorageKey, p.Index.CompressedSHA256, p.Index.CompressedBytes)
				a.occurrenceArtifact("key_exceptions", p.KeyExceptions)
			}
			a.occurrenceArtifact("row_exceptions", v.RowExceptions)
			a.occurrenceArtifact("deltas", v.Deltas)
			a.require("parser_contract", v.ParserVersion)
			a.require("producer_contract", v.PublisherVersion)
			a.occurrenceChecks(v.Checks)
		} else {
			var v occ.Manifest
			a.decode(&v, occ.ManifestSchemaVersion)
			a.id(v.OccurrenceSetID)
			a.sourceRelease(v.SourceReleaseID, v.SourceReleaseManifestSHA256)
			a.prior("a_occurrences", v.PriorOccurrenceSetID, "")
			a.artifacts(v.Artifacts)
			a.require("parser_contract", v.ParserVersion)
			a.occurrenceChecks(v.Checks)
		}
	case "e_occurrences":
		var v occ.ScheduleEOccurrenceManifest
		a.decode(&v, occ.ScheduleEOccurrenceSetSchemaVersion)
		a.id(v.OccurrenceSetID)
		a.sourceRelease(v.SourceReleaseID, v.SourceReleaseManifestSHA256)
		a.prior("e_occurrences", v.PriorOccurrenceSetID, "")
		a.occurrenceArtifact("occurrences", v.Occurrences)
		a.require("parser_contract", v.ParserVersion)
		a.occurrenceChecks(v.Checks)
	case "classic_occurrences":
		var v occ.ClassicManifest
		a.decode(&v, occ.ClassicManifestSchemaVersion)
		a.id(v.OccurrenceSetID)
		a.dataset(v.Dataset)
		a.sourceRelease(v.SourceReleaseID, v.SourceReleaseManifestSHA256)
		a.prior("classic_occurrences", v.PriorOccurrenceSetID, v.Dataset)
		a.artifacts(v.Artifacts)
		a.require("source_contract", v.SourceContract)
		a.require("parser_contract", v.ParserVersion)
		a.occurrenceChecks(v.Checks)
	case "release":
		var v rel.ReleaseManifest
		a.decode(&v, rel.ManifestSchemaVersion)
		a.id(v.ReleaseID)
		a.prior("release", v.PriorReleaseID, "")
		a.byteManifest("plan", v.PlanSHA256)
		a.byteManifest("acquisition", v.AcquisitionSHA256)
		a.byteManifest("stage", v.StageSHA256)
		for _, f := range v.Artifacts {
			a.acquired(f.StorageKey, f.SHA256, f.ByteCount)
		}
		for _, f := range v.StagedOutputs {
			a.staged(f)
		}
		a.require("release_inventory", v.InventoryVersion)
		a.releaseChecks(v.Checks)
	case "plan":
		var v rel.ReleasePlan
		a.decode(&v, rel.PlanSchemaVersion)
		a.id(digest(a.b))
		a.prior("release", v.PriorReleaseID, "")
		a.require("release_inventory", v.InventoryVersion)
		a.require("discovery_capture", "not_byte_pinned_by_plan")
	case "acquisition":
		var v rel.AcquisitionResult
		a.decode(&v, rel.AcquisitionSchemaVersion)
		a.id(digest(a.b))
		a.prior("release", v.PriorReleaseID, "")
		a.byteManifest("plan", v.PlanSHA256)
		for _, f := range v.Artifacts {
			a.acquired(f.StorageKey, f.SHA256, f.ByteCount)
		}
		a.require("release_inventory", v.InventoryVersion)
	case "stage":
		var v rel.StageResult
		a.decode(&v, rel.StageSchemaVersion)
		a.id(digest(a.b))
		a.prior("release", v.PriorReleaseID, "")
		a.byteManifest("plan", v.PlanSHA256)
		a.byteManifest("acquisition", v.AcquisitionSHA256)
		for _, f := range v.Outputs {
			a.staged(f)
		}
		a.require("release_inventory", v.InventoryVersion)
		a.require("extraction_runtime", "not_byte_pinned_by_stage")
		a.releaseChecks(v.Checks)
	default:
		return false
	}
	return true
}
