package occurrence

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/classic"
	release "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
)

func referenceFixtureRelease(t *testing.T, root, label, prior, cycle string, dataset classic.Dataset, rows []byte) (release.ReleaseManifest, string) {
	t.Helper()
	r, _ := classicSourceReleaseFixture(t, root, label, prior, dataset, cycle, rows)
	return referenceFixtureZIP(t, root, r, label, dataset, cycle, rows, false)
}

func referenceFixtureZIP(t *testing.T, root string, r release.ReleaseManifest, label string, dataset classic.Dataset, cycle string, body []byte, duplicate bool) (release.ReleaseManifest, string) {
	t.Helper()
	// Own all slices before mutating the fixture release.
	raw, _ := json.Marshal(r)
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatal(err)
	}
	spec := mustClassicSpec(t, dataset)
	var encoded bytes.Buffer
	w := zip.NewWriter(&encoded)
	if err := w.SetComment(label); err != nil {
		t.Fatal(err)
	}
	n := 1
	if duplicate {
		n++
	}
	for i := 0; i < n; i++ {
		f, err := w.Create(classicMember(spec, cycle))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(body); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	sha := referenceHash(encoded.Bytes())
	key := "raw/fec/sha256/" + sha
	writeFile(t, filepath.Join(root, key), encoded.Bytes())
	for i := range r.Artifacts {
		if r.Artifacts[i].SourceID != "fec:"+spec.Code+":"+cycle {
			continue
		}
		r.Artifacts[i].SHA256, r.Artifacts[i].ByteCount, r.Artifacts[i].StorageKey = sha, int64(encoded.Len()), key
		size := int64(encoded.Len())
		r.Artifacts[i].ContentLength = &size
	}
	for i := range r.StagedOutputs {
		if r.StagedOutputs[i].SourceID == "fec:"+spec.Code+":"+cycle {
			r.StagedOutputs[i].SourceArtifactSHA256 = sha
		}
	}
	raw, _ = json.Marshal(r)
	writeFile(t, filepath.Join(root, "releases/fec/manifests", r.ReleaseID+".json"), raw)
	if problems := release.ValidateKnownManifest(r); len(problems) != 0 {
		t.Fatal(problems)
	}
	return r, referenceHash(raw)
}

func referenceFixture(t *testing.T, cycle string, dataset classic.Dataset) (string, string, release.ReleaseManifest, string, []byte, ClassicFactManifest) {
	t.Helper()
	root := t.TempDir()
	spec := mustClassicSpec(t, dataset)
	fields := map[string]string{"CMTE_ID": "C00000001", "CMTE_NM": "REFERENCE FIXTURE", "CMTE_TP": "H", "CMTE_DSGN": "P", "CAND_ID": "H0ZZ00001", "CAND_ELECTION_YR": cycle, "FEC_ELECTION_YR": cycle, "LINKAGE_ID": "1"}
	values := make([]string, len(spec.Fields))
	for i, k := range spec.Fields {
		values[i] = fields[k]
	}
	rows := []byte(strings.Join(values, "|") + "\n")
	origin, od := referenceFixtureRelease(t, root, "reference-origin", "", cycle, dataset, rows)
	o, err := PublishClassic(context.Background(), origin, od, string(dataset), cycle, "reference-occur", Options{StorageRoot: root, ShardCount: 2, DiskAvailable: fixtureDiskAvailable})
	if err != nil {
		t.Fatal(err)
	}
	op := filepath.Join(root, classicEvidenceBase(spec), "manifests", o.OccurrenceSetID+".json")
	m, err := PublishClassicFacts(context.Background(), origin, od, op, "reference-facts", Options{StorageRoot: root, DiskAvailable: fixtureDiskAvailable})
	if err != nil {
		t.Fatal(err)
	}
	target, td := referenceFixtureRelease(t, root, "reference-target", origin.ReleaseID, cycle, dataset, rows)
	return root, filepath.Join(root, classicFactBase(spec), "manifests", m.FactSetID+".json"), target, td, rows, m
}

func TestClassicReferenceRepackagingProofReplaysAcrossCycles(t *testing.T) {
	for _, cycle := range []string{"2020", "2022", "2024", "2026"} {
		for _, dataset := range []classic.Dataset{classic.CommitteeMaster, classic.CandidateCommitteeLinkage, classic.CandidateMaster} {
			t.Run(cycle+"/"+string(dataset), func(t *testing.T) {
				root, path, target, td, _, m := referenceFixture(t, cycle, dataset)
				p, err := ProveClassicReference(context.Background(), root, path, string(dataset), target.ReleaseID, td)
				if err != nil {
					t.Fatal(err)
				}
				if p.Rows != 1 || p.SourceArchive == p.TargetArchive || p.SourceRelease == p.TargetRelease || p.FactSet.ID != m.FactSetID || !digestPattern.MatchString(p.ProofID) || !digestPattern.MatchString(p.ContentValuesSHA256) {
					t.Fatal(p)
				}
				again, err := ProveClassicReference(context.Background(), root, path, string(dataset), target.ReleaseID, td)
				if err != nil || !reflect.DeepEqual(p, again) {
					t.Fatal("proof changed", err)
				}
				for _, mutate := range []func(*ClassicReferenceProof){
					func(p *ClassicReferenceProof) { p.Cycle = "other" }, func(p *ClassicReferenceProof) { p.SourceContract = "other" },
					func(p *ClassicReferenceProof) { p.ParserVersion = "other" }, func(p *ClassicReferenceProof) { p.NormalizerVersion = "other" },
					func(p *ClassicReferenceProof) { p.MemberSHA256 = "other" }, func(p *ClassicReferenceProof) { p.ContentValuesSHA256 = "other" },
					func(p *ClassicReferenceProof) { p.Rows++ }, func(p *ClassicReferenceProof) { p.TargetRelease.SHA256 = "other" },
				} {
					changed := p
					mutate(&changed)
					if SameReferenceContent(p, changed) {
						t.Fatal("content/schema drift accepted")
					}
				}
			})
		}
	}
}

func TestClassicReferenceRejectsCorruptionAndFalseEquivalence(t *testing.T) {
	for _, mode := range []string{"changed_content", "wrong_zip_body", "duplicate_zip_member", "corrupt_zip", "corrupt_staged", "changed_fact", "changed_schema", "wrong_release_sha", "cancellation"} {
		t.Run(mode, func(t *testing.T) {
			root, path, target, td, rows, m := referenceFixture(t, "2024", classic.CommitteeMaster)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			spec := mustClassicSpec(t, classic.CommitteeMaster)
			switch mode {
			case "changed_content":
				target, td = referenceFixtureRelease(t, root, "reference-changed", target.ReleaseID, "2024", classic.CommitteeMaster, bytes.ReplaceAll(rows, []byte("FIXTURE"), []byte("CHANGED")))
			case "wrong_zip_body":
				target, td = referenceFixtureZIP(t, root, target, "changed-zip", classic.CommitteeMaster, "2024", bytes.ReplaceAll(rows, []byte("FIXTURE"), []byte("CHANGED")), false)
			case "duplicate_zip_member":
				target, td = referenceFixtureZIP(t, root, target, "duplicate", classic.CommitteeMaster, "2024", rows, true)
			case "corrupt_zip":
				for _, a := range target.Artifacts {
					if a.SourceID == "fec:cm:2024" {
						writeFile(t, filepath.Join(root, a.StorageKey), make([]byte, a.ByteCount))
					}
				}
			case "corrupt_staged":
				for _, s := range target.StagedOutputs {
					if s.SourceID == "fec:cm:2024" {
						writeFile(t, filepath.Join(root, s.StorageKey), make([]byte, s.CompressedByteCount))
					}
				}
			case "changed_schema":
				m.NormalizerVersion = "unreviewed"
				raw, _ := json.Marshal(m)
				writeFile(t, path, raw)
			case "changed_fact":
				facts := readArtifactRecords[ClassicFact](t, root, m.Facts)
				facts[0].SourceFields["CMTE_NM"] = "FORGED"
				b, _ := json.Marshal(facts[0])
				b = append(b, '\n')
				compressed, sha := compress(t, b)
				m.Facts.StorageKey = filepath.ToSlash(filepath.Join(classicFactBase(spec), "facts/sha256", sha[:2], sha+".jsonl.zst"))
				m.Facts.CompressedSHA256, m.Facts.CompressedBytes, m.Facts.UncompressedSHA256, m.Facts.UncompressedBytes = sha, uint64(len(compressed)), referenceHash(b), uint64(len(b))
				writeFile(t, filepath.Join(root, m.Facts.StorageKey), compressed)
				raw, _ := json.Marshal(m)
				writeFile(t, path, raw)
			case "wrong_release_sha":
				td = strings.Repeat("a", 64)
			case "cancellation":
				cancel()
			}
			if _, err := ProveClassicReference(ctx, root, path, string(classic.CommitteeMaster), target.ReleaseID, td); err == nil {
				t.Fatal("accepted", mode)
			}
		})
	}
}

func TestClassicReferenceContentPreservesDistinctFactAncestries(t *testing.T) {
	root, path, target, td, _, left := referenceFixture(t, "2024", classic.CommitteeMaster)
	spec := mustClassicSpec(t, classic.CommitteeMaster)
	o, err := PublishClassic(context.Background(), target, td, string(spec.Dataset), "2024", "reference-target-occur", Options{StorageRoot: root, ShardCount: 2, DiskAvailable: fixtureDiskAvailable})
	if err != nil {
		t.Fatal(err)
	}
	right, err := PublishClassicFacts(context.Background(), target, td, filepath.Join(root, classicEvidenceBase(spec), "manifests", o.OccurrenceSetID+".json"), "reference-target-facts", Options{StorageRoot: root, DiskAvailable: fixtureDiskAvailable})
	if err != nil {
		t.Fatal(err)
	}
	a, err := ProveClassicReference(context.Background(), root, path, string(spec.Dataset), target.ReleaseID, td)
	if err != nil {
		t.Fatal(err)
	}
	b, err := ProveClassicReference(context.Background(), root, filepath.Join(root, classicFactBase(spec), "manifests", right.FactSetID+".json"), string(spec.Dataset), target.ReleaseID, td)
	if err != nil || !SameReferenceContent(a, b) || a.ProofID == b.ProofID || left.FactSetID == right.FactSetID || a.Occurrences == b.Occurrences {
		t.Fatal("content equality hid provenance or failed", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	if a.SourceRelease.ID == b.SourceRelease.ID {
		t.Fatal("origin relabeled")
	}
}
