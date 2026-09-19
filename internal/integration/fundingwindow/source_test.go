package fundingwindow_test

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/classic"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	release "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulea"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleb"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulee"
)

var fixtureTime = time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)

func digest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func check(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func write(t *testing.T, path string, b []byte) string {
	t.Helper()
	check(t, os.MkdirAll(filepath.Dir(path), 0750))
	check(t, os.WriteFile(path, b, 0600))
	return digest(b)
}
func writeJSON(t *testing.T, path string, v any) string {
	t.Helper()
	b, err := json.MarshalIndent(v, "", "  ")
	check(t, err)
	return write(t, path, append(b, '\n'))
}

// Build synthetic source bytes by the parser's named columns. No downloaded
// donor data or fabricated pre-approved fact/calculation/graph manifests.
func copyRow(t *testing.T, names []string, values map[string]string) string {
	t.Helper()
	row := make([]string, len(names))
	positions := map[string]int{}
	for i, name := range names {
		row[i] = `\N`
		positions[name] = i
	}
	for name, value := range values {
		i, ok := positions[name]
		if !ok {
			t.Fatalf("unknown fixture column %s", name)
		}
		row[i] = value
	}
	return strings.Join(row, "\t") + "\n"
}
func classicRow(t *testing.T, dataset classic.Dataset, values map[string]string) []byte {
	t.Helper()
	spec, err := classic.Lookup(string(dataset))
	check(t, err)
	row := make([]string, len(spec.Fields))
	seen := map[string]bool{}
	for i, name := range spec.Fields {
		row[i] = values[name]
		seen[name] = true
	}
	for name := range values {
		if !seen[name] {
			t.Fatalf("unknown classic fixture column %s", name)
		}
	}
	return []byte(strings.Join(row, "|") + "\n")
}
func classicBody(t *testing.T, code, cycle string) []byte {
	t.Helper()
	switch code {
	case "cm":
		var b []byte
		for _, id := range []string{"C00000001", "C00000002", "C00000003"} {
			b = append(b, classicRow(t, classic.CommitteeMaster, map[string]string{"CMTE_ID": id, "CMTE_NM": "SYNTHETIC " + cycle + " " + id, "CMTE_TP": "Q", "CMTE_DSGN": "U", "CMTE_FILING_FREQ": "Q"})...)
		}
		return b
	case "cn":
		return classicRow(t, classic.CandidateMaster, map[string]string{"CAND_ID": "H0CA00001", "CAND_NAME": "SYNTHETIC CANDIDATE", "CAND_ELECTION_YR": cycle, "CAND_OFFICE": "H", "CAND_OFFICE_ST": "CA", "CAND_OFFICE_DISTRICT": "01", "CAND_PCC": "C00000003"})
	case "ccl":
		return classicRow(t, classic.CandidateCommitteeLinkage, map[string]string{"CAND_ID": "H0CA00001", "CAND_ELECTION_YR": cycle, "FEC_ELECTION_YR": cycle, "CMTE_ID": "C00000003", "CMTE_TP": "H", "CMTE_DSGN": "P", "LINKAGE_ID": "1"})
	default:
		dataset := classic.AllCandidatesSummary
		if code == "webl" {
			dataset = classic.CurrentCampaignsSummary
		}
		return classicRow(t, dataset, map[string]string{"CAND_ID": "H0CA00001", "CAND_NAME": "SYNTHETIC CANDIDATE", "TTL_RECEIPTS": "0"})
	}
}
func scheduleBody(t *testing.T, family, cycle string) []byte {
	t.Helper()
	year, err := strconv.Atoi(cycle)
	check(t, err)
	sender, recipient, day := "C00000001", "C00000002", cycle+"-12-31 00:00:00"
	if cycle == "2024" {
		sender, recipient, day = "C00000002", "C00000003", fmt.Sprintf("%d-01-01 00:00:00", year-1)
	}
	names := []string{}
	v := map[string]string{"sub_id": "100", "filing_form": "F3X", "two_year_transaction_period": cycle}
	switch family {
	case "a":
		for _, c := range schedulea.Columns() {
			names = append(names, c.Name)
		}
		v["cmte_id"], v["contbr_id"], v["clean_contbr_id"] = recipient, sender, sender
		v["contb_receipt_amt"], v["contb_receipt_dt"], v["receipt_tp"] = "42.50", day, "15K"
		v["entity_tp"], v["line_num"], v["schedule_type"] = "PAC", "11C", "SA"
	case "b":
		for _, c := range scheduleb.Columns() {
			names = append(names, c.Name)
		}
		v["cmte_id"], v["recipient_cmte_id"], v["clean_recipient_cmte_id"] = sender, recipient, recipient
		v["disb_amt"], v["disb_dt"], v["disb_tp"] = "42.50", day, "24K"
		v["line_num"], v["schedule_type"] = "23", "SB"
	case "e":
		delete(v, "two_year_transaction_period")
		v["election_cycle"] = cycle
		for _, c := range schedulee.Columns() {
			names = append(names, c.Name)
		}
		v["sub_id"] = cycle + "100"
		v["cmte_id"], v["s_o_cand_id"], v["s_o_cand_nm"] = sender, "H0CA00001", "SYNTHETIC CANDIDATE"
		v["s_o_cand_office"], v["s_o_cand_office_st"], v["s_o_cand_office_district"] = "H", "CA", "01"
		v["exp_amt"], v["exp_dt"], v["s_o_ind"] = "7.25", day, "S"
		return spendingBody(t, names, v, cycle, day)
	}
	return []byte(copyRow(t, names, v))
}

// Multiple members deliberately share one aggregate but have different native
// dates. These are synthetic values, never a real committee/candidate exception.
func spendingBody(t *testing.T, names []string, base map[string]string, cycle, day string) []byte {
	t.Helper()
	d, err := time.Parse("2006-01-02 15:04:05", day)
	check(t, err)
	next := d.AddDate(0, 0, 1).Format("2006-01-02 15:04:05")
	variants := []map[string]string{
		{"dissem_dt": next},
		{"exp_amt": "12.00", "exp_dt": next, "dissem_dt": day},
		{"exp_amt": "2.00", "s_o_ind": "O", "dissem_dt": day},
		{"exp_amt": "99.00", "memo_cd": "X"},
		{"exp_amt": `\N`},
		{"exp_amt": "3.00", "exp_dt": `\N`, "dissem_dt": day},
		{"exp_amt": "-1.25", "dissem_dt": day},
		{"exp_amt": "0.00", "dissem_dt": day},
		{"exp_amt": "4.00", "s_o_cand_id": `\N`},
		{"exp_amt": "6.00", "s_o_cand_id": "H0CA00009", "s_o_cand_nm": "NO MATCH"},
		{"exp_amt": "5.00", "s_o_cand_id": "H0CA00008", "dissem_dt": day},
		{"exp_amt": "8.00", "s_o_cand_nm": `\N`, "dissem_dt": day},
	}
	var out []byte
	for i, overrides := range variants {
		v := map[string]string{}
		for k, value := range base {
			v[k] = value
		}
		for k, value := range overrides {
			v[k] = value
		}
		v["sub_id"] = cycle + strconv.Itoa(100+i)
		out = append(out, copyRow(t, names, v)...)
	}
	return out
}

type fixtureSources struct {
	manifest  release.ReleaseManifest
	sha, path string
	restores  map[string]string
}

// Acquisition metadata is synthetic. Retained ZIP/member/COPY bytes are real
// and checksummed. Only pg_restore's external extraction process is substituted;
// the Schedule B COPY parser, Parquet publisher and all downstream loaders run.
func makeSources(t *testing.T, root string) fixtureSources {
	return makeSourcesWithA(t, root, "window-source-fixture-v1", func(t *testing.T, cycle string) []byte { return scheduleBody(t, "a", cycle) })
}

func makeSourcesWithA(t *testing.T, root, label string, bodyA func(*testing.T, string) []byte) fixtureSources {
	t.Helper()
	inventory := release.ActiveInventory()
	r := release.ReleaseManifest{Schema: "release-manifest.schema.json", SchemaVersion: release.ManifestSchemaVersion,
		InventoryVersion: inventory.InventoryVersion, ReleaseID: "fec-" + digest([]byte(label)),
		RunID: "window-fixture", PlanSHA256: digest([]byte("plan")), AcquisitionSHA256: digest([]byte("acquisition")), StageSHA256: digest([]byte("stage")),
		State: "published", SelectedAt: fixtureTime.Add(-time.Hour), PublishedAt: fixtureTime, Periods: inventory.Periods}
	f := fixtureSources{restores: map[string]string{}}
	stage := func(source, period, kind, selection, sourceSHA string, body []byte, fields int) {
		var buf bytes.Buffer
		z, err := zstd.NewWriter(&buf, zstd.WithEncoderConcurrency(1))
		check(t, err)
		_, err = z.Write(body)
		check(t, err)
		check(t, z.Close())
		sha := digest(buf.Bytes())
		key := "raw/fec/selected/sha256/" + sha[:2] + "/" + sha + ".zst"
		if kind == "relation" {
			family := "schedule-a"
			if source == release.ScheduleESourceID {
				family = "schedule-e"
			}
			key = "raw/fec/" + family + "/extracts/sha256/" + sha[:2] + "/" + sha + ".copy.zst"
		}
		write(t, filepath.Join(root, key), buf.Bytes())
		o := release.StagedOutput{SourceID: source, Disposition: "staged", SelectionKind: kind, Selection: selection, Period: period,
			Representation: "selected_member_zstd", SourceArtifactSHA256: sourceSHA, UncompressedByteCount: uint64(len(body)), UncompressedSHA256: digest(body),
			Compression: "zstd", CompressionLevel: 3, CompressedByteCount: uint64(buf.Len()), CompressedSHA256: digest(buf.Bytes()), StorageKey: key, DecompressionValidated: true, StagedAt: fixtureTime}
		if kind == "relation" {
			n := uint64(bytes.Count(body, []byte{'\n'}))
			o.RowCount, o.ContractedFieldCount = &n, &fields
			o.Representation = "postgresql_copy_text_data_rows_zstd"
		}
		r.StagedOutputs = append(r.StagedOutputs, o)
	}
	for _, s := range inventory.Sources {
		var archive []byte
		var member []byte
		if len(s.SelectedMembers) > 0 {
			member = classicBody(t, strings.Split(s.SourceID, ":")[1], s.Periods[0])
			var buf bytes.Buffer
			z := zip.NewWriter(&buf)
			w, err := z.Create(s.SelectedMembers[0])
			check(t, err)
			_, err = w.Write(member)
			check(t, err)
			check(t, z.Close())
			archive = buf.Bytes()
		} else {
			archive = []byte("PGDMP synthetic extraction boundary " + s.SourceID)
		}
		key, sh := "raw/fec/fixture-archives/"+digest(archive), digest(archive)
		write(t, filepath.Join(root, key), archive)
		size := int64(len(archive))
		r.Artifacts = append(r.Artifacts, release.PublishedArtifact{SelectedSource: release.SelectedSource{SourceID: s.SourceID, RequestURL: s.RequestURL, FinalURL: s.RequestURL,
			ObservedAt: r.SelectedAt, VersionIdentity: "version_id:fixture", VersionBasis: "version_id", VersionID: "fixture", ContentLength: &size}, ByteCount: size, SHA256: sh, StorageKey: key, AcquiredAt: fixtureTime.Add(-time.Minute)})
		if member != nil {
			stage(s.SourceID, s.Periods[0], "member", s.SelectedMembers[0], sh, member, 0)
		}
		for _, selection := range s.RelationSelections {
			if s.SourceID == release.ScheduleBSourceID {
				names := []string{}
				for _, c := range scheduleb.Columns() {
					names = append(names, c.Name)
				}
				sql := "COPY " + selection.Name + " (" + strings.Join(names, ", ") + ") FROM stdin;\n" + string(scheduleBody(t, "b", selection.Scope)) + "\\.\n"
				path := filepath.Join(root, "fixture-tools", selection.Scope+".sql")
				write(t, path, []byte(sql))
				exe := filepath.Join(root, "fixture-tools", "restore-"+selection.Scope)
				write(t, exe, []byte("#!/bin/sh\nexec /bin/cat "+strconv.Quote(path)+"\n"))
				check(t, os.Chmod(exe, 0700))
				f.restores[selection.Scope] = exe
				continue
			}
			var body []byte
			if s.SourceID == release.ScheduleESourceID {
				body = nil
				for _, cycle := range inventory.Periods {
					body = append(body, scheduleBody(t, "e", cycle)...)
				}
			} else {
				body = bodyA(t, selection.Scope)
			}
			stage(s.SourceID, selection.Scope, "relation", selection.Name, sh, body, selection.FieldCount)
		}
	}
	for _, id := range []string{"input_identity", "source_artifact_membership", "selected_output_membership", "output_integrity", "storage_budget"} {
		r.Checks = append(r.Checks, release.ReleaseCheck{ID: id, Passed: true, Severity: "block", Detail: "synthetic source acquisition boundary"})
	}
	if issues := release.ValidateManifest(inventory, r); len(issues) != 0 {
		t.Fatalf("fixture release: %+v", issues)
	}
	f.manifest = r
	f.path = filepath.Join(root, "releases/fec/manifests", r.ReleaseID+".json")
	f.sha = writeJSON(t, f.path, r)
	return f
}

func publicationOptions(root string) occ.Options {
	return occ.Options{StorageRoot: root, Clock: func() time.Time { return fixtureTime }, ShardCount: 2, RowsPerColumnarShard: 2, RowsPerColumnarRowGroup: 1, FreeFloorBytes: 1, WorkingMarginBytes: 1}
}
