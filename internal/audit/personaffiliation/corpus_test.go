package personaffiliation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportscope"
)

const fixtureDir = "../../../tests/fixtures/person-affiliation"
const corpusPin = "7dfba2320ea5acaf7cf3846cde13a4f1825cd9990b7c1629570475784012d3a4"

func hash(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

func TestRetainedSourceCorpus(t *testing.T) {
	r, err := Run(context.Background(), fixtureDir, corpusPin)
	if err != nil {
		t.Fatal(err)
	}
	again, err := Run(context.Background(), fixtureDir, corpusPin)
	if err != nil || !reflect.DeepEqual(r, again) {
		t.Fatal("replay differs", err)
	}
	if len(r.Cases) != 4 {
		t.Fatal("case conservation")
	}
	wantNames := []string{"JOHN CHAMBERS", "JOHN CHAMBERS", "DAVID DUFFIELD", "MICHAEL O'BRIEN"}
	wantEmployers := []string{"JC2VENTURES", "JC2VENTURES", "RIDGELINE INC", "VALLEY TECH SERVICES INC"}
	wantRoles := []int{5, 5, 2, 0}
	for i, c := range r.Cases {
		a := c.Assessment
		if *a.Appearance.Receipt.Name != wantNames[i] || *a.Appearance.Receipt.Employer != wantEmployers[i] {
			t.Fatal("source field mapping", c.ID)
		}
		if a.Appearance.Receipt.Cycle != 0 || a.Appearance.Receipt.Normalization != "audit_efile84_component_name_and_iso_day.v1" {
			t.Fatal("as-filed test view mislabeled as processed facts")
		}
		if a.Appearance.Receipt.Street1 != nil || a.Appearance.Receipt.ZIP != nil {
			t.Fatal("unneeded private-address fields in output")
		}
		if a.IdentityState != "no_name_employer_candidate" || len(a.CandidatePersonIDs) != 0 || len(a.Decisions) != wantRoles[i] {
			t.Fatal("frozen v1 baseline changed", c.ID)
		}
		if a.IdentityResolved || a.GraphPublicationApproved || a.TerminalEligible || a.FinancialAttribution {
			t.Fatal("screening promoted identity or money")
		}
		for _, d := range a.Decisions {
			if d.ScreeningMatch || d.SameCandidatePerson || d.TimeState != "unknown_time" || d.Claim.ValidFrom != "" || d.Claim.ValidThrough != "" || d.Claim.AsOf != "" {
				t.Fatal("invented role validity or match", c.ID)
			}
			if !strings.HasPrefix(d.Claim.Source.Locator, "bytes[") || len(d.Claim.Source.SHA256) != 64 {
				t.Fatal("missing role source span")
			}
		}
	}
	first, second := r.Cases[0], r.Cases[1]
	if first.TransactionID != "A-294781" || second.TransactionID != "A-294783" || first.RawDate != "20230930" || *first.Assessment.Appearance.Receipt.ReceiptDate != "2023-09-30" {
		t.Fatal("lost raw transaction/date or incorrect date conversion")
	}
	if first.Assessment.Appearance.Source == second.Assessment.Appearance.Source {
		t.Fatal("distinct occurrences collapsed")
	}
	if *r.Cases[3].Assessment.Appearance.Receipt.Occupation != "ENGINEER" {
		t.Fatal("reported occupation changed")
	}
	for _, d := range first.Assessment.Decisions {
		switch {
		case strings.HasSuffix(d.Claim.Source.Locator, "review=jc2-ceo"):
			if !d.NameMatch || d.EmployerMatch || d.RoleState != "leadership_or_control" {
				t.Fatal("letter/number employer mismatch concealed")
			}
		case strings.HasSuffix(d.Claim.Source.Locator, "review=jc2-near-name"):
			if d.NameMatch {
				t.Fatal("near-name rival silently merged")
			}
		case strings.HasSuffix(d.Claim.Source.Locator, "review=cisco-historical-ceo"):
			if d.Claim.Issue == "" {
				t.Fatal("year-only historical end discarded")
			}
		case strings.HasSuffix(d.Claim.Source.Locator, "review=cisco-emeritus"):
			if d.RoleState != "role_unknown" {
				t.Fatal("emeritus became current authority")
			}
		}
	}
	for _, d := range r.Cases[2].Assessment.Decisions {
		if d.NameMatch || d.EmployerMatch {
			t.Fatal("nickname or suffix silently resolved")
		}
	}
	b, err := os.ReadFile("../../../contracts/sources/fec/efile-format/v1/schedule-a-fields.json")
	if err != nil || hash(b) != layoutPin {
		t.Fatal("reviewed layout drift", err)
	}
}

func fixtureCopy(t *testing.T) (string, Corpus) {
	t.Helper()
	dir := t.TempDir()
	entries, err := os.ReadDir(fixtureDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		b, err := os.ReadFile(filepath.Join(fixtureDir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, e.Name()), b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	b, err := os.ReadFile(filepath.Join(dir, "corpus.json"))
	if err != nil {
		t.Fatal(err)
	}
	var c Corpus
	if err := json.Unmarshal(b, &c); err != nil {
		t.Fatal(err)
	}
	return dir, c
}

func writeCorpus(t *testing.T, dir string, c Corpus) string {
	t.Helper()
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "corpus.json"), b, 0600); err != nil {
		t.Fatal(err)
	}
	return hash(b)
}

func TestCorpusEvidenceFailures(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Corpus)
	}{
		{"unknown version", func(c *Corpus) { c.Version = "new" }},
		{"layout drift", func(c *Corpus) { c.Layout.SHA256 = strings.Repeat("a", 64) }},
		{"traversal", func(c *Corpus) { c.Sources[0].Body.Path = "../jc2-about.html" }},
		{"wrong source hash", func(c *Corpus) { c.Sources[0].Body.SHA256 = strings.Repeat("a", 64) }},
		{"wrong source size", func(c *Corpus) { c.Sources[0].Body.Bytes++ }},
		{"missing excerpt", func(c *Corpus) { c.Roles[0].Excerpts = []string{"absent evidence assertion"} }},
		{"nonunique excerpt", func(c *Corpus) { c.Roles[0].Excerpts = []string{"<div"} }},
		{"invented source", func(c *Corpus) { c.Roles[0].SourceID = "absent" }},
		{"prebound claim", func(c *Corpus) { c.Roles[0].Claim.Source.SHA256 = strings.Repeat("a", 64) }},
		{"duplicate role", func(c *Corpus) { c.Roles = append(c.Roles, c.Roles[0]) }},
		{"bad role date", func(c *Corpus) { c.Roles[0].Claim.ValidThrough = "2015" }},
		{"unevaluated role", func(c *Corpus) { r := c.Roles[0]; r.ID = "unused"; c.Roles = append(c.Roles, r) }},
		{"unbound role", func(c *Corpus) { c.Cases[0].RoleIDs = []string{"absent"} }},
		{"duplicate case role", func(c *Corpus) { c.Cases[0].RoleIDs = []string{"jc2-ceo", "jc2-ceo"} }},
		{"zero ordinal", func(c *Corpus) { c.Cases[0].Ordinal = 0 }},
		{"duplicate occurrence", func(c *Corpus) { c.Cases[1].Ordinal = c.Cases[0].Ordinal }},
		{"ordinal outside file", func(c *Corpus) { c.Cases[0].Ordinal = 99999 }},
		{"header selected as receipt", func(c *Corpus) { c.Cases[0].Ordinal = 1 }},
		{"source date invalid", func(c *Corpus) { c.Sources[0].ObservedOn = "not-a-day" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, c := fixtureCopy(t)
			tc.mutate(&c)
			if _, err := Run(context.Background(), dir, writeCorpus(t, dir, c)); err == nil {
				t.Fatal("accepted invalid evidence")
			}
		})
	}
	if _, err := Run(context.Background(), fixtureDir, strings.Repeat("a", 64)); err == nil {
		t.Fatal("accepted wrong corpus pin")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Run(ctx, fixtureDir, corpusPin); err == nil {
		t.Fatal("ignored cancellation")
	}
}

func TestSelectedOccurrenceProfile(t *testing.T) {
	var spec struct {
		Fields []struct {
			Name     string
			Sequence int
		}
	}
	b, err := os.ReadFile(filepath.Join(fixtureDir, "schedule-a-fields.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &spec); err != nil {
		t.Fatal(err)
	}
	positions := map[string]int{}
	for _, f := range spec.Fields {
		positions[f.Name] = f.Sequence - 1
	}
	b, err = os.ReadFile(filepath.Join(fixtureDir, "fec-1730162.fec"))
	if err != nil {
		t.Fatal(err)
	}
	row := bytes.Split(b, []byte{'\n'})[57]
	cover := &reportscope.Cover{Form: "F3N", CommitteeID: "C00845032"}
	for _, tc := range []struct{ name, field, value string }{
		{"nonreceipt", "form_type", "SB17"},
		{"wrong summary line", "form_type", "SA99"},
		{"organization", "entity_type", "ORG"},
		{"different filer", "filer_committee_id_number", "C12345678"},
		{"impossible day", "contribution_date", "20230230"},
		{"ISO date not efile date", "contribution_date", "2023-09-30"},
		{"source encoding", "contributor_first_name", "J\xff"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fields := strings.Split(string(row), "\x1c")
			fields[positions[tc.field]] = tc.value
			rec := reportscope.Record{Complete: true, Raw: []byte(strings.Join(fields, "\x1c"))}
			if _, err := selectedFields(rec, positions, cover); err == nil {
				t.Fatal("accepted unsupported occurrence")
			}
		})
	}
	for _, b := range [][]byte{bytes.Join(bytes.Split(row, []byte{0x1c})[:44], []byte{0x1c}), append(bytes.Clone(row), 0x1c)} {
		if _, err := selectedFields(reportscope.Record{Complete: true, Raw: b}, positions, cover); err == nil {
			t.Fatal("padded or truncated physical fields")
		}
	}
	if _, err := selectedFields(reportscope.Record{Raw: row}, positions, cover); err == nil {
		t.Fatal("accepted incomplete row")
	}
}

func TestRetainedBytesCannotBeSubstituted(t *testing.T) {
	for _, name := range []string{"fec-1730162.fec", "fec-1730162.headers", "jc2-about.html", "schedule-a-fields.json"} {
		t.Run(name, func(t *testing.T) {
			dir, _ := fixtureCopy(t)
			p := filepath.Join(dir, name)
			b, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			b[len(b)/2] ^= 1
			if err := os.WriteFile(p, b, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := Run(context.Background(), dir, corpusPin); err == nil {
				t.Fatal("tampered bytes accepted")
			}
		})
	}
	dir, c := fixtureCopy(t)
	target, err := filepath.Abs(filepath.Join(fixtureDir, "jc2-about.html"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(dir, "external.html")); err != nil {
		t.Fatal(err)
	}
	c.Sources[0].Body.Path = "external.html"
	if _, err := Run(context.Background(), dir, writeCorpus(t, dir, c)); err == nil {
		t.Fatal("followed source symlink")
	}
}

func TestAnnotationsAndInputOrderDoNotChangeScreening(t *testing.T) {
	dir, c := fixtureCopy(t)
	want, err := Run(context.Background(), dir, corpusPin)
	if err != nil {
		t.Fatal(err)
	}
	slices.Reverse(c.Sources)
	slices.Reverse(c.Roles)
	for i := range c.Roles {
		c.Roles[i].Reason = "Different reviewer explanation, not a matching input."
	}
	for i := range c.Cases {
		slices.Reverse(c.Cases[i].RoleIDs)
		c.Cases[i].Review = "A changed label cannot teach the resolver an identity."
	}
	got, err := Run(context.Background(), dir, writeCorpus(t, dir, c))
	if err != nil {
		t.Fatal(err)
	}
	for i := range want.Cases {
		a, _ := json.Marshal(want.Cases[i].Assessment)
		b, _ := json.Marshal(got.Cases[i].Assessment)
		if !bytes.Equal(a, b) {
			t.Fatal("annotation prose or input ordering affected decisions")
		}
	}
}
