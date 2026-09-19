package personaffiliation

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/companypage"
	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

func proseFixture(t *testing.T, body string) (companypage.Evidence, ProseResult) {
	t.Helper()
	e, err := companypage.Extract(context.Background(), []byte(body), companypage.Source{URL: "https://example.org/team", ObservedOn: "2026-09-17", SHA256: wikimedia.Hash([]byte(body))})
	if err != nil {
		t.Fatal(err)
	}
	r, err := ProposeProse(context.Background(), e)
	if err != nil {
		t.Fatal(err)
	}
	checkProseCitations(t, e, r)
	return e, r
}

func checkProseCitations(t *testing.T, e companypage.Evidence, r ProseResult) {
	t.Helper()
	if r.Policy != ProsePolicy || r.Source != e.Source || r.IdentityApproved || r.GraphPublicationApproved || r.FinancialAttribution || len(r.Entries)+r.OtherEntries != len(e.Entries) {
		t.Fatal("source, conservation or approval boundary")
	}
	counts := map[int][2]int{}
	check := func(c ProseCitation, fields ...ProseText) {
		t.Helper()
		entry := e.Entries[c.Entry]
		if entry.Kind != "text" || c.HTML != entry.Span {
			t.Fatal("raw citation is not the original text block")
		}
		for _, f := range append(fields, c.Matched) {
			if f.Start < c.Matched.Start || f.End > c.Matched.End || f.End > len(entry.Text) || f.End < f.Start || entry.Text[f.Start:f.End] != f.Text {
				t.Fatal("invented text or offsets", f)
			}
		}
	}
	for _, c := range r.Roles {
		check(c.Evidence, c.Person, c.Predicate, c.Role, c.Organization)
		if c.TimeWording != nil {
			check(c.Evidence, *c.TimeWording)
		}
		if c.Meaning != "unverified_relation_mention" || (c.TimeState != "role_validity_unknown" && c.TimeState != "explicit_time_wording_uninterpreted") {
			t.Fatal("syntax became a verified role or interval")
		}
		n := counts[c.Evidence.Entry]
		n[0]++
		counts[c.Evidence.Entry] = n
	}
	for _, c := range r.Aliases {
		check(c.Evidence, c.LeadingName, c.Parenthetical, c.FollowingName)
		if c.Meaning != "unverified_parenthetical_name_form" {
			t.Fatal("parentheses became a canonical alias")
		}
		n := counts[c.Evidence.Entry]
		n[1]++
		counts[c.Evidence.Entry] = n
	}
	for _, entry := range r.Entries {
		if counts[entry.Entry] != [2]int{entry.Roles, entry.Aliases} {
			t.Fatal("candidate census differs from text outcomes")
		}
	}
}

func TestProseExplicitClausesAndSourceOffsets(t *testing.T) {
	e, r := proseFixture(t, `<meta name="dateModified" content="2021-01-01"><p>Élodie <b>O’Neil</b> is the founder &amp; CEO of Example Corp.</p><p>Jo Li, President and General Manager of Other Company, leads the team.</p><p>Alex Example was not CEO of Example Corp from 2010 to 2015.</p><p>Sam Jones is a Member at Large for the Example Public Board.</p><p>Rita (Ri) Anne Jones met Robert (Bob) Smith.</p>`)
	if len(r.Roles) != 4 || len(r.Aliases) != 2 {
		t.Fatal(r.Roles, r.Aliases)
	}
	if r.Roles[0].Person.Text != "Élodie O’Neil" || r.Roles[0].Role.Text != "founder & CEO" || r.Roles[0].Organization.Text != "Example Corp" || r.Roles[0].TimeState != "role_validity_unknown" {
		t.Fatal(r.Roles[0])
	}
	if r.Roles[1].Person.Text != "Jo Li" || r.Roles[1].Organization.Text != "Other Company" || r.Roles[1].Predicate.Text != "," {
		t.Fatal("apposition changed", r.Roles[1])
	}
	if r.Roles[2].Predicate.Text != "was not" || r.Roles[2].TimeWording == nil || r.Roles[2].TimeWording.Text != "from 2010 to 2015" {
		t.Fatal("negation/history lost", r.Roles[2])
	}
	if r.Roles[3].Organization.Text != "Example Public Board" || r.Roles[3].Role.Text != "Member at Large" {
		t.Fatal("public board promoted to corporate employer")
	}
	if r.Aliases[0].LeadingName.Text != "Rita" || r.Aliases[0].FollowingName.Text != "Anne Jones" || r.Aliases[0].Parenthetical.Text != "Ri" {
		t.Fatal("parenthetical components invented", r.Aliases[0])
	}
	if r.Aliases[1].FollowingName.Text != "Smith" {
		t.Fatal("sentence punctuation became part of a name", r.Aliases[1])
	}
	before, _ := json.Marshal(e)
	again, err := ProposeProse(context.Background(), e)
	after, _ := json.Marshal(e)
	if err != nil || !reflect.DeepEqual(r, again) || !bytes.Equal(before, after) {
		t.Fatal("mutated source or nondeterministic replay", err)
	}
}

func TestProseAbstainsOnUnsupportedContextAndLayout(t *testing.T) {
	for _, body := range []string{
		`<h2>Example Corp</h2><h3>Alex Example</h3><p>Founder and Chairman</p>`,
		`<title>Example Corp</title><p>Alex Example is CEO.</p>`,
		`<p>If Alex Example is CEO of Example Corp, ask them.</p>`,
		`<p>They deny that Alex Example is CEO of Example Corp.</p>`,
		`<p>"Alex Example is CEO of Example Corp."</p>`,
		`<p>Alex Example is CEO of Example Corp?</p>`,
		`<p>Alex Example and Jo Li are directors of Example Corp.</p>`,
		`<p>Alex Example And Jo Li is CEO of Example Corp.</p>`,
		`<p>Alex (CEO) Example and Jo (Founder) Smith.</p>`,
		`<p>He is CEO of Example Corp.</p>`,
		`<script>Alex Example is CEO of Example Corp.</script>`,
		`<script type="application/ld+json">{"name":"Alex Example","jobTitle":"CEO"}</script>`,
	} {
		_, r := proseFixture(t, body)
		if len(r.Roles)+len(r.Aliases) != 0 {
			t.Fatal("unsupported material became a candidate", body, r)
		}
	}
	_, r := proseFixture(t, `<p>Alex Example is CEO of Other Company.</p><p>Alex Example is CEO of Other Company.</p>`)
	if len(r.Roles) != 2 || r.Roles[0].Evidence.Entry == r.Roles[1].Evidence.Entry || r.Roles[0].Organization.Text != "Other Company" {
		t.Fatal("deduplicated occurrences or substituted employer")
	}
	// There is no target-person input: a same-page namesake cannot inherit a role.
	_, r = proseFixture(t, `<p>Alex Q. Example is CEO of One Corp.</p><p>Alex R. Example is an engineer at Other Corp.</p>`)
	if len(r.Roles) != 2 || r.Roles[0].Person.Text == r.Roles[1].Person.Text || r.Roles[1].Role.Text != "engineer" {
		t.Fatal("namesake or employee promoted", r.Roles)
	}
	// A grammar match is not a semantic assertion. Do not silently claim this
	// baseline interprets denial or hypothetical context beyond the matched clause.
	_, r = proseFixture(t, `<p>Alex Example is CEO of Example Corp. That statement is false.</p>`)
	if len(r.Roles) != 1 || r.Roles[0].Meaning != "unverified_relation_mention" || r.Entries[0].State != "syntax_candidates_context_unassessed" {
		t.Fatal("unassessed context became truth")
	}
}

func TestProseValidationAndCancellation(t *testing.T) {
	e, _ := proseFixture(t, `<p>Alex Example is CEO of Example Corp.</p>`)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ProposeProse(ctx, e); err != context.Canceled {
		t.Fatal(err)
	}
	for _, alter := range []func(*companypage.Evidence){
		func(e *companypage.Evidence) { e.Contract = "wrong" },
		func(e *companypage.Evidence) { e.Source.SHA256 = "wrong" },
		func(e *companypage.Evidence) { e.Bytes = companypage.MaxBody + 1 },
		func(e *companypage.Evidence) { e.IdentityApproved = true },
	} {
		bad := e
		alter(&bad)
		if _, err := ProposeProse(context.Background(), bad); err == nil {
			t.Fatal("invalid lexical source accepted")
		}
	}
}

func TestProseRetainedSources(t *testing.T) {
	dir := "../../../tests/fixtures/person-affiliation"
	for _, tc := range []struct {
		file, pin, url string
		role           [3]string
		aliases        [][3]string
	}{
		{"jc2-about.html", "45fe693cfae5b459c92df0947f0b33efa386b56fc68fa160dd3c5c6332e3e391", "https://www.jc2ventures.com/about", [3]string{"John Chambers", "founder and CEO", "JC2 Ventures"}, nil},
		{"jc2-bio.html", "9bce701416c1345470ef55d1555f7875f0d4c4449f1c3a81f549d49cab717857", "https://www.jc2ventures.com/john-chambers", [3]string{"John Chambers", "founder and CEO", "JC2 Ventures"}, nil},
		{"ridgeline-leadership.html", "c91e3c5785ddf31a229ac50c6757dca476d8c5acb5cdaab234369ce892be4151", "https://ridgeline.ai/company/leadership", [3]string{}, nil},
		{"supplementary-v1/rick-reviglio.html", "c0a0ed7fc8cd9729aa775c98d423d9ecf70175f8cda3560e94b2fba63902e7ef", "https://goblueteam.com/rick-reviglio/", [3]string{"Rick Reviglio", "President and General Manager", "Western Nevada Supply"}, nil},
		{"supplementary-v1/jack-reviglio.html", "2e5bacfd4f2b85d086789ec66ea1f89725b7cf55f288b4cfcc4a0acf5b9abccf", "https://goblueteam.com/jack-reviglio/", [3]string{}, [][3]string{{"Theodore", "Ted", "Ray Reviglio"}, {"Richard", "Rick", "John Reviglio"}}},
		{"supplementary-v1/moana-culture.html", "104c26b89a92c3a76c04ec4a07c1890c6c813bc5f608867e2e79aa9e935fd7b1", "https://www.moananursery.com/our-culture/", [3]string{}, nil},
		{"supplementary-v1/gdc-alton-russell.html", "ebf9b5971c37c83759920e63e678ce711c0271c4e739ec287e732a7e17062427", "https://gdc.georgia.gov/alton-russell", [3]string{"Alton Russell", "Member at Large", "GDC Board of Corrections"}, nil},
	} {
		t.Run(tc.file, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(dir, tc.file))
			if err != nil {
				t.Fatal(err)
			}
			e, err := companypage.Extract(context.Background(), raw, companypage.Source{URL: tc.url, SHA256: tc.pin, ObservedOn: "2026-09-17"})
			if err != nil {
				t.Fatal(err)
			}
			r, err := ProposeProse(context.Background(), e)
			if err != nil {
				t.Fatal(err)
			}
			checkProseCitations(t, e, r)
			t.Logf("roles=%d aliases=%d text blocks=%d", len(r.Roles), len(r.Aliases), len(r.Entries))
			wantRoles := 0
			if tc.role != [3]string{} {
				wantRoles = 1
			}
			if len(r.Roles) != wantRoles || len(r.Aliases) != len(tc.aliases) {
				t.Fatal("retained-source candidate census changed", r.Roles, r.Aliases)
			}
			for _, role := range r.Roles {
				if [3]string{role.Person.Text, role.Role.Text, role.Organization.Text} != tc.role || role.TimeState != "role_validity_unknown" || role.TimeWording != nil {
					t.Fatal("source subject, organization, role or missing time changed", role)
				}
			}
			for i, alias := range r.Aliases {
				if [3]string{alias.LeadingName.Text, alias.Parenthetical.Text, alias.FollowingName.Text} != tc.aliases[i] {
					t.Fatal("source name form changed", alias)
				}
			}
			again, err := ProposeProse(context.Background(), e)
			if err != nil || !reflect.DeepEqual(r, again) {
				t.Fatal("retained replay changed", err)
			}
		})
	}
}
