package committeesummary

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
)

const contractDir = "../../../../contracts/sources/fec/committee-summary/v1/"

func fixture(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile(contractDir + "fixtures/sample.csv")
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func expectedFor(raw []byte) Expected {
	return Expected{Cycle: "2024", Bytes: int64(len(raw)), SHA256: digest(raw)}
}

func fixtureRows(t *testing.T) [][]string {
	t.Helper()
	rows, err := csv.NewReader(bytes.NewReader(fixture(t))).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	return rows[1:]
}

func csvBytes(t *testing.T, rows ...[]string) []byte {
	t.Helper()
	var out bytes.Buffer
	w := csv.NewWriter(&out)
	if err := w.Write(Fields()); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if err := w.Write(row); err != nil {
			t.Fatal(err)
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func TestCompiledSchemaAndExactFixture(t *testing.T) {
	var schema struct {
		Fields []string `json:"x-source-field-order"`
		Money  []string `json:"x-money-fields"`
		Dates  []string `json:"x-date-fields"`
	}
	rawSchema, err := os.ReadFile(contractDir + "record.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(rawSchema, &schema); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(schema.Fields, Fields()) || !slices.Equal(schema.Money, MoneyFields()) || !slices.Equal(schema.Dates, dateFields) {
		t.Fatal("compiled contract drift")
	}
	fields := Fields()
	fields[0] = "changed"
	if Fields()[0] != "Link_Image" {
		t.Fatal("mutable schema")
	}
	money := MoneyFields()
	money[0] = "changed"
	if MoneyFields()[0] != "INDV_CONTB" {
		t.Fatal("mutable money fields")
	}
	raw := fixture(t)
	r, err := NewReader(context.Background(), bytes.NewReader(raw), expectedFor(raw))
	if err != nil {
		t.Fatal(err)
	}
	physical := bytes.SplitAfter(raw, []byte("\n"))
	parsed := fixtureRows(t)
	var records []*Record
	offset := int64(len(physical[0]))
	for ordinal, expected := range parsed {
		row, err := r.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if row.Ordinal != uint64(ordinal+1) || row.Offset != offset || row.Length != int64(len(physical[ordinal+1])) || row.RawSHA256 != digest(physical[ordinal+1]) {
			t.Fatalf("wrong raw locator: %+v", row)
		}
		for i, name := range Fields() {
			if row.SourceFields[name] != expected[i] {
				t.Fatalf("record %d field %s changed", ordinal+1, name)
			}
		}
		offset += row.Length
		records = append(records, row)
	}
	if _, err := r.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if offset != int64(len(raw)) {
		t.Fatal("byte conservation failed")
	}
	if records[1].Money["COH_BOP"].State != Blank || records[1].Money["COH_BOP"].MinorUnits != nil {
		t.Fatal("blank cash changed")
	}
	if *records[2].Money["TTL_RECEIPTS"].MinorUnits != "32" || records[2].SourceFields["TTL_RECEIPTS"] != ".32" {
		t.Fatal("leading decimal changed")
	}
	if *records[3].Money["INDV_UNITEM_CONTB"].MinorUnits != "-6426498" {
		t.Fatal("negative subtotal changed")
	}
	if records[4].Dates["CVG_START_DT"].State != Invalid || records[4].SourceFields["CVG_START_DT"] != "99999999" {
		t.Fatal("invalid date erased")
	}
	if !slices.Contains(records[7].Issues, Issue{"reversed_interval", "CVG_START_DT"}) {
		t.Fatal("reversed interval not observed")
	}
	if records[5].SourceFields["CMTE_ID"] != records[6].SourceFields["CMTE_ID"] || records[5].SourceFields["CAND_ID"] == records[6].SourceFields["CAND_ID"] {
		t.Fatal("candidate references collapsed")
	}
	// Previously returned maps must not be reused for later records.
	if records[0].SourceFields["CMTE_ID"] != parsed[0][slices.Index(Fields(), "CMTE_ID")] {
		t.Fatal("record map was reused")
	}
}

func TestQuotedMultilineFieldsAndExactSpan(t *testing.T) {
	row := fixtureRows(t)[0]
	value := "commas, quotes \"and\"\nUTF-8: café\n\nretained"
	row[slices.Index(Fields(), "CMTE_NM")] = value
	raw := csvBytes(t, row)
	r, err := NewReader(context.Background(), bytes.NewReader(raw), expectedFor(raw))
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.SourceFields["CMTE_NM"] != value || got.Offset+got.Length != int64(len(raw)) || digest(raw[got.Offset:]) != got.RawSHA256 {
		t.Fatal("multiline CSV changed")
	}
}

func TestPhysicalFailuresNeverReturnSuccessfulVerification(t *testing.T) {
	original := csvBytes(t, fixtureRows(t)[0])
	headerLength := bytes.IndexByte(original, '\n') + 1
	short := slices.Clone(fixtureRows(t)[0][:len(columns)-1])
	extra := append(slices.Clone(fixtureRows(t)[0]), "extra")
	long := slices.Clone(fixtureRows(t)[0])
	long[0] = strings.Repeat("x", int(MaxRecordBytes))
	cases := map[string][]byte{
		"header":      bytes.Replace(original, []byte("CMTE_ID"), []byte("OTHER_ID"), 1),
		"header-only": original[:headerLength],
		"extra":       csvBytes(t, extra), "short": csvBytes(t, short),
		"blank-middle":     append(append(slices.Clone(original[:headerLength]), '\n'), original[headerLength:]...),
		"blank-end":        append(slices.Clone(original), '\n'),
		"missing-final-LF": original[:len(original)-1],
		"CRLF":             bytes.ReplaceAll(original, []byte("\n"), []byte("\r\n")),
		"record-limit":     csvBytes(t, long),
		"invalid-UTF8":     append(slices.Clone(original[:headerLength]), 0xff, '\n'),
		"bad-quote":        append(slices.Clone(original[:headerLength]), []byte("\"unterminated\n")...),
		"bare-quote":       append(slices.Clone(original[:headerLength]), []byte("bad\"quote\n")...),
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			out, err := Verify(context.Background(), bytes.NewReader(raw), expectedFor(raw), nil)
			if err == nil || out.Complete {
				t.Fatalf("accepted invalid physical data: %+v, %v", out, err)
			}
		})
	}
	for name, expected := range map[string]Expected{
		"bad-hash":      {Cycle: "2024", Bytes: int64(len(original)), SHA256: strings.Repeat("0", 64)},
		"truncated":     {Cycle: "2024", Bytes: int64(len(original)) + 1, SHA256: digest(original)},
		"extra-bytes":   {Cycle: "2024", Bytes: int64(len(original)) - 1, SHA256: digest(original)},
		"over-limit":    {Cycle: "2024", Bytes: MaxArtifactBytes + 1, SHA256: digest(original)},
		"wrong-cycle":   {Cycle: "2022", Bytes: int64(len(original)), SHA256: digest(original)},
		"invalid-cycle": {Cycle: "2023", Bytes: int64(len(original)), SHA256: digest(original)},
	} {
		t.Run(name, func(t *testing.T) {
			out, err := Verify(context.Background(), bytes.NewReader(original), expected, nil)
			if err == nil || out.Complete {
				t.Fatal("accepted mismatched artifact or partition")
			}
		})
	}
}

func TestRowLimitAndStickyFailure(t *testing.T) {
	raw := fixture(t)
	r, err := NewReader(context.Background(), bytes.NewReader(raw), expectedFor(raw))
	if err != nil {
		t.Fatal(err)
	}
	r.ordinal = MaxRows
	_, first := r.Next(context.Background())
	_, second := r.Next(context.Background())
	var physical *PhysicalError
	if !errors.As(first, &physical) || physical.Code != "row_limit" || first != second {
		t.Fatal(first, second)
	}
}

type brokenReader struct{ err error }

func (r brokenReader) Read([]byte) (int, error) { return 0, r.err }

func TestCancellationReadAndObserverFailures(t *testing.T) {
	raw := fixture(t)
	boom := errors.New("read failed")
	if _, err := Verify(context.Background(), brokenReader{boom}, expectedFor(raw), nil); !errors.Is(err, boom) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Verify(ctx, bytes.NewReader(raw), expectedFor(raw), nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	out, err := Verify(ctx, bytes.NewReader(raw), expectedFor(raw), func(*Record) error { cancel(); return nil })
	if !errors.Is(err, context.Canceled) || out.Complete {
		t.Fatal(out, err)
	}
	out, err = Verify(context.Background(), bytes.NewReader(raw), expectedFor(raw), func(*Record) error { return boom })
	if !errors.Is(err, boom) || out.Complete {
		t.Fatal(out, err)
	}
}

func TestTypedInvalidFieldsRemainRecords(t *testing.T) {
	row := fixtureRows(t)[0]
	for k, v := range map[string]string{"TTL_RECEIPTS": "NaN", "CMTE_ID": "bad", "CAND_ID": "bad", "CVG_END_DT": "20230229"} {
		row[slices.Index(Fields(), k)] = v
	}
	raw := csvBytes(t, row)
	out, err := Verify(context.Background(), bytes.NewReader(raw), expectedFor(raw), nil)
	if err != nil || !out.Complete || out.Rows != 1 || out.RowsWithIssues != 1 {
		t.Fatal(out, err)
	}
	if out.Money["TTL_RECEIPTS"].Invalid != 1 || out.Dates["CVG_END_DT"].Invalid != 1 || out.Multiplicity.UnindexedCommitteeRows != 1 || out.Multiplicity.UnindexedCompositeRows != 1 {
		t.Fatal("typed issues lost")
	}
	if out.Equations["cash"].Invalid != 1 {
		t.Fatal("invalid equation operands used")
	}
}

func TestVerificationReplayAndMultiplicity(t *testing.T) {
	raw := fixture(t)
	a, err := Verify(context.Background(), bytes.NewReader(raw), expectedFor(raw), nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Verify(context.Background(), bytes.NewReader(raw), expectedFor(raw), nil)
	if err != nil || !reflect.DeepEqual(a, b) {
		t.Fatal("nondeterministic verification", err)
	}
	if a.Rows != 8 || a.Multiplicity.CommitteeIDs != 7 || a.Multiplicity.RepeatedCommitteeIDs != 1 || a.Multiplicity.EqualNonCandidateGroups != 1 || a.Multiplicity.ConflictingNonCandidateGroups != 0 || a.TerminalAttributionEligible {
		t.Fatalf("bad profile: %+v", a)
	}
	row := fixtureRows(t)[0]
	other := slices.Clone(row)
	other[slices.Index(Fields(), "CAND_ID")] = "H0CA00001"
	conflict := slices.Clone(row)
	conflict[slices.Index(Fields(), "TTL_RECEIPTS")] = "1"
	raw = csvBytes(t, row, row, other, conflict)
	a, err = Verify(context.Background(), bytes.NewReader(raw), expectedFor(raw), nil)
	if err != nil {
		t.Fatal(err)
	}
	p := a.Multiplicity
	if a.Rows != 4 || p.ExactDuplicateExtraRows != 1 || p.DuplicateCompositeExtraRows != 2 || p.ConflictingCompositeKeys != 1 || p.ConflictingNonCandidateGroups != 1 {
		t.Fatalf("duplicates lost: %+v", p)
	}
}

func TestExactMoneyAndDateNormalization(t *testing.T) {
	for raw, want := range map[string]string{".27": "27", "-.01": "-1", "0": "0", "-0.00": "0", "92233720368547758.07": "9223372036854775807", "-92233720368547758.08": "-9223372036854775808"} {
		got, issue := parseMoney(raw)
		if issue != "" || got.State != Valid || *got.MinorUnits != want {
			t.Fatal(raw, got, issue)
		}
	}
	for _, raw := range []string{"NaN", "1e3", ".000", "1.000", "1.", " 1", "+1", "1,000", "92233720368547758.08"} {
		got, issue := parseMoney(raw)
		if issue == "" || got.State != Invalid || got.MinorUnits != nil {
			t.Fatal(raw, got)
		}
	}
	for _, raw := range []string{"99999999", "20230229", "20240230", "00000101", "01/01/2024", "2024111"} {
		if parseDate(raw).State != Invalid {
			t.Fatal(raw)
		}
	}
	if parseDate("").State != Blank || parseDate("20240229").State != Valid {
		t.Fatal("date states changed")
	}
}

func TestIssueExampleCapAndOverflowSafeEquation(t *testing.T) {
	row := fixtureRows(t)[0]
	row[slices.Index(Fields(), "CVG_START_DT")] = "99999999"
	rows := make([][]string, maxIssueExamples+5)
	for i := range rows {
		rows[i] = row
	}
	raw := csvBytes(t, rows...)
	out, err := Verify(context.Background(), bytes.NewReader(raw), expectedFor(raw), nil)
	if err != nil || out.IssueCounts["invalid_date"] != uint64(len(rows)) || len(out.IssueExamples["invalid_date"]) != maxIssueExamples {
		t.Fatal("issue counts were truncated", err)
	}
	for name, value := range map[string]string{"COH_BOP": "92233720368547758.07", "TTL_RECEIPTS": ".01", "TTL_DISB": "0", "COH_COP": "-92233720368547758.08"} {
		row[slices.Index(Fields(), name)] = value
	}
	raw = csvBytes(t, row)
	out, err = Verify(context.Background(), bytes.NewReader(raw), expectedFor(raw), nil)
	if err != nil || out.Equations["cash"].Different != 1 {
		t.Fatal("equation wrapped signed cents", err)
	}
}

func TestFingerprintFieldBoundaries(t *testing.T) {
	a, b := sha256.New(), sha256.New()
	hashString(a, "ab")
	hashString(a, "c")
	hashString(b, "a")
	hashString(b, "bc")
	if bytes.Equal(a.Sum(nil), b.Sum(nil)) {
		t.Fatal("field boundaries disappeared")
	}
}
