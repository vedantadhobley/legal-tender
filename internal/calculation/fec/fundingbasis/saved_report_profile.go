package fundingbasis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"slices"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/summaryassertion"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportmetadata"
)

const maxSavedProfileBytes = 384 << 20

// readLineProfile revalidates pinned derived evidence, not the bulk body. Its
// recorded physical verification must agree with the verified summary release.
// This manual diagnostic is not a substitute for fact publication/readback.
func readLineProfile(ctx context.Context, path, expected, root string, s summaryassertion.WindowComparison) (ReportLineProfile, reportmetadata.Artifact, error) {
	var p ReportLineProfile
	a := reportmetadata.Artifact{Path: path}
	if len(expected) != 64 {
		return p, a, fmt.Errorf("require a lowercase profile SHA-256 pin")
	}
	if b, err := hex.DecodeString(expected); err != nil || hex.EncodeToString(b) != expected {
		return p, a, fmt.Errorf("require a lowercase profile SHA-256 pin")
	}
	if err := ctx.Err(); err != nil {
		return p, a, err
	}
	f, err := os.Open(path)
	if err != nil {
		return p, a, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maxSavedProfileBytes {
		return p, a, fmt.Errorf("profile must be a nonempty regular file within 384 MiB")
	}
	h := sha256.New()
	r := &io.LimitedReader{R: io.TeeReader(f, h), N: maxSavedProfileBytes + 1}
	d := json.NewDecoder(r)
	d.DisallowUnknownFields()
	if err := d.Decode(&p); err != nil {
		return p, a, err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF || r.N == 0 {
		return p, a, fmt.Errorf("trailing or oversized profile JSON")
	}
	a.Bytes, a.SHA256 = maxSavedProfileBytes+1-r.N, hex.EncodeToString(h.Sum(nil))
	if a.Bytes != info.Size() || a.SHA256 != expected {
		return p, a, fmt.Errorf("profile identity mismatch")
	}
	if err := validateSavedLineProfile(ctx, p); err != nil {
		return p, a, err
	}
	if p.Cycle != s.Cycle || p.SummaryInput != s.SummaryInput || p.SummaryCalculationID != s.SummaryCalculationID {
		return p, a, fmt.Errorf("profile/summary lineage mismatch")
	}
	source, err := profileSource(root, s.SummaryInput, s.Cycle)
	if err != nil {
		return p, a, err
	}
	if !reflect.DeepEqual(p.Source, source) {
		return p, a, fmt.Errorf("profile/source release mismatch")
	}
	return p, a, ctx.Err()
}

func validateSavedLineProfile(ctx context.Context, p ReportLineProfile) error {
	b, err := newReportLineProfiler(p.Cycle)
	if err != nil {
		return err
	}
	if p.SchemaVersion != b.out.SchemaVersion || p.Policy != b.out.Policy || p.LinePolicy != b.out.LinePolicy || p.IndividualPolicy != b.out.IndividualPolicy ||
		p.ComparisonReady || p.TerminalEligible || !slices.Equal(p.NotEstablished, b.out.NotEstablished) ||
		len(p.Forms) == 0 || len(p.Forms) > maxReportProfileGroups || len(p.Reports) == 0 || len(p.Reports) > maxReportProfileGroups {
		return fmt.Errorf("unsupported or incomplete saved line profile")
	}
	v, s := p.Verification, p.Source
	if v.SchemaVersion != "legal-tender.schedule-a-verification.v1" || !v.Complete || v.ExpectedPeriod != p.Cycle ||
		s.RowCount == nil || v.Rows != *s.RowCount || v.Rows != p.Total.Rows || v.ValidRows != v.Rows || v.InvalidRows != 0 ||
		v.CompressedBytes != s.CompressedByteCount || v.CompressedSHA256 != s.CompressedSHA256 ||
		v.UncompressedBytes != s.UncompressedByteCount || v.UncompressedSHA256 != s.UncompressedSHA256 ||
		v.ElapsedMilliseconds != 0 || len(v.Issues) != 0 || len(v.Checks) != 6 {
		return fmt.Errorf("saved profile physical verification mismatch")
	}
	checks := map[string]bool{}
	for _, c := range v.Checks {
		if !c.Passed || checks[c.ID] {
			return fmt.Errorf("invalid saved profile verification check")
		}
		checks[c.ID] = true
	}
	for _, name := range []string{"row_validity", "row_count", "compressed_byte_count", "compressed_sha256", "uncompressed_byte_count", "uncompressed_sha256"} {
		if !checks[name] {
			return fmt.Errorf("missing saved profile verification check")
		}
	}
	b.out = p
	b.out.ProfileID, b.out.Forms, b.out.Reports = "", []ReportLineProfileForm{}, []ReportLineProfileGroup{}
	for _, g := range p.Forms {
		if _, found := b.forms[g.Key]; found {
			return fmt.Errorf("duplicate saved form key")
		}
		m := g.Measures
		b.forms[g.Key] = &m
	}
	for i := range p.Reports {
		if err := ctx.Err(); err != nil {
			return err
		}
		g := &p.Reports[i]
		k, m := g.Key, g.Measures
		for _, c := range []Cell{k.Committee, k.File, k.Form, k.Schedule, k.Line, k.Memo, k.ReportType, k.ReportYear} {
			if !c.Present && c.Value != "" {
				return fmt.Errorf("noncanonical source null")
			}
		}
		if _, found := b.reports[k]; found {
			return fmt.Errorf("duplicate saved report key")
		}
		if m.Rows == 0 || m.Rows > p.Total.Rows || m.Known > m.Rows || m.Unknown > m.Rows || m.PositiveRows > m.Rows || m.NegativeRows > m.Rows || m.ZeroRows > m.Rows ||
			g.Dates.Missing > m.Rows || g.Dates.Invalid > m.Rows || g.Dates.BeforeCycle > m.Rows || g.Dates.InCycle > m.Rows || g.Dates.AfterCycle > m.Rows ||
			!slices.Contains([]string{"true", "false", "source_null"}, k.Individual) ||
			(k.Disposition == "reviewed_nonmemo_line" && m.Unknown != 0) || (k.Disposition == "unresolved_line_amount" && m.Known != 0) ||
			k.Disposition != reportLineDisposition(k.ReportLineKey, m.Known > 0) {
			return fmt.Errorf("invalid saved line group")
		}
		b.reports[k] = g
	}
	// Reuse the producer's exact conservation, ordering and content identity.
	canonical, err := b.finish(ctx)
	if err != nil {
		return err
	}
	if p.ProfileID == "" || canonical.ProfileID != p.ProfileID || !reflect.DeepEqual(canonical.Forms, p.Forms) || !reflect.DeepEqual(canonical.Reports, p.Reports) {
		return fmt.Errorf("saved profile content identity or ordering mismatch")
	}
	return nil
}
