package receiptparticipants

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/fundingbasis"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/receipts"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

func Load(path, expectedID string) (Result, error) {
	if !digest(expectedID) || filepath.Base(path) != "manifest.json" {
		return Result{}, fmt.Errorf("exact participant manifest.json and expected identity required")
	}
	f, err := os.Open(path)
	if err != nil {
		return Result{}, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, (8<<20)+1))
	if err != nil {
		return Result{}, err
	}
	if len(b) > 8<<20 {
		return Result{}, fmt.Errorf("oversized participant manifest")
	}
	return DecodeManifest(b, expectedID)
}

// DecodeManifest allows consumers to hash and validate the same manifest bytes.
func DecodeManifest(b []byte, expectedID string) (Result, error) {
	if !digest(expectedID) || len(b) > 8<<20 {
		return Result{}, fmt.Errorf("invalid participant manifest identity or size")
	}
	var r Result
	var err error
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err = d.Decode(&r); err != nil {
		return Result{}, err
	}
	if d.Decode(new(any)) != io.EOF {
		return Result{}, fmt.Errorf("trailing manifest data")
	}
	if r.SchemaVersion != Version || r.Policy != Policy || r.AppearanceRole != ContributorRole || r.IdentityResolved || r.ReferenceQualification || r.FinancialEligibility || r.AdditionalConduitAmount != "0" ||
		r.InventoryPolicy != fundingbasis.Policy || r.SourceRolePolicy != fundingbasis.EvidencePolicy || r.IndividualPolicy != receipts.ContractID+"@"+receipts.ContractVersion || r.CommitteePolicy != committeeflows.ContractID+"@"+committeeflows.ContractVersion ||
		r.CalculationID != expectedID || logicalID(r) != expectedID || !digest(r.FactSetID) || !digest(r.ManifestSHA256) || !digest(r.BuildSHA256) || r.Census.Rows == 0 || r.SourceRows < r.Census.Rows || r.SourceRows > math.MaxInt64 || r.OutputBytes > 32<<30 || r.Workers < 1 || r.Workers > 8 {
		return Result{}, fmt.Errorf("participant identity or evidence boundary mismatch")
	}
	if !((r.State == "complete_cycle_participant_index" && r.Scope == "complete_published_schedule_a_cycle" && r.Census.Rows == r.SourceRows) || (r.State == "complete_bounded_participant_benchmark" && r.Scope == "selected_whole_shards_not_complete_cycle")) {
		return Result{}, fmt.Errorf("participant scope/state mismatch")
	}
	var rows, size, last uint64
	names := make(map[string]bool)
	for _, f := range r.Files {
		if f.First <= last || f.Last < f.First || f.Last > r.SourceRows || f.Rows != f.Last-f.First+1 || !digest(f.SourceSHA256) || !digest(f.SHA256) || !digest(f.ValuesSHA256) || f.Name == "" || filepath.Base(f.Name) != f.Name || names[f.Name] || f.Bytes == 0 || f.Bytes > r.OutputBytes-size || f.Rows > r.Census.Rows-rows {
			return Result{}, fmt.Errorf("invalid participant file descriptor")
		}
		names[f.Name] = true
		if r.State == "complete_cycle_participant_index" && f.First != last+1 {
			return Result{}, fmt.Errorf("participant occurrence gap")
		}
		last = f.Last
		rows += f.Rows
		size += f.Bytes
	}
	if rows != r.Census.Rows || size != r.OutputBytes || r.Census.MemoRows > rows {
		return Result{}, fmt.Errorf("participant census mismatch")
	}
	var amounts uint64
	for _, n := range []uint64{r.Census.UnknownAmounts, r.Census.PositiveAmounts, r.Census.NegativeAmounts, r.Census.ZeroAmounts} {
		if n > rows-amounts {
			return Result{}, fmt.Errorf("participant amount census overflow")
		}
		amounts += n
	}
	if amounts != rows {
		return Result{}, fmt.Errorf("participant amount census mismatch")
	}
	for _, m := range []map[string]uint64{r.Census.Components, r.Census.Routes, r.Census.Conduits, r.Census.Earmarks} {
		var total uint64
		for _, n := range m {
			if n > rows-total {
				return Result{}, fmt.Errorf("participant census overflow")
			}
			total += n
		}
		if total != rows {
			return Result{}, fmt.Errorf("participant state census mismatch")
		}
	}
	return r, nil
}

type Inspection struct {
	SchemaVersion        string               `json:"schema_version"`
	CalculationID        string               `json:"participant_calculation_id"`
	AppearanceID         string               `json:"appearance_id"`
	FactSetID            string               `json:"fact_set_id"`
	Role                 string               `json:"appearance_role"`
	Participant          Row                  `json:"participant"`
	Source               fundingbasis.Receipt `json:"source"`
	IdentityResolved     bool                 `json:"contributor_identity_resolved"`
	FinancialEligibility bool                 `json:"financial_eligibility"`
}

func Inspect(ctx context.Context, root, facts, manifest, expectedID string, ordinal uint64) (Inspection, error) {
	i, err := OpenInspector(ctx, root, facts, manifest, expectedID)
	if err != nil {
		return Inspection{}, err
	}
	return i.Inspect(ctx, ordinal)
}

// Inspector holds one verified, immutable input scope for several source
// lookups. Opening it hashes the complete backing once, not once per witness.
// Each lookup still verifies its complete compact shard and full source row.
type Inspector struct {
	root, dir, participantSHA string
	manifest                  occ.ScheduleAColumnarManifest
	participant               Result
}

type InspectionScope struct {
	ParticipantID, FactSetID, ManifestSHA256, Cycle, SourceReleaseID, SourceReleaseManifestSHA256, ParticipantSHA256 string
	Rows                                                                                                             uint64
}

func OpenInspector(ctx context.Context, root, facts, manifest, expectedID string) (*Inspector, error) {
	if filepath.Base(manifest) != "manifest.json" {
		return nil, fmt.Errorf("exact participant manifest.json required")
	}
	f, err := os.Open(manifest)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, (8<<20)+1))
	if err != nil {
		return nil, err
	}
	r, err := DecodeManifest(b, expectedID)
	if err != nil {
		return nil, err
	}
	m, d, err := occ.LoadPublishedScheduleAColumnarManifest(ctx, root, facts)
	if err != nil {
		return nil, err
	}
	if filepath.Base(facts) != m.FactSetID+".json" || m.FactSetID != r.FactSetID || d != r.ManifestSHA256 || m.Cycle != r.Cycle || m.Counts.Facts != r.SourceRows {
		return nil, fmt.Errorf("participant/source ancestry mismatch")
	}
	for _, f := range r.Files {
		backed := false
		for _, s := range m.Shards {
			if f.First == s.FirstSourceRowOrdinal && f.Last == s.LastSourceRowOrdinal && f.SourceSHA256 == s.SHA256 {
				backed = true
				break
			}
		}
		if !backed {
			return nil, fmt.Errorf("participant shard ancestry mismatch")
		}
	}
	h := sha256.Sum256(b)
	return &Inspector{root: root, dir: filepath.Join(filepath.Dir(manifest), "data"), participantSHA: hex.EncodeToString(h[:]), manifest: m, participant: r}, nil
}

func (i *Inspector) Scope() InspectionScope {
	return InspectionScope{
		ParticipantID:               i.participant.CalculationID,
		FactSetID:                   i.manifest.FactSetID,
		ManifestSHA256:              i.participant.ManifestSHA256,
		Cycle:                       i.manifest.Cycle,
		SourceReleaseID:             i.manifest.SourceReleaseID,
		SourceReleaseManifestSHA256: i.manifest.SourceReleaseManifestSHA256,
		ParticipantSHA256:           i.participantSHA,
		Rows:                        i.participant.SourceRows,
	}
}

func (i *Inspector) Inspect(ctx context.Context, ordinal uint64) (Inspection, error) {
	return inspectVerified(ctx, i.root, i.manifest, i.participant, i.dir, ordinal)
}

func inspectVerified(ctx context.Context, root string, m occ.ScheduleAColumnarManifest, r Result, dir string, ordinal uint64) (Inspection, error) {
	if ordinal == 0 || ordinal > r.SourceRows {
		return Inspection{}, fmt.Errorf("appearance ordinal out of range")
	}
	var row Row
	found := false
	for _, f := range r.Files {
		if ordinal >= f.First && ordinal <= f.Last {
			backed := false
			for _, s := range m.Shards {
				if s.FirstSourceRowOrdinal == f.First && s.LastSourceRowOrdinal == f.Last && s.SHA256 == f.SourceSHA256 {
					backed = true
					break
				}
			}
			if !backed {
				return Inspection{}, fmt.Errorf("participant shard ancestry mismatch")
			}
			_, err := ReadShard(ctx, dir, f, func(v Row) error {
				if uint64(v.Ordinal) == ordinal {
					row = cloneOwned(v)
					found = true
				}
				return nil
			})
			if err != nil {
				return Inspection{}, err
			}
			break
		}
	}
	if !found {
		return Inspection{}, fmt.Errorf("appearance absent from declared participant scope")
	}
	source, err := fundingbasis.ReadSourceOccurrence(ctx, root, m, ordinal)
	if err != nil {
		return Inspection{}, err
	}
	evidence, err := fundingbasis.SourceEvidenceFromReceipt(source)
	if err != nil {
		return Inspection{}, err
	}
	if !reflect.DeepEqual(project(evidence), row) {
		return Inspection{}, fmt.Errorf("participant/source value mismatch")
	}
	id, err := AppearanceID(r.FactSetID, ordinal)
	if err != nil {
		return Inspection{}, err
	}
	return Inspection{SchemaVersion: "legal-tender.fec.receipt-participant-inspection.v1", CalculationID: r.CalculationID, AppearanceID: id, FactSetID: r.FactSetID, Role: ContributorRole, Participant: row, Source: source}, nil
}

func cloneOwned(r Row) Row {
	for _, p := range []*string{&r.Component, &r.IndividualDecision, &r.CommitteeDecision, &r.ReceiptRole, &r.SourceRoute, &r.EarmarkState, &r.ConduitState, &r.ReferenceState, &r.AmountState} {
		*p = strings.Clone(*p)
	}
	for _, p := range []**string{&r.Recipient, &r.ReportedSourceID, &r.ReportedConduitID, &r.Entity, &r.Contributor, &r.CleanContributor, &r.ConduitID, &r.ReceiptType} {
		if *p != nil {
			v := strings.Clone(**p)
			*p = &v
		}
	}
	if r.Amount != nil {
		v := *r.Amount
		r.Amount = &v
	}
	return r
}
