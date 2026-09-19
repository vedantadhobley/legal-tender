package receiptconduits

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"reflect"

	policy "github.com/vedantadhobley/legal-tender/internal/calculation/fec/earmarkassociation"
	xs "github.com/vedantadhobley/legal-tender/internal/storage/externalsort"
)

// ReadAdditions verifies both complete disposition streams and the group
// evidence. Only new associations reach visit. A downstream writer must not
// publish completion until this function succeeds. Qualified root context is
// capped at 100,000; source members are never accumulated in memory.
func ReadAdditions(ctx context.Context, baselinePath string, baseline Result, updatedPath string, updated Result, visit func(Decision) error) error {
	g := updated.Groups
	if g == nil || baseline.SchemaVersion != Version || g.BaselineID != baseline.CalculationID || g.BaselineValues != baseline.Decisions.ValuesSHA256 || updated.ParticipantID != baseline.ParticipantID || updated.ParticipantSHA256 != baseline.ParticipantSHA256 || updated.TopologyID != baseline.TopologyID || updated.TopologySHA256 != baseline.TopologySHA256 || updated.FactSetID != baseline.FactSetID || updated.FactManifestSHA256 != baseline.FactManifestSHA256 || updated.SourceRows != baseline.SourceRows || updated.Cycle != baseline.Cycle {
		return fmt.Errorf("additions require exact baseline ancestry")
	}
	_, sha, err := readManifest(baselinePath)
	if err != nil {
		return err
	}
	if sha != g.BaselineSHA256 {
		return fmt.Errorf("baseline manifest bytes differ")
	}
	if err := VerifyGroupBacking(ctx, updatedPath, updated); err != nil {
		return err
	}
	groupReader, err := xs.Open(ctx, filepath.Join(filepath.Dir(updatedPath), "data"), g.Decisions)
	if err != nil {
		return err
	}
	defer groupReader.Close()
	type root struct {
		id        string
		remaining uint64
	}
	roots := map[uint64]root{}
	for {
		r, err := groupReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		v, err := DecodeGroup(r, updated.SourceRows)
		if err != nil {
			return err
		}
		if v.Decision.State == policy.SharedAssociation {
			if len(roots) >= 100000 {
				return fmt.Errorf("qualified group context exceeds 100000 bound")
			}
			roots[v.Related] = root{*v.Decision.ConduitID, v.Decision.ObservedPeers}
		}
	}
	a, err := xs.Open(ctx, filepath.Join(filepath.Dir(baselinePath), "data"), baseline.Decisions)
	if err != nil {
		return err
	}
	defer a.Close()
	b, err := xs.Open(ctx, filepath.Join(filepath.Dir(updatedPath), "data"), updated.Decisions)
	if err != nil {
		return err
	}
	defer b.Close()
	var previous, changed, total uint64
	oldStates, newStates, amounts := map[string]uint64{}, map[string]uint64{}, map[string]uint64{}
	for {
		x, xe := a.Next()
		y, ye := b.Next()
		if xe == io.EOF && ye == io.EOF {
			break
		}
		if xe != nil || ye != nil {
			return fmt.Errorf("disposition streams incomplete or corrupt")
		}
		xd, err := DecodeDecision(x, baseline.SourceRows)
		if err != nil {
			return err
		}
		yd, err := DecodeDecision(y, updated.SourceRows)
		if err != nil {
			return err
		}
		if xd.Ordinal <= previous || xd.Ordinal != yd.Ordinal {
			return fmt.Errorf("changed disposition membership")
		}
		previous = xd.Ordinal
		total++
		oldStates[xd.State]++
		newStates[yd.State]++
		amounts[xd.AmountComparison]++
		if reflect.DeepEqual(x, y) {
			continue
		}
		if xd.State != sharedRejection || yd.State != policy.SharedAssociation || yd.ConduitID == nil || xd.Related != yd.Related || xd.AmountComparison != yd.AmountComparison {
			return fmt.Errorf("non-additive disposition change")
		}
		r, ok := roots[yd.Related]
		if !ok || r.remaining == 0 || r.id != *yd.ConduitID {
			return fmt.Errorf("new association lacks complete group evidence")
		}
		r.remaining--
		roots[yd.Related] = r
		changed++
		if err = visit(yd); err != nil {
			return err
		}
	}
	for _, r := range roots {
		if r.remaining != 0 {
			return fmt.Errorf("unconsumed qualified group members")
		}
	}
	if total != baseline.EligibleRoleRows || total != updated.EligibleRoleRows || changed != g.ChangedRows || !equalCounts(oldStates, baseline.States) || !equalCounts(newStates, updated.States) || !equalCounts(amounts, baseline.Amounts) || !equalCounts(amounts, updated.Amounts) {
		return fmt.Errorf("additive publication conservation differs")
	}
	return nil
}
