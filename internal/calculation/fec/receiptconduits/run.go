package receiptconduits

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
	"slices"
	"sync"
	"syscall"
	"time"

	policy "github.com/vedantadhobley/legal-tender/internal/calculation/fec/earmarkassociation"
	participants "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
	refs "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptreferences"
	xsort "github.com/vedantadhobley/legal-tender/internal/storage/externalsort"
)

func readManifest(path string) ([]byte, string, error) {
	if filepath.Base(path) != "manifest.json" {
		return nil, "", fmt.Errorf("exact manifest.json required")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, "", err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil {
		return nil, "", err
	}
	if len(b) > 1<<20 {
		return nil, "", fmt.Errorf("manifest exceeds 1MiB")
	}
	h := sha256.Sum256(b)
	return b, hex.EncodeToString(h[:]), nil
}

func Run(ctx context.Context, o Options) (Result, error) {
	start := time.Now()
	if !digest(o.BuildSHA256) || o.Workers < 1 || o.Workers > 8 || o.RunRows < 1 || o.RunRows > 100000 || o.FanIn < 2 || o.FanIn > 16 || o.MaxWorkspaceBytes == 0 || o.MaxWorkspaceBytes > 32<<30 || o.OutputDirectory == "" {
		return Result{}, fmt.Errorf("bounded workers/runs/fan-in, build identity and new <=32GiB workspace required")
	}
	if _, err := os.Lstat(o.OutputDirectory); !os.IsNotExist(err) {
		return Result{}, fmt.Errorf("new output directory required")
	}
	b, psha, err := readManifest(o.Participants)
	if err != nil {
		return Result{}, err
	}
	p, err := participants.DecodeManifest(b, o.ParticipantID)
	if err != nil {
		return Result{}, err
	}
	t, tsha, err := refs.LoadTopology(o.Topology, o.TopologyID)
	if err != nil {
		return Result{}, err
	}
	if p.State != "complete_cycle_participant_index" || p.SourceRows != t.SourceRows || p.FactSetID != t.FactSetID || p.ManifestSHA256 != t.FactManifestSHA256 || p.Cycle != t.Cycle {
		return Result{}, fmt.Errorf("complete participant/topology source ancestry required")
	}
	if o.GroupBaseline != "" || o.GroupBaselineID != "" {
		if o.profile != nil {
			return Result{}, fmt.Errorf("group publication and diagnostic profile are separate commands")
		}
		b, sha, err := readManifest(o.GroupBaseline)
		if err != nil {
			return Result{}, err
		}
		baseline, err := DecodeManifest(b, o.GroupBaselineID)
		if err != nil {
			return Result{}, err
		}
		if baseline.SchemaVersion != Version || baseline.ParticipantID != p.CalculationID || baseline.ParticipantSHA256 != psha || baseline.TopologyID != t.CalculationID || baseline.TopologySHA256 != tsha || baseline.FactSetID != p.FactSetID || baseline.FactManifestSHA256 != p.ManifestSHA256 || baseline.SourceRows != p.SourceRows || baseline.Cycle != p.Cycle {
			return Result{}, fmt.Errorf("exact unchanged v1 baseline ancestry required")
		}
		o.groups = &groupWork{baseline: baseline, sha: sha, groups: make([]xsort.File, len(p.Files)), changes: make([]xsort.File, len(p.Files))}
	}
	var fs syscall.Statfs_t
	if err = syscall.Statfs(filepath.Dir(o.OutputDirectory), &fs); err != nil {
		return Result{}, err
	}
	metadataReserve := uint64(1 << 20)
	if o.profile != nil {
		metadataReserve += 1 << 20
	}
	if fs.Bavail*uint64(fs.Bsize) < o.MaxWorkspaceBytes+metadataReserve {
		return Result{}, fmt.Errorf("workspace plus manifest reserve exceed free disk")
	}
	if err = ctx.Err(); err != nil {
		return Result{}, err
	}
	if err = os.Mkdir(o.OutputDirectory, 0750); err != nil {
		return Result{}, err
	}
	space, err := xsort.NewWorkspace(filepath.Join(o.OutputDirectory, "data"), o.MaxWorkspaceBytes)
	if err != nil {
		return Result{}, err
	}
	w := work{ctx: ctx, space: space, p: p, t: t, o: o, read: func(ctx context.Context, i int, visit func(participants.Row) error) (participants.Census, error) {
		return participants.ReadShard(ctx, filepath.Join(filepath.Dir(o.Participants), "data"), p.Files[i], visit)
	}}
	r, err := w.calculate(filepath.Join(filepath.Dir(o.Topology), "data"))
	if err != nil {
		return Result{}, err
	}
	if o.groups != nil {
		if o.Progress != nil {
			o.Progress("verifying exact baseline and applying complete shared-group decisions")
		}
		r, err = w.applyGroups(r)
		if err != nil {
			return Result{}, err
		}
	}
	r.SchemaVersion = Version
	r.State = "complete_cycle_conduit_evidence"
	r.Policy = Policy
	if r.Groups != nil {
		r.SchemaVersion, r.Policy = GroupVersion, GroupPublicationPolicy
	}
	r.AssociationPolicy = policy.Policy
	r.BuildSHA256 = o.BuildSHA256
	r.ParticipantID = p.CalculationID
	r.ParticipantSHA256 = psha
	r.TopologyID = t.CalculationID
	r.TopologySHA256 = tsha
	r.FactSetID = p.FactSetID
	r.FactManifestSHA256 = p.ManifestSHA256
	r.Cycle = p.Cycle
	r.SourceRows = p.SourceRows
	r.OtherRows = r.SourceRows - r.EligibleRoleRows
	r.OtherDisposition = "not_a_non_memo_reviewed_earmark"
	r.AdditionalAmount = "0"
	r.Workers = o.Workers
	r.RunRows = o.RunRows
	r.FanIn = o.FanIn
	r.PeakWorkspaceBytes, r.RetainedBytes = space.Stats()
	r.ElapsedMS = time.Since(start).Milliseconds()
	var usage syscall.Rusage
	if err = syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return Result{}, err
	}
	r.PeakRSSBytes = uint64(usage.Maxrss) * 1024
	r.CalculationID = identity(r)
	if err = ctx.Err(); err != nil {
		return Result{}, err
	}
	if err = save(o.OutputDirectory, r); err != nil {
		return Result{}, err
	}
	return r, nil
}

func (w work) calculate(topologyDir string) (Result, error) {
	var mu sync.Mutex
	log := func(s string) {
		mu.Lock()
		defer mu.Unlock()
		if w.o.Progress != nil {
			w.o.Progress(s)
		}
	}
	log("verifying and partitioning complete reference endpoint topology")
	tops, err := w.split(topologyDir, w.t.Endpoints, true)
	if err != nil {
		return Result{}, err
	}
	requests := make([]xsort.File, len(w.p.Files))
	immediate := make([]xsort.File, len(w.p.Files))
	censuses := make([]participants.Census, len(w.p.Files))
	eligible := make([]uint64, len(w.p.Files))
	log("classifying all participant occurrences and building sole-peer requests")
	err = parallel(w.ctx, w.o.Workers, len(w.p.Files), func(ctx context.Context, i int) error {
		var err error
		requests[i], immediate[i], censuses[i], eligible[i], err = w.collect(i, ctx, tops[i])
		if err == nil {
			log(fmt.Sprintf("participant shard %d classified", i))
		}
		return err
	})
	if err != nil {
		return Result{}, err
	}
	if !sameCensus(sumCensus(censuses), w.p.Census) {
		return Result{}, fmt.Errorf("complete participant census mismatch")
	}
	log("merging and partitioning peer requests with bounded fan-in")
	merged, err := w.mergeLevel(requests)
	if err != nil {
		return Result{}, err
	}
	parts, err := w.split(w.space.Dir, merged, false)
	if err != nil {
		return Result{}, err
	}
	if err = w.space.Remove(merged); err != nil {
		return Result{}, err
	}
	resolved := make([]xsort.File, len(parts))
	log("joining both endpoint roles through the shared association policy")
	err = parallel(w.ctx, w.o.Workers, len(parts), func(ctx context.Context, i int) error {
		var err error
		resolved[i], err = w.qualify(ctx, i, tops[i], parts[i])
		if err != nil {
			return err
		}
		if err = w.space.Remove(parts[i]); err != nil {
			return err
		}
		if err = w.space.Remove(tops[i]); err != nil {
			return err
		}
		log(fmt.Sprintf("peer shard %d qualified", i))
		return nil
	})
	if err != nil {
		return Result{}, err
	}
	log("merging source-ordered association dispositions")
	f, err := w.mergeLevel(append(immediate, resolved...))
	if err != nil {
		return Result{}, err
	}
	log("verifying every disposition against complete participant membership")
	r, err := w.verifyMembership(f)
	if err != nil {
		return Result{}, err
	}
	var expected uint64
	for _, n := range eligible {
		expected += n
	}
	if r.EligibleRoleRows != expected {
		return r, fmt.Errorf("association membership conservation")
	}
	r.Decisions = f
	return r, nil
}

func (w work) verifyMembership(f xsort.File) (Result, error) {
	parts, err := w.split(w.space.Dir, f, false)
	if err != nil {
		return Result{}, err
	}
	results := make([]Result, len(parts))
	err = parallel(w.ctx, w.o.Workers, len(parts), func(ctx context.Context, i int) error {
		r, err := xsort.Open(ctx, w.space.Dir, parts[i])
		if err != nil {
			return err
		}
		defer r.Close()
		v, nextErr := r.Next()
		out := Result{States: map[string]uint64{}, Amounts: map[string]uint64{}}
		_, err = w.read(ctx, i, func(row participants.Row) error {
			if nextErr != nil && nextErr != io.EOF {
				return nextErr
			}
			if !policy.Applies(evidence(row)) {
				if nextErr == nil && v.Ordinal <= uint64(row.Ordinal) {
					return fmt.Errorf("decision for inapplicable occurrence")
				}
				return nil
			}
			if nextErr != nil || v.Ordinal != uint64(row.Ordinal) {
				return fmt.Errorf("missing or duplicate association disposition")
			}
			d, err := DecodeDecision(v, w.p.SourceRows)
			if err != nil {
				return err
			}
			out.EligibleRoleRows++
			out.States[d.State]++
			out.Amounts[d.AmountComparison]++
			if d.ConduitID != nil {
				out.Qualified++
			}
			v, nextErr = r.Next()
			return nil
		})
		if err != nil {
			return err
		}
		if nextErr != io.EOF {
			return fmt.Errorf("trailing association disposition")
		}
		results[i] = out
		return w.space.Remove(parts[i])
	})
	if err != nil {
		return Result{}, err
	}
	out := Result{States: map[string]uint64{}, Amounts: map[string]uint64{}}
	for _, r := range results {
		out.EligibleRoleRows += r.EligibleRoleRows
		out.Qualified += r.Qualified
		for k, n := range r.States {
			out.States[k] += n
		}
		for k, n := range r.Amounts {
			out.Amounts[k] += n
		}
	}
	if out.EligibleRoleRows != f.Rows {
		return out, fmt.Errorf("decision count mismatch")
	}
	return out, nil
}

func save(dir string, r Result) error {
	return saveJSON(dir, "manifest.json", r)
}

func saveJSON(dir, name string, r any) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if len(b)+1 > 1<<20 {
		return fmt.Errorf("manifest exceeds 1MiB reserve")
	}
	p := filepath.Join(dir, "."+name+".tmp")
	f, err := os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0640)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = f.Write(append(b, '\n')); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Link(p, filepath.Join(dir, name)); err != nil {
		return err
	}
	return os.Remove(p)
}

func Load(path, expected string) (Result, error) {
	b, _, err := readManifest(path)
	if err != nil {
		return Result{}, err
	}
	return DecodeManifest(b, expected)
}

// DecodeManifest lets downstream publishers validate and hash the same bytes.
func DecodeManifest(b []byte, expected string) (Result, error) {
	if len(b) > 1<<20 {
		return Result{}, fmt.Errorf("oversized conduit manifest")
	}
	var r Result
	var err error
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err = d.Decode(&r); err != nil {
		return r, err
	}
	if d.Decode(new(any)) != io.EOF {
		return r, fmt.Errorf("trailing conduit manifest")
	}
	if err = validateGroupManifest(r); err != nil {
		return r, err
	}
	if !digest(expected) || r.CalculationID != expected || identity(r) != expected || r.AssociationPolicy != policy.Policy || r.State != "complete_cycle_conduit_evidence" || r.FinancialEligibility || r.IdentityResolved || r.AdditionalAmount != "0" ||
		!digest(r.BuildSHA256) || !digest(r.ParticipantID) || !digest(r.ParticipantSHA256) || !digest(r.TopologyID) || !digest(r.TopologySHA256) || !digest(r.FactSetID) || !digest(r.FactManifestSHA256) || r.SourceRows == 0 || r.EligibleRoleRows > r.SourceRows || r.OtherRows != r.SourceRows-r.EligibleRoleRows || r.OtherDisposition != "not_a_non_memo_reviewed_earmark" || r.Decisions.Rows != r.EligibleRoleRows || r.Qualified != r.States["reported_earmark_memo_association"]+r.States[policy.SharedAssociation] || !digest(r.Decisions.SHA256) || !digest(r.Decisions.ValuesSHA256) {
		return r, fmt.Errorf("conduit identity/scope/count mismatch")
	}
	for _, m := range []map[string]uint64{r.States, r.Amounts} {
		var n uint64
		for _, v := range m {
			if v > r.EligibleRoleRows-n {
				return r, fmt.Errorf("conduit census overflow")
			}
			n += v
		}
		if n != r.EligibleRoleRows {
			return r, fmt.Errorf("conduit census mismatch")
		}
	}
	if r.SourceRows > math.MaxInt64 || r.Workers < 1 || r.Workers > 8 || r.Decisions.Name == "" || filepath.Base(r.Decisions.Name) != r.Decisions.Name {
		return r, fmt.Errorf("invalid conduit resource/descriptor bounds")
	}
	for k := range r.States {
		if !slices.Contains(states, k) {
			return r, fmt.Errorf("unknown conduit state")
		}
	}
	for k := range r.Amounts {
		if !slices.Contains(amounts, k) {
			return r, fmt.Errorf("unknown amount comparison")
		}
	}
	return r, nil
}
