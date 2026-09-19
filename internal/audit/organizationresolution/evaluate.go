// Package organizationresolution evaluates the resolver against reviewed test
// assertions. Labels never enter the production resolver or approve graph edges.
package organizationresolution

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	resolver "github.com/vedantadhobley/legal-tender/internal/calculation/organizationresolution"
	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

const Version = "organization-evaluation.v1"
const maxCorpusBytes = 1 << 20
const maxSourceBytes = 2 << 20

var qid = regexp.MustCompile(`^Q[1-9][0-9]*$`)

type Source struct {
	ID         string `json:"id"`
	Kind       string `json:"kind"`
	URL        string `json:"url"`
	ObservedOn string `json:"observed_on"`
	Path       string `json:"path"`
	SHA256     string `json:"sha256"`
	Bytes      int    `json:"bytes"`
	Excerpt    string `json:"excerpt"`
}

// A label is a reviewed test assertion, not automatically extracted truth.
// Same-entity labels are not an exhaustive blacklist of all other QIDs.
type Label struct {
	Query    int      `json:"query_index"`
	Text     string   `json:"reported_text"`
	QID      string   `json:"qid"`
	Relation string   `json:"relation"`
	Reason   string   `json:"reason"`
	Sources  []string `json:"source_ids"`
}

type Corpus struct {
	Version       string   `json:"schema_version"`
	CaptureSHA256 string   `json:"capture_sha256"`
	ReviewedOn    string   `json:"reviewed_on"`
	ReviewBasis   string   `json:"review_basis"`
	Scope         string   `json:"scope"`
	Sources       []Source `json:"sources"`
	Labels        []Label  `json:"labels"`
}

type Options struct {
	CaptureDirectory, CaptureSHA256 string
	CorpusPath, CorpusSHA256        string
	BuildSHA256                     string
	ProposalPolicy                  string
}

type LabelResult struct {
	Label          Label  `json:"label"`
	Outcome        string `json:"outcome"`
	ObservedLabel  string `json:"observed_label,omitempty"`
	CandidateState string `json:"resolver_candidate_state,omitempty"`
}

type Case struct {
	Index          int                        `json:"query_index"`
	Query          wikimedia.Query            `json:"query"`
	ResolverState  string                     `json:"resolver_state"`
	ProposedQID    string                     `json:"proposed_qid,omitempty"`
	ProposalReview string                     `json:"proposal_review"`
	Reviewed       bool                       `json:"has_review_labels"`
	SourceIssue    string                     `json:"source_issue,omitempty"`
	Labels         []LabelResult              `json:"labels"`
	UnreviewedQIDs []string                   `json:"unreviewed_observed_qids"`
	Baseline       *resolver.ProposalBaseline `json:"baseline,omitempty"`
	NameCandidates []resolver.Candidate       `json:"name_candidates,omitempty"`
}

type Counts struct {
	Queries                      int `json:"queries"`
	SourceUsable                 int `json:"source_usable"`
	SourceUnusable               int `json:"source_unusable"`
	ReviewedQueries              int `json:"reviewed_queries"`
	UnreviewedQueries            int `json:"unreviewed_queries"`
	ObservedCandidatePairs       int `json:"observed_candidate_pairs"`
	ReviewedObservedPairs        int `json:"reviewed_observed_pairs"`
	UnreviewedObservedPairs      int `json:"unreviewed_observed_pairs"`
	SupportedProposals           int `json:"supported_proposals"`
	ContradictedProposals        int `json:"contradicted_proposals"`
	UnreviewedProposals          int `json:"unreviewed_proposals"`
	Abstentions                  int `json:"abstentions"`
	PositiveLabels               int `json:"positive_labels"`
	PositiveProposed             int `json:"positive_proposed"`
	PositiveRetrievedNotProposed int `json:"positive_retrieved_not_proposed"`
	PositiveNotRetrieved         int `json:"positive_not_retrieved"`
	PositiveEntityMissing        int `json:"positive_entity_missing"`
	PositiveUnassessed           int `json:"positive_unassessed_source_failure"`
	NegativeLabels               int `json:"negative_labels"`
	NegativeProposed             int `json:"negative_proposed"`
	NegativeNotProposed          int `json:"negative_not_proposed"`
	NegativeUnassessed           int `json:"negative_unassessed_source_failure"`
}

type Result struct {
	Version                     string `json:"schema_version"`
	BuildSHA256                 string `json:"build_sha256"`
	CaptureSHA256               string `json:"capture_sha256"`
	CorpusSHA256                string `json:"corpus_sha256"`
	ResolverPolicy              string `json:"resolver_policy"`
	Scope                       string `json:"scope"`
	Counts                      Counts `json:"counts"`
	Cases                       []Case `json:"cases"`
	IdentityPublicationApproved bool   `json:"identity_publication_approved"`
}

// Run reads only pinned local files. It verifies primary-source snapshot bytes
// and excerpt presence, not whether a human's interpretation is true. That review
// assertion remains explicit and versioned in the corpus.
func Run(o Options) (Result, error) {
	if !resolver.ValidPolicy(o.ProposalPolicy) {
		return Result{}, fmt.Errorf("unsupported organization proposal policy")
	}
	if !wikimedia.Digest(o.BuildSHA256) || !wikimedia.Digest(o.CorpusSHA256) {
		return Result{}, fmt.Errorf("build and corpus SHA-256 required")
	}
	replay, err := wikimedia.Read(o.CaptureDirectory, o.CaptureSHA256)
	if err != nil {
		return Result{}, err
	}
	root, err := os.OpenRoot(filepath.Dir(o.CorpusPath))
	if err != nil {
		return Result{}, err
	}
	defer root.Close()
	raw, err := read(root, filepath.Base(o.CorpusPath), maxCorpusBytes)
	if err != nil {
		return Result{}, err
	}
	if wikimedia.Hash(raw) != o.CorpusSHA256 {
		return Result{}, fmt.Errorf("evaluation corpus digest mismatch")
	}
	var corpus Corpus
	if err = strictjson.Decode(raw, &corpus); err != nil {
		return Result{}, err
	}
	// Zero is a valid first query, but an absent or null index must not silently
	// bind an annotation to it. Other required scalar fields reject their zero value.
	var presence struct {
		Labels []struct {
			Query *int `json:"query_index"`
		} `json:"labels"`
	}
	if err = json.Unmarshal(raw, &presence); err != nil {
		return Result{}, err
	}
	for _, l := range presence.Labels {
		if l.Query == nil {
			return Result{}, fmt.Errorf("review label query index absent or null")
		}
	}
	if err = validate(corpus, replay); err != nil {
		return Result{}, err
	}
	for _, s := range corpus.Sources {
		body, err := read(root, s.Path, maxSourceBytes)
		if err != nil {
			return Result{}, err
		}
		if len(body) != s.Bytes || wikimedia.Hash(body) != s.SHA256 || !bytes.Contains(body, []byte(s.Excerpt)) {
			return Result{}, fmt.Errorf("review source bytes, digest or excerpt mismatch")
		}
	}
	// Labels are not arguments to the resolver, under either proposal policy.
	proposals, err := resolver.ResolveWithPolicy(replay, o.BuildSHA256, o.ProposalPolicy)
	if err != nil {
		return Result{}, err
	}
	return evaluate(proposals, corpus, o.CorpusSHA256), nil
}

func read(root *os.Root, name string, limit int) ([]byte, error) {
	if !filepath.IsLocal(name) || filepath.Clean(name) != name {
		return nil, fmt.Errorf("invalid evaluation file locator")
	}
	info, err := root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > int64(limit) {
		return nil, fmt.Errorf("evaluation file type/size")
	}
	f, err := root.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, int64(limit)+1))
	if err != nil {
		return nil, err
	}
	if len(b) > limit {
		return nil, fmt.Errorf("evaluation read budget")
	}
	return b, nil
}

func validate(c Corpus, r wikimedia.Replay) error {
	if c.Version != Version || c.CaptureSHA256 != r.CaptureSHA256 || strings.TrimSpace(c.ReviewBasis) == "" || strings.TrimSpace(c.Scope) == "" || len(c.Sources) == 0 || len(c.Sources) > 32 || len(c.Labels) == 0 || len(c.Labels) > 100 {
		return fmt.Errorf("invalid evaluation version, scope or budget")
	}
	if _, err := time.Parse("2006-01-02", c.ReviewedOn); err != nil {
		return fmt.Errorf("invalid review date")
	}
	sources := map[string]Source{}
	for _, s := range c.Sources {
		if s.ID == "" || sources[s.ID].ID != "" || !wikimedia.Digest(s.SHA256) || s.Bytes < 1 || s.Bytes > maxSourceBytes || strings.TrimSpace(s.Excerpt) == "" || len(strings.Fields(s.Excerpt)) > 25 {
			return fmt.Errorf("invalid source review identity, digest, excerpt or budget")
		}
		if s.Kind != "first_party" && s.Kind != "official_registry" {
			return fmt.Errorf("review labels require a primary source outside Wikimedia")
		}
		u, err := url.Parse(s.URL)
		if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return fmt.Errorf("invalid primary-source URL")
		}
		host := strings.ToLower(u.Hostname())
		for _, suffix := range []string{"wikipedia.org", "wikidata.org", "wikimedia.org"} {
			if host == suffix || strings.HasSuffix(host, "."+suffix) {
				return fmt.Errorf("Wikimedia observation cannot corroborate itself")
			}
		}
		if _, err = time.Parse("2006-01-02", s.ObservedOn); err != nil || s.ObservedOn > c.ReviewedOn {
			return fmt.Errorf("invalid source observation time")
		}
		if !filepath.IsLocal(s.Path) || filepath.Clean(s.Path) != s.Path {
			return fmt.Errorf("invalid primary-source locator")
		}
		sources[s.ID] = s
	}
	seen := map[string]bool{}
	used := map[string]bool{}
	for _, l := range c.Labels {
		if l.Query < 0 || l.Query >= len(r.Observations) || l.Text != r.Observations[l.Query].Query.Text || !qid.MatchString(l.QID) || strings.TrimSpace(l.Reason) == "" || len(l.Sources) == 0 {
			return fmt.Errorf("invalid review query, QID or basis")
		}
		key := fmt.Sprintf("%d/%s", l.Query, l.QID)
		if seen[key] {
			return fmt.Errorf("duplicate or conflicting evaluation label")
		}
		seen[key] = true
		if l.Relation != "same_entity" && l.Relation != "different_entity" {
			return fmt.Errorf("unsupported review relation")
		}
		local := map[string]bool{}
		for _, id := range l.Sources {
			if sources[id].ID == "" || local[id] {
				return fmt.Errorf("unknown or repeated review source")
			}
			local[id] = true
			used[id] = true
		}
	}
	if len(used) != len(sources) {
		return fmt.Errorf("unused review source")
	}
	return nil
}

func evaluate(proposals resolver.Result, c Corpus, corpusSHA string) Result {
	out := Result{Version: Version, BuildSHA256: proposals.BuildSHA256, CaptureSHA256: proposals.CaptureSHA256, CorpusSHA256: corpusSHA, ResolverPolicy: proposals.Policy, Scope: c.Scope, Cases: []Case{}}
	for i, d := range proposals.Decisions {
		x := Case{Index: i, Query: d.Evidence.Query, ResolverState: d.State, ProposedQID: d.ProposedQID, SourceIssue: d.Evidence.Issue, ProposalReview: "abstained", Labels: []LabelResult{}, UnreviewedQIDs: []string{}}
		x.Baseline = d.Baseline
		for _, candidate := range d.Candidates {
			if len(candidate.Matches) > 0 {
				x.NameCandidates = append(x.NameCandidates, candidate)
			}
		}
		out.Counts.Queries++
		usable := d.Evidence.Issue == ""
		if !usable {
			out.Counts.SourceUnusable++
			x.ProposalReview = "source_unusable"
		} else {
			out.Counts.SourceUsable++
			if d.ProposedQID != "" {
				x.ProposalReview = "unreviewed_proposal"
			} else {
				out.Counts.Abstentions++
			}
		}
		for _, label := range c.Labels {
			if label.Query != i {
				continue
			}
			x.Reviewed = true
			l := LabelResult{Label: label, Outcome: "unassessed_source_failure"}
			if e, ok := d.Evidence.Entities[label.QID]; ok {
				l.ObservedLabel = e.Labels["en"].Value
			}
			for _, candidate := range d.Candidates {
				if candidate.QID == label.QID {
					l.CandidateState = candidate.State
					break
				}
			}
			positive := label.Relation == "same_entity"
			if positive {
				out.Counts.PositiveLabels++
			} else {
				out.Counts.NegativeLabels++
			}
			switch {
			case !usable:
				if positive {
					out.Counts.PositiveUnassessed++
				} else {
					out.Counts.NegativeUnassessed++
				}
			case d.ProposedQID == label.QID:
				if positive {
					l.Outcome = "supported_proposal"
					out.Counts.PositiveProposed++
					x.ProposalReview = l.Outcome
				} else {
					l.Outcome = "contradicted_proposal"
					out.Counts.NegativeProposed++
					x.ProposalReview = l.Outcome
				}
			case !positive:
				l.Outcome = "not_proposed"
				out.Counts.NegativeNotProposed++
			case discovered(d.Evidence, label.QID) && !retrieved(d.Evidence, label.QID):
				l.Outcome = "entity_missing"
				out.Counts.PositiveEntityMissing++
			case retrieved(d.Evidence, label.QID):
				l.Outcome = "retrieved_not_proposed"
				out.Counts.PositiveRetrievedNotProposed++
			default:
				l.Outcome = "not_retrieved"
				out.Counts.PositiveNotRetrieved++
			}
			x.Labels = append(x.Labels, l)
		}
		if x.Reviewed {
			out.Counts.ReviewedQueries++
		} else {
			out.Counts.UnreviewedQueries++
		}
		for _, id := range wikimedia.PageIDs(d.Evidence.Pages) {
			out.Counts.ObservedCandidatePairs++
			reviewed := false
			for _, l := range x.Labels {
				if l.Label.QID == id {
					reviewed = true
					break
				}
			}
			if reviewed {
				out.Counts.ReviewedObservedPairs++
			} else {
				out.Counts.UnreviewedObservedPairs++
				x.UnreviewedQIDs = append(x.UnreviewedQIDs, id)
			}
		}
		switch x.ProposalReview {
		case "supported_proposal":
			out.Counts.SupportedProposals++
		case "contradicted_proposal":
			out.Counts.ContradictedProposals++
		case "unreviewed_proposal":
			out.Counts.UnreviewedProposals++
		}
		out.Cases = append(out.Cases, x)
	}
	return out
}

func retrieved(o wikimedia.Observation, id string) bool {
	for _, p := range o.Pages {
		if p.Props["wikibase_item"] == id {
			if e, ok := o.Entities[id]; ok && e.Missing == nil {
				return true
			}
		}
	}
	return false
}

func discovered(o wikimedia.Observation, id string) bool {
	for _, p := range o.Pages {
		if p.Props["wikibase_item"] == id {
			return true
		}
	}
	return false
}
