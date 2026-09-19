# Person identity and dated affiliation: proposed acceptance policy

**Status: candidate/time evaluation implemented, 2026-09-16; the first concrete
identity-rule trial was rejected and removed after real-source review. Production
identity acceptance remains unimplemented.** After the
[dated evidence check](../audit/dated-person-role-evidence-2026-09-16.md) and this
proposal, the user approved the next bounded implementation. The concrete evidence
candidate/time rules below have a separate Go evaluator. Neither identity-acceptance
route is enabled. The [feasibility audit](../audit/person-identity-rule-feasibility-2026-09-16.md)
rejected the first conjunctive identity experiment because it mixed identity with
role timing and required evidence that official sources do not normally provide.
Existing screening, graph and money behavior is unchanged.

## Recommendation

Keep useful candidate connections, but distinguish them from accepted identity
links. Decide person identity, organization identity, role meaning and role timing
separately. A stronger result on one question must not fill a gap in another.
These are evidence classifications, not calibrated probability scores.

| Question | Proposed distinction |
|---|---|
| Who does this FEC appearance describe? | Candidate, corroborated identity decision, ambiguous/conflicted, or unassessed |
| Which organization does the reported employer name identify? | Name correspondence versus separately resolved organization |
| What relationship is asserted? | Employee, executive, board role, founder, ownership or control, preserving source wording |
| Does that role apply on the receipt date? | Source-supported, point-only/unknown, inferred continuity, or explicit contrary/conflicting evidence |

An external person/company relationship can remain useful even when no FEC
appearance is resolved to that person. Conversely, knowing the person does not
prove a particular employer or role on every receipt date. Preserve each occurrence;
do not collapse receipts into one person record as a side effect of matching.

## 1. Identity: propose broadly, accept with separate corroboration

**Candidate generation:** use the existing explained name/employer comparisons.
Source-attested aliases can generate candidates; spelling, nickname, omitted-initial
or embedding suggestions are not identity decisions. Keep the original strings and
the reason for each comparison. A missing initial is neither equality nor proof
that the people differ. No per-person substitutions or donation threshold.

**Proposed acceptance gate:** allow a versioned, reversible identity decision by
one of two routes, with source references and conflict checks in either case:

1. An authoritative person identifier or explicit cross-reference anchored to both
   input identities, with matching namespace and entity scope. An identifier found
   by name search does not satisfy this route; a receipt/transaction ID is not a
   person identifier.
2. A supported name correspondence, employer context beyond string similarity,
   and additional corroboration that distinguishes the candidate from plausible
   rivals. This must connect to the FEC appearance, not merely enrich a biography.
   A concrete example to test is a separately corroborated fuller name plus dated
   work/location context consistent with the reported fields. A generic title or
   city alone is insufficient. Evidence must not be recycled from the name search
   and counted again as independent confirmation.

Route 2 is a minimum evidence requirement, **not a production acceptance policy**.
The first concrete signal combination failed feasibility review; its exact occupation,
personal-locality, receipt-day and blanket-rival requirements were not a sound
identity rule. The replacement must be evaluated on real positives and contrasting
rivals before enabling automatic acceptance. Do not replace that work with an
invented score, source-count threshold or a vague runtime `looks_correct` flag.
Straightforward formatting changes need no special case; actual identity judgments
need an explicit rule. Accepted resolution still means an evidence-backed inference,
not that the FEC supplied a canonical person ID.

Relevant safeguards:

- Preserve namesakes. A positively corroborated candidate need not be blocked by
  every person sharing a name, but a rival satisfying the same distinguishing
  evidence blocks a unique decision. Missing employer data or an expired role is
  not proof that a rival is a different donor.
- Record discovery failures and the searched scope. One returned result is not
  global uniqueness; failed required evidence checks cannot pass acceptance.
- Multiple URLs, Wikipedia plus its linked Wikidata item, and copies of a press
  release do not establish independent corroboration. Unknown source dependence
  stays unknown. Do not count repeated receipts as independent identity proof.
- Resolve private companies, brands, parents and subsidiaries separately. If only
  employer-name correspondence is known, keep that context without inventing a
  legal-entity link. Two same-name LEIs remain two candidates.
- Do not merge identities transitively through uncertain candidate links. A
  correction can supersede a derived decision without rewriting source facts.

This should ultimately run in Go without per-record human intervention. The current
review annotations evaluate proposed rules; they are not a production whitelist or
an automatic prose extractor. Unknown cases may remain unknown in an automated run.

## 2. Time: use disclosed periods, label continuity as inference

Recommended first policy:

| Available evidence for the same resolved person, organization and role | Proposed treatment at receipt date |
|---|---|
| Explicit role period covers the day, at supported precision | Source-supported role timing; retain contradictory claims if present |
| Explicit role observation on that day | Source-supported point, not lifetime service |
| Only an earlier or later observation | Show the dated role and elapsed time; no automatic forward/backward extension |
| Two distinct dated observations bracket the day | May show a separate continuity hypothesis, subject to the conditions below |
| Explicit end before the day, or start after it | This reported role period does not cover the day; not proof of no other term |
| Coarse boundary contains the day, unknown date or conflicting role evidence | Preserve uncertainty; do not force a day-level answer |

For a continuity hypothesis, use the nearest usable observations on each side for
the same role and endpoints. Both need genuine role-time support; page retrieval,
modification dates and duplicate republications do not supply a second observation.
Do not bridge an explicit termination, incompatible intervening assertion or known
break/reappointment. Keep all input observations, the gap length and coverage
limitations. Absence of a discovered job change is not proof that none occurred.
Different simultaneous jobs are not automatically conflicting.

Even two agreeing observations do not prove uninterrupted service. Label the
result **inferred continuity**, keep it separate from source-supported timing, and
make inclusion explicit in queries. Do not turn a point into an open-ended period
or fill a whole FEC cycle. No 30/180/730-day cutoff is justified by the current
sample; expose the actual gap rather than treating any gap length as confirmed.
An initial implementation need not materialize continuity hypotheses at all.

## 3. Corporate context is not corporate funding

Preserve all disclosed relationships. A leadership-oriented view may select
executive, board or explicitly supported controlling-owner roles. Ordinary
employment, founder status and ownership without control remain distinct, useful
context; none is automatically leadership. A large donation is not a role test.

Even with an accepted person link and supported executive role, the contribution
remains the individual's reported transaction. Do not route its dollars through an
employer edge, multiply totals across affiliations, infer corporate policy positions
or change terminal eligibility. Future associated-donor rollups require explicit
identity/time filters and overlap handling; they are not corporate contributions.
The [product contract](./product-contract.md) already requires this separation.

## Applying the proposal to the retained cases

These are policy walkthroughs over retained evidence, not new identity decisions:

- **Chambers / JC2:** candidate correspondence improves with the dated corporate
  disclosure. That is not yet a tested route-2 identity acceptance. The May 2023
  role point alone does not cover September's receipts. Today's biography may be
  another observation, but capture date alone does not establish an explicit role
  observation then or prove continuity through the intervening years.
- **Duffield / Ridgeline:** the dated role point improves temporal context. The
  David/Dave/full-name correspondence still needs supported identity binding, not
  a global nickname expansion. Twenty-five days is not a special acceptance rule.
- **Namesakes and same-name organizations:** retain relevant alternatives. More
  favorable role dates or first search position cannot manufacture uniqueness.
- **Reported engineer:** retain the reported employer and occupation. Missing
  enrichment does not establish ordinary-employee status or lack of influence.

## Replacement identity evidence contract

The candidate/time evaluator remains implemented. The first identity trial is
rejected. The additive `person-identity-evidence.v2` classifier now keeps person
correspondence separate from relationship meaning and timing. Its retained and
synthetic evaluation is recorded in the
[v2 audit](../audit/person-identity-evidence-v2-2026-09-16.md).

The classifier compares caller-authenticated structured name components and returns
the existing explained organization-name rules plus an explicit `CO`/`COMPANY`
legal-designator rule. Missing middle or suffix fields remain uncertainty; a
conflicting supplied distinguishing component does not block. Unattested first-name
variants remain visible gaps and do not acquire nickname equivalence. Text
correspondence is never legal-entity identity.

Reported occupation meaning is separately bound to the exact FEC appearance and
raw occupation text. Broad role, specific role, denial, source issue and mismatch
states never change the person-candidate result. The existing role timelines preserve
point/period applicability and explicit contradiction. Distinct origins remain
independence-unassessed rather than becoming corroboration.

Optional locality evidence preserves personal, business, issuer, `c/o`, mailing or
unknown context and an independent as-of comparison. It never becomes mandatory or
implies residence. The result carries only supported/unassessed candidate and
abstention states; it has no numeric score or winner. All identity, graph and money
approval flags remain false.

The [automated affiliation report](./affiliation-enrichment.md) now connects existing
discovery and parsed-source rules. It preserves full source names, aliases and coarse
dates rather than inventing the structured/day-only fields expected by this review
classifier. It keeps all retrieved candidates, including name variants and missing
employer evidence. The first live additional sample exposes source coverage as the
next gap. Organization-identity evidence, independent corroboration and acceptance
remain open; this sample supports no population accuracy claim.

For time, first preserve point/period evidence and its relationship to the receipt
date. Test continuity separately if selected; it must not silently change the
current [day-only screening](./person-affiliation-testing.md) or
[coarse-date diagnostic](./person-binding-diagnostic.md). No source acquisition or
financial changes are prerequisites for reviewing this policy.

## Implemented scope

`personaffiliation.AssessEvidence(appearance, observations, inferContinuity)` is a
pure, additive Go evaluator. It accepts at most 10,000 typed role observations for
one exact FEC appearance. It reuses format-only person-name comparison, existing
explained organization-name rules, role categories and strict day/zone-free-timestamp
comparison. The old `Assess` and parsed-Wikidata `AssessBinding` are unchanged.

Each observation supplies the existing `Claim`, an explicit asserted/denied
polarity, an optional specific source-qualified role ID and an optional origin
reference. A denial means an explicit source assertion, never a missing role or an
old term. An origin identifies the underlying observation shared by republications;
different body hashes alone do not prove distinct observations. The caller must
authenticate these semantics and source bytes. This is not an untrusted-input API
or an automatic source/prose adapter. Review annotations are still test inputs.

The result keeps every source occurrence and returns:

- Per-observation name/employer correspondence, role meaning and date comparison.
- All name/employer candidate IDs, including expired or issue-bearing rivals;
  correspondence is not affirmative employment evidence or identity approval.
- Separate timelines keyed by source person, organization, broad role category
  and specific role ID. There is no cross-namespace identity merge.
- Source support and explicit denial at the receipt day, including a conflict
  state when both occur. Unassessed evidence remains visible even when another
  observation supports that day. Source support does not mean verified truth.
- Optional continuity hypotheses, never financial or identity acceptance.

The continuity option requires nearest positive point observations on each side,
with known, distinct underlying origins and the same specific role. CEO and CFO
cannot be joined merely because both classify as executives. Copies on one side
remain separate evidence occurrences but do not become independent confirmation;
an origin reused across both sides blocks inference. A nearer point with unknown
origin cannot be skipped for an older convenient pair.

Known starts/ends inside the proposed bridge, intersecting denials and unassessed
source constraints block continuity. An unscoped assertion of the same role
category, or an unmapped role on the same person/organization, also blocks a
specific-role bridge rather than disappearing through grouping. Such assertions
remain in their own timelines. Other known roles and organizations are not treated
as mutually exclusive. Missing/invalid receipt dates do not use the cycle as a
fallback. All supplied observations survive, including those outside the selected
date and those with unknown role dates.

Day-level intervals retain inclusive endpoints. Coarse role dates must remain in
the existing precision-aware diagnostic or be supplied as an explicit unassessed
issue; this evaluator never manufactures day bounds. An earlier period ending
before the day does not become a denial of all possible later service. It also does
not invalidate a separately supported later term by itself.

The source-qualified timeline can be assessed even when no donor name corresponds.
It describes that external subject, not an accepted relationship for the FEC donor.
All identity, graph-publication and financial flags remain false. There is no CLI,
new dependency, fetch, model call, scheduled asset or graph writer in this change.
Origins, specific-role interpretation and cross-source identity still need source
integration; an arbitrary caller-provided origin cannot certify independence.

### Tests and retained-data result

Synthetic tests cover positive/negative source support, conflicting assertions,
nearest points, unknown/reused origins, role changes, term breaks, unscoped roles,
namesakes, legal-suffix correspondence, multiple organizations, missing/coarse/
invalid dates, deterministic order-independent replay and source conservation.
They also enforce separation of employee, founder, owner, executive and board roles.

The existing dated-source test now invokes the new evaluator on the same verified
FEC appearances and **reviewed** corporate claims. Both Chambers occurrences retain
a name/employer candidate; Duffield's name variant remains unbridged. All retain
only earlier role points and produce no continuity hypothesis, even with the option
enabled. No capture day, fabricated alias, origin or accepted identity fills a gap.

```sh
go test ./internal/calculation/personaffiliation ./internal/audit/personaffiliation
```

Focused synthetic and retained-source tests, the full Go suite, `go vet ./...` and
race tests for both person-affiliation packages pass in the existing memory-capped,
network-disabled Go container. Formatting, documentation links and original fixture
hashes also pass. This is evidence evaluation, not completed person resolution.
## Concrete identity-rule trial (rejected)

The removed `person-identity-corroboration-trial.v1` required exact structured
name components, literal employer/occupation agreement, independent personal
city/state evidence and explicit receipt-day coverage. It also blocked a proposal
whenever any supplied rival remained, even when source evidence distinguished the
rival. Synthetic tests only proved that code implemented those requirements.

The real-source feasibility audit found no passing case. More importantly, it
found strong candidates that failed for the wrong reasons. SEC reporting addresses
often describe a company or filing context rather than a person's residence;
official sources use semantically equivalent titles instead of FEC text; and
identity remains stable when a role observation does not cover the transaction
day. The blanket-rival rule contradicted this document's narrower requirement that
a rival satisfying the **same distinguishing evidence** blocks a unique decision.

The experimental evaluator, its synthetic tests and the trial-only city/state
audit projection were removed. No retained source bytes or accepted evaluator
behavior changed. The rejected conditions remain documented in the
[audit](../audit/person-identity-rule-feasibility-2026-09-16.md) so a replacement
cannot quietly reintroduce them. Production identity, graph and money approval
remain false.
