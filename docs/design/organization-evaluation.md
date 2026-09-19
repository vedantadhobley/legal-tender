# Organization-matching evaluation and corroboration

Implemented 2026-09-15: a reusable, offline Go evaluator over exact captures and
reviewed test assertions. The [candidate resolver](./organization-resolution.md)
now offers a frozen v1 baseline and opt-in v2 name proposals. No organization
identities, employment assertions or financial edges are published by this work.

## What is implemented

`pipeline entities evaluate-organizations` reads a pinned capture and a separately
pinned corpus. It runs the selected proposal policy without the labels, then
compares its results with the reviewed assertions. The implementation lives in
`internal/audit/organizationresolution`; it is not a production lookup table.

The [retained corpus](../../tests/fixtures/organization-resolution/corpus-v1.json)
contains source-backed FEC queries, reviewed positive correspondences and explicit
counterexamples. It is a diagnostic set, not a representative or held-out sample.
The primary-source snapshots and complete Wikimedia response bodies are retained
under [test fixtures](../../tests/fixtures/organization-resolution/README.md), not
only in temporary storage.

Each annotation binds an exact query ordinal/text, QID, relation, rationale and
primary-source references. The corpus binds the capture digest. The evaluator
checks strict JSON, exact source-body hashes/sizes, excerpt presence, bounded
regular files, confined paths, dates and duplicate/conflicting annotations.
Obvious Wikimedia self-corroboration is rejected. Those checks verify artifact
integrity and annotation structure; they do not prove the reviewer's interpretation
or independently authenticate a publisher.

The current `same_entity` annotations mean reviewed organizational referents for
candidate matching. They are not unique legal-registration, transaction-time
employment or ownership proofs. The annotations were source-reviewed during
development; they are not automatically generated labels or user-approved identity
decisions. Future revisions must retain their own corpus identity.

## Separate outcomes and denominators

The evaluator distinguishes:

- A reviewed positive proposed by the resolver.
- A reviewed positive found in the search window but not proposed.
- A known positive not discovered in that window.
- A discovered positive whose entity metadata is missing.
- A proposal contradicted by an explicit negative annotation.
- A proposed QID that has not been reviewed.
- An unusable source observation, including failed or unattempted requests.

An unlabeled alternative to a positive is **not automatically negative**. QID
duplicates, related organizations and broader entity scopes require their own
review. Unreviewed observed QIDs remain explicit in every case. Counts distinguish
queries from query/QID pairs, and label outcomes from proposal outcomes. Repeated
QIDs across different queries are separate pairs, not separate organizations.

The evaluator reports exact counts, not an aggregate accuracy score. In particular,
zero incorrect proposals with zero proposals does not establish high precision.
Source failures are not retrieval misses. The fixture proves conservation of every
outcome, including missing entities and unreviewed proposals.

## First retained result

The Go builder selected the first twenty distinct nonblank connected-organization
strings from the pinned 2024 committee facts. Five lookups completed; Wikipedia
then returned HTTP 429 with `Retry-After: 35`. The fetcher stopped as designed.
The failed query and fourteen unattempted queries remain explicit in the fixture.
We did not bypass throttling, reinterpret failures as absence, or silently replace
the capture with a retry.

There are 25 observed query/QID pairs. Four are reviewed: two positive
correspondences and two counterexamples, across two queries. The other 21 pairs
remain unreviewed. Eighteen queries have no review labels, including all fifteen
unusable observations.

Both reviewed positives were retrieved but not proposed under
`organization-name-proposals.v1`. One separates `1199` and `SEIU` where the
official organization name joins them; the other includes `Corporation` where
the captured item label is shorter. The review uses the organization's
[own history page](https://www.1199seiu.org/history) and the company's
[2024 announcement](https://www.1stsource.com/news/1st-source-corporation-announces-the-promotion-of-andrea-short-to-president/),
plus the captured item metadata. This does not assert legal-entity equivalence
from names or a shared website alone.

The two counterexamples—a disambiguation page and a software concept—were not
proposed. Related umbrella/historical union items remain unreviewed; search
co-occurrence does not establish sameness or a relationship.

The result identifies a matching limitation after successful retrieval. It does
not establish how frequent that limitation is across FEC employer names or the
whole connected-organization population.

## V2 replay on the same evidence

`--proposal-policy organization-name-proposals.v2` evaluates the broader
[name rules](./organization-resolution.md#versioned-proposal-rules-not-identity-authority)
without editing the capture, corpus labels or primary-source snapshots. V1 remains
the default and retains its original results. Each v2 case includes the baseline
state/proposed QID and all matching candidates with source-qualified reasons,
including competing matches when the resolver abstains.

| Outcome on the retained diagnostic corpus | V1 | V2 |
|---|---:|---:|
| Source-usable queries | 5 | 5 |
| Source-unusable queries | 15 | 15 |
| Reviewed positive proposals | 0 | 2 |
| Reviewed positives retrieved but not proposed | 2 | 0 |
| Annotated negative proposals | 0 | 0 |
| Unreviewed proposals | 0 | 0 |
| Abstentions among source-usable queries | 5 | 3 |
| Unreviewed observed query/QID pairs | 21 | 21 |

The joined-letter/number case now matches through the boundary rule. The omitted
`Corporation` case matches through the legal-suffix rule. No proper names, QIDs,
query positions or reviewed answers are production exceptions. The two remaining
counterexamples are not proposed, and failures/unreviewed alternatives stay
unchanged. This is a regression result on the examples that motivated the change,
not held-out validation, broad accuracy or identity acceptance.

Synthetic tests cover unrelated near names, semantic words, differing legal
designators, empty/numeric stems, accents, spelling, reordered tokens, source
failures and missing metadata. An exact v1 winner plus a broader v2 rival becomes
ambiguous. Duplicate aliases cannot manufacture corroboration. Reordered aliases
and pages preserve candidate explanations; original source evidence retains its
order. Both policies are tested independently of the review labels.

## Corroboration requirements before identity publication

These are review requirements for the next implementation, **not an implemented
automatic acceptance policy**:

| Evidence | Permitted use | Insufficient conclusion |
|---|---|---|
| Name, alias or spelling variant | Generate and explain a candidate | Canonical identity or verified employment |
| Wikipedia page and linked Wikidata item | Locate contextual evidence | Two independent confirmations |
| Exact authoritative identifier, with namespace and entity scope | Strong identity evidence when independently anchored on both sides | A name lookup that merely returned an identifier |
| Explicit first-party name/alias assertion | Corroborate the named organization in its stated context | Subsidiary/parent, brand/legal entity or historical successor equality |
| Website, location or industry | Context and conflict detection | Uniqueness; several group entities can share these |
| Dated employment, ownership or parent statement | A separate directed, time-qualified relationship assertion | Redirect all associated contribution dollars to that organization |

Further constraints:

- Keep reported input, candidate discovery, corroboration and any accepted
  identity assertion separate and independently versioned.
- Retain competing identities and contradictory identifiers. Do not force a
  winner because a search returns one leading result.
- Check source independence. Repeated copies of one assertion are not separate
  confirmations; calling an evidence source `first_party` is a review assertion,
  not something this evaluator verifies automatically.
- Distinguish corporations, subsidiaries, brands, unions, associations and
  committees. Corporate-family relationships are not identity merges. The
  [2024 company source](https://www.1stsource.com/news/1st-source-corporation-announces-the-promotion-of-andrea-short-to-president/)
  explicitly distinguishes the corporation from its bank subsidiary.
- A donor's reported address must not be treated as the employer's address.
  A committee's mailing address must not silently become its connected
  organization's headquarters.
- Observation date is not relationship validity. Later evidence does not by
  itself prove employment or ownership at an earlier transaction date.
- Benchmark names/QIDs and reviewed answers must never become production
  exceptions or a hidden identity dictionary. Tests verify that changing a
  label does not change the resolver's proposal.

## Run and verify

```sh
legal-tender pipeline entities evaluate-organizations \
  --capture tests/fixtures/organization-resolution/capture-v1 \
  --expected-capture-sha256 6ef200cd16c436544a350c2940e70f677aa35b60ce3da9aacbd522de6e30ade8 \
  --corpus tests/fixtures/organization-resolution/corpus-v1.json \
  --expected-corpus-sha256 6475f3beaefc2ab64f4ce72cfb5b4135e6a7084ffd2c0ba7c1ce6cf494415792
```

Run the same command with `--proposal-policy organization-name-proposals.v2`
to evaluate the broader proposer. Omit that flag for the unchanged baseline.

Exit 0 means the diagnostic completed, not that matching passed a quality gate.
Malformed, corrupted or unbound evidence returns an error. The output always has
`identity_publication_approved: false`. The command never fetches sources, creates
edges or changes existing financial selections.

Tests cover the real retained capture, every outcome/denominator, evidence
corruption, quote mismatch, source substitution, path escape, symlinks, future
observation dates, duplicate/conflicting labels, wrong query text, self-corroboration
and label/resolver separation. The existing Go release-source allowlist includes
these test fixtures, so offline builds retain the corpus. Fresh CLI replay is
byte-identical for the same executable and inputs.
The v2 CLI also passes fresh-process byte comparison; v1 matches the prior
development executable's diagnostic output apart from the executable digest.
The complete Go suite, static analysis and targeted audit/resolver/source/CLI race
tests pass. No Python data-plane code, model dependency or graph import was added.

The [registry corroboration slice](./organization-corroboration.md) now adds a
retained GLEIF observation and synthetic parent/subsidiary, historical-name,
qualified/deprecated-claim and competing-identifier tests. The reviewed annotation
corpus remains unchanged; those tests are not new real identity labels.
Next: address missing identifiers using reviewed independent records and broaden
the real corpus. Identity publication still needs an acceptance policy; no graph
rollout follows from either diagnostic alone.
