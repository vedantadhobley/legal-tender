# Person identity evidence v2 evaluation

**Result:** `person-identity-evidence.v2` is implemented as an evidence
classifier. It does not resolve a donor, publish a graph edge or attribute money.
The replacement fixes the rejected v1 category errors without inventing a new
confidence score or person-specific rule.

This is still a development-selected feasibility sample, not an accuracy study.
The source facts and external-source limits are inherited from the
[v1 feasibility review](./person-identity-rule-feasibility-2026-09-16.md).

## Contract

`personaffiliation.AssessIdentityEvidence` accepts one exact FEC appearance and
bounded caller-authenticated evidence. Its outputs keep these dimensions separate:

| Dimension | Rule |
|---|---|
| Name | Compare explicit prefix, first, middle, last and suffix fields. Exact first/last can support correspondence. A missing middle or suffix remains uncertainty. Conflicting supplied middle/suffix fields distinguish a rival. An unattested first-name variant remains a visible noncandidate gap. No nickname table runs. |
| Organization | Return the existing explained exact, digit/letter-boundary and legal-suffix rules plus a local `CO`/`COMPANY` legal-designator rule. Text correspondence is not legal-entity identity. |
| Role meaning | A source adapter must bind an interpretation to the exact reported occupation text. Broad and specific role correspondence, mismatch, denial and unassessed semantics remain separate. No title dictionary is embedded in the classifier. |
| Role time | Reuse the source-qualified timeline evaluator. Role coverage or absence cannot change the person-candidate result. Explicit support, denial, conflict and optional continuity remain visible. |
| Provenance | Report one origin, unknown dependence, or multiple origins with independence unassessed. Different hashes or URLs never become corroboration automatically. |
| Locality | Preserve personal, business, issuer, `c/o`, mailing and unknown contexts with a separate as-of comparison. Locality is optional and never gates identity. Nonpersonal context is not residence evidence. |
| Rivals | Only exact or optional-component-compatible name evidence at a corresponding organization enters the blocking candidate set. A conflicting supplied distinguishing component does not block. Missing discovery still limits the supplied scope. |

Every result retains the source occurrence, an explained state and immutable false
identity, graph-publication and financial-attribution flags. A
`supported_candidate_identity_unresolved` state means that the supplied name and
organization text correspond; it is deliberately not an accepted identity.

## Retained evaluation

The unchanged offline corpus now replays through v2 in addition to preserving its
frozen v1 baseline:

| Case | v2 result |
|---|---|
| Two Chambers appearances | Each has one exact name/organization candidate and one same-organization rival whose middle component is absent from the FEC appearance. The result is `supported_candidate_with_unresolved_rivals`; role semantics and unknown role time remain separate. |
| Duffield appearance | Employer text corresponds, but `David`/`Dave` has no source-attested variant assertion. The decisions preserve `first_name_variant_unassessed`; no candidate is manufactured. |
| Unsearched engineer | No external observations were supplied. The result abstains with no candidate; occupation text alone creates no employee, executive or identity assertion. |

The Catsimatidis contrast remains a web-evidence review, not an executable retained
claim. The generic contract tests cover its relevant structure: `CO`/`COMPANY`
correspondence, an exact supplied middle component, and an omitted suffix that keeps
a same-organization rival unresolved. A conflicting supplied middle component does
not block. No current web page was copied into the corpus or treated as historical
source bytes.

Synthetic tests also preserve explicit relationship contradiction, unknown source
dependence, multiple origins without inferred independence, optional typed locality,
source conservation, deterministic replay and strict input binding. They contain no
real-person exception table.

## Boundary and next decision

V2 is a better diagnostic contract, not production resolution. Its structured name
fields, occupation semantics and source-origin meanings are caller-authenticated.
The current retained corpus supplies reviewed test annotations; no automatic source
adapter emits v2 observations in a scheduled run.

Before identity publication, select and test a source bridge that can provide:

- exact structured external name fields and source-attested name variants;
- explicit organization-identity evidence beyond text correspondence;
- source-qualified role meanings and origin/dependence information;
- a recorded discovery scope and failures for each appearance.

That bridge must replay offline and preserve abstention. It is a separate decision;
this evaluation authorizes no canonical merge, affiliation edge, terminal decision,
money allocation, crawler, model call or Dagster asset.

## Verification

```sh
go test ./internal/calculation/personaffiliation ./internal/audit/personaffiliation
```

The focused packages, full Go suite, `go vet ./...` and focused race tests pass in
the pinned, memory-capped, network-disabled Go container. Formatting and diff checks
also pass.
