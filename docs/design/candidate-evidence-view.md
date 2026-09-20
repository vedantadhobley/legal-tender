# Integrated candidate evidence view

Status: implemented as `build-candidate-evidence`, a read-only Go command with
JSON output and an optional readable report. The compact `build-candidate-dossier`
presentation now joins that pinned report to accepted independent-expenditure
interpretations. The milestone is reproducible candidate evidence, not terminal
classification or pooled-dollar allocation.
Those remain independent,
versioned policies. This view builds on the [upstream trace](./candidate-upstream.md),
[receipt inventory](./committee-funding-basis.md), and
[summary review](./summary-receipt-compatibility.md).

This completed consumer does not publish donor, employer or corporate graph
connections. The next core milestone is the
[connected funding-graph completion plan](./connected-funding-graph.md), not a GUI.
Complete selected committee ancestry is not complete original-product coverage.

## Contract

- Select an exact candidate, source cycle, observation bundle, linkage manifest,
  and receipt inventory. Verify their backing and reject incompatible receipt
  identities. Never resolve an input through a changing latest pointer.
- Retain the unchanged upstream trace, including its exclusive candidate-boundary
  accounting, exact edge membership, authorization evidence, and cyclic groups.
- Join receipt populations to reached committees and candidate-linked committees
  with authorized or unresolved status. Keep individual/committee overlap, memo,
  unknown amount, signed adjustments, and receipt roles visible. Do not sum the
  network's receipts into candidate money.
- Include one source-backed shortest-hop witness per reached non-root committee.
  These are connectivity witnesses, not chronological or dollar-allocation paths.
  Full graph membership remains in the trace and pinned shared observations;
  do not copy every upstream transaction into every candidate result.
- Optionally attach reviewed summary assertions for candidate-linked committees.
  Verify/group the summary once per run. Different source releases, source blanks,
  conflicting assertions, and unverified reporting periods remain explicit.
  A summary neither fills receipt detail nor blocks the observation view.
- Emit a machine-readable result and a readable Markdown presentation of that
  same result. The presentation must distinguish observations from funding totals,
  and coverage absence from an explicit reported zero.

## Reproduction and boundaries

The result identity binds all embedded evidence, the inventory calculation,
source manifests, policies, candidate/cycle parameters, and executable SHA-256.
Execution timestamps and local filesystem paths do not enter the result identity.
The same executable and retained inputs must reproduce identical result bytes.
Retain the executable or reproducible build inputs alongside source snapshots;
a hash alone cannot restore deleted evidence.

Terminal policy, allocation policy, and terminal amounts are null at this layer.
The nested historical trace's allocated zero means that method made no allocation;
it is not a claim that terminal donors gave zero. Missing incoming observations
and missing masters never establish terminal status. Detail inventory totals
include memo and other reporting populations; they are not cash balances, unique
payments, lower bounds, or complete candidate receipts.

This is a read-only Go consumer of published inputs. It requires neither a new
bulk scan nor a live Arango/Dagster service. It uses the graph's exact observation
bundle; it does not publish another graph or activate weekly automation.

## Run and inspect

```bash
legal-tender pipeline fec build-candidate-evidence \
  --storage-root /storage --basis-result <exact-inventory-json> \
  --observation-bundle <exact-bundle> --linkage-facts <exact-manifest> \
  --committee-summary-facts <optional-exact-summary-manifest> \
  --cycle <cycle> --candidate <candidate-id> --report <new-markdown-path>
```

JSON goes to stdout; progress and failures go to stderr. `--report` never
overwrites an existing file. Omit `--committee-summary-facts` when summary context
is not wanted; no fallback source is fetched. An explicitly supplied corrupt or
wrong-cycle summary fails the command. A valid summary with comparison blockers
stays visible without promoting financial use or failing the receipt view.

The JSON embeds the unchanged `committee_trace`, sorted `connection_witnesses`,
and `committees` joined by exact ID. Candidate authorization and reachability
remain independent: an unresolved candidate-linked committee can have receipt
evidence without being reached from authorized roots. The existing
`inspect-funding-receipts` command pages full rows for any selected committee;
the inventory calculation and receipt fact identities in the view pin that lookup.

Summary context is limited to authorized and unresolved candidate-linked
committees in this version. Upstream committee receipt populations remain available
even without summary context. No inferred person/corporation edges are added.

## Named report and concrete paths (v2)

Add `--view-version v2` to the same command. Optionally provide
`--candidate-master-facts <exact-manifest>` for a candidate name. The default
remains v1, and candidate-name inputs require v2. The v2 JSON nests the unchanged
v1 evidence under `evidence`; it adds name assertions, exact reference identities,
and path examples. Its report hash binds the complete envelope. This is a
presentation version, not a new receipt or identity-resolution policy.

Committee names come from the trace's exact committee-master fact set, verified
against each reached node's master fact ID. Candidate names are optional pinned
same-cycle display context; a different source release does not change candidate
authorization or amounts. Each name retains the raw string and fact/occurrence
IDs. Missing, blank and conflicting names remain explicit; no fuzzy or online
fallback is used. Markdown escapes source labels without changing the JSON.

The report shows one complete witness chain at each of the first three available
positive hop distances, choosing the first committee ID at each distance. This
is deterministic sampling, not donor ranking. Each hop retains its source ordinal,
SUB_ID, endpoints, reported date, role and signed amount. Known dates that decrease,
missing dates, same-day observations, and nonpositive amounts receive separate
warnings. No path amount is calculated: connectivity does not establish cash
availability or that the same dollars traversed the chain.

The readable view separates memo evidence, formats money without floating point,
and explains existing role and coverage states. In particular, an `unresolved`
committee-flow role does not mean an unknown amount or unresolved donor; ordinary
individual receipts can fall outside that role map. The underlying source and
calculation codes remain unchanged in JSON.

The [named-report gate](../audit/candidate-report-2026-09-11.md) verifies the
same two retained inputs, exact name-source membership, unchanged core evidence,
complete example paths and byte-identical JSON/Markdown replay.

## Source-row drilldown

Use the v2 report's ID and a source ordinal from a concrete connection witness
or candidate-boundary observation:

```bash
legal-tender pipeline fec inspect-candidate-connection \
  --storage-root /storage --candidate-report <retained-v2-json> \
  --expected-report-id <report-id> --source-row-ordinal <ordinal> \
  --report <new-drilldown-markdown-path>
```

The command checks the parent and nested evidence content identities. It requires
the separately supplied expected report ID; it never follows a current pointer
or silently uses another report. Inputs must be regular files no larger than
128 MiB. Unknown JSON fields, duplicate keys and malformed encodings fail.

The existing source reader verifies the exact published calculation, source
ancestry and selected membership. It compares every observation field, hashes
the requested shard and seeks the full row through that same open file. The result
preserves all 99 physical fields, including nulls, empty strings, filing references,
raw source values and typed metadata. No new source fetch or receipt calculation
runs. Startup still verifies complete A/B backing; only selected rows are decoded.
A long-lived reader can reuse that startup verification, but this CLI opens a new
reader per invocation.

Verification is scoped: the source observation is independently verified; the
parent is content-checked, not fully recalculated. Parent names are inherited
display assertions. This lookup does not independently resolve authorization,
amendments, memo meaning, donor identity or cash availability. Its connection ID
binds the lookup executable, parent document digest, source ancestry and returned
row. The [drilldown gate](../audit/candidate-connection-2026-09-11.md) records exact
physical-row readback, preserved parents and byte-identical replay.

## Compact candidate dossier (v1)

```bash
legal-tender pipeline fec build-candidate-dossier \
  --storage-root /storage --candidate-report <retained-v2-json> \
  --expected-report-id <report-id> \
  --candidate-interpretations <exact-manifest-or-verified-current-pointer>
```

The dossier verifies the exact parent report and complete candidate-interpretation
artifact. It keeps candidate-linked receipt populations, representative committee
paths and source-backed names while omitting the parent's thousands of upstream
committee populations. It adds four separate Schedule E views: reported endpoint,
safe default, inferred alternative and conflicting alternative. Each relevant
interpretation is stored once with all applicable roles. The view subtotals overlap
and are explicitly non-additive; support and opposition remain separate.

The two evidence domains must share a cycle. They need not claim the same source
release. The dossier records the exact Schedule A, committee-flow and Schedule E
release IDs and reports whether receipt and independent-spending snapshots match.
It does not silently align or replace either source. Terminal policy, allocation,
person/corporation identity, lobbying and legislative evidence remain unset. The
[real 2024 dossier gate](../audit/candidate-dossier-2026-09-20.md) passes for both
retained candidates.

## Acceptance

Verify joined populations against the complete saved inventory, unchanged trace
identity, every witness's source/endpoint/hop continuity, candidate-boundary
conservation, explicit summary blockers, and exact replay. Fixtures cover empty
populations, unknown/negative amounts, overlap, ambiguous authorization, cyclic
connectivity, changed input identities, and cancellation. A real candidate run
must produce an inspectable result, not only another validation report.

The [real 2024 results](../audit/candidate-evidence-2026-09-11.md) pass this gate
for two previously data-selected candidates. This is not an all-cycle publication
or a complete financial attribution result.
