# Reported receipt roles at committee boundaries

Status: implemented in Go. Full regression, targeted race and static checks pass.
The [retained 2024 gate](../audit/terminal-receipt-roles-2026-09-13.md) passes the
complete corpus and byte-identical fresh-process replay with eight/four readers.

This joins reported contributor-role and committee-identity evidence to the
[terminal-boundary assessment](./terminal-source-assessment.md). It does not
choose a terminal definition, resolve people/corporations or allocate dollars.

## Scope and grain

`profile-terminal-receipt-roles` reopens the exact generation and recomputes the
unchanged boundary assessment. It profiles every committee in the selected A/B
endpoint union, not only nodes matching a proposed terminal rule. Each profile
joins to both topology results by exact committee ID. The receipt evidence is
Schedule A in both cases; the shared profile must not be added twice.

The complete [participant index](./receipt-participant-index.md) is the access
source. Its role, inventory, membership, conflict and memo decisions come
unchanged from the [source-role policy](./receipt-source-evidence.md). Every row
belongs to one of three conserving populations: recipient in the scoped committee
union, syntactically valid recipient outside that union, or unresolved recipient.

All appearances remain distinct. Duplicate-looking occurrences, amendments,
memo rows, negative amounts, zeroes and unknown amounts are not removed. Counts
describe occurrences, not unique donors, effective payments or money. Original
index and full source facts remain untouched and available for future queries;
this derived profile does not replace their grain.

Within each recipient, joint role groups retain source route, inventory component,
individual/committee membership decisions, receipt role, reported entity label,
memo flag, entity conflict, individual overlap and source-ID master-presence state.
Null and empty entity labels remain distinct. Groups count amount signs without
summing money and record the earliest source ordinal for drilldown.

Earmark, structured-conduit and reported-reference distributions are separate
marginals. They conserve each recipient's population but cannot answer joint
predicates involving role groups; those require the original rows. Structured
annotations are not the separate qualified conduit join or additional money.

A committee with no matching occurrence gets an explicit zero profile: no row in
this exact participant publication, not no funding, complete date coverage or a
terminal source. These zeros do not fill financial bases or summary gaps.

## Identity evidence without entity resolution

The accepted `reported_source_committee_id` joins by exact ID to **all** committee
masters in the pinned receipt reference publication, not only selected flow-graph
endpoints. Outcomes distinguish no routed committee assertion, an exact same-cycle
master fact, and an ID absent from that exact master publication.

Raw contributor IDs do not override source routing. An entity label, unresolved
individual route or name never becomes a committee identity through this join.
Missing same-cycle evidence is not a historical-registration check and does not
establish that an identifier is invalid.

The output catalogs each routed source ID encountered at scoped recipients, its
master fact ID when present, occurrence count and earliest ordinal. The catalog
pins its master input separately from the boundary graph's reference provenance;
the generation's reference-content proof remains the bridge between repackaging.

Full reported committee-master documents from the already verified flow model
are attached for every scoped committee, including missing-master documents.
Organization-related attributes remain assertions, not resolved corporate-family
relationships or proof of corporate payment. No edges are added from that text.

Names, addresses, employer and occupation text remain in complete source facts.
Selected witnesses expose those fields, but this command does **not** index or
resolve that text corpus-wide. Person/corporation resolution and terminal
eligibility remain false.

## Streaming and verification

`receiptparticipants.Inspector.Scan` supplies a complete-publication parallel
reader. One to eight workers own independent shard streams and local profile maps.
Borrowed rows are consumed synchronously; retained strings are owned copies.
There is no shared per-occurrence mutex or serial row dispatcher.

The existing reader verifies file bytes, schema, ordinal range, canonical values
and shard census. The scanner verifies the merged publication census before
accepting the profiles. Failure or cancellation returns no accepted partial result.
The profile pass needs no raw download or 99-column source decode. Generation
opening still verifies its retained backing; witnesses read complete source rows.

A shared one-million worker-map-entry cap bounds recipient, role-group, annotation
and source-ID state. Exceeding it fails without truncation. This is a resource
guard, not a donor threshold. Worker-local duplication affects resource use, not
accepted output. The retained runner uses a 4 GiB container cap, 2 GiB Go limit
and eight CPUs. No standing service budget changes.

The command selects the earliest occurrence for each observed source-route and
source-ID-state category, plus conflict/overlap cases when present. Shared
occurrences are inspected once. Each selected row must reproduce its profile key
and recipient and match its stored receipt edge and full source occurrence.
This does not repeat every receipt edge's full-field publication readback.

Completion boundaries are rechecked before return. The enclosing result pins the
generation/checksum, actual executable, boundary assessment, participant/master
inputs, census, profiles, ID catalog and witnesses. Worker count and timings are
not semantic identity fields. Fresh replay with different workers must reproduce
the expected profile ID and every byte. Immutable publications remain required;
there is no cross-database transaction against privileged concurrent mutation.

## Command and next work

Use the [generation read flags](./funding-neighborhoods.md) with:

```text
legal-tender pipeline fec profile-terminal-receipt-roles [exact generation flags]
  --workers 8 [--expected-profile-id SHA256]
```

Workers default to four, accepting one through eight. No candidate, committee,
cycle, row-limit or terminal-policy override exists. Output is an audit artifact,
not a proposed interactive payload. No graph import, pointer, Dagster activation,
source fetch or financial calculation changes.

The [reported identity view](./reported-identity-assertions.md) now exposes the
source-grain employer/occupation and organization fields separately from this
role profiler. Person/organization resolution remains unimplemented. Keep
ambiguity and time scope explicit. Complete the user-requested
[interpretation review](./pre-attribution-review.md) before choosing terminal
rules or allocation; connectivity alone cannot establish ultimate origins.
