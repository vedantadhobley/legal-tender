# Reported identity assertions

Status: implemented. Fixture, regression, race and static checks pass; the
[retained-data gate](../audit/reported-identity-assertions-2026-09-13.md) passes
the complete 2024 scan, exact replay and sampled independent reader checks.
This is a source-backed typed view, not
resolved identity or a new graph publication. The
[pre-attribution review](./pre-attribution-review.md) remains required.

## Grain and meaning

Each Schedule A row exposes the exact reported recipient, contributor IDs,
entity label, full/structured name, address, employer, occupation and receipt
date. Its key is the immutable fact-set ID plus source ordinal, the same key
used to derive a reported contributor appearance. All published occurrences
remain: no amount threshold, memo/amendment filter, name deduplication or
employer stoplist. Null, empty, whitespace and unusual text remain distinct.

Every pinned committee-master fact exposes its reported committee ID/name,
organization-type code and connected-organization text, with its exact fact
and occurrence IDs. Blank strings remain evidence. This is one-to-one with
published **facts**, not all classic source occurrences: the existing classic
normalizer excludes invalid/duplicate occurrences and preserves them upstream.

These are assertions made in disclosures, not verified employment or ownership.
A receipt date is not employment start/end or an organization's validity period.
Employer text does not turn a person's contribution into corporate money. Search
vectors in `contributor_*_text` are not substituted for reported natural text.
The view performs no person/corporation resolution and creates no financial,
employment, ownership or terminal-source edges.

## Storage and verification

The full facts already support named-column reads. The view reuses them instead
of copying hundreds of millions of text rows. Its manifest pins both source
fact sets, their complete input manifests, source release ancestry, field
mapping, producer executable and ordered value digests. Each source remains an
independent assertion population; sharing a cycle does not establish a shared
release or reconcile the two sources.

The fixed receipt mapping contains 18 raw text fields plus ordinal, cycle and
normalization-state metadata. Committee assertions contain four raw fields plus
fact/occurrence locators. Per-field counts follow the explicit ordered column
lists in the manifest. Canonical value hashing includes null tags and length
framing, so null/empty, structured-name fields and duplicate-looking occurrences
cannot collapse into the same sequence. No case folding or date inference occurs.

Full publication requires exact input backing, complete Schedule A schema,
dense ordinals and cycle agreement, all committee fact envelopes/raw fields,
and complete conservation. Per-field counts distinguish null, empty and
nonempty values without interpreting them. Shard-local readers bound memory
and preserve source order within shards; worker scheduling cannot change the
logical result. Callback values are borrowed, and consumers must discard their
work if a scan fails before verified completion.

Go defaults to four independent shard workers, accepting one through eight,
with 2,048-row reader batches. A receipt row exceeding 1 MiB of selected text,
or a CM artifact exceeding one million facts/512 MiB decoded bytes, fails
without truncation. These are resource guards, not donor thresholds. The
one-shot gate uses a 4 GiB container cap and 2 GiB Go memory limit. Progress
and operational timing are separate from deterministic result identity.

The view is not a standalone backup or a replacement for the source facts.
Retention must keep every referenced source artifact. No Arango mutation,
source download, pointer replacement or Dagster activation is part of this step.

## Acceptance

Fixture tests must prove exact field mapping, null/empty/text preservation,
duplicate-looking appearance separation, changed backing/schema rejection,
failure/cancellation, and worker-varied replay. A complete retained-data scan
and fresh-process replay are required before claiming live-cycle acceptance.
Neither a sample nor implementation alone establishes that gate.

## Command and consumers

```text
legal-tender pipeline fec build-reported-identity-assertions
  --storage-root /storage
  --schedule-a-facts <exact immutable manifest>
  --committee-facts <exact immutable manifest>
  --cycle <cycle> --workers 4
  [--expected-view-id <sha256>]
```

The command emits its result only after the complete scan succeeds. A retained
runner writes pending output and promotes it on success; the command does not
advance a current pointer. Fresh replay with the expected view ID must produce
identical JSON despite worker changes. There is no partial/sampled success mode.

`identityassertions.Scan` also accepts optional Go row callbacks. They receive
all assertions directly from source facts; they must not mutate borrowed fields
and must discard downstream work if final verification fails. `Decode` checks
the pinned manifest identity and invariants, not source backing; a full `Scan`
replay with that expected ID checks backing and all projected values.

This is a verified view, **not** a name lookup index: name searches still need
source scans until a measured workload justifies a separate access structure.
An appearance remains keyed by exact fact-set ID and ordinal, compatible with
the existing participant graph; this step does not publish the view into that
graph or attach resolved corporate/person entities.
