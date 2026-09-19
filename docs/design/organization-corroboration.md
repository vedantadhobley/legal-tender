# Organization registry corroboration

Implemented bounded Go slice, 2026-09-15. This adds exact-identifier GLEIF capture
and offline corroboration assessment to the [name proposer](./organization-resolution.md).
It does not approve FEC identity connections, graph edges, employment, ownership
or terminal-dollar attribution. It does not use the
[evaluation labels](./organization-evaluation.md) as runtime inputs.

## Source and scope

GLEIF publishes legal-entity reference data through its
[API](https://www.gleif.org/en/lei-data/gleif-api), based on its Golden Copy data.
The selected endpoint is `GET /api/v1/lei-records/{lei}`. The
[draft source contract](../../contracts/sources/gleif/lei-record/v1/contract.json)
pins the interpreted response boundaries and accepted opaque source fields.
Its status remains draft pending broader source acceptance. A full local registry
index, name-search acquisition and recurring refresh are not implemented here.
The separate [registry-name discovery slice](./organization-registry-discovery.md)
now supplies bounded name search without a preexisting LEI. It does not alter this
exact-identifier contract or turn registry candidates into accepted identities.

The request selector reads [Wikidata P1278](https://www.wikidata.org/wiki/Property:P1278)
statements from every successfully retrieved query/QID pair in a pinned Wikimedia
capture, not just name winners. It preserves statement ordinals, IDs and raw JSON,
including ranks, qualifiers and references. Unsupported property-level shapes use
an explicit issue with index `-1`; missing properties produce no claims.

The selector validates LEI syntax/check digits, deduplicates only network requests
and sorts them. Valid deprecated or qualified identifiers may be fetched as
evidence; that does not make them eligible for corroboration. Invalid identifiers
remain issues and are never repaired. The saved request set binds the Wikimedia
capture and `organization-registry-requests.v1`. Offline assessment reconstructs
that exact set and rejects missing, extra or substituted registry inputs.

No names, addresses or amounts are sent to GLEIF. No LEI claim means no request,
not that the organization lacks a registry identity. There is no name-search,
parent lookup or alternate-source fallback. Wikipedia and Wikidata remain linked
community evidence, not two independent confirmations.

## Capture and parsing

The fetcher allows at most twenty direct requests, one second apart, with a
30-second request timeout, five-minute run deadline, 2 MiB response cap and
40 MiB aggregate cap. It uses a fixed HTTPS origin, no inherited proxy, no
redirects and no credentials. The first HTTP, transport, read or schema failure
stops later requests. Failed bodies and unattempted slots remain explicit;
HTTP 404 is not converted to a missing legal entity.

New directories only. Each response retains its URL, observation time, status,
selected nonsecret headers, byte length and SHA-256. A final manifest is required
for replay; an interrupted capture without it is not a completed snapshot.
There is no latest pointer or source-release mutation.

The reader verifies exact manifest/body hashes, file confinement, regular files,
request/response conservation, stop behavior, strict JSON and interpreted shapes.
Requested LEI, response `data.id`, `attributes.lei` and the source self-link must
agree. Each observation records a source-shape fingerprint. Unknown keys at an
interpreted boundary retain their bytes but block usable parsing.

The parsed view keeps legal name, alternative names and transliterations separate,
plus entity/registration status, publisher update/renewal/snapshot dates and the
complete original response. Nullable renewal dates remain null. Known nested
addresses, events, mappings and relationship payloads are explicitly opaque in
this source version: they are retained, not silently normalized or followed.
An opaque value's presence does not approve its semantics for later calculations.

## Assessment rule

`organization-registry-corroboration.v1` retains the exact v2 name proposals and
their v1 baselines. It compares each valid claimed LEI against its saved registry
observation and exposes every candidate, claim and comparison.

| Evidence condition | Result |
|---|---|
| No P1278 claim | `no_lei_claim`; no fabricated identifier |
| Invalid, unsupported, deprecated or qualified claim | Explicit claim state; not eligible for name/identifier corroboration |
| Valid identifier with failed registry observation | Source issue retained; no record asserted |
| Exact LEI record observed | `lei_record_observed_fec_identity_unresolved` |
| One distinct unqualified, nondeprecated LEI; no unsupported/qualified competing claims; v2 name candidate; exact normalized registry legal-name agreement with both reported text and an item label/alias/page title | `name_and_lei_correspondence_fec_identity_unresolved` |

The final row describes a correspondence among the supplied observations, **not
an accepted identity**. Normalized exact means the original v1 case/punctuation
rule, without suffix removal, translation or spelling repair. Alternative and
previous names get separate comparison evidence; they cannot substitute for the
legal name in that rule. A historical name without validity dates cannot establish
transaction-time identity. Repeated identical statements are not additional votes.

Competing current identifiers and unresolved name candidate sets remain explicit
blockers. Non-`ACTIVE`/`ISSUED` statuses get a separate review blocker; unknown
codes remain source text. `LAPSED` is not entity absence. Even an observed
name/identifier correspondence does not establish present registry validity.
GLEIF's `corroborationLevel` describes publisher validation and is never copied
into this application's approval flags.

Every candidate retains `fec_input_has_no_independent_lei_binding` and
`transaction_time_identity_unverified`. The current FEC query inputs contain a
reported string and provenance, not an independently anchored LEI. A QID-supplied
LEI followed by a successful registry lookup cannot manufacture that missing
binding. All identity/employment/ownership/financial approval flags remain false.

## Commands

```sh
legal-tender pipeline entities capture-organization-registry \
  --capture WIKIMEDIA_CAPTURE_DIRECTORY \
  --expected-capture-sha256 WIKIMEDIA_CAPTURE_SHA256 \
  --output NEW_REGISTRY_DIRECTORY \
  --user-agent 'LegalTender/organization-research (YOUR_CONTACT_URL)'

legal-tender pipeline entities corroborate-organizations \
  --capture WIKIMEDIA_CAPTURE_DIRECTORY \
  --expected-capture-sha256 WIKIMEDIA_CAPTURE_SHA256 \
  --registry-capture REGISTRY_CAPTURE_DIRECTORY \
  --expected-registry-sha256 REGISTRY_CAPTURE_SHA256
```

The first command derives requests, captures them and returns an assessment. The
second has no network capability. Exit 0 means sources were usable, not identity
approval; failed Wikimedia or registry observations produce structured partial
results and exit 1. Corrupt or wrongly bound artifacts return an error without a
result. Missing identifiers are assessed gaps, not source failures.

## Retained result and verification

The unchanged [Wikimedia/corpus fixtures](../../tests/fixtures/organization-resolution/README.md)
contain twenty queries, five with usable sources and twenty-five retrieved
query/QID pairs. Twenty-four pairs have no LEI claim. The sole claimed LEI occurs
on an item that is not a name proposal for the reported organization.

The normal Go selector/fetcher captured that one registry record in 3,464 bytes.
The record's entity status is `ACTIVE` and its registration status is `LAPSED`;
those are separate assertions. It yields an observed LEI record, no name/LEI
correspondence and no FEC identity approval. The two previously reviewed name
proposals still have no LEI claims and stay unresolved by this method. The fifteen
failed/unattempted Wikimedia queries remain partial; registry success does not
erase them. This is a limited diagnostic result, not a coverage estimate.

The new [registry capture](../../tests/fixtures/organization-resolution/gleif-capture-v1/capture.json)
is pinned at `da1a319d7a91d87f6e0d2dcbe9ab10443f8e1beee890a0fd31ac966e922bd027`;
the original Wikimedia/corpus digests and annotations are unchanged. The retained
test reconstructs requests from source evidence and checks all candidate states.

Synthetic tests cover exact correspondence without approval, parent/subsidiary
names, previous names, qualified/deprecated/invalid claims, competing identifiers,
duplicate statements, lapsed/unknown statuses, failed HTTP/schema observations,
size budgets, cancellation, confinement, digest mismatch and source substitution.
The source fixture deliberately uses a valid-format test LEI with invented names;
it is not an assertion about the real entity. Runtime rules contain no named-entity
exceptions. The full Go suite, targeted race checks, static analysis and the
GLEIF/Wikimedia source-schema tests pass; fresh CLI replay is byte-identical.
The unrelated existing FEC report-scope metadata mismatch remains in the
[deferred queue](../todo.md); no full source-registry acceptance is claimed.

The separate [SEC issuer path](./organization-issuer-discovery.md) now implements
bulk capture/parsing and offline FEC-name-to-CIK candidates without a Wikimedia or
LEI dependency. Its live directory gate and fresh replay now pass with one CIK
candidate and no identity approval. It does not reinterpret this GLEIF capture or
fill its missing LEIs.
Next retain independently reviewed records and a broader real test population.
Do not make an LEI mandatory for
all organizations, approve identities from a shared website, or treat missing LEIs
as an exclusion from the financial graph. Graph publication, historical identity
validity and relationship resolution remain separate unfinished work.
