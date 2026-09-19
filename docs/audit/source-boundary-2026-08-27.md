# Source-boundary audit — 2026-08-27

> **Status:** Point-in-time evidence for the Go redesign. This audit describes
> the local corpus and Python implementation on 2026-08-27. It is not the
> target parser contract.

## Scope and method

This audit examined:

- The raw and derived stores under
  `~/workspace/data/legal-tender/{raw,dumps}/`.
- Downloaded FEC header files and representative 2024 archive members/rows.
- `src/assets/fec/`, `src/utils/fec_schema.py`, and
  `src/utils/arango_schema.py`.
- The unused lobbying and Congress clients and the current official APIs.
- Current official FEC, LDA.gov, Congress.gov, GLEIF, SEC, IRS, and FARA source
  catalogs where they affect target planning.

No source data or running database was modified.

## Local raw inventory

The raw store has these FEC archives for each of the 2020, 2022, 2024, and
2026 cycle directories:

| Family | Local artifacts |
|---|---|
| Candidate and committee identity | `cn.zip`, `cm.zip`, `ccl.zip` |
| Summary | `weball.zip`, `webl.zip`, `webk.zip` |
| Transactions | `indiv.zip`, `pas2.zip`, `oth.zip` |
| Sync metadata | `metadata.json` |

There is also `legislators/current.yaml` plus its sync metadata. No local raw
archive was found for processed Schedule A/B/E, LDA.gov, FARA, IRS political
organizations, GLEIF, SEC, electioneering communications, communication costs,
or operating expenditures.

The classic `indiv.zip` files dominate local storage:

| Cycle | Approximate archive size |
|---|---:|
| 2020 | 5.87 GB |
| 2022 | 5.22 GB |
| 2024 | 4.24 GB |
| 2026 | 1.75 GB |

The derived FEC dump store contains gzip JSONL exports of the same parsed
families. Its `indiv` exports range from about 1.39 GB to 4.41 GB compressed.
These exports are derived legacy state, not raw evidence for the redesign.

### Refresh state is not one coherent snapshot

File modification times show mixed source refreshes. Some 2020 and 2024
transaction archives date from May 6, while most 2022/2026 transactions and
master/summary artifacts date from June 21. The legislator snapshot dates from
June 28.

Per-file recency is useful, but a directory does not identify one atomic input
snapshot. Reproducing an answer requires a manifest of exact artifact digests,
not the cycle-directory name or modification time.

## Header audit

Local downloaded headers contain these ordered field counts:

| Header | Fields | Local SHA-256 prefix |
|---|---:|---|
| `cn.csv` | 15 | `86fe00a87b53` |
| `cm.csv` | 15 | `9b5bfd7764a5` |
| `ccl.csv` | 7 | `9de902716f8c` |
| `indiv.csv` | 21 | `944a1f765c8b` |
| `oth.csv` | 21 | `944a1f765c8b` |
| `pas2.csv` | 22 | `9ce58b057663` |
| `oppexp.csv` | 25 | `7480956254f3` |
| `weball.csv` | 30 | `81d1ed7c74f6` |
| `webl.csv` | 30 | `81d1ed7c74f6` |
| `webk.csv` | 27 | `fcfc1e05fc2e` |

Fresh copies of the current official FEC `cn`, `cm`, `ccl`, `indiv`, `oth`,
and `pas2` header files matched the corresponding local files byte-for-byte.
The legacy header research was therefore materially useful and those six
files are not currently stale.

The summary headers need stronger provenance. `weball` and `webl` legitimately
share the observed 30-field shape, but the FEC bulk catalog describes them as
different artifacts with different population semantics:

- `weball` is the all-candidate summary for candidates with financial activity
  during the period.
- `webl` is one summary record per current House/Senate campaign.

Their shared columns do not make them interchangeable. Neither local header
contains separate itemized and unitemized individual fields; it contains
`TTL_INDIV_CONTRIB`. Target documentation must name exact publisher artifacts
and fields instead of referring to a generic “candidate summary.”

Representative first rows of each 2024 small/medium archive matched the local
header field count. This is only a shape sample, not full-corpus validation.

## Legacy parser findings

### Silent shape repair

`FECSchema.parse_line` splits stripped text on `|`. When a row is short, it
pads empty strings. It then constructs `dict(zip(fields, values))`, which
silently discards every extra value when a row is long.

Consequences:

- An outdated header can appear to parse successfully.
- A missing field can shift meaning without a hard failure.
- New publisher columns disappear without evidence.
- A present empty source value and a physically absent value become
  indistinguishable.

This is the highest-priority parser defect for the rewrite.

### Decoding and record selection

Every reviewed FEC asset decodes with `errors="ignore"`. Invalid bytes vanish
before record hashing or issue reporting. The assets collect `.txt` member
names and select the first match. The source contract does not identify the
expected member or define multiple-member behavior.

The assets also strip decoded rows. That makes whitespace and record-boundary
evidence unavailable to a later audit.

### Destructive identity and value conversion

The transaction assets derive Arango keys and write with
`on_duplicate="replace"`. Rows sharing the chosen key do not remain distinct
source occurrences. Invalid monetary strings become `None`; valid values
become binary floating-point numbers. Original money text does not survive as
a separate typed/raw pair.

Several assets skip rows missing their expected primary identifier. The raw
archive still contains those rows, but the parsed store and asset metadata do
not provide a conservation ledger for them.

### Schemas are descriptive, not enforced

`src/utils/arango_schema.py` builds loose JSON Schemas with
`additionalProperties: true`. Its default validation level is `none`, so the
schemas are UI documentation rather than write enforcement.

The manual type table contains an evident semantic error: `CAND_ST1`, the
candidate street-address field, is listed as a possible date. Numeric fields
permit number, string, or null. This shows why a field-name list plus global
type guesses cannot replace source-specific semantic contracts.

## Lobbying findings

`src/api/lobbying_api.py` is unused and points to the obsolete
`lda.senate.gov/api/v1/` host. It assumes offset/limit pagination, has no API
key support, and captures neither raw pages nor source revisions. It should not
be carried into the rewrite.

The current official API root is `https://lda.gov/api/v1/`. It exposes:

- Filings.
- Contribution reports.
- Registrants, clients, and lobbyists.
- Filing types, lobbying issue codes, government entities, geography, and
  contribution item types.

No current official all-report bulk dump was found. LDA.gov directs complete
downloads through the REST API. The retired Senate quarterly XML archive ends
at 2022 Q1, omits LD-203, and should be treated as historical comparison
evidence rather than the canonical seed. The current API publishes an OpenAPI
contract at `https://lda.gov/api/openapi/v1/`.

The filings response preserves more useful grain than the legacy design
assumed. A filing can contain structured registrant and client identities,
reported income or expenses, lobbying activity sections, issue text,
lobbyists with covered positions, government entities, affiliations, foreign
entities, and termination state.

The contribution endpoint is also material. LD-203 contribution items include
contributor, payee, honoree, contribution type, amount, and date. They should
be ingested with lobbying rather than deferred indefinitely. Any match to an
FEC transaction is a cross-source evidence link, not another amount to add.

Observed API data includes implausible historical `dt_posted` years for some
legacy records. The target must retain those source values and issue states; it
must not repair them silently.

LDA.gov currently rate-limits unauthenticated clients to 15 requests per
minute and registered API-key clients to 120 per minute. Its terms require the
retrieval date and a disclaimer that the Senate Office of Public Records
cannot vouch for data or analyses after retrieval. Those are source-contract
requirements, not presentation cleanup for later.

## Congressional-context findings

The legacy `src/api/congress_api.py` cannot be accepted as a target client:

- Some list operations pass `congress` and `chamber` as query parameters where
  the current API defines path-specific resources.
- `get_votes` calls `/roll-call-vote`, which is not a current documented
  Congress.gov endpoint. The current API exposes House-vote resources; Senate
  vote data needs its own official source plan.
- The client does not capture raw responses, schema fingerprints, or complete
  pagination evidence.

The `unitedstates/congress-legislators` YAML gives convenient Bioguide/FEC
crosswalks but is a secondary community-maintained source. It can corroborate
or seed resolution; official member, committee, and vote assertions need
official source contracts.

## Source opportunities confirmed

The official source review confirmed these additions:

- FEC PostgreSQL dumps contain processed Schedule A, B, E, and committee
  history from 1975 onward.
- Daily raw electronic and paper `.fec` filings can supply filing-revision
  evidence that weekly processed snapshots cannot recreate retroactively.
- FEC bulk sources also cover electioneering communications, communication
  costs, party-coordinated expenditures, lobbyist/registrant committees,
  bundled filings, and operating expenditures.
- GLEIF supplies official LEI reference and parent/child relationship data in
  bulk and by API.
- SEC EDGAR supplies official registrant and filing identities for public
  companies.
- IRS publishes Forms 8871/8872 political-organization disclosures.
- DOJ FARA supplies daily bulk CSV/XML plus an API for foreign-agent data.

These sources answer different questions. Their availability is not a reason
to ingest all of them before the first vertical slice.

## Disposition

| Legacy element | Disposition | Reason |
|---|---|---|
| Downloaded official FEC headers | `KEEP AS EVIDENCE` | Six reviewed headers are current and exact, but incomplete as semantic contracts. |
| CSV header drives parsing at runtime | `REPLACE` | Upstream changes must not alter production behavior without review. |
| Pad/truncate row shape | `REMOVE` | Destroys schema-drift evidence. |
| UTF-8 decode with ignored errors | `REMOVE` | Destroys source bytes and field content. |
| Float money | `REMOVE` | Use signed integer cents plus raw source text. |
| Replace duplicate source keys | `REMOVE` | Preserve occurrences and immutable versions. |
| First `.txt` ZIP member | `REPLACE` | Member selection belongs in the source contract. |
| Loose Arango schema at level `none` | `REPLACE` | Validate source/fact contracts before publication; database validation is secondary defense. |
| Classic `indiv` as canonical detail | `REPLACE` | Processed Schedule A is richer and already accepted for the first slice. |
| `oth`/`pas2` | `COMPARE` | Useful validation/backfill evidence; exact target role follows Schedule A/B/E corpus comparison. |
| Obsolete lobbying client | `REMOVE` | Rebuild against LDA.gov filings, identities, constants, and LD-203 reports. |
| Legacy Congress client | `REMOVE` | Endpoint assumptions are stale; rebuild per official source contract. |

The target response is defined in the
[source-contract design](../design/source-contracts.md) and
[source catalog](../design/source-catalog.md).

## Primary references checked

- [FEC bulk data catalog](https://www.fec.gov/data/browse-data/?tab=bulk-data)
- [FEC processed schedule dump README](https://www.fec.gov/files/bulk-downloads/data-dump/schedules/README.txt)
- [LDA API root](https://lda.gov/api/v1/)
- [LDA filings](https://lda.gov/api/v1/filings/)
- [LDA contribution reports](https://lda.gov/api/v1/contributions/)
- [LDA API terms](https://lda.gov/api/tos/)
- [Congress.gov API](https://api.congress.gov/)
- [GLEIF API](https://www.gleif.org/en/lei-data/gleif-api)
- [IRS political-organization disclosures](https://www.irs.gov/charities-non-profits/political-organizations/political-organization-filing-and-disclosure)
- [DOJ FARA bulk data](https://efile.fara.gov/ords/fara/f?p=107:21)
