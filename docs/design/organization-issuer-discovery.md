# Independent issuer candidate discovery

Implemented Go source/parser/CLI, offline tests and a retained live directory gate,
2026-09-15. The bounded fetch succeeded and fresh CLI replay was byte-identical.
This is a missing-identifier candidate path, not accepted identity resolution or
new financial graph coverage.

## Why this source

The [SEC directory documentation](https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data)
describes `company_tickers.json` as ticker/CIK/conformed-name associations. SEC
explicitly does not guarantee its scope or accuracy. Its
[fair-access guidance](https://www.sec.gov/about/developer-resources) requires
declared automated access and moderated requests.

The [draft source contract](../../contracts/sources/sec/company-tickers/v1/contract.json)
pins this one bulk endpoint. It is narrower than all EDGAR registrants and is not
a universal organization registry. Unions, private employers and other nonissuers
must not become ineligible because this directory does not contain them.

Unlike the [exact-LEI path](./organization-corroboration.md), this lookup takes the
original source-backed FEC query snapshot, not a QID or candidate-supplied identifier.
It assesses every query independently of Wikipedia success. The two paths preserve
separate provenance; this is not a hidden fallback that rewrites GLEIF outcomes.

## Source boundary

`capture-issuer-directory` makes one request to the fixed SEC HTTPS URL. It sends
no FEC names or source references. There is no auth, inherited proxy, redirect,
retry, alternative host, latest pointer or schedule. The caller supplies a declared
user agent through `SEC_USER_AGENT` or an explicit `--user-agent` override; the
program validates header syntax, not contact reachability. Help output does not
print the configured value. The operator-approved contact lives in the gitignored
`.env`, not in source code or committed fixtures. The CLI reads the exported
variable; it does not load the rest of `.env` or export unrelated credentials.
Full capture and discovery results contain the contact-bearing request metadata
and must remain private. Only the unchanged public response body is a shared fixture.

The request has a 30-second timeout and a 16 MiB body cap. Parsing allows at most
100,000 source rows. These are operational budgets, not entity-selection rules.
Over-budget bodies retain a labeled prefix and never become usable sources.
Normal captures retain the exact response body, URL, observation time, selected
nonsecret headers, status, lengths, hashes and fetch executable digest. A new
directory and final manifest are required; old captures cannot be overwritten.

The offline reader verifies pinned manifest/body bytes and confined regular files.
The parser rejects duplicate JSON keys, invalid UTF-8/surrogates, trailing JSON,
null/missing fields, wrong numeric CIK shapes and unknown fields. Each source row
must contain exactly `cik_str`, `ticker` and `title`. Numeric object keys need not
be contiguous. Source titles/tickers and all duplicate-CIK rows remain intact.
The ten-digit CIK string is derived separately from the original numeric value.

HTTP errors, wrong media/encoding, transport failures and schema drift are explicit
unusable observations. They never become an empty accepted directory. A directory
row's observation date is not its name's validity date or an employment interval.

## Candidate rule

`organization-issuer-candidates.v1` uses the existing explained
`organization-name-proposals.v2` transformations without changing their vocabulary.
There are no benchmark names, tickers, CIKs, row numbers, fuzzy scores, parent
inference or LLM calls in the runtime rule. `CO`/`COMPANY` is still unsupported;
semantic words such as `BANK` and `HOLDINGS` remain significant.

Every source title is compared to each query. Every matching row retains its exact
row key, source fields and name transformation. Matches group by CIK; several
tickers on one CIK are not several independent confirmations. Every matching CIK
survives, including broader alternatives to an exact match.

| Condition | State |
|---|---|
| HTTP/transport/schema source failure | `source_unusable` |
| No matching source title under this policy | `no_name_candidate_in_directory` |
| One distinct matching CIK | `single_issuer_candidate_identity_unresolved` |
| Several distinct matching CIKs | `ambiguous_issuer_candidates` |

All queries keep their original FEC references and explicit missing independent
FEC-to-CIK binding, transaction-time validity and directory-scope/accuracy blockers.
A singleton is still a name-based candidate. It does not establish a canonical
identity, employment, parent ownership or dollar attribution. All four approval
flags remain false. Existing Wikimedia/GLEIF results and Arango graphs are unchanged.

## Commands

```sh
legal-tender pipeline entities capture-issuer-directory \
  --output NEW_SEC_CAPTURE_DIRECTORY \
  --user-agent 'LegalTender/organization-research (YOUR_REACHABLE_CONTACT)'

legal-tender pipeline entities discover-issuer-organizations \
  --queries SOURCE_BACKED_QUERIES.json \
  --expected-queries-sha256 EXACT_QUERY_DIGEST \
  --issuer-capture SEC_CAPTURE_DIRECTORY \
  --expected-issuer-sha256 EXACT_CAPTURE_MANIFEST_DIGEST
```

Capture returns compact metadata and parsing status, not another copy of the bulk
body. Discovery has no network capability. Its result binds both exact inputs,
their provenance, directory row/distinct-CIK counts, policy and current executable.
Exit 0 means the source was usable, not that any identity was accepted. Unusable
observations return structured results and exit 1. Corrupt/wrongly pinned inputs
return an error without candidate output.

## Verification and next gate

Synthetic source tests cover closed shapes, exact bytes, row/byte limits, duplicate
CIKs, file confinement, corrupted pins, cancellation, failed HTTP/media/encoding and
schema changes. Candidate tests cover repeated tickers, broad rivals to exact names,
letter/number boundaries, parent/bank/holding distinctions and unsupported suffixes
or misspellings. CLI tests prove deterministic replay, source-failure propagation
and independent handling of all twenty retained FEC queries. That last test uses
an invented directory: it proves query conservation, not real match accuracy.

The full Go suite, targeted race checks, static analysis and focused
SEC/GLEIF/Wikimedia schema tests pass. The unrelated existing FEC report-scope
metadata mismatch is not part of this source acceptance. Source acceptance remains
draft; this diagnostic set is not a population-wide accuracy measurement.

## Retained live gate

The normal Go fetcher made one request and received HTTP 200 on 2026-09-15 at
17:55:26 UTC. The directory has 10,422 rows and 8,022 distinct CIKs in 797,759 bytes.
All rows pass the strict parser, including repeated-CIK ticker occurrences.
The exact [public response body](../../tests/fixtures/organization-resolution/sec-company-tickers-v1.json)
is retained separately from private contact-bearing capture metadata.

All twenty pinned FEC queries were evaluated, regardless of the prior Wikimedia
request failures. The unchanged name rule produced one CIK candidate:
`1ST SOURCE CORPORATION` matched source title `1ST SOURCE CORP`, ticker `SRCE`,
CIK `0000034782`, through the existing explained legal-suffix rule. This adds a
directory identifier candidate where the captured Wikidata item had no LEI.
It does not independently prove that FEC's referent is that legal registrant.

Nineteen queries have no name candidate under this policy. In particular, the
source contains `3M CO`, but query `3M COMPANY` remains unmatched because that
suffix pair is unsupported. Do not rewrite the policy to force a benchmark result
or interpret these nineteen outcomes as nineteen absent organizations.
The previously retained company announcement distinguishes a corporation from
its bank; neither it nor this directory permits a parent/subsidiary merge.
No new reviewed identity labels or financial connections were added.

Exact pins:

| Input/output | SHA-256 |
|---|---|
| Public directory body | `82cd5fd9ccffda811b93ba76070460dd41429c02c00e726f83deb71f553c6cff` |
| Private capture manifest | `580f44b20809205d6d2de5abea453ebb54804082738211d10e3259d1f0af5921` |
| Existing FEC query file | `9150ee14df2ba7e6da512b3d728d0086c1bad346effe09f56d19985da11cba42` |
| Development probe executable | `c28a4e1bddd840b166016b47d7983d4e118bf3601cf8f1fd2a36b5c50343f7f0` |
| Each of two fresh CLI results | `6746bc400ee8304a1b62cb0090ddc9d5849a808d7f4c77d71d0c8792c4e6dbce` |

The local private probe directory is `.cache/sec-issuer-zg6mxMcd/`; it is not
production storage or a committed recovery bundle. Both discovery processes
exited 0 and produced byte-identical output. The public fixture regression reads
the exact body and queries, checking all rows, decisions, source locators and
unchanged approval flags. It does not pretend to reconstruct private HTTP metadata.

The [filed registrant check](./organization-filed-identity.md) now adds an explicit
filing capture and tagged name/CIK comparison. Its live gate corroborates the full
name without approving identity or rewriting this directory rule. Next review the
identity-edge evidence policy. Broader/nonissuer evidence, GLEIF name search,
automatic submissions/filing selection and historical identity rules remain open;
SEC directory membership is not mandatory.
