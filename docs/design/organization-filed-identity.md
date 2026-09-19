# Filed registrant identity evidence

Implemented bounded Go capture, parser and offline comparison, 2026-09-15.
The live filing capture and two fresh offline runs pass. This strengthens one
organization candidate with tagged registrant evidence; it publishes no identity
or financial edges. The source contract remains draft.

## Purpose and selection boundary

The [issuer directory](./organization-issuer-discovery.md) supplied a conformed
name and CIK candidate for original FEC organization text. This check asks whether
an explicitly selected filing tags a corresponding registrant name and CIK.
It does not assume a second SEC resource independently binds the FEC referent.

The [SEC API documentation](https://www.sec.gov/search-filings/edgar-application-programming-interfaces)
describes submissions metadata with current/former names and filing references.
Those references can support later automatic selection; that API's recent-filings
block is not the complete filing history. This slice requires an explicit CIK,
accession and primary-document basename. Automatic selection, amendment precedence,
recurring acquisition and historical-name interpolation are **not implemented**.
The runtime contains no company names, named CIK exceptions or benchmark lookup table.

## Source and interpretation boundary

The [source contract](../../contracts/sources/sec/inline-registrant-identity/v1/contract.json)
uses the existing bounded SEC HTTP capture path. One request goes to a constructed
SEC Archives HTTPS URL. It uses the approved private request contact, no auth,
inherited proxy, redirect or retry, and a 30-second timeout and 16 MiB body cap.
The output directory must be new. Preserve exact body bytes, status, selected
headers, capture/build hashes and observation time. Failed responses stay explicit.
Contact-bearing manifests and results stay private; shared fixtures contain only
unchanged public response bodies.

The parser accepts a strict XML subset, not arbitrary browser HTML or full XBRL:

- One XHTML root; UTF-8 or validated ASCII. Reject malformed XML, DTD/directives,
  duplicate attributes/context IDs, excessive depth and source-budget violations.
- Select only inline-XBRL 2013 `nonNumeric` facts whose resolved QName is an
  official DEI year-namespace `EntityRegistrantName` or `EntityCentralIndexKey`.
  Prefix spelling is not a hardcoded identity rule. Untagged prose supplies no facts.
- Preserve every selected occurrence's original ID, context reference, decoded
  text, namespace, ordinal, byte span and span hash. Offsets are zero-based,
  start-inclusive and end-exclusive in the original response body.
- Retain the referenced context's exact byte span, identifier/scheme and reporting
  period or instant. Check the context and reported CIK against the requested CIK.
  Context dates do not establish employment or legal-name validity.
- Unsupported transformations/extra selected-fact attributes, continuation, nested
  non-XHTML content, dimensions, missing contexts, duplicate fact IDs and conflicting
  CIKs make the affected facts ineligible. Do not flatten those shapes into accepted
  identity text. Unrelated filing content remains opaque retained source bytes.

The parser permits at most 1,024 selected facts, 8,192 decoded bytes per selected
fact, 50,000 contexts and depth 128. These are resource limits, not company rules.
A well-formed source with no eligible identity facts is incomplete evidence, not
a confirmed absence or a successful identity match.

## Comparison rule

`organization-filed-registrant-comparison.v1` recomputes directory candidates from
the pinned original queries and directory capture. It then verifies the pinned
filing capture and compares the FEC text to eligible tagged registrant names using
the existing format-only `Normalize` rule. It does not add suffix equivalences.

At least one usable name and one usable CIK fact are required. Any ineligible
selected identity fact blocks the positive comparison. Multiple distinct normalized
registrant names stay conflicting. Repeated equivalent occurrences retain all
locators; they are not independent confirmations. The matching state is
`reported_name_matches_filed_registrant`, not `identity_verified`.

All directory alternatives survive. Other candidate CIKs remain
`filing_not_inspected`; inspecting one does not clear directory ambiguity.
Original FEC references and independent-binding/time blockers remain intact.
Identity publication, employment, ownership and financial-attribution flags stay
false. No ArangoDB writes, money selection or terminal-policy changes occur.

## Commands

```sh
legal-tender pipeline entities capture-issuer-filing \
  --cik EXACT_TEN_DIGIT_CIK \
  --accession EXACT_ACCESSION \
  --document PRIMARY_DOCUMENT.htm \
  --output NEW_PRIVATE_CAPTURE_DIRECTORY

legal-tender pipeline entities inspect-issuer-filing \
  --cik EXACT_TEN_DIGIT_CIK \
  --accession EXACT_ACCESSION \
  --document PRIMARY_DOCUMENT.htm \
  --filing-capture PRIVATE_FILING_CAPTURE_DIRECTORY \
  --expected-filing-sha256 EXACT_FILING_MANIFEST_DIGEST \
  --queries ORIGINAL_FEC_QUERIES.json \
  --expected-queries-sha256 EXACT_QUERY_DIGEST \
  --issuer-capture PRIVATE_DIRECTORY_CAPTURE_DIRECTORY \
  --expected-issuer-sha256 EXACT_DIRECTORY_MANIFEST_DIGEST
```

Capture reads exported `SEC_USER_AGENT`; `--user-agent` overrides it. The CLI does
not load all of `.env`, and help does not reveal the configured contact. Redirect
full output to private files. Inspection is entirely offline. Invalid pins or
source substitution fail without a comparison. An unusable filing yields a
structured failed result and exit 1; exit 0 means usable source processing, not
identity approval or absence of comparison blockers.

## Retained real gate

The [official filing index](https://www.sec.gov/Archives/edgar/data/34782/000003478225000025/0000034782-25-000025-index.htm)
identifies accession `0000034782-25-000025`, filed 2025-02-18, reporting through
2024-12-31, with primary document `source-20241231.htm`. The
[annual report](https://www.sec.gov/Archives/edgar/data/34782/000003478225000025/source-20241231.htm)
tags `1st Source Corporation` and `0000034782`. Its prose distinguishes the
corporation from its bank subsidiary; a parent/bank merge is not justified.

The normal Go capture received HTTP 200 on 2026-09-15 at
18:38:49.150966299 UTC. All 4,263,684 response bytes match the retained
[public filing fixture](../../tests/fixtures/organization-resolution/sec-annual-report-v1.htm).
The checker extracts two selected facts with context `c-1`, reporting from
2024-01-01 through 2024-12-31 and no dimensions:

| Tagged concept | Source bytes | Result |
|---|---|---|
| `EntityCentralIndexKey` | `[1224, 1325)` | `0000034782` |
| `EntityRegistrantName` | `[357872, 357983)` | `1st Source Corporation` |

Both facts retain context bytes `[5115, 5378)`. The original query at index 2,
`1ST SOURCE CORPORATION`, matches the filed name under format-only normalization.
All twenty original directory decisions remain in the result; the nineteen with
no directory candidate are not relabeled. No reviewed identity labels change.

| Input/output | SHA-256 |
|---|---|
| Public filing body | `d4ce073c085d7193a586565759038b28e08aca3c4a166566de437b6eca9ce1cd` |
| Private filing capture manifest | `be3565cca6d5622039f9be14bbc9d45f136307e4a127b7fd734f00576579d0ba` |
| Existing directory capture manifest | `580f44b20809205d6d2de5abea453ebb54804082738211d10e3259d1f0af5921` |
| Original FEC query file | `9150ee14df2ba7e6da512b3d728d0086c1bad346effe09f56d19985da11cba42` |
| Development probe executable | `6e248d02608d3e64cea99c97eaba20822afc995a0745a10d332cdb85cb3350a8` |
| Each fresh offline comparison | `30fd0fea846c09a8c2bb7b4295fd7558c6eafc1d4f6b05a55cca9f879a7a470c` |

The private local gate directory is `.cache/sec-filing-P8lc6Hp9/`. Both inspection
processes exited 0 and their outputs compare byte-for-byte. This is a development
probe, not a production deployment or a reproducible release-build claim.

## Independent FEC context and remaining decision

A separate read-only review verified the compressed artifact backing the query's
committee-master fact before reading that fact:

- Manifest: `e6699bdf7174bf3865060b14e1025fa45852bac54fd50b5a4b3743d1dd97ac1d`.
- Artifact: `c6a162cd9d9798d6d72917a070b5ff275ac81a594bf26ffafa2dfa7845f7cc46`.
- Fact: `68b22c730846529a613889f0b3e8f16d28ab74383f7bc936220deb8b27d8e08d`.

That fact reports `C00181529`, connected organization `1ST SOURCE CORPORATION`,
and a committee mailing address of `P O BOX 1602`, South Bend, IN `46634`.
The SEC submissions research observation and filing index report the same mailing
address after formatting differences. **The FEC fields describe the committee's
mailing address, not explicitly the connected organization's own address.** This
is supporting context, not a silently promoted organization-address identity join.
The automated checker does not use that manually reviewed address correspondence.

The bounded FEC registration-page request returned HTTP 403. Its retained response
does not establish any Form 1 contents, absence or mismatch. Do not bypass the block
or infer registration fields. Initial research responses remain private in
`.cache/issuer-review-yff6q8tI/`; the public submissions body is separately retained
as a research fixture, not an implemented submissions source adapter.

Next review the organization identity-edge policy: which independent identifiers
or explicit source relationships permit a resolved assertion, which combinations
remain name-based inferences, and how uncertainty and time attach to that assertion.
This small parser gate is not grounds to build a universal filing crawler or defer
that policy discussion behind more diagnostics. Broader/nonissuer coverage remains
separate work. No individual employment or corporate ownership follows from the
registrant check.

## Verification

Tests cover synthetic and real tagged names, namespace aliases, exact source spans,
missing/invalid contexts, transformations, duplicates, conflicting CIKs, malformed
XML, budgets, capture/reference/pin substitution, failed HTTP observations, CLI
contact privacy and byte-identical replay. Comparison tests retain competing CIKs,
parent/bank distinctions, incomplete evidence and all unapproved flags.
Go tests, vet, focused race checks and SEC/GLEIF/Wikimedia source metadata tests
are the scoped acceptance gates. The pre-existing FEC report-scope/shared-metadata
mismatch remains deferred and is not evidence against this parser's passing gate.
