# LDA source schema audit

> **Status:** Source-contract evidence. This document records the official
> LDA.gov API shape and representative live observations made on 2026-08-27.
> It does not authorize an ingestion implementation or an effective-amendment
> calculation yet. The resulting draft
> [machine-readable contracts](../../contracts/sources/lda/) remain under
> review.

## Purpose

The LDA OpenAPI document is useful evidence, but it is not accurate enough to
generate production Go types without review. This audit defines the source
objects and their grain, records observed differences from OpenAPI, and fixes
the time meanings that the eventual machine-readable contracts must preserve.

The official OpenAPI artifact retrieved on 2026-08-27 reported version `1.0.0`
and SHA-256 digest:

```text
6c5f13c470bf1b4b09071db32d10df72372210fd2c2be921214e3c94f47dde8a
```

The accepted common [source boundary](./source-contracts.md) still applies:
raw responses are immutable evidence, parsing retains raw values, and an
upstream schema change cannot silently change production behavior.

## Contract boundary

The LDA source needs separate contracts for:

| Contract | Publisher object |
|---|---|
| `lda/filings` | LD-1 registration and LD-2 activity filing versions. |
| `lda/contribution-reports` | LD-203 report versions and their contribution items. |
| `lda/registrants` | Current registrant master profiles. |
| `lda/clients` | Current client master profiles and registrant relationships. |
| `lda/lobbyists` | Current lobbyist master profiles and registrant relationships. |
| `lda/constants` | Filing, issue, government-entity, geography, name-affix, and contribution-item code lists. |
| `lda/printable-documents` | Exact LD-1, LD-2, and LD-203 document bytes plus form-level assertions. |

Printable filing documents are a second official representation. They need a
document contract and digest, not conversion into reconstructed API JSON.
The selected [API/document comparison](../audit/lda-api-printable-comparison-2026-08-27.md)
shows that neither representation is lossless.

The retired Senate XML archive remains comparison evidence only. The current
API is the proposed canonical structured source if a later accepted decision
promotes the lobbying slice.

## Source objects and identities

### Filing objects

One object from `/api/v1/filings/` is one disclosed filing version. Its strong
publisher key is `filing_uuid`.

| Child object | Source occurrence identity | Notes |
|---|---|---|
| Lobbying activity | `filing_uuid` + JSON array index | No activity ID is published. Preserve issue code, description, lobbyists, and government entities together. |
| Activity lobbyist | Filing + activity index + lobbyist index | The nested lobbyist has a source person ID; the association and covered-position text do not. |
| Activity government entity | Filing + activity index + entity index | The entity has a constants-table ID. Association scope is limited for legacy imports. |
| Conviction disclosure | `filing_uuid` + array index | Contains a nested lobbyist, date, and description but no disclosure-row ID. |
| Foreign entity | `filing_uuid` + array index | No stable entity ID is published in the filing payload. |
| Affiliated organization | `filing_uuid` + array index | No stable organization ID is published in the filing payload. |

Array position is a source locator, not an assertion that the publisher will
never reorder a corrected payload. Each child occurrence also needs a content
digest.

### Contribution-report objects

One object from `/api/v1/contributions/` is one LD-203 report version. Its
strong publisher key is `filing_uuid`.

| Child object | Source occurrence identity | Notes |
|---|---|---|
| PAC name | `filing_uuid` + array index | A string assertion, not a resolved committee identity. |
| Contribution item | `filing_uuid` + JSON array index | No item ID or FEC transaction ID is published. Preserve a content digest. |

An item contains contribution type, contributor name, payee name, honoree
name, amount, and date. Only an item whose source type is `feca` is a candidate
for FEC financial reconciliation. A match links two disclosures of the same
activity; it must not create a second amount.

### Master objects

Registrant, client, and lobbyist numeric IDs are opaque LDA-scoped identities.
House registrant IDs, client IDs, names, and addresses are separate source
assertions and do not authorize an entity merge.

Nested master objects in a historical filing are not necessarily historical
profiles. A filing posted in 2024 was observed with a nested registrant
`dt_updated` in 2026. Filing-level registrant address fields must therefore
remain separate from the nested current-master snapshot.

## Filing field groups

The physical contract preserves every OpenAPI field. The following groups
define their source meaning without prematurely designing graph vertices.

| Group | Fields |
|---|---|
| Filing identity | `url`, `filing_uuid`, `filing_type`, `filing_type_display` |
| Reporting period | `filing_year`, `filing_period`, `filing_period_display` |
| Source document | `filing_document_url`, `filing_document_content_type` |
| Reported money | `income`, `expenses`, `expenses_method`, `expenses_method_display` |
| Publication and lifecycle | `posted_by_name`, `dt_posted`, `termination_date` |
| As-filed registrant location | `registrant_country`, `registrant_ppb_country`, `registrant_address_1`, `registrant_address_2`, `registrant_different_address`, `registrant_city`, `registrant_state`, `registrant_zip` |
| Nested objects | `registrant`, `client`, `lobbying_activities`, `conviction_disclosures`, `foreign_entities`, `affiliated_organizations` |

`income` and `expenses` are nullable decimal strings with different reporting
meanings. Preserve them separately. Do not add them into one universal
lobbying amount. `expenses_method` describes how an expense filer calculated
its value and remains independent of the amount.

API null is not a numeric value. In the sampled LD-2 amendment family, every
printable form checked the under-$5,000 expense band while the API returned
`expenses=null`. The normalized evidence needs these amount states:

| State | Meaning |
|---|---|
| `reported_value` | The source supplied a numeric point; reporting method and precision remain separate. |
| `threshold_band` | The form selected the under-$5,000 band; the exact amount is unknown. |
| `source_blank` | An applicable source control was present but blank. |
| `not_applicable` | The other reporting role or amount column applies. |
| `invalid` | Source values or controls conflict or cannot be parsed. |
| `unobserved` | API null has not been disambiguated because the printable document is absent. |

Do not coerce any non-point state to zero, $4,999, a midpoint, or a fabricated
transaction. The official LDA guidance describes Method A values at or above
$5,000 as good-faith estimates rounded to the nearest $10,000. A losslessly
parsed decimal is therefore not automatically an exact economic amount.
Method- and period-specific source rules determine whether a reported point
can produce a bounded interval. Later calculations follow the shared
[money-measure contract](./money-measures.md).

The $5,000 LD-2 reporting band is separate from the threshold that determines
whether an entity must register at all. Registration thresholds are adjusted
over time and differ for an outside lobbying firm's income and an
organization's in-house lobbying expenses. Activity below an applicable
registration threshold can therefore be absent from the LDA corpus, not merely
present as a threshold band. That is a coverage boundary and cannot be turned
into a numeric zero or finite missing-money estimate. The adapter needs a
versioned threshold table by reporting period before historical coverage claims
can publish. The draft
[`lda.money.v1` rule table](../../contracts/sources/lda/rules/v1/rules.json)
now covers 2013 through 2028. It remains a draft publication gate while Method
A boundary rounding and Methods B/C accounting semantics are unresolved.

The API's top-level `registrant_country` field was observed as the display
value `United States of America`, while nested `registrant.country` was `US`.
Field names do not prove that similarly named values use the same code system.

### Lobbying activities

Each activity preserves:

- `general_issue_code` and `general_issue_code_display`.
- `description` and `foreign_entity_issues` as source text.
- Each nested lobbyist's source ID and structured name.
- Association fields `covered_position` and `new`.
- Each nested government entity's constants-table ID and name.

The sampled LD-1 form listed two general-issue codes once above one shared
specific-issues and lobbyist section. The API expanded that form group into
two activity objects and duplicated the shared fields. Preserve the API child
occurrences and the printable form group independently. A later projection may
relate or coalesce them, but it must not interpret expansion as two separately
reported activities or double-count the shared section.

For filings posted before February 14, 2021, the publisher warns that imported
government entities can be filing-wide even when repeated inside activity
objects. Those associations normalize as `filing_scope`. Later source-native
associations normalize as `activity_scope`. A `filing_scope` observation does
not prove an issue-to-agency relationship.

### Other filing children

Conviction disclosures preserve the nested lobbyist, disclosure date, and
description. Foreign entities preserve name, contribution, ownership
percentage, addresses, and principal-place-of-business fields. Affiliated
organizations preserve name, optional URL, addresses, and
principal-place-of-business fields.

Foreign-entity `contribution` and `ownership_percentage` are nullable decimal
strings with distinct units. Neither is an LDA filing income value.

## Contribution-report field groups

| Group | Fields |
|---|---|
| Report identity | `url`, `filing_uuid`, `filing_type`, `filing_type_display` |
| Reporting period | `filing_year`, `filing_period`, `filing_period_display` |
| Source document | `filing_document_url`, `filing_document_content_type` |
| Filer | `filer_type`, `filer_type_display`, `registrant`, `lobbyist` |
| Publication and contact | `dt_posted`, `contact_name`, `comments`, address fields |
| Report state | `no_contributions`, `pacs`, `contribution_items` |

Observed filer types include `organization` and `lobbyist`. An organization
report has a registrant and a null lobbyist. A lobbyist report contains both
the associated registrant and lobbyist. The contract must express this as a
conditional shape instead of inventing an empty lobbyist.

As with LD-1/LD-2, nested LD-203 registrant and lobbyist objects can reflect a
current master rather than historical as-filed text. The sampled document
named an employer differently from the nested API master updated in 2026.
Preserve both assertions.

`no_contributions=true` was observed with an empty contribution-item array.
The API schema permits `null`, so the normalized state remains tri-state:
explicit yes, explicit no, and source null. Source null must not be converted
to explicit no.

Each contribution item preserves:

| Field | Source type | Meaning |
|---|---|---|
| `contribution_type` | Code string | `feca`, `he`, `me`, `ple`, or `pic`. |
| `contribution_type_display` | String | Publisher label for the code. |
| `contributor_name` | String | Name as disclosed; not a resolved identity. |
| `payee_name` | String | Payee as disclosed; not necessarily an FEC committee. |
| `honoree_name` | String | Honoree as disclosed; not necessarily a resolved candidate. |
| `amount` | Nullable decimal string | Losslessly parsed reported item amount. |
| `date` | Date string | Reported contribution or expense date. |

Preserve source array and form order when assigning occurrence locators. The
sampled mixed-item report used the same nonchronological order in both
representations; sorting by date first would destroy source identity.

## Time contract

LDA does not have an FEC election-cycle field. The source contract preserves
these independent time meanings:

| Field | Time meaning |
|---|---|
| `filing_year` + `filing_period` | Publisher reporting interval. Preserve the codes and displays. |
| `dt_posted` | Offset timestamp when LDA.gov posted this filing version. Useful for finding new or changed disclosures. |
| `termination_date` | Date the registration relationship reports termination. It does not delete the filing. |
| `client.effective_date` | Effective date on the client/registrant relationship assertion. |
| `registrant.dt_updated` | Update timestamp for the current registrant master profile. It is not the filing date. |
| `contribution_items[].date` | Exact reported date of an LD-203 item. |
| `conviction_disclosures[].date` | Exact reported conviction date. |
| Retrieval timestamp | Legal Tender observation time recorded in the snapshot manifest. |
| Publication timestamp | Legal Tender publication time for a validated projection. |

`dt_posted` can be used as an incremental-fetch partition without becoming the
identity or reporting period of a fact. Changing a Dagster partition from a
day to a week or replaying a filing-year backfill does not change any source
identity.

FEC-cycle, calendar-year, rolling-window, and as-of views are later
calculations over preserved source times. The LDA parser does not stamp every
fact with one canonical FEC cycle. In particular, quarterly LD-2 income or
expenses must not be divided into invented daily transactions merely to align
them with FEC data.

## Filing versions and amendments

The publisher gives every original, amendment, termination, and no-activity
report a distinct `filing_uuid`. It does not publish an
`amends_filing_uuid` relationship.

A sampled 2024 LD-2 family contained one `Q1` original followed by three `1A`
amendments for the same registrant, client, year, and period. A sampled 2024
LD-203 family contained one `MM` original followed by one `MA` amendment. The
API supplied no direct parent pointer in either family.

All four sampled LD-2 documents rendered complete forms, including money-band
controls and activity sections. That supports complete successive versions for
this family only. The sampled LD-203 printable original and amendment did not
visibly identify amendment status, so the API `filing_type` is required source
evidence for that pair.

Therefore:

- Preserve and expose every filing version.
- Treat any supersession chain as a named Legal Tender inference.
- Order candidate versions by `dt_posted`, while retaining equal-time and
  anomalous-order states.
- Validate business-key rules against printable documents before publishing
  effective totals.
- Never interpret termination or no-activity codes as an instruction to erase
  reported money.

The last point is material: a sampled `Q1Y` no-activity report had no activity
objects but reported `$20,000.00` of income.

## Observed OpenAPI differences

These observations are reasons to maintain a reviewed Legal Tender contract
instead of generating Go types directly from OpenAPI.

| Path or behavior | OpenAPI | Observed API response | Contract treatment |
|---|---|---|---|
| `client.client_id` | String | Number in all 25 sampled 2017 filings and all 25 sampled 2024 filings. | Accept string or integer physically; retain raw JSON kind; normalize to opaque source text only after validation. |
| `expenses_method` | Nullable lowercase enum `a`, `b`, or `c` | Uppercase `A` was present in sampled amendment and termination filings. | Accept observed upper- and lowercase codes physically; preserve the raw code; normalize only under a reviewed code rule. |
| `expenses_method_display` | Not nullable | Frequently `null`. | Nullable source field. |
| `registrant_ppb_country` | Not nullable | Frequently `null`. | Nullable source field. |
| Lobbyist `prefix_display` and `suffix_display` | Not nullable | Frequently `null` when the corresponding code is null. | Nullable source fields. |
| Contribution-report `lobbyist` | Object, not marked nullable | `null` for organization filers. | Conditional nullable field based on observed filer shape. |
| Required filing fields | Only six nested objects/arrays are marked required. | Identifiers and reporting fields were present in samples but are not declared required. | Preserve missing-field states; require essential identity fields only at normalized-publication gates. |
| Child identifiers | No activity or item ID. | Array position is the only row locator. | Use filing UUID + JSON path/index + content digest. |
| Amendment relationship | No parent field. | Related versions require a business-key query. | Preserve versions; infer effective chains separately. |
| Nullable LD-2 money | Nullable decimal with no amount-band state. | `expenses=null` while the printable form checked under $5,000. | Retain raw null and qualify it with a separate document-derived amount state. |
| LD-1 activity grain | One array object per API activity. | One form-level issue-code list can expand into multiple objects with duplicated shared text and lobbyists. | Preserve API occurrences and document form groups independently. |
| Historical names | Nested master objects look filing-associated. | Current names can differ from names printed as filed. | Never overwrite document assertions with mutable masters. |
| Pagination size | No effective maximum in the schema. | Values above 25 are capped or ignored. | Contract and test the observed maximum independently of OpenAPI. |

The sample set establishes real counterexamples, not a complete nullability or
enumeration census. The raw parser must retain unrecognized additive fields
and record type changes rather than discard them.

## Representative observations

| Form/state | Observed behavior |
|---|---|
| `RR` registration | Null income and expenses; registration activities can be present. |
| `Q1` report | Filing-level income or expenses plus activity sections. |
| `Q1Y` no activity | Empty activity array can coexist with reported income. |
| `1T` termination | Termination date can coexist with expenses and activity. |
| `1A` amendment family | One original and multiple separately keyed amendments. |
| LD-2 under-threshold amount | Printable under-$5,000 selection can coexist with API null. |
| LD-203 organization filer | Registrant present and lobbyist null. |
| LD-203 lobbyist filer | Registrant and lobbyist both present. |
| LD-203 no contributions | Explicit true state with an empty item array. |
| LD-203 FECA item | Reported date and decimal amount plus disclosed contributor, payee, and honoree names. |

## Contract decisions for implementation

The draft source contracts and fixtures enforce these choices; the eventual Go
adapter must use the same boundary:

1. Preserve every complete page response before typed parsing.
2. Preserve object-level raw JSON and observed JSON kind for fields whose
   publisher schema conflicts with reality.
3. Use lossless decimal parsing; never pass LDA money through binary floating
   point, and never confuse representation precision with measurement
   precision.
4. Distinguish absent, explicit null, empty string, empty array, and invalid
   typed values.
5. Preserve source codes and display values independently.
6. Keep filing-level as-filed fields separate from mutable nested master
   profiles.
7. Treat source numeric IDs as opaque, source-scoped identifiers.
8. Keep storage/acquisition partitions separate from source time semantics.
9. Block normalized publication when essential identity, money, date, or
   conditional-filer invariants fail; never block raw capture for an additive
   unknown field.
10. Preserve exact printable-document bytes and publish API/document comparison
    states rather than silently choosing one representation.

Hand-written Go adapter types should implement the accepted contract. Generated
OpenAPI types can be comparison evidence, but the observed inconsistencies make
them unsuitable as the sole parser boundary.

## Remaining schema work

Before implementation begins:

- Expand the current fixtures to termination-amendment, no-activity-amendment,
  conviction, foreign-entity, affiliated-organization, and non-empty PAC-list
  shapes. The current set already covers ordinary registration/reporting,
  no-activity income, termination, amendment, both LD-203 filer types, and all
  five LD-203 item types.
- Expand the selected API/printable comparison to those remaining edge shapes,
  PDF documents, and pre-February-2021 imported government entities.
- Measure `client_id` JSON kinds and conditional nulls across the intended
  2017-2024 backfill, not only sampled first pages.
- Validate whether child-array ordering changes when a known filing UUID is
  observed again.
- Review and accept the pinned constants snapshots and unknown-code behavior.
- Close the open rounding-boundary and Methods B/C questions in the draft
  period-aware monetary rule table, then accept it as a publication gate.
- Review the draft filing, contribution-report, master, and constants contracts
  before using them as publication gates.
- Test candidate original/amendment/termination families before accepting an
  effective-filing calculation.

## Official references

- [LDA public data home](https://lda.gov/system/public/)
- [LDA API root](https://lda.gov/api/v1/)
- [LDA OpenAPI contract](https://lda.gov/api/openapi/v1/)
- [LDA rendered API documentation](https://lda.gov/api/redoc/v1/)
- [LDA API terms](https://lda.gov/api/tos/)
- [Official LDA reporting guidance](https://www.senate.gov/legislative/resources/pdf/S1guidance.pdf)
- [Official LDA registration thresholds](https://www.senate.gov/legislative/Public_Disclosure/new_thresholds.htm)
