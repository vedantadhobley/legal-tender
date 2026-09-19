# LDA API and printable-document comparison — 2026-08-27

## Scope

This audit compared canonical LDA.gov API JSON with the exact publisher-rendered
printable HTML for:

- One LD-1 registration.
- An ordinary LD-2 quarterly report.
- An LD-2 no-activity report with income.
- An LD-2 termination with expenses and activity.
- One LD-2 family containing an original and three amendments.
- An LD-203 organization zero report.
- LD-203 reports covering all five contribution-item types.
- One LD-203 original/amendment pair.

The checked-in [printable-document fixtures](../../contracts/sources/lda/printable-documents/v1/fixtures/)
preserve the exact response bodies. The associated API fixtures and source
schemas live in the other [LDA contracts](../../contracts/sources/lda/).

This is a selected counterexample set, not a statistical corpus sample. It can
disprove unsafe assumptions. It cannot establish a universal amendment or
document-layout rule.

## Result

The API is not a lossless replacement for the printable filing, and the
printable filing is not a lossless replacement for the API. Both are official
source representations and both are required for a complete evidence model.

| Evidence | API JSON | Printable document |
|---|---:|---:|
| Filing UUID, source IDs, standardized constants | Strong | Usually absent |
| Amendment type on LD-2 | Strong | Checked form control |
| Amendment type on sampled LD-203 pair | Strong | Not rendered |
| Current registrant/client/lobbyist master profiles | Strong | No |
| Historical as-filed names and addresses | Incomplete | Strong |
| Numeric point at or above reporting threshold | Strong | Strong |
| Checked under-threshold amount band | Missing from sampled API | Strong |
| Form-level grouping and checkbox state | Transformed or missing | Strong |
| Canonical government-entity IDs | Strong | No |
| Agency text as filed | Standardized | Strong |
| Exact source document bytes and signature certification | No | Strong |

No projection may silently prefer one representation. Differences become
explicit comparison facts.

## Material findings

### Current master data leaks into historical API records

The sampled 2024 LD-1 printable registration named the registrant **Smith
Dawson & Andrews** at **1150 Connecticut Ave, NW**. The API retained that
as-filed address in filing-level fields but embedded the current registrant
master name **SMITH GARSON**, current address **750 First Street NE**, and a
2026 master update timestamp.

The same registration printed lobbyist **Kierstin Stradford**, while the API's
nested current lobbyist profile said **Kierstin Stradford-Patterson**.

A sampled 2024 LD-203 report printed employer **Strogen Strategic
Sustainability, LLC**. Its API response embedded current registrant master
**STROGEN LLC** with a 2026 update timestamp.

Consequences:

- Nested API masters are current identity context, not historical as-filed
  assertions.
- Filing-level API address fields remain useful where supplied, but they do not
  recover every as-filed name, address, contact, or description.
- Printable names and addresses must normalize as document assertions tied to
  that filing version.
- Entity resolution can connect current and historical assertions later; it
  must not overwrite either.

### A null API amount can mean an under-threshold report

The sampled LD-2 original and all three amendments checked **Less than
$5,000** for expenses and checked method A. Their API records returned:

```text
expenses = null
expenses_method = "A"
```

Therefore `income=null` or `expenses=null` does not prove zero, no activity,
or no reportable money. The normalized amount state needs at least:

| State | Meaning |
|---|---|
| `reported_value` | Source supplied a numeric point; reporting method and precision remain separate. |
| `threshold_band` | Printable form checked the under-$5,000 band; exact amount is unknown. |
| `source_blank` | Applicable field was present but blank without a selected band. |
| `not_applicable` | The other reporting role/column applies. |
| `invalid` | Controls or values conflict or cannot be parsed. |
| `unobserved` | API null cannot yet be disambiguated because the printable document is absent. |

An under-threshold value must remain a band. Do not convert it to zero, $4,999,
a midpoint, or an invented transaction. A later aggregate can expose bounds
or an incomplete exact-total state under an accepted calculation contract.

The separate no-activity fixture confirmed the opposite combination: the form
checked **No Lobbying Issue Activity** and still reported a $20,000 income
point. No-activity is an activity-section state, not a money state.

This comparison proves representation agreement, not economic precision. LDA
reporting guidance can require good-faith estimates and rounding for numeric
LD-2 values. The source contract must retain that method and precision instead
of calling every losslessly parsed decimal exact money.

### LD-1 API activity objects can change the form's grain

The sampled LD-1 printable registration had one form-level list containing
issue codes `BUD` and `DIS`, followed by one shared specific-issues statement
and one shared lobbyist list. The API emitted two `lobbying_activities`
objects—one for each code—and repeated the same description and lobbyists in
both.

That API shape is useful for code-based lookup, but it must not be treated as
proof that the filer disclosed two independent activities. Preserve both:

- The API activity occurrence and its JSON-array identity.
- The printable LD-1 issue group and its shared form-level context.

Any later coalescing or graph projection is a versioned calculation. It cannot
double-count a shared LD-1 section because the API expanded its code list.

### Standardized API labels can differ from as-filed text

One amendment printed agency text such as `Defense Security Services`, `U.S.
SENATE`, and `U.S. HOUSE OF REPRESENTATIVES`. The API returned standardized
government-entity IDs and current constant names, including `Defense Security
Assistance Agency`, `SENATE`, and `HOUSE OF REPRESENTATIVES`.

The printable source text and API entity assertions are both evidence. The
entity ID supports stable graph linkage; the printable text shows what the
filer actually supplied. Neither should replace the other.

The sampled presidential-inaugural LD-203 document rendered the type label as
`Presidential Inaugural Comm`, while the API supplied code `pic` and the full
constant label. The truncated visual label must not change the item type.

### Selected LD-2 amendments rendered complete forms

The sampled LD-2 family contained one original and three amendments. Each
amendment rendered page-one state, money-band controls, method, signatures,
and complete activity sections. Later amendments changed the activity set and
specific-issue text rather than presenting a small explicit delta.

This supports treating those four records as complete successive filing
versions. It does not prove that every historical filing family, imported
legacy form, termination amendment, or no-activity amendment follows the same
rule. The global effective-filing calculation remains blocked pending a wider
family sample.

### Printable LD-203 did not expose amendment status

The sampled LD-203 original and amendment had separate UUIDs and API types
`MM` and `MA`. Their printable documents showed the same organization, period,
zero-report state, and full form structure. The printable amendment did not
render an amendment label or checked amendment control.

For this representation, `filing_type` from the API is required amendment
evidence. Signature timestamps differed and matched the respective API
`dt_posted` values in the sample, but those fields remain separate time
meanings rather than a universal equality rule.

### LD-203 item content and ordering agreed

Across the selected FECA, honorary-expense, meeting-expense, presidential-
library, and inaugural-committee reports, the API and printable document
agreed on contributor, payee, honoree, amount, and item date after display
format normalization.

The mixed honorary-expense report preserved the same nonchronological item
order in both representations. Source order is therefore evidence. Do not sort
items by date before assigning their occurrence locators.

## Contract consequences

1. Printable documents are a separate draft source contract, not attachments
   to an API record afterthought.
2. The raw layer preserves complete API pages and exact document bytes
   independently.
3. API-only filing facts can publish as partial evidence when a document is
   unavailable. A complete as-filed projection and any interpretation of a
   null LD-2 money field require the document or an explicit
   `unobserved` state with a printable-document reason.
4. Historical document backfill is required before Legal Tender claims
   complete LDA identity, form-grain, or amount-band coverage for that period.
5. API/document comparisons produce explicit `match`, `representation_only`,
   `normalized_difference`, `conflict`, or `unobserved` states under a
   versioned rule.
6. Effective amendments remain a later calculation over preserved versions;
   the comparison did not authorize last-write-wins globally.

## Remaining validation

- Sample termination amendments and no-activity amendments.
- Sample filings with convictions, foreign entities, affiliated
  organizations, non-empty LD-203 PAC lists, and PDF documents.
- Compare pre-February-2021 government-entity imports with their printable
  forms.
- Measure document availability and byte volume for the intended 2017-2024
  backfill.
- Re-fetch known UUIDs to test whether document bytes or child-array ordering
  can change in place.
- Validate effective-family rules across multiple registrants, filer roles,
  periods, and historical form generations.
