# Person identity-rule feasibility review

**Result: reject `person-identity-corroboration-trial.v1`.** The bounded review
found no real passing case and, more importantly, found strong identity candidates
that failed for reasons unrelated to person identity. The experimental Go evaluator,
its synthetic tests and its trial-only city/state audit projection were removed.
The candidate/role-time evaluator remains unchanged.

This is a development-selected feasibility check, not an accuracy study. It
reviews three existing cases and one additional appearance in the same retained
FEC filing. It does not search the full donor population or accept an identity.

## Evidence reviewed

The retained FEC source is report 1730162. Relevant as-filed appearances are:

| Appearance | FEC fields used in review |
|---|---|
| John Chambers, two occurrences | `JOHN CHAMBERS`; `JC2VENTURES`; `CEO`; Palo Alto, CA; 2023-09-30 |
| David Duffield | `DAVID DUFFIELD`; `RIDGELINE INC`; `EXECUTIVE`; Incline Village, NV; 2023-09-30 |
| John A. Catsimatidis | `JOHN A CATSIMATIDIS`; `UNITED REFINING CO`; `CHAIRMAN`; New York, NY; 2023-09-30 |

The first three are already selected by the retained person-affiliation corpus.
The Catsimatidis row is physical ordinal 2778 in the same complete source. It was
reviewed in place; it was not added as a new runtime exception or corpus label.

External sources inspected:

- Sprinklr's [2023 proxy](https://www.sec.gov/Archives/edgar/data/1569345/000114036123023208/ny20007009x1_def14a.htm)
  identifies John Chambers as JC2 Ventures' CEO and a Sprinklr director. Its
  biography is a May 2023 point, not September role coverage.
- Bloom Energy's [2023 proxy](https://www.sec.gov/Archives/edgar/data/1664703/000119312523090977/d455267ddef14a.htm)
  uses `John T. Chambers` and reports `2017 – Present: Founder, Chief Executive
  Officer, JC2 Ventures`. Its title and name text differ from the FEC text.
- SEC reporting-owner records use `CHAMBERS JOHN T` with CIK `0001198044` and
  an issuer-context San Jose address. That does not corroborate the FEC Palo Alto
  locality or prove residence.
- Ridgeline's retained September 2023 announcement names `Dave Duffield` as
  founder/co-CEO. The FEC says `David Duffield` and generic `EXECUTIVE`.
- SEC reporting-owner records use `DUFFIELD DAVID A`, CIK `0000938071`, and a
  `c/o Workday` Pleasanton address. That address represents the filing context,
  not evidence against the FEC's Incline Village locality.
- United Refining's current [team page](https://www.urc.com/team) says John A.
  Catsimatidis has been chairman and CEO since February 1986. It separately lists
  John A. Catsimatidis Jr., whose roles were Chief Investment Officer and Executive
  Vice President in 2023 and changed to president/COO in 2024.
- A 2009 [Schedule 13D](https://www.sec.gov/Archives/edgar/data/1405037/000119312509251718/dsc13d.htm)
  gives John A. Catsimatidis a New York address and corporate role context. It is
  too old to assert his locality on the 2023 receipt day.

These pages were inspected on 2026-09-16. Existing retained bodies and hashes were
not replaced. The United Refining and additional SEC pages are web evidence only,
not offline fixtures or proof that today's bytes existed in 2023.

## Trial outcomes

| Case | Useful evidence | Why v1 still failed |
|---|---|---|
| Chambers | Exact common name, two FEC occurrences, employer correspondence, CEO evidence and a real same-employer near-name | Some sources add middle initial `T`; titles vary; no matching personal locality covers the receipt day; the blanket-rival rule retained John J. Chambers even though his reported role differs. |
| Duffield | Employer correspondence and a first-party role point 25 days before the receipt | `David`/`Dave`, omitted/added middle initial and `EXECUTIVE`/co-CEO remain unresolved correspondences; the role point is not receipt-day coverage; the SEC address belongs to the Workday filing context. |
| Catsimatidis | Exact FEC middle initial, long-running first-party chairman/CEO statement, matching New York context and a same-employer father/son contrast with dated role differences | `CO`/`Company` and `CHAIRMAN`/`Chairman of the Board and CEO` fail literal comparisons; 2009 locality does not cover 2023; v1 blocks the son merely because he is a supplied rival. |

Zero real cases pass. This is not evidence of high precision. The trial abstains
because it asks for an unusual combination of exact source shapes, not because all
three identities are disproven.

## Why the rule is rejected

1. **It mixes person identity with role validity.** A person does not become a
   different person because available employment evidence misses the receipt day.
   Identity confidence and transaction-time affiliation need separate results.
2. **Literal occupation equality is not a semantic role rule.** `CEO`, `Chief
   Executive Officer`, `Chairman`, and `Chairman of the Board and CEO` can overlap
   while preserving meaningful distinctions. Formatting equality cannot decide it.
3. **Personal locality is neither routinely available nor stable.** SEC owner
   addresses can be issuer or `c/o` addresses. Company headquarters, residence,
   mailing address and FEC-reported locality are different evidence kinds. A
   locality can corroborate or contradict a candidate only when its meaning and
   time are known; it cannot be mandatory.
4. **The rival rule is too broad.** The accepted proposal said a rival satisfying
   the same distinguishing evidence blocks uniqueness. V1 instead blocked every
   supplied rival, including distinguishable middle-name, suffix and role cases.
5. **Source independence cannot be inferred from URLs or hashes.** Corporate
   biographies on different issuer sites can derive from the same supplied bio.
   Unknown dependence must remain explicit, but requiring two apparently distinct
   pages does not solve it.

Adding more sources would not repair these category errors. No source pipeline or
model inference was added.

## Requirements for a replacement

A replacement trial should:

- Return identity-candidate strength separately from relationship meaning and
  receipt-date applicability.
- Preserve exact FEC and external name components, but allow source-attested name
  variants and evaluate omitted middle/suffix fields as uncertainty rather than
  automatic equality or inequality.
- Compare organization identity and role semantics with explained, versioned rules;
  never use raw employer/title equality as a proxy for either.
- Treat locality, address, age and other attributes as typed optional evidence.
  Preserve whether an address is personal, business, issuer, `c/o`, mailing or
  unknown, and never infer residence from it.
- Keep every rival, but block a unique proposal only while that rival remains
  plausible under the same distinguishing evidence. Missing required discovery
  still makes the search scope incomplete.
- Represent one-source support, corroborated support, dependency-unknown support,
  contradiction and abstention separately. Do not reduce them to a numeric score.
- Test the same rule on Chambers, Duffield, Catsimatidis, their real rivals and an
  ordinary-employee contrast before any production publication.

No FEC receipt, source assertion, graph edge, terminal state or amount changed as
a result of this review.

The follow-up [v2 evidence classifier](./person-identity-evidence-v2-2026-09-16.md)
implements these separation requirements without restoring the rejected conjunction.
It remains a diagnostic with no accepted identity or publication authority.
