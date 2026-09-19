# Schedule A receipt master-gap audit

> **Observation date:** 2026-08-31 America/New_York  
> **Cycle:** 2024  
> **Calculation set:** `f8f2eefacff34b7b420246e2e665db525a1b13320673ee8b30620450dc5c39d4`  
> **Status:** Complete; all graph placeholders classified without identity backfill

## Question

Why did the first candidate-receipt ArangoDB projection need 143 candidate and
156 committee `missing_master_fact` placeholders? Can facts from another
cycle or source snapshot safely replace those missing profiles?

The placeholders reproduce official-source disagreement. They are not lost
graph writes. The selected 2024 candidate-committee linkage and summary facts
refer to IDs that the same coordinated release's 2024 candidate or committee
master does not contain. A different 2024 release and the 2020, 2022, and 2026
cycle masters resolve none of the candidates and only one zero-dollar
committee. Those comparison facts cannot safely become silent current-cycle
profiles.

## Reproducible boundary

The new `audit-receipt-master-gaps` command reads the immutable compact
receipt calculation, its exact three classic input fact sets, the exact
same-release 2024 masters, one different-release 2024 master snapshot, and
the other three active-cycle masters. It verifies every artifact before use.

The calculation belongs to source release
`fec-a6e6f777c3c506b100a7709815bc89f2ae84749faf4343a4589f7ec1bac066a2`.
The mutable 2024 master pointers had advanced to release
`fec-76b6660f70406bf0d11537885de172883cdb8af548bfa7df35078a721c8c759a`
before this audit. The command rejected that mismatch. The completed run used
the older immutable 2024 master manifests as the calculation-time boundary
and the newer immutable facts only as comparison assertions.

| Input | Exact identity |
|---|---|
| Receipt result manifest SHA-256 | `30eac10e320aa9f9dd06564ad8b793795f8821bce2228baa505df592eb34bfbf` |
| Receipt result artifact SHA-256 | `6a4497efaf35cae1f3223d1553ff02296aaa57099cd3672d236bc2981ebb4ade` |
| Selected 2024 candidate master | `eb251d4982a5f82f4172cf3d25eda4a908b96af258bc67478689b37c393aa3ec` |
| Selected 2024 committee master | `e6699bdf7174bf3865060b14e1025fa45852bac54fd50b5a4b3743d1dd97ac1d` |
| Different-release 2024 candidate master | `3b2fe38aa7b32edf02c08bf47672f00b6f4710563df7ff31344ccac73dc5d1f6` |
| Different-release 2024 committee master | `26c2ae5089155cf134257f381060050c5bd4ef64b400bfc37b92d4f3cc16d19c` |
| Historical candidate masters | `c063e72e…`, `dcf487d5…`, `206adf10…` for 2020, 2022, and 2026 |
| Historical committee masters | `5827c1de…`, `33bd9264…`, `bb90044a…` for 2020, 2022, and 2026 |

The report preserves each comparison fact's release, fact set, fact ID, name,
and typed profile fields. It never inserts those facts into the selected 2024
master index. It also resolves every calculation relationship and source
summary back to its exact immutable classic fact.

## Complete result

| Measure | Count |
|---|---:|
| Candidate calculation results / unique candidate references | 8,175 |
| Unique committee references | 8,338 |
| Candidates absent from the selected 2024 master | 143 |
| Committees absent from the selected 2024 master | 156 |
| Missing candidates found in the different 2024 snapshot | 0 |
| Missing candidates found in another active-cycle master | 0 |
| Missing committees found in the different 2024 snapshot | 0 |
| Missing committees found only in another active-cycle master | 1 |
| Candidate and committee gap rows left unclassified | 0 |

The one historical committee match is `C00734400`, named `CYRUS SAJNA FOR
CONGRESS COMMITTEE` in the 2020 master. Its 2024 calculation relationship to
candidate `H0TX14234` carries no attributed receipt rows or money. It remains
a historical assertion, not a 2024 master replacement.

### Origin and money exposure

The gap is almost entirely zero-dollar linkage scaffolding:

- 136 candidate placeholders come only from candidate-committee linkage
  facts.
- Six come from linkage facts plus both candidate summary datasets, but have
  no attributed itemized receipt rows.
- One comes from linkages, both summaries, and an itemized receipt subtotal.
- 155 committee placeholders come only from linkage facts.
- One committee placeholder also carries an itemized receipt subtotal.

Only candidate `P40010415` and committee `C00831388` carry money. Two negative
Schedule A records total **−$8,400,000.00**. The exact linkage fact identifies
the committee as principal and authorized for the 2024 presidential candidate
and both summary facts identify the candidate as `NORRIS, JIM ALEXANDER SR`.
Those summaries report $13.78 million in total individual contributions and
$35.43 million in total receipts through 2024-12-31. Neither ID occurs in the
selected master facts, the different-release 2024 masters, or the supplied
other-cycle masters.

This is a source-integrity conflict with real negative adjustment exposure.
It is not evidence that the amount should be discarded, renamed, or attached
to a guessed profile.

## Decision consequence

1. Keep `missing_master_fact` as an explicit source-quality state. Do not use
   a mutable `current/` pointer or another cycle's profile to patch a frozen
   calculation.
2. Keep the 298 zero-dollar linkage-only or summary-only assertions as
   evidence, but let investigative views distinguish them from monetary
   receipt components. A relationship assertion is not funding.
3. Preserve the one −$8.4 million component with its missing-profile warning,
   signed amount, two record identities, linkage fact, and summary assertions.
   Do not erase an official adjustment because its master profile is absent.
4. Treat candidate and committee masters as versioned profile assertions, not
   referential-integrity authorities over every other FEC product.
5. Proceed with the receiver-reported committee-flow gate independently. Its
   Schedule A endpoints may also lack master profiles, so the flow calculation
   must conserve source IDs first and attach profile coverage separately.

## Validation

The audit passed candidate and committee gap conservation, emitted one row for
every missing ID, verified every supporting linkage and summary fact, and
proved comparison facts remained separate assertions. Focused Go tests cover
historical-only, absent, duplicate-result, malformed-money, and relationship-
only cases.
