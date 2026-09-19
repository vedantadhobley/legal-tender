# Processed Schedule A versus classic-file overlap

> **Observation date:** 2026-08-28  
> **Processed snapshot:** FEC Schedule A dump published 2026-08-23  
> **Classic snapshot:** locally captured `indiv24.zip` and `oth24.zip` on
> 2026-05-06  
> **Period:** 2023/2024  
> **Status:** Complete exact `SUB_ID` comparison

## Question

Does the complete processed Schedule A relation add product-relevant evidence
over the classic `indiv` and `oth` files already used by the Python pipeline?

The answer is **yes**. The classic files cover most of the high-dollar
itemized-individual amount, but not most of its transaction grain. They also
omit a material set of non-individual, non-memo receipt rows. The processed
Schedule A baseline remains justified; classic files remain independent
comparison evidence.

## Inputs and gates

| Input | Exact member | Rows | Invalid | Duplicate `SUB_ID` rows |
|---|---|---:|---:|---:|
| Processed Schedule A `2023_2024` | zstd COPY relation | 264,085,601 | 0 | 0 |
| Classic `indiv24.zip` | `itcont.txt` | 58,208,756 | 0 | 0 |
| Classic `oth24.zip` | `itoth.txt` | 18,667,435 | 0 | 0 |

The processed pass reproduced the accepted 182,881,290,413-byte
uncompressed digest and 15,126,887,041-byte compressed digest. It completed in
746.618 seconds. Temporary duplicate-check shards were removed when the
command completed.

The classic ZIPs contain publisher observations captured more than three
months before the processed snapshot. A row present in only one side can
therefore reflect product scope, a later amendment, or publisher revision lag.
This audit does not merge those observations. Processed rows passed the full
81-field source validator. Classic-row validity here covers the exact 21-field
shape and decimal `SUB_ID` used for overlap, not every classic field's
semantics.

## Exact record overlap

The two classic products contain disjoint `SUB_ID` sets in this observation.

| Processed Schedule A membership | Rows | Signed source amount |
|---|---:|---:|
| In classic `indiv` | 56,376,674 | $15,219,526,762.99 |
| In classic `oth` | 18,194,330 | $7,278,243,905.14 |
| In neither classic product | 189,514,597 | $30,911,385,770.95 |
| **Processed total** | **264,085,601** | **$53,409,156,439.08** |

The rows absent from both classic products are 71.76% of processed Schedule A
rows and 57.88% of its signed row amounts. Those amounts are not a money-flow
total: the ledger includes memo rows, conduit-related rows, adjustments, and
other receipt roles that require named counting rules.

The processed snapshot contains 96.85% of the older classic `indiv` IDs and
97.47% of the older classic `oth` IDs. The older classic observations contain
1,832,082 `indiv` and 473,105 `oth` IDs absent from the later processed
snapshot. That asymmetry is why classic files cannot be patched into the
processed view.

## Accepted itemized-individual calculation

This cohort applies the accepted rule from the calculation contract:
`is_individual=true`, `memo_cd!=X`, and a valid amount.

| Classic membership | Rows | Signed amount |
|---|---:|---:|
| In classic `indiv` | 53,347,507 | $12,683,915,243.78 |
| In classic `oth` | 1,551 | $45,288,755.44 |
| In neither | 168,856,391 | $3,158,365,091.91 |
| **Full processed cohort** | **222,205,449** | **$15,887,569,091.13** |

Classic files omit 75.99% of this accepted cohort's rows and 19.88% of its
signed amount. The missing processed rows average about $18.70. This is the
specific evidence the raw row-count comparison lacked: the complete processed
relation adds substantial disclosed small-dollar transaction grain, not just
amendment or memo noise.

## Other receipt evidence

The old `oth` parser retained only `ENTITY_TP` values `PAC`, `COM`, `PTY`, and
`ORG`. It retained 377,505 rows from the May classic snapshot; only 207,333 of
those IDs remain in the August processed snapshot. The other 170,172 are
snapshot-relative differences, not rows to inject into the newer view.

Applying the old four-code shape directly to processed Schedule A finds
19,687,461 rows and $34.68 billion in signed row amounts, including 19,452,574
rows absent from both classic products. This is not a valid flow total. Most of
the additional amount sits in memo and conduit-related shapes.

A narrower diagnostic cohort—`is_individual=false`, `memo_cd!=X`, and a valid
amount—contains:

| Classic membership | Rows | Signed amount |
|---|---:|---:|
| In classic `indiv` | 2,832,052 | $2,258,454,238.44 |
| In classic `oth` | 1,128,794 | $4,571,089,627.00 |
| In neither | 1,334,580 | $1,774,437,614.67 |
| **Diagnostic total** | **5,295,426** | **$8,603,981,480.11** |

The missing 1.33 million rows and $1.77 billion show material additional
non-individual receipt evidence. This diagnostic still does not define
committee flow: receipt type, contributor committee identity, memo and conduit
structure, refunds, transfers, and conservation need the future receipt and
graph calculation contracts.

## Decision consequence

1. Keep the selected processed Schedule A partitions as the canonical
   processed receipt baseline and reconciliation source.
2. Do not replace that baseline with `indiv` plus `oth`.
3. Keep each classic product independently versioned for coverage comparison
   and legacy parity. Never union snapshot-relative `SUB_ID` differences into
   a processed calculation.
4. Preserve the complete ledger, but publish money only through named memo,
   contributor-class, receipt-role, and conduit rules.
5. Continue using raw electronic filings for routine increments. This audit
   changes neither the no-weekly-full-dump rule nor the selected-period storage
   strategy.

## Reproduction

The Go command is:

```text
legal-tender pipeline fec audit-schedule-a-overlap \
  --schedule-a <fec_fitem_sched_a_2023_2024.copy.zst> \
  --indiv <indiv24.zip> \
  --oth <oth24.zip> \
  --period 2024
```

It emits `legal-tender.schedule-a-classic-overlap.v1` JSON, hashes the exact
consumed members while streaming, conserves signed cents without floating
point, reports source-code breakdowns, and fails on invalid rows or duplicate
processed `SUB_ID` values.
