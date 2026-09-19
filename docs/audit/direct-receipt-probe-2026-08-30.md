# Direct 2024 candidate-receipt probe

> **Observation date:** 2026-08-30 America/New_York  
> **Source release:** `fec-a6e6f777c3c506b100a7709815bc89f2ae84749faf4343a4589f7ec1bac066a2`  
> **Schedule A occurrence set:** `f030fef44b1cc7d41326b04fc7b39bbe7e72a2fcf775e1c0c9ff5670cbec7ec6`  
> **Cycle:** 2024  
> **Status:** Complete; all blocking probe checks passed

## Question

Can the accepted candidate itemized-individual receipt calculation run
directly over the lossless staged Schedule A relation without first creating
hundreds of millions of verbose occurrence, normalized-fact, and receipt-
decision JSON records?

The answer is **yes**. The direct probe validated and hashed the complete
source relation once, reused the published uniqueness evidence, routed
relevant rows through the same Go calculator used by normalized facts, and
published candidate results only.

## Inputs

The probe pinned one coordinated source release and these already-published
2024 inputs:

- Processed Schedule A staged relation: 264,085,606 rows, 14,765,199,882
  compressed bytes, and 182,881,299,512 uncompressed bytes.
- Candidate-committee linkage facts: 8,619.
- All-candidates summary facts: 3,826.
- Current-campaigns summary facts: 2,368.
- Schedule A occurrence evidence proving that all 264,085,606 `SUB_ID` values
  are valid and unique.

The probe trusted only the occurrence manifest's already-published uniqueness
result. It did not reread the 113.45 GB occurrence, natural-index, and bootstrap
change artifacts. It independently replayed the staged source through the
complete 81-field validator and verified both compressed and uncompressed
digests at EOF.

## Runtime and storage

| Measure | Direct probe | Rejected Schedule A fact pass |
|---|---:|---:|
| Rows completed | 264,085,606 | 113,000,000 before interruption |
| Elapsed | 7m 19.454s | 59m elapsed at interruption |
| Complete output | 1,016,233 compressed bytes | 33,683,816,923 partial bytes |
| Projected complete row output | 1.02 MB | about 78.7 GB |
| Per-row output | none | full 81-field source JSON plus typed duplication |

The direct pass sustained about 601,000 source rows per second. A live sample
at 70 million rows showed 148% CPU and 76.81 MiB container memory use. The
cgroup peak was 10,960,072,704 bytes; that measurement includes charged file
cache as well as process memory and remained below the 16 GiB worker cap.

The probe wrote 8,175 candidate results. Their JSONL representation is
17,481,529 bytes before compression and 1,016,233 bytes after zstd compression.
It wrote no receipt facts, per-row receipt decisions, production calculation
manifest, or active pointer.

## Source decision conservation

The accepted source predicate classified every Schedule A row exactly once:

| Decision | Rows |
|---|---:|
| Included itemized-individual receipt | 222,205,451 |
| Excluded non-individual | 23,960,090 |
| Excluded memo subtotal | 17,920,063 |
| Unresolved individual classification | 0 |
| Unresolved amount | 2 |
| **Total** | **264,085,606** |

Included signed source amounts sum to **$15,887,569,341.13**. This is a
calculated component, not total candidate-controlled receipts.

Only 32,154,142 rows belonged to a committee with a candidate route in the
accepted linkage facts. The other 231,931,464 rows still participated in
source-level decision conservation but required no candidate accumulator work.
No routed included row had an invalid receipt date.

## Candidate results

| State | Candidates |
|---|---:|
| Complete | 7,440 |
| Partial | 30 |
| Not comparable | 705 |
| **Total** | **8,175** |

The result artifact retains every candidate's authorized and unresolved
committee relationships, signed component total, included/excluded/unresolved
counts, committee subtotals, and separate reconciliation against each
`weball` and `webl` summary assertion.

Summary differences are an `individual_detail_gap`, not an equality gate.
`TTL_INDIV_CONTRIB` can include unitemized individual receipts outside this
itemized Schedule A component. The first probe manifest's percentage-band
summary also omitted exact zero-summary comparisons from its nested “within”
buckets. That reporting-only defect did not affect any receipt decision,
candidate result, or stored reconciliation. Direct-probe schema v2 corrects
the buckets for future runs; the v1 manifest remains immutable evidence of the
executed run.

## Decision consequence

The source and calculation are worth retaining. The tested physical JSON
representation is not.

1. Keep the immutable FEC dump, selected staged relations, classic facts, and
   compact candidate result.
2. Do not resume or automate `publish-schedule-a-facts` in its current JSONL
   form.
3. Replace repeated full-row JSON with one partitioned, typed, columnar fact
   representation that preserves all 81 source fields and row lineage once.
4. Let calculations read only their required columns. A fact-set manifest may
   use a versioned predicate plus an exact input index when that is proven
   equivalent; it need not store one duplicate decision document per row.
5. Redesign Schedule A occurrence and bootstrap-change persistence. The staged
   bytes are already lossless evidence; occurrence metadata must not multiply
   one 14.77 GB cycle into 113.45 GB of JSON.
6. Put query-bearing entity and monetary projections in ArangoDB. Do not load
   every raw receipt as a verbose graph document.

The abandoned 33.68 GB fact artifact is unpublished and safe to remove after
an explicit cleanup authorization.
