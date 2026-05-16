# Data Quality

How to read the data-quality / data-provenance fields on `funding_channels` output, why they exist, and what they catch.

## Why this doc exists

On 2026-05-16, the pipeline confidently reported Adam Hamawy (NJ-12) as "99.4% grassroots, 0.4% whale, 0% unaccounted, $544K direct funding." Every part of that breakdown was made up — we had **zero itemized donor records** for him. The pipeline silently fell back to FEC's webl/weball summary totals and routed the entire amount into "grassroots" because there were no records to tier into whales.

That kind of silent fallback is the most dangerous failure mode this project can ship: output that *looks* like ground truth but isn't, because the consumer has no way to tell. The `data_quality` block is the structural fix — every consumer can now read the provenance and render it honestly.

## The block

`candidates.funding_channels.aggregate.individuals.data_quality` (and the same shape in every `funding_channels.by_cycle[cycle].individuals.data_quality`):

```jsonc
{
  "detail_coverage": 1.0,                   // 0.0–1.0; fraction of individual money backed by itemized records
  "individuals_itemized": 12_345_000,       // dollars from indiv.zip records
  "individuals_summary_only": 0,            // dollars from FEC summary fallback
  "primary_source": "indiv_zip"             // "indiv_zip" | "fec_summary" | "mixed" | "unknown"
}
```

| Field | Meaning |
|---|---|
| `detail_coverage` | What fraction of the candidate's individual-channel money has actual itemized donor records behind it. `1.0` = full detail; `0.0` = entirely from FEC summary, no donor names known. |
| `individuals_itemized` | Dollar value of individual money whose source is itemized (indiv.zip). |
| `individuals_summary_only` | Dollar value of individual money whose source is FEC's candidate/committee summary totals (webl, weball) with no per-donor records behind it. |
| `primary_source` | One word for the dominant source. `mixed` if a candidate's principal committees disagree (e.g., one cycle has itemization, another doesn't). |

## How to interpret

### `detail_coverage` ≥ 99%

The pipeline's whale/grassroots split, top-donor lists, and corporate-network analyses are operating on real records. Treat the output as a faithful representation of FEC's itemized data.

### `detail_coverage` between 1% and 99%

The candidate's individual money is *partially* covered. The portion backed by itemized records is real; the portion that's `summary_only` was routed to grassroots by default (because there's nothing to tier). Headline percentages will be skewed — a candidate with 50% coverage and "70% grassroots" might actually be 40% grassroots once Q1 2026 itemized records ingest.

### `detail_coverage` close to 0

**The whale/grassroots breakdown is fiction.** The pipeline knows the candidate's total receipts (FEC publishes this) but has no donor-level breakdown. Any claim about "this candidate is grassroots-funded" or "they have a lot of max-out donors" cannot be supported from this output. Wait for the next data sync or query FEC directly.

## Why this happens (the fallback design)

`src/assets/enrichment/committee_receipts.py` computes `total_from_individuals` for each committee via this preference order:

1. If `indiv.zip` has itemized records for the committee in this cycle, sum those (`source='indiv_zip'`).
2. Else, fall back to `webl.TTL_INDIV_CONTRIB` / `weball.TTL_INDIV_CONTRIB` (`source='fec_summary'`).
3. Else, $0.

The fallback exists because **FEC's summary file is published faster than the itemized bulk file.** A candidate filing a Q1 2026 quarterly by April 30, 2026 will appear in `webl.zip` (FEC's committee summary) within a few days, but the corresponding itemized records may not be in `indiv.zip` until weeks later. Without the fallback, we'd report $0 receipts for recently-filing candidates, which is worse than reporting the correct total with `summary_only` provenance.

What was missing before 2026-05-16 was **surfacing the fallback to the consumer.** Now it's a first-class field; `view_candidate.py` and any downstream consumer can render it.

## Consumer responsibilities

| Consumer | What to do when `detail_coverage < 0.999` |
|---|---|
| `view_candidate.py` | Renders a yellow banner above Channel 4 naming the `summary_only` dollars and `primary_source`. |
| Future web UI | Show a "data coverage" indicator on candidate cards. Hide or grey out the whale/grassroots breakdown when coverage < 50%. |
| Scripts (e.g., `donor_network_overlap.py`) | Whale-pool queries are graph-based and will simply return empty pools for low-coverage candidates — they don't lie, just return nothing. Report the empty result honestly. |
| Validation gates | A new gate is queued: "for candidates with N≥100 itemized records, the whale-vs-grassroots dollar split matches a direct ranking of those records." Catches the case where the breakdown is right *because of* itemized data, not despite missing it. |

## What `data_quality` does NOT yet capture

- **Per-record freshness.** The latest `TRANSACTION_DT` across a candidate's underlying indiv records is the truest "as of" date. Not currently surfaced in the data_quality block; tracked as todo "per-candidate data freshness indicator."
- **IE and PAC-direct coverage.** Today's block only describes the *individuals* channel. PAC contributions and IE spending are graph-traced from different sources (pas2, oth, spent_on) and have their own coverage characteristics — but we don't surface those.
- **The `unaccounted` figure still reads 0% for fully-fallback candidates.** Should expand to include `individuals_summary_only` as part of "uncertain attribution"; in the queue.

## Honest-render checklist

Before publishing any candidate-specific claim derived from `funding_channels`:

1. Check `individuals.data_quality.detail_coverage`. If < 0.99, the breakdown is partial.
2. If `primary_source == 'fec_summary'`, the whale list and grassroots claim are inferred-by-default, not measured.
3. The total (`total_funding`, `direct_funding`) is from FEC's own number and is reliable.
4. The Ch1 organizational direct, Ch2/Ch3 IE channels are independent of `indiv.zip` coverage — they're graph-traced from PAC committees. They remain reliable when `detail_coverage` is low.

If you skip step 1, you will eventually publish a confident claim that doesn't survive contact with the underlying records. That's how Hamawy looked clean.
