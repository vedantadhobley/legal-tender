# Independent-expenditure candidate resolution audit — 2026-08-31

## Scope

This audit records the first complete 2024 publication of the versioned
per-fact Schedule E candidate-reference calculation. The calculation runs
after accepted effective membership and before any resolved graph grouping.
It does not mutate Schedule E facts or the existing ArangoDB probe.

## Exact inputs and output

- Source release:
  `fec-76b6660f70406bf0d11537885de172883cdb8af548bfa7df35078a721c8c759a`
- Schedule E fact set:
  `f38758f7f151505b892a217c856a3fdc81a93ca0ab0670906baed677951921ee`
- Effective calculation set:
  `315227127d5707ff3246508d487e1d0be358e715d48b9829cee83f697cac8ce4`
- Candidate-master fact set:
  `3b2fe38aa7b32edf02c08bf47672f00b6f4710563df7ff31344ccac73dc5d1f6`
- Candidate-resolution calculation set:
  `e947b7ff3e9231864d582526972e07a7acf3a8301c2ffb43e76de8e340ee43ba`
- Method:
  `legal-tender.fec.independent-expenditure-candidate-resolution-method.v2`

All inputs belong to one coordinated release and passed immutable-manifest,
backing-artifact, cycle, and release checks. The output contains one dense
decision for each attributed effective fact. Its 58,288 records occupy
52,482,150 uncompressed bytes and 5,487,677 compressed bytes.

## Method

The resolver compares each Schedule E candidate reference with the same-cycle
candidate master. It normalizes names to an uppercase Unicode alphanumeric
token multiset. Office context requires:

- office `P` for president;
- office `S` plus state for Senate; or
- office `H` plus state and normalized district for House.

It performs no fuzzy, nickname, semantic, LLM, committee-name, `pas2`, or
cross-cycle matching. Candidate election year remains evidence and does not
select identity.

The ordered outcome policy is:

1. Confirm when the reported ID has the exact name and office context.
2. Resolve to a different ID only when exactly one candidate has the exact
   context.
3. Keep multiple exact candidates ambiguous.
4. When the reported ID exists but context cannot corroborate it, retain the
   reported ID as `unverified` rather than dropping the fact.
5. When the reported ID is absent and no unique exact context exists, retain
   the fact and amount as unresolved without a resolved candidate ID.

This distinction matters. An initial rejected method required exact context
even for IDs present in candidate master. It falsely put 10,936 facts into a
non-projectable conflict state. Its immutable audit artifact remains retained,
but the canonical pointer advanced to method v2.

## Complete 2024 result

| State | Facts | Signed amount | Graph identity |
|---|---:|---:|---|
| Confirmed | 45,185 | $3,475,818,717.05 | Exact reported ID |
| Resolved | 1,811 | $299,813,805.04 | Unique exact-context ID |
| Unverified | 10,996 | $543,060,178.01 | Reported ID retained |
| Ambiguous | 0 | $0.00 | None |
| Unresolved | 296 | $18,549,639.21 | None |
| **Total** | **58,288** | **$4,337,242,339.31** | — |

The projectable population is 57,992 facts and $4,318,692,700.10, or
99.492177% of facts and 99.572317% of signed amount. The unresolved population
is 0.507823% of facts and 0.427683% of signed amount. These are coverage
measures, not permission to hide the remainder.

The 1,053 facts whose reported IDs are absent from the 2024 candidate master
split into 757 resolved facts and 296 unresolved facts. The resolved bucket
also contains 1,054 facts whose present reported ID conflicts with one unique
exact context. That includes real source states such as candidate replacement,
office changes, and incorrect candidate references; the resolver preserves the
reported ID beside the selected ID and evidence codes.

The `unverified` bucket contains 10,936 facts with usable but uncorroborated
context and 60 facts with insufficient context. These retain a present
candidate-master ID while making the missing corroboration queryable.

## Gates and replay

All seven blocking checks passed:

1. exact input lineage;
2. cycle and release coherence;
3. effective-membership replay;
4. one decision per attributed fact;
5. exact signed-cent conservation;
6. no silent candidate-ID replacement; and
7. preservation of every unverified, ambiguous, and unresolved fact.

The initial v2 publication completed the corpus scan in about 3.3 seconds. An
unchanged replay returned the same calculation set and decision artifact in
about 0.4 seconds without rewriting either artifact.

## Verdict and next boundary

The per-fact identity gate passes for 2024. It is implemented in Go, published
immutably, exposed as a partitioned Dagster asset, and backed by a
machine-readable calculation contract and deterministic policy fixtures.

The existing ArangoDB probe still groups the earlier reported-ID calculation.
It must not be relabeled as resolved. The next implementation boundary is a
resolved spender-candidate-stance aggregate derived from these decisions,
with confirmed, resolved, and unverified counts retained on each group and the
296 unresolved facts conserved outside candidate edges. Only then should the
readiness bundle and Arango projection consume the new calculation.
