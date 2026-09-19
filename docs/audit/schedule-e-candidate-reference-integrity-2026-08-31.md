# Schedule E candidate-reference integrity audit — 2026-08-31

> **Follow-up:** The required per-fact calculation is now implemented and
> passed the complete 2024 gate. See the
> [candidate-resolution publication audit](./independent-expenditure-candidate-resolution-2026-08-31.md).

## Scope

This audit explains the 92 candidate IDs referenced by the accepted 2024
effective Schedule E calculation but absent from the coordinated 2024
candidate-master facts. It tests local history, official candidate lookup,
source-row identity, amount exposure, time fields, and possible amended-row
duplicates.

The audit does not rewrite source facts, choose replacement IDs, or modify the
isolated ArangoDB probe. All dollar figures below are exact signed sums from
the immutable 2024 Schedule E fact and calculation artifacts.

## Input identity

- Source release:
  `fec-76b6660f70406bf0d11537885de172883cdb8af548bfa7df35078a721c8c759a`
- Schedule E fact set:
  `f38758f7f151505b892a217c856a3fdc81a93ca0ab0670906baed677951921ee`
- Effective calculation set:
  `315227127d5707ff3246508d487e1d0be358e715d48b9829cee83f697cac8ce4`
- Candidate-master fact set:
  `3b2fe38aa7b32edf02c08bf47672f00b6f4710563df7ff31344ccac73dc5d1f6`
- ArangoDB probe:
  `lt_ie_probe_2024_c198c1c0957db3f9`

The FEC describes the candidate master as one row per candidate who registered
with the FEC or appeared on a state ballot list. Its `CAND_ELECTION_YR` is a
separate field, and one candidate ID normally persists while that person runs
for the same office. See the
[official candidate-master description](https://www.fec.gov/campaign-finance-data/candidate-master-file-description/).

## Exposure

The 92 missing IDs have syntactically valid prefixes: 62 House, 16 Senate, and
14 presidential. They occur on 1,119 preserved Schedule E facts and produce
199 effective spender-candidate-stance groups.

| Measure | Value |
|---|---:|
| All source facts | 1,119 |
| All source-fact amount | $52,968,683.79 |
| Included non-memo facts | 1,053 |
| Included signed amount | $52,059,391.58 |
| Support amount | $17,516,474.61 |
| Opposition amount | $34,542,916.97 |
| Excluded memo-X amount | $909,292.21 |

The five largest reported-ID groups are:

| Reported ID | Reported name | Included amount |
|---|---|---:|
| `H4NC01053` | Don Davis | $7,960,242.84 |
| `P60012143` | Joseph Biden | $7,022,921.14 |
| `S2PA00217` | Robert P. Casey Jr. | $6,160,855.13 |
| `H2NC01149` | Don Davis | $5,221,307.86 |
| `H0VA07133` | John McGuire | $5,207,741.74 |

The amount is not missing. Its candidate identity is not established by the
reported ID alone.

## Local history result

Only 18 of the 92 reported IDs occur in any of the four retained
candidate-master source products:

| Candidate-master presence | IDs |
|---|---:|
| No 2020, 2022, 2024, or 2026 row | 74 |
| 2020 only | 7 |
| 2020 and 2022 | 3 |
| 2020 and 2026 | 2 |
| 2022 only | 3 |
| 2026 only | 3 |

The 74 absent IDs also have no local candidate-summary,
candidate-committee-linkage, or committee-master candidate reference. Within
the coordinated local release, they are Schedule E-only assertions.

Name evidence points away from a history backfill. At the reported-ID level,
61 of the 92 IDs have at least one exact normalized Schedule E name in the
2024 candidate master under a different ID. Another 13 have a simple
last-name and first-token candidate. Eight reported IDs carry names that map
to more than one candidacy, so resolution must occur per fact and use office
context.

One strong counterexample is `H2KY01158`. The same reported ID appears on five
Mary Peltola memo rows for an Alaska House race and one included Kamala Harris
presidential row. A single `reported ID -> candidate` map cannot represent
both assertions.

## Official candidate lookup

A read-only lookup of all 92 IDs used the official OpenFEC candidate endpoint
on 2026-08-31. The API is diagnostic evidence; the coordinated bulk release
remains the source boundary. See the
[official OpenFEC API documentation](https://api.open.fec.gov/developers/).

| Lookup result | IDs | Included amount |
|---|---:|---:|
| Candidate record exists, but no 2024 cycle | 55 | $34,263,121.58 |
| No candidate record for the reported ID | 37 | $17,796,270.00 |
| Candidate record includes 2024 | 0 | $0.00 |

Of the 55 recognized historical IDs, 14 have an exact normalized name match
to at least one Schedule E name. The other 41 do not. A non-exact name is not
by itself proof of a different person: some are punctuation, nickname, or
spelling variants. Several are objective contradictions, however. Examples
include a Kamala Harris row whose reported ID resolves to Katrina Harris, a
Tim Walz presidential row whose ID resolves to a House candidate named Jim
Walz, and a Vicente Gonzalez row whose ID resolves to Rey Gonzalez.

Eight recognized IDs contradict the reported office or state. Two more
reported IDs have an office or state encoded in the ID that contradicts the
Schedule E office or state but do not exist in the candidate endpoint. These
checks prove that a syntactically valid candidate ID is not sufficient
identity evidence.

## Conservative per-fact resolution probe

The audit joined each of the 1,053 included facts to the 2024 candidate master
by exact normalized name, then required office-specific context:

- presidential rows require a presidential candidate;
- Senate rows require office and state; and
- House rows require office, state, and district.

No fuzzy match was accepted. `CAND_ELECTION_YR` remained an independent
dimension.

| Probe state | Facts | Signed amount |
|---|---:|---:|
| Unique exact name and office, master election year 2024 | 699 | $31,145,403.52 |
| Unique exact name and office, other master election year | 58 | $2,364,348.85 |
| Exact name exists but office context conflicts | 9 | $40,170.59 |
| No exact normalized-name match | 287 | $18,509,468.62 |

This is a coverage measurement, not an accepted resolver. It proves that a
large share can be linked deterministically without trusting the reported ID,
while $18.51 million still needs stronger name, candidacy, filing, and time
evidence. It also exposes poor-but-preserved publisher strings such as the
2024 candidate-master row `NICK, LALOTA` against Schedule E's
`LALOTA, NICHOLAS JOSEPH`.

## Duplicate and amendment probe

Every included missing-ID fact has a transaction ID. Forty facts totaling
$9,607,705.09 share a committee-plus-transaction-ID pair with a row carrying
another candidate ID. Those are not established duplicate corrections.

The detailed families show committees reusing labels such as `SE.1` and
`SE.10` across different filings, candidates, dates, and amounts. The 40 facts
collapse to 30 such reused-label families. No missing-ID fact has a row under
another candidate ID with the same normalized candidate name, stance, amount,
expense date, dissemination date, and payee.

Do not deduplicate or choose an amendment winner from committee plus
transaction ID. Filing identity and source occurrence remain required, as the
existing effective-calculation contract already states.

## Time evidence

The product cycle is not the activity date or the candidate's election year.
The facts preserve those clocks independently.

- `P60019015` has 19 included Barack Obama rows totaling $617,887.31. They
  belong to a 2023 year-end filing in the 2024 product partition. Every row has
  expenditure date 2023-10-30, while dissemination dates range across 2008
  and 2012.
- `H2MD04356` has two Donna Edwards rows totaling -$38,089.52. Their activity
  dates and report year are 2023.
- `H0VA07133` has 107 included John McGuire rows totaling $5,207,741.74 with
  2024 activity dates. The official ID lookup recognizes the same reported ID
  outside the 2024 cycle, but the 2024 candidate master does not.

These are preserved source states, not parser-created dates. Candidate
resolution must consume the row's report, expenditure, dissemination,
election, and office context. It must not infer a 2024 candidacy merely from
the selected `election_cycle=2024` partition.

## Verdict

The 92 placeholders are not a simple missing-master problem. They are
unresolved Schedule E candidate-reference assertions. History can contribute
evidence, but copying historical metadata onto the reported ID would silently
misidentify some rows.

The isolated graph remains valid as a physical and arithmetic probe: it
conserves the calculation's reported-ID groups and exact money. Its candidate
attribution is not production-safe for the 199 affected edges and
$52,059,391.58 attached to them.

Before projecting another cycle, define a versioned per-fact candidate-
reference calculation that:

1. preserves the reported candidate ID, name, office, state, district, and all
   source lineage;
2. resolves a separate candidacy identity from candidate history and
   row-specific office and time evidence;
3. records resolved, ambiguous, conflicting, historical-context, and
   unresolved states without silent rewrites;
4. represents vice-presidential or ticket references without inventing a
   presidential candidate ID; and
5. groups graph results only after resolution, while retaining an explicit
   unresolved amount.

The current effective calculation can remain immutable reported-ID evidence.
A later resolved-attribution calculation must consume the Schedule E facts and
the same accepted inclusion predicate because the current grouped result no
longer has enough per-fact evidence to repair identity safely.
