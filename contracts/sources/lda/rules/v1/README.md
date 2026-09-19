# LDA monetary rules v1

[`rules.json`](./rules.json) is the period-aware source-rule table for LDA
registration coverage and LD-2 monetary fields. It is `draft` because Method A
rounding boundaries and Methods B/C accounting semantics still need direct
rule evidence before they can produce derived intervals.

Registration thresholds are coverage boundaries. They explain why lobbying
activity can be absent from the disclosed corpus; they do not bound the amount
of any observed filing and do not authorize a missing-money estimate.

The under-$5,000 LD-2 control is different. When the printable filing selects
it, the filing supplies a reported `[0, $5,000)` band for the applicable
non-negative income or expense field. Numeric Method A values retain their
reported point and `$10,000` precision increment. The rule set deliberately
blocks conversion of that rounded point into a finite interval until the
boundary convention is established.

The current target backfill is inside the supported 2013–2028 period. Any
request before 2013 must fail rule selection instead of borrowing a later
threshold.
