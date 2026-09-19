# Schedule A/B alignment audit result

This contract records a diagnostic comparison between the accepted Schedule A
receiver-reported committee-flow cohort and physically valid Schedule B rows
from the same FEC publisher batch.

The audit uses directed committee endpoints, calendar date, and signed exact
cents. It assigns candidate precedence in this order:

1. exact endpoint, date, and amount;
2. unique endpoint and amount with a date disagreement;
3. unique endpoint and date with an amount conflict; and
4. unmatched evidence.

Multiplicity makes a candidate ambiguous. These states are evidence for the
future reconciliation policy. They are not reconciliation facts, do not
select effective Schedule B rows, and never merge or add the two reported
amounts.
