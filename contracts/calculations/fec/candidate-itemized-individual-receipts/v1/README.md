# Candidate itemized-individual receipts v1

This calculation selects the FEC-processed Schedule A itemized-individual
component for a candidate's same-cycle authorized committees and reconciles
that resolved component with each candidate summary independently.

It does not calculate total candidate-controlled receipts. Summary totals
remain source assertions and never fill missing or unresolved receipt detail.

The Go publisher consumes one Schedule A fact set plus the same-cycle linkage,
all-candidates summary, and current-campaign summary fact sets. It streams the
Schedule A artifact once while routing receipts to every candidate in the
cycle and publishes two
immutable artifacts: one source-level decision per Schedule A fact and one
result per candidate. The manifest pins both artifacts and every input fact
set.
