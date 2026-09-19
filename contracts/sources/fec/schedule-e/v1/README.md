# Processed FEC Schedule E source contract

This draft contract preserves one immutable occurrence for each row in the
weekly all-history `disclosure.fec_fitem_sched_e` relation. The strict Go
parser validates the exact 80-column PostgreSQL COPY shape and keeps nulls,
signed exact cents, dates, action and memo states, lineage, candidate context,
purpose, and source links without selecting effective records.

The 2026-08-30 dump passed a complete 548,318-row parser replay. Fact
publication and the effective-independent-expenditure calculation remain
separate gates.
