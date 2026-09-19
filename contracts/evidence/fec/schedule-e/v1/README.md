# Processed Schedule E occurrence evidence

This contract selects one two-year election cycle from the immutable
all-history `disclosure.fec_fitem_sched_e` relation. It preserves every
selected physical row, its byte coordinates, source digest, `sub_id`, and
release lineage. It does not apply amendment, memo, support/oppose, or
spending policy.

The source-wide `sub_id` uniqueness check is blocking. The manifest also
conserves all source rows across the selected cycle, other cycles, and null
cycle values.
