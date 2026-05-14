"""Shared configuration constants.

Single source of truth for values that were previously duplicated across
many modules. When adding a new election cycle or updating FEC limits,
edit here and re-import; do not copy literals into individual modules.
"""

# Active election cycles. Add a new cycle here when extending coverage
# (e.g. "2028" after FEC publishes 2027-2028 cycle data). Tuple form is
# intentional: callers that need a list do `list(ACTIVE_CYCLES)` so the
# mutable copy doesn't accidentally feed back into the shared tuple.
ACTIVE_CYCLES = ("2020", "2022", "2024", "2026")


# FEC per-election individual contribution limits, indexed every 2 years
# by FEC for inflation. Source:
# https://www.fec.gov/help-candidates-and-committees/candidate-taking-receipts/contribution-limits/
#
# Used by `donors` to mark whales (donors at-or-above the per-election
# limit at any single committee in a cycle).
#
# Maintenance: when adding a new cycle to ACTIVE_CYCLES, also add its
# per-election limit here. The Stop hook (Phase 1+) can warn if a cycle
# is added without a matching limit.
PER_ELECTION_LIMITS = {
    "2020": 2_800,
    "2022": 2_900,
    "2024": 3_300,
    "2026": 3_500,
}
