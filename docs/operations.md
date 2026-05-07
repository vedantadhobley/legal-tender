# Operations Runbook

> **Status**: stub. Phase 4 (production-readiness) fills this in.

The "how do I…" doc. Lives in heads / chat scrollback today; should live here.

Topics to cover (Phase 4):

## Common ops

- How to start/stop the dev stack
- How to start/stop the prod stack
- How to run a manual sync for one cycle (override `cycles` config)
- How to materialize a single asset with custom config
- How to inspect a stuck Dagster run
- How to query ArangoDB from the host vs. inside a container
- How to read Dagster logs / the compute_logs/ directory

## Recovery / disaster

- How to restore Arango from `dumps/` after a corruption
- How to recover from a partial pipeline run (which assets to re-materialize)
- How to recover from a corrupted `wikidata_cache.json`
- How to drain + redeploy without losing in-flight runs
- How to roll back to a previous prod image

## Troubleshooting

- Common errors and their meanings (e.g., the int/float `MetadataValue` issue in spent_on)
- What a Wikidata 429 storm looks like (and how to abort cleanly)
- ArangoDB OOM signs and tuning knobs (see commit `fb34a44`)
- Dagster's "stuck" states and how to identify them

## Schedules + automation

- How the weekly Sunday refresh works (once enabled in prod)
- How to pause/resume schedules
- How to inject a one-off run vs. amending the schedule
