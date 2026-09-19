# Orchestration fixtures

[Committee-summary manifest](./committee-summary-manifest.json) is the synthetic
Go output retained by `TestPublishReleaseBoundSummaryAndReplay` during the
[publication gate](../../docs/audit/committee-summary-publication-2026-09-08.md).
It comes from the checked-in [sample CSV](../../contracts/sources/fec/committee-summary/v1/fixtures/sample.csv),
not a real committee population. The fixture includes retained typed issues.

Orchestration tests treat the manifest as an opaque result and validate it with
the [manifest schema](../../contracts/facts/fec/committee-summary/v1/manifest.schema.json).
Its local test URLs, hashes, and timestamps are fixture evidence, not runtime
configuration or accepted financial policy. Domain behavior remains tested in Go.
