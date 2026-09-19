# FEC release contract v4

Implemented extension of the frozen [v3 inventory](../v3/). V4 is now the active
source release after the [real publication gate](../../../../docs/audit/fec-v4-publication-2026-09-10.md)
passed, including all four same-release summary fact sets and replay. Default
discovery still selects v3. The [manual summary Dagster gate](../../../../docs/audit/committee-summary-dagster-2026-09-10.md)
passes without automatic triggers; discovery migration and weekly activation
remain separate. The earlier implementation gate alone
did not create a live release.

V4 preserves all v3 source specifications and adds one whole committee-summary
CSV for each selected cycle. `artifact_format=committee_summary_csv` selects
the entire artifact directly, without ZIP member selection or a COPY extract.
The inventory has 27 artifacts and 25 staged outputs. Go requires exact ordered
membership, contract, URL, partition, and format equality with this registry.

All sources undergo metadata discovery and post-capture version checks. The
strict committee-summary verifier gates acquired CSV shape and source cycle;
typed date/identity issues remain preserved evidence. The planner can reuse the
23 v3 artifacts only when freshly observed publisher versions are unchanged.
It cannot skip observing them or claim old research captures share a release.

The [summary fact publisher](../../../facts/fec/committee-summary/v1/) consumes
an exact immutable v4 manifest. Financial grouping, summary/receipt comparison,
and allocation are separate calculations. The
[release strategy](../../../../docs/design/fec-release-strategy.md) owns activation;
the [publication gate](../../../../docs/audit/committee-summary-publication-2026-09-08.md)
records fixture integration and complete real artifact tests.
