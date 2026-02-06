# Deprecated Scripts

These scripts have been deprecated in favor of Dagster assets.

**DO NOT RUN THESE DIRECTLY** - They bypass Dagster and break lineage tracking.

## What to use instead:

| Old Script | Dagster Equivalent |
|------------|-------------------|
| `regen_funding_sources.py` | `dagster job execute -m src -j upstream_job` |
| `regen_funding_sources_v2.py` | `dagster job execute -m src -j upstream_job` |
| `regen_committee_receipts.py` | `dagster job execute -m src -j enrichment_job` (runs `committee_receipts`) |
| `populate_wikidata.py` | `dagster job execute -m src -j enrichment_job` (runs `wikidata_corporate_resolution`) |
| `prefetch_wikidata.py` | Wikidata cache is now integrated into `wikidata_corporate_resolution` |
| `fix_enrichment.py` | `dagster job execute -m src -j enrichment_job` |
| `fix_cycles.py` | Fixed in the FEC parser assets |

## Running through Dagster:

```bash
# Inside the dagster container
docker compose -f docker-compose.dev.yml exec dagster-webserver \
  dagster job execute -m src -j <job_name>

# Or use the Dagster UI at http://localhost:4300
```

## Available Jobs:

- `fec_pipeline_job` - Full pipeline
- `enrichment_job` - All enrichments (classification, Wikidata, clustering)
- `aggregation_job` - Pre-computed summaries (Five Pies)
- `upstream_job` - Just Five Pies refresh
