# Legal Tender

## Project Overview

Legal Tender analyzes the influence of donors on US politicians by orchestrating data collection, enrichment, and AI-driven analysis. The project aims to:

1. Collect and keep up-to-date a list of all current US Congress members (House & Senate).
2. Gather and update donor data for each politician (amounts, names, organizations) using the Election (FEC) API.
3. Profile each unique donor to determine which policies they support or oppose (using AI/NLP and web research).
4. Fetch and update upcoming bills and voting data from the Congress.gov API.
5. Use AI to compare each bill with donor policy stances, scoring each bill for each donor (+1 to -1 scale).
6. Aggregate scores and donation amounts to predict how politicians may vote and quantify donor influence.
7. Store all data in MongoDB for auditability, traceability, and further analysis.

## Data Requirements

1. **Current Politicians**
	- List of all current US Congress members (House & Senate).
	- Source: [Congress.gov API](https://api.congress.gov/)

2. **Donor Data**
	- List of donors for each politician, including donation amounts.
	- Source: [Election (FEC) API](https://api.open.fec.gov/developers/)

3. **Upcoming Bills**
	- Data on upcoming bills, including text, summaries, and voting records.
	- Source: [Congress.gov API](https://api.congress.gov/)

4. **Lobbying Data**
	- Federal lobbying filings and clients.
	- Source: [Senate LDA API](https://lda.senate.gov/api/redoc/v1/)

## Orchestration & Automation

- **Dagster** is used to orchestrate and schedule all data fetches, updates, and AI analysis flows.
- Data syncs (e.g., for politicians, bills, donors) run on a daily schedule, ensuring MongoDB always reflects the latest state.
- Each entity (politician, donor, bill) is upserted by its unique ID for reliability and auditability.
- Audit fields (e.g., last_updated) and change logs are maintained for traceability.

## AI/NLP Workflow

1. For each unique donor, use AI/NLP to generate a profile of policy stances (FOR/AGAINST tables) using web research and public data.
2. For each new or updated bill, use AI/NLP to extract key policy areas and compare them to donor stances.
3. Score each bill for each donor (+1 to -1) based on alignment.
4. Aggregate scores and donation amounts to predict politician voting behavior and donor influence.

## Next Steps

1. Implement Dagster jobs for:
	- Fetching/updating Congress members
	- Fetching/updating donor data
	- Fetching/updating bills and votes
	- AI/NLP donor and bill analysis
2. Store and audit all data in MongoDB.
3. Build scoring, prediction, and visualization modules.
4. Document and test all jobs for reliability.


## Technology Stack

- **Orchestration**: Dagster (workflow scheduling and monitoring)
- **Data Storage**: MongoDB (application data), PostgreSQL (Dagster metadata)
- **Containerization**: Docker & Docker Compose
- **Language**: Python 3.11

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│               Dagster Webserver (Port 3000)                  │
│                     User Interface & API                     │
└────────────┬───────────────────────────┬────────────────────┘
             │                           │
   ┌─────────▼─────────┐       ┌────────▼──────────┐
   │  Dagster Daemon   │       │  PostgreSQL       │
   │  - Schedules      │       │  - Run Storage    │
   │  - Sensors        │       │  - Event Logs     │
   │  - Run Queues     │       │  - Asset Metadata │
   └─────────┬─────────┘       └───────────────────┘
             │
   ┌─────────▼─────────┐       ┌───────────────────┐
   │    MongoDB        │       │  Mongo Express    │
   │  - Members        │◄──────┤  (Port 8081)      │
   │  - Bills          │       │  Admin UI         │
   │  - Donations      │       └───────────────────┘
   └───────────────────┘
```

## Quick Start

### Prerequisites
- Docker and Docker Compose
- `.env` file with API keys (see `.env.example`)

### Start Services

**Production Mode** (default - isolated, secure):
```bash
./start.sh
# or manually: docker compose up -d
```

**Development Mode** (hot-reload, code changes apply instantly):
```bash
./start.sh -dev
# or manually: docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d
```

**Wipe & Restart** (clear all data):
```bash
./start.sh -v           # Production mode, wipe volumes
./start.sh -dev -v      # Dev mode, wipe volumes
```

### Access Applications
- **Dagster UI**: http://localhost:3000 (job management and monitoring)
- **Mongo Express**: http://localhost:8081 (database UI, credentials: `ltuser`/`ltpass`)

### Development vs Production Mode

**Use Production Mode** (`./start.sh`) when:
- Running in production/staging environments
- You want isolated, secure containers
- Code changes are infrequent
- **Trade-off**: Must rebuild after each code change (~30-60s)

**Use Development Mode** (`./start.sh -dev`) when:
- Actively developing and testing jobs
- Making frequent code changes
- Debugging issues locally
- **Benefits**: Code changes apply instantly (no rebuild), database ports exposed for local tools
- **Trade-off**: Less isolated (your local code folder is mounted in container)

## Working with Data Assets

### Available Assets
- **`congress_members`**: Current members of U.S. Congress (House + Senate) from Congress.gov API
- **`member_donor_data`**: Donor contributions for all Congress members from OpenFEC API (depends on congress_members)

### Materializing Assets (Refreshing Data)

**Via Dagster UI** (Recommended):
1. Open http://localhost:3000
2. Navigate to **"Assets"** tab
3. Click on an asset to view details
4. Click **"Materialize"** to refresh the data
5. View metadata, lineage graph, and run history

**Via Command Line:**
```bash
# Materialize a specific asset
docker compose exec dagster-webserver dagster asset materialize --select congress_members -m src

# Materialize all assets
docker compose exec dagster-webserver dagster asset materialize --select "*" -m src

# List all assets
docker compose exec dagster-webserver dagster asset list -m src
```

### Available Jobs

**Asset Materialization Jobs:**
- **`congress_pipeline`**: Refreshes congress_members data only
- **`donor_pipeline`**: Refreshes member_donor_data only (requires congress_members)
- **`full_pipeline`**: Refreshes all data in dependency order (recommended for scheduled runs)

**Via Command Line:**
```bash
# Run a specific job
docker compose exec dagster-webserver dagster job execute -m src -j full_pipeline

# List all jobs
docker compose exec dagster-webserver dagster job list -m src
```

**Note**: The auto-generated `__ASSET_JOB` appears in the job list but is redundant - use the explicit jobs above instead.

## Project Structure

```
legal-tender/
├── src/
│   ├── __init__.py           # Dagster definitions (assets, jobs, resources)
│   ├── assets/               # Data assets (data products)
│   │   ├── congress.py       # congress_members asset
│   │   └── donors.py         # member_donor_data asset
│   ├── jobs/                 # Asset materialization jobs
│   │   └── asset_jobs.py     # congress_pipeline, donor_pipeline, full_pipeline
│   ├── resources/            # Shared resources
│   │   └── mongo.py          # MongoDB resource
│   ├── api/                  # API clients (Congress, Election, Lobbying)
│   └── utils/                # Utility functions
├── dagster.yaml              # Dagster instance configuration
├── workspace.yaml            # Code location configuration
├── docker-compose.yml        # Service definitions
├── Dockerfile                # Multi-stage container build
└── requirements.txt          # Python dependencies
```

## Development

### Adding New Assets

1. **Create an asset file** in `src/assets/` (e.g., `bills.py`):
   ```python
   from dagster import asset, AssetExecutionContext, Output, MetadataValue
   from src.resources.mongo import MongoDBResource
   from src.api.congress_api import get_bills

   @asset(
       name="congress_bills",
       description="Current bills from Congress.gov API",
       group_name="congress",
       compute_kind="api",
   )
   def congress_bills_asset(
       context: AssetExecutionContext,
       mongo: MongoDBResource
   ) -> Output[list]:
       """Fetch and persist current bills."""
       bills = get_bills()
       
       with mongo.get_client() as client:
           collection = mongo.get_collection(client, "bills")
           # Store bills...
       
       return Output(
           value=bills,
           metadata={
               "total_bills": len(bills),
               "preview": MetadataValue.json(bills[:3]),
           }
       )
   ```

2. **Export the asset** in `src/assets/__init__.py`:
   ```python
   from src.assets.bills import congress_bills_asset
   
   __all__ = [
       "congress_members_asset",
       "member_donor_data_asset",
       "congress_bills_asset",
   ]
   ```

3. **Update definitions** in `src/__init__.py`:
   ```python
   from src.assets import congress_members_asset, member_donor_data_asset, congress_bills_asset
   
   defs = Definitions(
       assets=[congress_members_asset, member_donor_data_asset, congress_bills_asset],
       resources={"mongo": mongo_resource},
   )
   ```

4. **Restart services**:
   ```bash
   docker compose restart dagster-webserver dagster-daemon
   ```

5. **Materialize in UI**: Go to Assets tab → Click your new asset → Materialize

### Asset Dependencies

To make an asset depend on another:
```python
@asset
def donor_analysis(congress_members, member_donor_data):
    # This asset depends on both congress_members and member_donor_data
    # Dagster will automatically materialize them first
    pass
```

### Viewing Data

Access MongoDB via Mongo Express at http://localhost:8081:
- **Database**: `legal_tender`
- **Collections**: `members`, `bills`, etc.
- **Credentials**: `ltuser` / `ltpass`

## Configuration

### Credentials
All services use standardized credentials:
- **Username**: `ltuser`
- **Password**: `ltpass`

### Environment Variables
Required in `.env`:
- `CONGRESS_API_KEY`: Congress.gov API key
- `ELECTION_API_KEY`: OpenFEC API key  
- `LOBBYING_API_KEY`: Senate LDA API key
- `MONGO_URI`: MongoDB connection string (default: `mongodb://ltuser:ltpass@mongo:27017/admin`)

## Troubleshooting

### Services won't start
```bash
# Check logs
docker compose logs

# Restart services
docker compose restart

# Clean restart
docker compose down -v && docker compose up -d
```

### Permission errors
All containers run as non-root user `dagster` (UID 1000). If you encounter permission issues, rebuild:
```bash
docker compose build --no-cache
docker compose up -d
```

### Database connection issues
```bash
# Check PostgreSQL
docker compose exec dagster-postgres pg_isready -U ltuser -d dagster

# Check MongoDB
docker compose exec mongo mongosh --eval "db.adminCommand('ping')"
```

---