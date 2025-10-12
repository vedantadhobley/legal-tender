# Legal Tender

> **Latest Update**: Implemented organized data repository with smart downloads that check `Last-Modified` headers. Downloads are now independent assets with weekly scheduling. See [Data Repository](#data-repository) and [Data Sync Strategy](#data-sync-strategy) sections below.

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

### Data Pipeline Flow

```
Weekly Schedule (Sunday 2 AM UTC)
         │
         ▼
   data_sync_asset
   • Check Last-Modified headers
   • Download if remote newer
   • Legislators file (1MB)
   • FEC bulk data (~4MB)
         │
         │ Dependencies
         ▼
   member_fec_mapping_asset
   • Load cached data
   • Build member→FEC mapping
   • Extract committee IDs
   • Validate FEC IDs
         │
         ▼
     MongoDB
   Collection: member_fec_mapping
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
- **`data_sync`**: Downloads and syncs all external data (legislators, FEC bulk data) - checks Last-Modified headers, only downloads if remote is newer
- **`congress_members`**: Current members of U.S. Congress (House + Senate) from Congress.gov API
- **`member_donor_data`**: Donor contributions for all Congress members from OpenFEC API (depends on congress_members)
- **`member_fec_mapping`**: Complete member→FEC mapping from legislators file + FEC bulk data (depends on data_sync)

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
- **`data_sync_job`**: Downloads only - syncs legislators and FEC bulk data
- **`congress_pipeline`**: Refreshes congress_members data only
- **`donor_pipeline`**: Refreshes member_donor_data only (requires congress_members)
- **`member_fec_mapping_job`**: Builds member FEC mapping (requires data_sync)
- **`refactored_pipeline`**: Complete pipeline - downloads data + builds member FEC mapping (recommended)
- **`full_pipeline`**: Original pipeline - refreshes congress_members + donor data

**Via Command Line:**
```bash
# Run the new refactored pipeline (data sync + FEC mapping)
docker compose exec dagster-webserver dagster job execute -m src -j refactored_pipeline

# Run just the data sync
docker compose exec dagster-webserver dagster job execute -m src -j data_sync_job

# List all jobs
docker compose exec dagster-webserver dagster job list -m src
```

### Schedules

**Weekly Sunday Schedules** (must be enabled in Dagster UI):
- **`weekly_data_sync_schedule`**: Downloads fresh data every Sunday at 2 AM UTC
- **`weekly_pipeline_schedule`**: Full pipeline (download + process) every Sunday at 3 AM UTC

To enable:
1. Open Dagster UI → **Overview** → **Schedules**
2. Find `weekly_pipeline_schedule`
3. Click **Start Schedule**

## Project Structure

```
legal-tender/
├── data/                     # Local data repository (mounted in Docker)
│   ├── legislators/          # GitHub legislators data
│   │   ├── current.yaml      # Current members with FEC IDs
│   │   └── metadata.json
│   ├── fec/                  # FEC bulk data
│   │   ├── 2024/             # 2024 election cycle
│   │   │   ├── candidates.zip
│   │   │   ├── committees.zip
│   │   │   ├── linkages.zip
│   │   │   ├── summaries/
│   │   │   └── transactions/
│   │   └── 2026/             # 2026 election cycle
│   └── congress_api/         # ProPublica API cache
├── src/
│   ├── __init__.py           # Dagster definitions (assets, jobs, schedules)
│   ├── assets/               # Data assets
│   │   ├── data_sync.py      # Downloads & syncs external data
│   │   ├── member_mapping.py # Builds member→FEC mapping
│   │   ├── congress.py       # congress_members asset
│   │   └── donors.py         # member_donor_data asset
│   ├── jobs/                 # Asset jobs
│   │   └── asset_jobs.py     # All job definitions
│   ├── schedules/            # Automated schedules
│   │   └── __init__.py       # Weekly Sunday schedules
│   ├── data/                 # Data repository management
│   │   └── repository.py     # DataRepository class
│   ├── resources/            # Shared resources
│   │   └── mongo.py          # MongoDB resource
│   ├── api/                  # API clients
│   │   ├── congress_legislators.py  # GitHub legislators API
│   │   ├── fec_bulk_data.py         # FEC bulk data API
│   │   ├── congress_api.py          # ProPublica Congress API
│   │   ├── election_api.py          # OpenFEC API
│   │   └── lobbying_api.py          # Senate LDA API
│   └── utils/                # Utility functions
├── inspect_data.py           # Repository inspection tool
├── dagster.yaml              # Dagster instance configuration
├── workspace.yaml            # Code location configuration
├── docker-compose.yml        # Service definitions
├── Dockerfile                # Multi-stage container build
└── requirements.txt          # Python dependencies
```

## Data Repository

All downloaded data is organized in the `data/` directory with a clean structure:

### Directory Layout
- **`data/legislators/`**: Congress members data from GitHub (1MB)
- **`data/fec/2024/`**: 2024 election cycle FEC bulk data
- **`data/fec/2026/`**: 2026 election cycle FEC bulk data
- **`data/congress_api/`**: Cached ProPublica API responses

### File Naming
Friendly names instead of cryptic FEC codes:
- `candidates.zip` (was `cn24.zip`)
- `committees.zip` (was `cm24.zip`)
- `linkages.zip` (was `ccl24.zip`)
- `independent_expenditures.zip` (was `oppexp24.zip`)

### Inspection Tools

**`inspect_data.py`** - View repository contents and stats:
```bash
python3 inspect_data.py              # Overview of downloaded files
python3 inspect_data.py --metadata   # Detailed metadata
python3 inspect_data.py --json       # JSON output
```

**`test_data_repository.py`** - Test downloads and structure:
```bash
python3 test_data_repository.py      # Test structure + optional downloads
```

## Data Sync Strategy

### Smart Downloads
The `data_sync` asset checks remote `Last-Modified` headers before downloading:
1. If local file doesn't exist → download
2. Check remote `Last-Modified` timestamp
3. Compare to local file modification time
4. Only download if remote is newer
5. Fallback to 7-day age check if remote check fails

### Configuration
```python
DataSyncConfig(
    force_refresh=False,           # Force re-download
    cycles=["2024", "2026"],       # FEC cycles to sync
    sync_legislators=True,         # Legislators file
    sync_fec_core=True,            # Core files (~4MB)
    sync_fec_summaries=False,      # Summaries (~7MB)
    sync_fec_transactions=False,   # Transactions (~4GB)
    check_remote_modified=True,    # Check Last-Modified headers
)
```

### Presets
- **Minimal** (fast, ~4MB): legislators + core FEC files
- **Standard** (recommended, ~11MB): + summaries
- **Complete** (large, ~4GB): + transaction files

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