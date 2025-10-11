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

## Running Jobs

### Available Jobs
- **`api_test_job`**: Validates API connectivity for Congress, Election, and Lobbying APIs
- **`member_ingestion_job`**: Fetches current Congress members and syncs to MongoDB

### Via Dagster UI (Recommended)
1. Open http://localhost:3000
2. Navigate to "Jobs" → Select a job → Click "Launch Run"
3. Monitor execution and view logs in real-time

### Via Command Line
```bash
# List all available jobs
docker compose exec dagster-webserver dagster job list -m src

# Execute a specific job
docker compose exec dagster-webserver dagster job execute -m src -j api_test_job
docker compose exec dagster-webserver dagster job execute -m src -j member_ingestion_job
```

## Project Structure

```
legal-tender/
├── src/
│   ├── __init__.py           # Dagster definitions (jobs export)
│   ├── jobs/                 # Job definitions
│   │   ├── api_test.py       # API validation job
│   │   └── member_ingestion.py  # Congress member sync job
│   ├── api/                  # API clients (Congress, Election, Lobbying)
│   └── utils/                # Utility functions
├── dagster.yaml              # Dagster instance configuration
├── workspace.yaml            # Code location configuration
├── docker-compose.yml        # Service definitions
├── Dockerfile                # Multi-stage container build
└── requirements.txt          # Python dependencies
```

## Development

### Adding New Jobs

1. **Create a job file** in `src/jobs/` (e.g., `my_feature.py`):
   ```python
   from dagster import job, op, OpExecutionContext

   @op
   def do_something(context: OpExecutionContext):
       context.log.info("Doing something...")
       # Your logic here

   @job
   def my_feature_job():
       """Job description."""
       do_something()
   ```

2. **Export the job** in `src/jobs/__init__.py`:
   ```python
   from src.jobs.my_feature import my_feature_job
   
   __all__ = ["api_test_job", "member_ingestion_job", "my_feature_job"]
   ```

3. **Update definitions** in `src/__init__.py`:
   ```python
   from src.jobs import api_test_job, member_ingestion_job, my_feature_job
   
   defs = Definitions(jobs=[api_test_job, member_ingestion_job, my_feature_job])
   ```

4. **Restart services**:
   ```bash
   docker compose restart dagster-webserver dagster-daemon
   ```

### Naming Convention
- **File name**: `my_feature.py`
- **Job function**: `my_feature_job()`
- **Pattern**: File name matches job name (minus `_job` suffix)

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