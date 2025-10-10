# Legal Tender - Quick Start Guide

## Prerequisites

- Docker and Docker Compose installed
- `.env` file with API keys (see `.env.example`)

## Starting the Application

### Production Mode
```bash
docker compose up --build -d
```

### Development Mode (with hot reloading)
```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml up --build
```

## Accessing Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **Dagster UI** | http://localhost:3000 | N/A |
| **Mongo Express** | http://localhost:8081 | ltuser / ltpass |
| **MongoDB** (dev only) | localhost:27017 | ltuser / ltpass |
| **PostgreSQL** (dev only) | localhost:5432 | ltuser / ltpass |

## Running Jobs

### Via Dagster UI
1. Open http://localhost:3000
2. Navigate to "Jobs" in the sidebar
3. Select a job:
   - `api_test_job` - Validate API connectivity
   - `member_ingestion_job` - Fetch and store Congress members
4. Click "Launch Run"
5. Monitor execution in real-time

### Via CLI
```bash
# List all jobs
docker compose exec dagster-webserver dagster job list -m src

# Execute a specific job
docker compose exec dagster-webserver dagster job execute -m src -j api_test_job
docker compose exec dagster-webserver dagster job execute -m src -j member_ingestion_job
```

## Viewing Data

### MongoDB (via Mongo Express)
1. Go to http://localhost:8081
2. Login with `ltuser` / `ltpass`
3. Select `legal_tender` database
4. View collections (e.g., `members`)

### MongoDB (via CLI)
```bash
docker compose exec mongo mongosh -u ltuser -p ltpass --authenticationDatabase admin
use legal_tender
db.members.countDocuments()
db.members.findOne()
```

## Common Commands

```bash
# View logs
docker compose logs -f dagster-webserver
docker compose logs -f dagster-daemon
docker compose logs -f mongo

# Restart Dagster after code changes (production)
docker compose restart dagster-webserver dagster-daemon

# Stop all services
docker compose down

# Stop and remove all data
docker compose down -v

# Rebuild after dependency changes
docker compose up --build -d
```

## Troubleshooting

### Services won't start
```bash
# Clean slate
docker compose down -v
docker compose up --build -d
```

### Jobs not appearing in UI
- Check `src/__init__.py` exports `defs`
- Verify `workspace.yaml` points to correct location
- Check logs: `docker compose logs dagster-webserver`

### Can't connect to MongoDB
- Verify MongoDB is healthy: `docker compose ps mongo`
- Check credentials in `.env` match `ltuser`/`ltpass`
- Ensure services are in same Docker network

### PostgreSQL connection errors
- Wait for PostgreSQL to be fully ready
- Check healthcheck status: `docker compose ps dagster-postgres`
- Verify environment variables

## Development Workflow

1. **Make code changes** in `src/`
2. **In development mode**, changes are picked up automatically
3. **In production mode**, restart services:
   ```bash
   docker compose restart dagster-webserver dagster-daemon
   ```

## Project Structure

```
legal-tender/
├── src/
│   ├── __init__.py          # Dagster Definitions
│   ├── jobs/                # Job definitions
│   │   ├── api_validation.py
│   │   └── member_ingestion.py
│   ├── api/                 # API clients
│   └── utils/               # Utilities
├── dagster_home/            # Dagster instance configuration
│   └── dagster.yaml
├── docker-compose.yml       # Base services
├── docker-compose.dev.yml   # Development overrides
└── workspace.yaml           # Dagster workspace
```

## Next Steps

1. ✅ Validate API keys: Run `api_test_job`
2. ✅ Ingest members: Run `member_ingestion_job`
3. 📅 Add schedules for daily runs
4. 🔔 Add sensors for event-driven workflows
5. 🎯 Build donor ingestion pipeline
6. 🏛️ Build bill analysis pipeline
7. 🤖 Add AI/NLP analysis jobs
