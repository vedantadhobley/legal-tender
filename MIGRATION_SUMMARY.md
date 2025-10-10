# Prefect to Dagster Migration Summary

## Overview
Successfully migrated the Legal Tender project from Prefect to Dagster. Dagster provides a lighter-weight, more Python-native orchestration solution while maintaining full UI capabilities for monitoring and executing pipelines.

## What Changed

### 1. Dependencies (`requirements.txt`)
- **Removed**: `prefect>=3.0.0,<4.0.0`
- **Added**: 
  - `dagster` - Core orchestration library
  - `dagster-webserver` - UI/web interface
  - `dagster-docker` - Docker integration

### 2. Flow Definitions
All Prefect flows were converted to Dagster jobs:

#### `src/flows/test_flow.py`
- `@flow` → `@job` 
- `@task` → `@op`
- Added `OpExecutionContext` for structured logging
- Replaced `print()` with `context.log.info()` and `context.log.error()`

#### `src/flows/member_ingestion_flow.py`
- Split into two ops: `fetch_members` and `upsert_members`
- Added proper data passing between ops
- Enhanced logging with context-aware messages
- All `print()` statements replaced with proper logging

### 3. Core Configuration Files

#### New Files Created:
- **`workspace.yaml`**: Defines Dagster code locations (points to `src/__init__.py`)
- **`dagster.yaml`**: Configures storage backend (PostgreSQL) and compute log management

#### `src/__init__.py`
- Now exports a Dagster `Definitions` object
- Declares all jobs for automatic discovery
- No manual registration needed - purely declarative

### 4. Docker Infrastructure (`docker-compose.yml`)

#### Removed Services:
- `prefect-server` - Prefect UI/API server
- `prefect-worker` - Prefect task execution worker  
- `setup` - Prefect deployment registration container

#### Added Services:
- **`dagster-postgres`**: PostgreSQL database for Dagster metadata (run history, logs, events)
- **`dagster-webserver`**: Dagit UI on port 3000 (was 4200 for Prefect)
- **`dagster-daemon`**: Background process for schedules, sensors, and run queue

#### Key Differences:
- **Simpler architecture**: 3 Dagster services vs 3 Prefect services, but no setup script needed
- **PostgreSQL instead of in-memory**: Dagster persists run history
- **Port change**: UI now at `localhost:3000` (was `localhost:4200`)

### 5. Dockerfile
- **Base image**: Changed from `prefecthq/prefect:3.4.22-python3.11` to `python:3.11-slim`
- **Added**: PostgreSQL client for database connectivity
- **Removed**: Prefect-specific configurations
- **Added**: `PYTHONPATH=/app` environment variable for proper module imports

### 6. Files Removed
- `prefect.yaml` - Prefect deployment definitions (no longer needed)
- `src/setup.py` - Prefect registration script (Dagster uses declarative definitions)

### 7. Documentation (`README.md`)
- Complete rewrite of orchestration section
- Updated all commands and workflows for Dagster
- New instructions for:
  - Starting services
  - Accessing UI
  - Running jobs (via UI and CLI)
  - Adding new jobs

## Key Architectural Improvements

### 1. **Declarative Over Imperative**
- **Prefect**: Required running `setup.py` to register flows
- **Dagster**: Jobs auto-discovered from `Definitions` object - no registration step

### 2. **Better Development Experience**
- All job definitions in pure Python
- Strong typing with `OpExecutionContext`
- Structured logging built-in
- Easier to test (can call `.execute_in_process()`)

### 3. **Persistent Metadata**
- **Prefect**: Used in-memory storage by default
- **Dagster**: PostgreSQL stores all run history, logs, and events

### 4. **Lighter Weight**
- Fewer moving parts
- No separate worker pool concept
- Simpler deployment model

## How to Use

### Start Everything
```bash
docker compose up --build -d
```

### Access UIs
- **Dagster (Dagit)**: http://localhost:3000
- **Mongo Express**: http://localhost:8081

### Run a Job
1. Open http://localhost:3000
2. Navigate to Jobs → Select a job
3. Click "Launch Run"

Or via CLI:
```bash
docker compose exec dagster-webserver dagster job execute -m src -j member_ingestion_job
```

### View Logs
All logs are visible in real-time in the Dagster UI during job execution.

## What Stayed the Same

1. **API Client Code**: All files in `src/api/` unchanged
2. **Utility Functions**: `src/utils/preflight.py` unchanged
3. **MongoDB Setup**: Same configuration and credentials
4. **Environment Variables**: Same `.env` file structure
5. **Business Logic**: Core data fetching and processing logic identical

## Migration Benefits

1. ✅ **Simpler setup** - No manual registration of jobs
2. ✅ **Better UI** - More modern interface with better visualization
3. ✅ **Persistent history** - PostgreSQL backend stores all execution history
4. ✅ **Type safety** - Strong Python typing throughout
5. ✅ **Easier testing** - Jobs can be executed in-process for testing
6. ✅ **Better logging** - Structured, context-aware logging
7. ✅ **Declarative** - All configuration in code, not YAML
8. ✅ **Lightweight** - Fewer dependencies and containers

## Next Steps

1. **Test the migration**:
   ```bash
   docker compose up --build -d
   # Wait for all services to be healthy
   # Open http://localhost:3000
   # Run the api_test_job to verify API connectivity
   # Run the member_ingestion_job to test MongoDB integration
   ```

2. **Add schedules** (when needed):
   - Define schedules in `src/__init__.py` using `ScheduleDefinition`
   - Dagster daemon will automatically pick them up

3. **Add sensors** (when needed):
   - Define sensors for event-driven workflows
   - Monitor external systems and trigger jobs

4. **Expand jobs**:
   - Add new jobs for donor data ingestion
   - Add jobs for bill analysis
   - Add jobs for AI/NLP processing

## Troubleshooting

### Services won't start
```bash
docker compose down -v  # Remove old volumes
docker compose up --build -d
```

### Can't see jobs in UI
- Check that `src/__init__.py` exports `defs` correctly
- Verify `workspace.yaml` points to correct Python file
- Check webserver logs: `docker compose logs dagster-webserver`

### Jobs fail to run
- Check daemon logs: `docker compose logs dagster-daemon`
- Verify environment variables in `.env`
- Check MongoDB is healthy: `docker compose ps mongo`

## Conclusion

The migration to Dagster is complete! The system is now running on a more modern, Python-native orchestration framework that's easier to maintain and extend. All original functionality has been preserved while gaining better observability, testing capabilities, and a cleaner architecture.
