# Dagster Quick Reference

## Starting the System

```bash
# Start all services
docker compose up --build -d

# View logs
docker compose logs -f

# Stop all services
docker compose down

# Stop and remove all data (fresh start)
docker compose down -v
```

## Accessing UIs

- **Dagster (Dagit)**: http://localhost:3000
- **Mongo Express**: http://localhost:8081

## Running Jobs

### Via Dagster UI
1. Go to http://localhost:3000
2. Click "Jobs" in the left sidebar
3. Select a job (`api_test_job` or `member_ingestion_job`)
4. Click "Launch Run" button
5. Monitor progress in real-time

### Via Command Line
```bash
# List all available jobs
docker compose exec dagster-webserver dagster job list -m src

# Execute a specific job
docker compose exec dagster-webserver dagster job execute -m src -j api_test_job
docker compose exec dagster-webserver dagster job execute -m src -j member_ingestion_job

# View job backfills (for scheduled runs)
docker compose exec dagster-webserver dagster job backfill --help
```

## Viewing Logs

### Live Logs in UI
1. Go to http://localhost:3000
2. Click "Runs" in left sidebar
3. Click on any run
4. View structured logs with severity levels

### Container Logs
```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f dagster-webserver
docker compose logs -f dagster-daemon
docker compose logs -f mongo
```

## Common Operations

### Restart Dagster (after code changes)
```bash
docker compose restart dagster-webserver dagster-daemon
```

### Access MongoDB
```bash
# Via Mongo Express UI
# http://localhost:8081

# Via mongosh CLI
docker compose exec mongo mongosh -u ltuser -p ltpass --authenticationDatabase admin

# Then in mongosh:
use legal_tender
db.members.find().limit(5)  # View first 5 members
db.members.countDocuments()  # Count total members
```

## Development Workflow

### Adding a New Job

1. **Create the job file** (e.g., `src/flows/my_new_job.py`):
```python
from dagster import op, job, OpExecutionContext

@op
def my_operation(context: OpExecutionContext):
    context.log.info("Doing something...")
    # Your logic here
    return "result"

@job(name="my_new_job", description="My new job description")
def my_new_job():
    my_operation()
```

2. **Register in `src/__init__.py`**:
```python
from src.flows.my_new_job import my_new_job

defs = Definitions(
    jobs=[
        test_api_keys_job,
        member_ingestion_job,
        my_new_job,  # Add here
    ],
)
```

3. **Restart Dagster**:
```bash
docker compose restart dagster-webserver dagster-daemon
```

4. **Run your job**: Visit http://localhost:3000

### Adding a Schedule

In `src/__init__.py`:
```python
from dagster import Definitions, ScheduleDefinition
from src.flows.member_ingestion_flow import member_ingestion_job

# Run every day at midnight
daily_member_ingestion = ScheduleDefinition(
    name="daily_member_ingestion",
    job=member_ingestion_job,
    cron_schedule="0 0 * * *",  # midnight daily
)

defs = Definitions(
    jobs=[
        test_api_keys_job,
        member_ingestion_job,
    ],
    schedules=[
        daily_member_ingestion,
    ],
)
```

Then restart services and enable the schedule in the UI.

## Debugging

### Check if PostgreSQL is ready
```bash
docker compose exec dagster-postgres pg_isready -U dagster
```

### Check if MongoDB is ready
```bash
docker compose exec mongo mongosh --eval "db.adminCommand('ping')" --quiet
```

### View Dagster daemon health
```bash
docker compose exec dagster-daemon dagster-daemon health
```

### Access Dagster Python environment
```bash
docker compose exec dagster-webserver python
>>> from src import defs
>>> defs.get_all_job_defs()
```

## Environment Variables

Required in `.env`:
```bash
# API Keys
CONGRESS_API_KEY=your_key_here
ELECTION_API_KEY=your_key_here
LOBBYING_API_KEY=your_key_here  # optional

# MongoDB (already set in docker-compose)
MONGO_URI=mongodb://ltuser:ltpass@mongo:27017/admin
MONGO_DB=legal_tender

# Dagster PostgreSQL (already set in docker-compose)
DAGSTER_POSTGRES_USER=dagster
DAGSTER_POSTGRES_PASSWORD=dagster
DAGSTER_POSTGRES_DB=dagster
DAGSTER_POSTGRES_HOST=dagster-postgres
```

## Testing

### Test locally (without Docker)
```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export CONGRESS_API_KEY=your_key
export ELECTION_API_KEY=your_key
export MONGO_URI=mongodb://ltuser:ltpass@localhost:27017/admin

# Run a job in-process
python -c "from src.flows.test_flow import test_api_keys_job; test_api_keys_job.execute_in_process()"
```

### Test API connectivity
```bash
docker compose exec dagster-webserver python src/api/test_api_keys.py
```

## Useful Links

- **Dagster Docs**: https://docs.dagster.io/
- **Dagster Concepts**: https://docs.dagster.io/concepts
- **Job API Reference**: https://docs.dagster.io/_apidocs/jobs
- **Op API Reference**: https://docs.dagster.io/_apidocs/ops

## Common Issues

### "Module not found" errors
- Check `PYTHONPATH` is set to `/app` in Dockerfile
- Verify files are mounted correctly in docker-compose volumes

### Jobs don't appear in UI
- Check `workspace.yaml` points to correct file
- Verify `src/__init__.py` exports `defs`
- Restart webserver: `docker compose restart dagster-webserver`

### Can't connect to MongoDB
- Verify MongoDB is healthy: `docker compose ps mongo`
- Check credentials match in `.env` and docker-compose
- Verify network connectivity: both services in same Docker network

### PostgreSQL connection errors
- Wait for PostgreSQL to be fully ready (check healthcheck)
- Verify environment variables are set correctly
- Check logs: `docker compose logs dagster-postgres`
