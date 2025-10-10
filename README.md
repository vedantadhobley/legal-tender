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


## Dagster Workflow

This project uses **Dagster** for workflow orchestration with a lightweight, dockerized setup.

## How It Works

- Dagster provides a UI (Dagit) to visualize, monitor, and execute data pipelines
- All jobs are defined in `src/flows/` using Dagster's `@op` and `@job` decorators
- The `src/__init__.py` file exports a `Definitions` object that Dagster discovers automatically
- PostgreSQL is used for Dagster's run storage and event logs

## Architecture

- **dagster-webserver**: Serves the Dagit UI and handles job execution
- **dagster-daemon**: Runs background processes for schedules and sensors
- **dagster-postgres**: Stores Dagster metadata (run history, logs, etc.)
- **mongo**: Stores application data (members, bills, etc.)

## Step-by-Step Usage

1. **Start All Services**

	```bash
	docker compose up --build -d
	```

	This will start:
	- Dagster webserver (UI)
	- Dagster daemon (for schedules)
	- PostgreSQL (for Dagster metadata)
	- MongoDB (for application data)
	- Mongo Express (database UI)

2. **Access Dagster UI**

	- Open http://localhost:3000 to view and manage jobs
	- View job definitions, execution history, and logs

3. **Access MongoDB UI**

	- Open http://localhost:8081 for Mongo Express
	- View stored data (members, bills, etc.)

## Running Jobs

### Via Dagster UI

1. Navigate to http://localhost:3000
2. Click on a job (e.g., `member_ingestion_job` or `api_test_job`)
3. Click "Launch Run" to execute the job
4. Monitor progress in real-time

### Via Command Line

```bash
# Run a specific job
docker compose exec dagster-webserver dagster job execute -m src -j member_ingestion_job

# List all jobs
docker compose exec dagster-webserver dagster job list -m src
```

## Adding New Jobs

1. Create a new file in `src/flows/` (e.g., `my_new_flow.py`)
2. Define your ops and job using Dagster decorators:
   ```python
   from dagster import op, job, OpExecutionContext

   @op
   def my_op(context: OpExecutionContext):
       context.log.info("Running my op")

   @job
   def my_job():
       my_op()
   ```
3. Import the job in `src/__init__.py` and add it to the `Definitions` object
4. Restart Dagster: `docker compose restart dagster-webserver dagster-daemon`
5. The new job will appear in the Dagster UI

## Notes

- **No manual registration needed**: Jobs are automatically discovered from `src/__init__.py`
- **All jobs are declarative**: Define everything in Python code
- **Lightweight**: No cloud dependencies, runs entirely in Docker

---