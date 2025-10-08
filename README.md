# Legal Tender

## Project Overview

Legal Tender analyzes the influence of donors on US politicians by orchestrating data collection, enrichment, and AI-driven analysis. The project aims to:

1. Collect and keep up-to-date a list of all current US Congress members (House & Senate).
2. Gather and update donor data for each politician (amounts, names, organizations) using the FEC API.
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
	- Source: [FEC API](https://api.open.fec.gov/developers/)

3. **Upcoming Bills**
	- Data on upcoming bills, including text, summaries, and voting records.
	- Source: [Congress.gov API](https://api.congress.gov/)

4. **Lobbying Data**
	- Federal lobbying filings and clients.
	- Source: [Senate LDA API](https://lda.senate.gov/api/redoc/v1/)

## Orchestration & Automation

- **Prefect** is used to orchestrate and schedule all data fetches, updates, and AI analysis flows.
- Data syncs (e.g., for politicians, bills, donors) run on a daily schedule, ensuring MongoDB always reflects the latest state.
- Each entity (politician, donor, bill) is upserted by its unique ID for reliability and auditability.
- Audit fields (e.g., last_updated) and change logs are maintained for traceability.

## AI/NLP Workflow

1. For each unique donor, use AI/NLP to generate a profile of policy stances (FOR/AGAINST tables) using web research and public data.
2. For each new or updated bill, use AI/NLP to extract key policy areas and compare them to donor stances.
3. Score each bill for each donor (+1 to -1) based on alignment.
4. Aggregate scores and donation amounts to predict politician voting behavior and donor influence.

## Next Steps

1. Implement Prefect flows for:
	- Fetching/updating Congress members
	- Fetching/updating donor data
	- Fetching/updating bills and votes
	- AI/NLP donor and bill analysis
2. Store and audit all data in MongoDB.
3. Build scoring, prediction, and visualization modules.
4. Document and test all flows for reliability.


## Prefect Registration Workflow


# Prefect Registration Workflow

This project uses a dedicated **setup container** to register all Prefect flows and deployments. This ensures that all orchestration is managed by Prefect, and no legacy app logic runs by default.

## How It Works

- The `setup` service in `docker-compose.yml` builds the project image and runs `setup.py`.
- `setup.py` scans the `src/flows/` directory and registers each flow as a Prefect deployment.
- The setup container exits after registration, ensuring idempotency and auditability.

## Step-by-Step Usage

1. **Build and Start Services**

	```bash
	docker compose up --build prefect-server mongo mongo-express
	# Wait for Prefect server and MongoDB to be ready (see logs)
	```

2. **Register Prefect Flows/Deployments**

	```bash
	docker compose run --rm setup
	# This runs setup.py, which registers all flows in src/flows/ as Prefect deployments
	# The container will exit after registration
	```


3. **Start Prefect Worker**

	```bash
	docker compose up -d prefect-worker
	# The worker will pick up scheduled or manual flow runs
	```

4. **Access Prefect UI**

	- Open http://localhost:4200 to view and manage flows, deployments, and runs.

## Notes

- **Idempotency:** Running the setup container multiple times is safe; it will re-register flows as needed.
- **Adding New Flows:** Add new flow scripts to `src/flows/` and re-run the setup container to register them.
- **No Legacy Entrypoint:** The setup container does not run any legacy app logic; all execution is Prefect-driven.

## Example: Registering a New Flow

1. Add `src/flows/my_new_flow.py`.
2. Run:
	```bash
	docker compose run --rm setup
	```
3. The new flow will appear in the Prefect UI as a deployment.

---