# Legal Tender

## Project Overview

Legal Tender analyzes financial influence on US politicians by tracking ALL money flows - not just direct donations, but the far more powerful independent expenditures (Super PAC spending) that determine who gets elected in the first place. The project:

1. **Collects Congress member data**: All current House & Senate members from Congress.gov API
2. **Maps to FEC records**: Links each member to their FEC candidate ID and campaign committees
3. **Tracks comprehensive financial influence**:
   - **Financial summaries**: Total receipts, disbursements, cash on hand, debt
   - **Direct contributions** (Schedule A): Who donates TO campaigns (limited: $3,300 individual, $5,000 PAC)
   - **Independent expenditures** (Schedule E): Who spends FOR/AGAINST candidates (UNLIMITED - this is where real power lies)
   - Future: Loans, party coordinated spending, electioneering (dark money ads)
4. **Reveals power dynamics**: Super PACs like AIPAC's "United Democracy Project" spent $26M to defeat ONE candidate (Cori Bush) - that's not influence, that's choosing who can even vote in Congress
5. **Enables analysis**: With complete financial profiles stored in MongoDB, we can identify which donors/Super PACs control which politicians and predict voting behavior

## Data Sources & Money Flows

### 1. Congress Members
- **Source**: [Congress.gov API](https://api.congress.gov/)
- **Data**: All current House & Senate members with bioguide IDs

### 2. Financial Influence Tracking
- **Source**: [OpenFEC API](https://api.open.fec.gov/developers/)
- **Money Vehicles**:
  - **Schedule A** (Direct Contributions): Donations TO campaigns
    - Individuals: $3,300 max per election
    - PACs: $5,000 max per election
    - These buy access but don't determine WHO gets elected
  - **Schedule E** (Independent Expenditures): Spending FOR/AGAINST candidates
    - **UNLIMITED** spending by Super PACs, unions, corporations
    - Cannot coordinate with campaigns but can spend infinite money
    - **This is real power**: Determines primary winners, removes incumbents
    - Example: AIPAC's "United Democracy Project" spent $26M AGAINST Cori Bush, $12M AGAINST Jamaal Bowman - both defeated
  - **Financial Summaries**: Total raised, spent, cash on hand, debt
  - **Future additions**:
    - Schedule C (Loans): Self-funding and loan patterns
    - Schedule F (Party Coordinated Spending): National party support
    - Electioneering Communications: Dark money "issue ads"

### 3. Bills & Voting (Future)
- **Source**: [Congress.gov API](https://api.congress.gov/)
- **Data**: Bill text, votes, sponsorship - to correlate with financial influence

### 4. Lobbying Data (Future)
- **Source**: [Senate LDA API](https://lda.senate.gov/api/redoc/v1/)
- **Data**: Federal lobbying filings to complete the influence picture

## Orchestration & Automation

- **Dagster** is used to orchestrate and schedule all data fetches, updates, and AI analysis flows.
- Data syncs (e.g., for politicians, bills, donors) run on a daily schedule, ensuring MongoDB always reflects the latest state.
- Each entity (politician, donor, bill) is upserted by its unique ID for reliability and auditability.
- Audit fields (e.g., last_updated) and change logs are maintained for traceability.

## MongoDB Collections

### `members`
Complete Congress member data from Congress.gov API with bioguide IDs, party, state, district, etc.

### `member_finance`
Financial profile for each member with FEC data:
```javascript
{
  bioguide_id: "B001230",  // Tammy Baldwin
  member_name: "Baldwin, Tammy",
  fec_candidate_id: "S2WI00094",
  
  financial_summary: {
    total_receipts: 15234567.89,
    total_disbursements: 12345678.90,
    cash_on_hand: 2888888.99,
    debts_owed: 0,
    coverage_end_date: "2024-09-30"
  },
  
  direct_contributions: {
    total_contributors: 10000,
    total_amount: 5234567.89,
    top_donors: [
      {
        name: "ActBlue",
        total: 1234567.89,
        contribution_count: 2500
      }
      // ... top 100 donors
    ]
  },
  
  independent_expenditures: {
    for: {  // Spending TO SUPPORT this candidate
      total_amount: 8765432.10,
      total_expenditures: 150,
      top_spenders: [
        {
          committee_name: "Senate Majority PAC",
          committee_id: "C00484642",
          total: 3456789.12,
          expenditure_count: 45
        }
        // ... top 20 Super PACs supporting
      ]
    },
    against: {  // Spending TO OPPOSE this candidate
      total_amount: 1234567.89,
      total_expenditures: 25,
      top_spenders: [
        {
          committee_name: "Conservative Group",
          committee_id: "C00123456",
          total: 876543.21,
          expenditure_count: 15
        }
        // ... top 20 Super PACs opposing
      ]
    }
  },
  
  last_updated: "2024-10-11T12:34:56Z"
}
```

### Future Collections
- **`unique_donors`**: Aggregate view of all individual/organizational donors across all members
- **`super_pacs`**: Super PAC profiles with all candidates they support/oppose and total spending
- **`bills`**: Upcoming bills, votes, sponsors
- **`lobbying`**: Federal lobbying filings

## Understanding the Power Dynamic

### Direct Contributions (Limited Influence)
- Individual donors: Max $3,300 per election
- PACs: Max $5,000 per election
- **Purpose**: Buy access, influence HOW elected officials vote
- **Example**: Tech industry donates to both parties to get favorable regulations

### Independent Expenditures (Ultimate Power)
- Super PACs, unions, corporations: **UNLIMITED** spending
- Cannot coordinate with campaigns but can spend any amount FOR or AGAINST
- **Purpose**: Determine WHO gets elected in the first place
- **Example**: AIPAC's "United Democracy Project" (Committee C00799031):
  - Spent $26M AGAINST Cori Bush → She lost her primary
  - Spent $12M AGAINST Jamaal Bowman → He lost his primary
  - Spent $34M FOR Jimmy Gomez → He won
  - **This isn't lobbying, it's choosing who sits in Congress**

### Why This Matters
If you can spend unlimited money to elect/defeat candidates, you don't need to lobby them later. You already own them. This is why tracking Schedule E (independent expenditures) is more important than Schedule A (direct contributions).

## Next Steps

**Current Phase (Complete)**:
- ✅ Map all 538 Congress members to FEC candidate IDs
- ✅ Collect financial summaries for all members
- ✅ Track direct contributions (Schedule A, top 100 donors)
- ✅ Track independent expenditures (Schedule E, top 20 spenders, separate for/against)

**Next Phase**:
1. Run full_pipeline job to populate MongoDB with all financial data
2. Verify AIPAC/UDP spending appears correctly for targeted members
3. Create aggregate collections (unique_donors, super_pacs)
4. Add additional money vehicles (loans, party spending, electioneering)
5. Fetch bill and voting data to correlate with financial influence
6. Build analysis/visualization layer to show who controls which politicians


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
- **`member_fec_mapping`**: Maps each Congress member to their FEC candidate ID and campaign committees (depends on congress_members)
- **`member_finance`**: Complete financial profile for each member including:
  - Financial summary (receipts, disbursements, cash on hand, debt)
  - Direct contributions (Schedule A - top 100 donors)
  - Independent expenditures (Schedule E - Super PAC spending FOR/AGAINST, top 20 spenders)
  - Future: loans, party spending, electioneering

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
- **`finance_mapping`**: Maps all Congress members to FEC candidate IDs (depends on congress_members)
- **`finance_pipeline`**: Fetches complete financial data for all mapped members (depends on member_fec_mapping)
- **`full_pipeline`**: Runs complete pipeline: congress_members → member_fec_mapping → member_finance (recommended for scheduled runs)

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
│   │   └── donors.py         # member_fec_mapping + member_finance assets
│   ├── jobs/                 # Asset materialization jobs
│   │   └── asset_jobs.py     # congress_pipeline, finance_mapping, finance_pipeline, full_pipeline
│   ├── resources/            # Shared resources
│   │   └── mongo.py          # MongoDB resource
│   ├── api/                  # API clients
│   │   ├── congress_api.py   # Congress.gov API client
│   │   ├── election_api.py   # OpenFEC API client (all FEC schedules)
│   │   └── lobbying_api.py   # Senate LDA API client
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
- **Collections**: `members`, `member_finance`, etc.
- **Credentials**: `ltuser` / `ltpass`

**Query Examples** (via mongosh):
```javascript
// Find members with AIPAC opposition spending
db.member_finance.find({
  "independent_expenditures.against.top_spenders": {
    $elemMatch: { committee_name: /United Democracy Project/i }
  }
})

// Top 10 members by Super PAC support
db.member_finance.find({}).sort({
  "independent_expenditures.for.total_amount": -1
}).limit(10)

// Members with most total debt
db.member_finance.find({}).sort({
  "financial_summary.debts_owed": -1
}).limit(10)
```

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