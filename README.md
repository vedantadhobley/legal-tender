# Legal Tender 💰🏛️# Legal Tender



> **Track the money. Follow the influence. Expose the connections.**> **Latest Update**: Implemented organized data repository with smart downloads that check `Last-Modified` headers. Downloads are now independent assets with weekly scheduling. See [Data Repository](#data-repository) and [Data Sync Strategy](#data-sync-strategy) sections below.



A comprehensive system for analyzing financial influence in US politics by connecting campaign finance, lobbying activity, and voting records.## Project Overview



---Legal Tender analyzes the influence of donors on US politicians by orchestrating data collection, enrichment, and AI-driven analysis. The project aims to:



## 🎯 Project Vision1. Collect and keep up-to-date a list of all current US Congress members (House & Senate).

2. Gather and update donor data for each politician (amounts, names, organizations) using the Election (FEC) API.

Legal Tender reveals the hidden connections between money and power in Congress by:3. Profile each unique donor to determine which policies they support or oppose (using AI/NLP and web research).

4. Fetch and update upcoming bills and voting data from the Congress.gov API.

1. **Tracking Campaign Finance**: Who donates to politicians? How much? From which industries?5. Use AI to compare each bill with donor policy stances, scoring each bill for each donor (+1 to -1 scale).

2. **Monitoring Lobbying**: Which corporations and interest groups are lobbying which members on which bills?6. Aggregate scores and donation amounts to predict how politicians may vote and quantify donor influence.

3. **Analyzing Votes**: How do donations and lobbying correlate with voting behavior?7. Store all data in MongoDB for auditability, traceability, and further analysis.

4. **Scoring Influence**: Quantify donor influence on policy decisions using AI-driven analysis.

## Data Requirements

### The Goal

Build a transparent, auditable system that answers questions like:1. **Current Politicians**

- "Does this senator vote in favor of their top donors?"	- List of all current US Congress members (House & Senate).

- "Which industries have the most influence on this committee?"	- Source: [Congress.gov API](https://api.congress.gov/)

- "Are lobbying dollars predictive of bill outcomes?"

2. **Donor Data**

---	- List of donors for each politician, including donation amounts.

	- Source: [Election (FEC) API](https://api.open.fec.gov/developers/)

## 📊 Current Status (October 2025)

3. **Upcoming Bills**

### ✅ What's Working	- Data on upcoming bills, including text, summaries, and voting records.

	- Source: [Congress.gov API](https://api.congress.gov/)

**Campaign Finance Pipeline** (Bulk FEC Data)

- ✅ **538 members** with validated FEC candidate IDs4. **Lobbying Data**

- ✅ **4 election cycles** tracked (2020, 2022, 2024, 2026) = 8 years of data	- Federal lobbying filings and clients.

- ✅ **Per-cycle financial summaries**: total raised, spent, cash on hand, debts	- Source: [Senate LDA API](https://lda.senate.gov/api/redoc/v1/)

- ✅ **Career totals**: aggregated across all cycles for rankings

- ✅ **Smart downloads**: Only fetches data when remote files are updated## Orchestration & Automation

- ✅ **Weekly automation**: Scheduled pipeline runs every Sunday at 2 AM UTC

- **Dagster** is used to orchestrate and schedule all data fetches, updates, and AI analysis flows.

**Data Coverage**- Data syncs (e.g., for politicians, bills, donors) run on a daily schedule, ensuring MongoDB always reflects the latest state.

- 🏛️ All current House members (435)- Each entity (politician, donor, bill) is upserted by its unique ID for reliability and auditability.

- 🏛️ All current Senators (100) including Class 3 (not running until 2028)- Audit fields (e.g., last_updated) and change logs are maintained for traceability.

- 💰 Financial summaries from FEC webl files (~2-3MB per cycle)

- 🗂️ Member→FEC mapping with validated candidate and committee IDs## AI/NLP Workflow



**Architecture**1. For each unique donor, use AI/NLP to generate a profile of policy stances (FOR/AGAINST tables) using web research and public data.

- 🐳 Fully Dockerized with Dagster orchestration2. For each new or updated bill, use AI/NLP to extract key policy areas and compare them to donor stances.

- 🗄️ MongoDB for application data, PostgreSQL for Dagster metadata3. Score each bill for each donor (+1 to -1) based on alignment.

- 📅 Automated weekly schedules (deactivated by default for manual control)4. Aggregate scores and donation amounts to predict politician voting behavior and donor influence.

- 🔍 Clean data repository structure with smart caching

## Next Steps

### 🚧 What's Next

1. Implement Dagster jobs for:

**Phase 1: Individual Donor Data** (Next Priority)	- Fetching/updating Congress members

- 📥 Download FEC `indiv` files (~1.5GB per cycle)	- Fetching/updating donor data

- 👤 Parse individual contribution records (donor names, employers, amounts, dates)	- Fetching/updating bills and votes

- 🏢 Link donors to industries/organizations	- AI/NLP donor and bill analysis

- 💵 Enable "which industries fund this person?" analysis2. Store and audit all data in MongoDB.

3. Build scoring, prediction, and visualization modules.

**Phase 2: PAC & Transfer Data**4. Document and test all jobs for reliability.

- 📥 Download FEC `pas2` files (~700MB per cycle)

- 🔗 Track PAC→Candidate transfers

- 🎯 Identify corporate PAC influence## Technology Stack

- 📊 Separate individual vs organizational money

- **Orchestration**: Dagster (workflow scheduling and monitoring)

**Phase 3: Independent Expenditures (Dark Money)**- **Data Storage**: MongoDB (application data), PostgreSQL (Dagster metadata)

- 📥 Download FEC `oppexp` files (~300MB per cycle)- **Containerization**: Docker & Docker Compose

- 💸 Track Super PAC spending FOR/AGAINST candidates- **Language**: Python 3.11

- 🕵️ Uncover "dark money" influence (often exceeds candidate's own spending)

## Architecture

**Phase 4: Lobbying Data Integration**

- 📥 Fetch Senate LDA API lobbying disclosures```

- 🏢 Link corporations to lobbyists to bills┌─────────────────────────────────────────────────────────────┐

- 📋 Track which bills are being lobbied on│               Dagster Webserver (Port 3000)                  │

- 🔗 Connect donors → lobbying → votes│                     User Interface & API                     │

└────────────┬───────────────────────────┬────────────────────┘

**Phase 5: Bill Voting Records**             │                           │

- 📥 Fetch Congress.gov API voting data   ┌─────────▼─────────┐       ┌────────▼──────────┐

- 🗳️ Track how members vote on bills   │  Dagster Daemon   │       │  PostgreSQL       │

- 🧮 Correlate donations/lobbying with votes   │  - Schedules      │       │  - Run Storage    │

- 🎯 Score donor influence on policy outcomes   │  - Sensors        │       │  - Event Logs     │

   │  - Run Queues     │       │  - Asset Metadata │

**Phase 6: AI/NLP Analysis**   └─────────┬─────────┘       └───────────────────┘

- 🤖 Profile donors by policy stances (FOR/AGAINST)             │

- 📄 Analyze bill text for policy alignment   ┌─────────▼─────────┐       ┌───────────────────┐

- 📊 Score bills for each donor (+1 to -1 scale)   │    MongoDB        │       │  Mongo Express    │

- 🎯 Predict voting behavior based on donor influence   │  - Members        │◄──────┤  (Port 8081)      │

   │  - Bills          │       │  Admin UI         │

---   │  - Donations      │       └───────────────────┘

   └───────────────────┘

## 🏗️ Architecture```



### System Overview### Data Pipeline Flow



``````

┌─────────────────────────────────────────────────────────────┐Weekly Schedule (Sunday 2 AM UTC)

│               Dagster Webserver (Port 3000)                  │         │

│                     Orchestration & Monitoring               │         ▼

└────────────┬───────────────────────────┬────────────────────┘   data_sync_asset

             │                           │   • Check Last-Modified headers

   ┌─────────▼─────────┐       ┌────────▼──────────┐   • Download if remote newer

   │  Dagster Daemon   │       │  PostgreSQL       │   • Legislators file (1MB)

   │  - Schedules      │       │  - Run History    │   • FEC bulk data (~4MB)

   │  - Sensors        │       │  - Event Logs     │         │

   │  - Run Queues     │       │  - Metadata       │         │ Dependencies

   └─────────┬─────────┘       └───────────────────┘         ▼

             │   member_fec_mapping_asset

   ┌─────────▼─────────┐       ┌───────────────────┐   • Load cached data

   │    MongoDB        │       │  Mongo Express    │   • Build member→FEC mapping

   │  - Members        │◄──────┤  (Port 8081)      │   • Extract committee IDs

   │  - Financial Data │       │  Admin UI         │   • Validate FEC IDs

   │  - Mappings       │       └───────────────────┘         │

   └───────────────────┘         ▼

```     MongoDB

   Collection: member_fec_mapping

### Data Pipeline Flow```



```## Quick Start

Weekly Schedule (Sunday 2 AM UTC)

         │### Prerequisites

         ▼- Docker and Docker Compose

   ┌──────────────────────────────────┐- `.env` file with API keys (see `.env.example`)

   │      data_sync_asset             │

   │  • Check remote Last-Modified    │### Start Services

   │  • Download if updated           │

   │  • Legislators (1MB)             │**Production Mode** (default - isolated, secure):

   │  • FEC bulk data (~10MB)         │```bash

   └──────────┬───────────────────────┘./start.sh

              │# or manually: docker compose up -d

              │ Dependencies```

              ▼

   ┌──────────────────────────────────┐**Development Mode** (hot-reload, code changes apply instantly):

   │   member_fec_mapping_asset       │```bash

   │  • Build member→FEC mapping      │./start.sh -dev

   │  • Validate candidate IDs        │# or manually: docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d

   │  • Extract committee IDs         │```

   └──────────┬───────────────────────┘

              │**Wipe & Restart** (clear all data):

              │ Dependencies```bash

              ▼./start.sh -v           # Production mode, wipe volumes

   ┌──────────────────────────────────┐./start.sh -dev -v      # Dev mode, wipe volumes

   │  member_financial_summary_asset  │```

   │  • Parse webl files (4 cycles)   │

   │  • Deduplicate by (ID,date,cycle)│### Access Applications

   │  • Aggregate per-cycle totals    │- **Dagster UI**: http://localhost:3000 (job management and monitoring)

   │  • Compute career totals         │- **Mongo Express**: http://localhost:8081 (database UI, credentials: `ltuser`/`ltpass`)

   └──────────┬───────────────────────┘

              │### Development vs Production Mode

              ▼

          MongoDB**Use Production Mode** (`./start.sh`) when:

   member_financial_summary collection- Running in production/staging environments

   {- You want isolated, secure containers

     by_cycle: {2020: {...}, 2024: {...}},- Code changes are infrequent

     career_totals: {...},- **Trade-off**: Must rebuild after each code change (~30-60s)

     latest_cycle: "2026",

     cycles_with_data: 3**Use Development Mode** (`./start.sh -dev`) when:

   }- Actively developing and testing jobs

```- Making frequent code changes

- Debugging issues locally

---- **Benefits**: Code changes apply instantly (no rebuild), database ports exposed for local tools

- **Trade-off**: Less isolated (your local code folder is mounted in container)

## 🚀 Quick Start

## Working with Data Assets

### Prerequisites

- Docker & Docker Compose### Available Assets

- `.env` file with API keys (copy from `.env.example`)

**Current Pipeline (Bulk Data Approach):**

### Start Services- **`data_sync`**: Downloads legislators file + FEC bulk data (~4MB). Smart caching with Last-Modified headers - only downloads if remote files are newer than local cache.

- **`member_fec_mapping`**: Builds complete member profiles (~538 docs in MongoDB). For each member, creates:

```bash  - Validated FEC candidate IDs (their campaign committees)

# Production mode (isolated, secure)  - Committee IDs (PACs and other committees they control)

./start.sh  - Bio info (name, party, state, district, term dates)

  - External IDs (bioguide, govtrack, opensecrets, etc.)

# Development mode (hot-reload, code changes apply instantly)  - Optional: Photo URL, social media, office contact (via ProPublica API)

./start.sh -dev  

  **Output**: MongoDB collection `member_fec_mapping` - the foundation for connecting members to financial data.

# Wipe & restart (clear all data)

./start.sh -v           # Production**Legacy Assets (API-based, deprecated):**

./start.sh -dev -v      # Development- **`congress_members`**: Fetches members from ProPublica Congress API

```- **`member_donor_data`**: Fetches donor data from OpenFEC API (depends on congress_members)



### Access Applications### Materializing Assets (Refreshing Data)

- **Dagster UI**: http://localhost:3000 (orchestration & monitoring)

- **Mongo Express**: http://localhost:8081 (database admin, `ltuser`/`ltpass`)**Via Dagster UI** (Recommended):

1. Open http://localhost:3000

---2. Navigate to **"Assets"** tab

3. Click on an asset to view details

## 📁 Project Structure4. Click **"Materialize"** to refresh the data

5. View metadata, lineage graph, and run history

```

legal-tender/**Via Command Line:**

├── data/                     # Local data repository (mounted in Docker)```bash

│   ├── legislators/          # Congress members with FEC IDs# Materialize a specific asset

│   ├── fec/                  # FEC bulk data by cycledocker compose exec dagster-webserver dagster asset materialize --select congress_members -m src

│   │   ├── 2020/

│   │   ├── 2022/# Materialize all assets

│   │   ├── 2024/docker compose exec dagster-webserver dagster asset materialize --select "*" -m src

│   │   └── 2026/

│   └── congress_api/         # API response cache# List all assets

│docker compose exec dagster-webserver dagster asset list -m src

├── src/```

│   ├── __init__.py           # Dagster definitions (assets, jobs, schedules)

│   │### Available Jobs

│   ├── assets/               # Data pipeline assets

│   │   ├── data_sync.py      # Downloads legislators + FEC bulk data**Current Jobs (Bulk Data Approach - Recommended):**

│   │   ├── member_mapping.py # Builds member→FEC ID mapping- **`data_sync_job`**: Downloads legislators file + FEC bulk data (~4MB). Smart caching checks remote Last-Modified headers.

│   │   └── financial_summary.py  # Aggregates financial summaries- **`member_fec_mapping_job`**: Builds complete member→FEC mapping (~538 profiles) with validated FEC IDs and committee IDs. Takes ~30 seconds (or ~5 min with ProPublica enhancement).

│   │- **`bulk_data_pipeline_job`**: Complete pipeline - runs data_sync + member_fec_mapping in sequence. **Use this for full refresh.**

│   ├── jobs/                 # Job definitions

│   │   └── asset_jobs.py     # Materialization jobs**Legacy Jobs (API-based, deprecated):**

│   │- **`congress_pipeline`**: Fetches members from ProPublica API

│   ├── schedules/            # Automated schedules- **`donor_pipeline`**: Fetches donor data from OpenFEC API

│   │   └── __init__.py       # Weekly pipeline schedule- **`full_pipeline`**: Legacy pipeline combining congress + donor jobs

│   │

│   ├── data/                 # Data repository management**Via Command Line:**

│   │   └── repository.py     # Smart caching, downloads```bash

│   │# Run the complete bulk data pipeline (recommended)

│   ├── resources/            # Shared resourcesdocker compose exec dagster-webserver dagster job execute -m src -j bulk_data_pipeline_job

│   │   └── mongo.py          # MongoDB resource

│   │# Or run individual jobs:

│   ├── api/                  # API clientsdocker compose exec dagster-webserver dagster job execute -m src -j data_sync_job

│   │   ├── congress_legislators.py  # GitHub legislators APIdocker compose exec dagster-webserver dagster job execute -m src -j member_fec_mapping_job

│   │   ├── fec_bulk_data.py         # FEC bulk downloads

│   │   ├── congress_api.py          # ProPublica Congress API# List all jobs

│   │   ├── election_api.py          # OpenFEC APIdocker compose exec dagster-webserver dagster job list -m src

│   │   └── lobbying_api.py          # Senate LDA API```

│   │

│   └── utils/                # Utilities### Schedules

│

├── docker-compose.yml        # Production services**Weekly Sunday Schedules** (must be manually enabled in Dagster UI):

├── docker-compose.dev.yml    # Development overrides- **`weekly_data_sync`**: Downloads fresh data every Sunday at 2 AM UTC (~4MB download, ~30 seconds)

├── Dockerfile                # Multi-stage build- **`weekly_bulk_data_pipeline`**: Complete pipeline every Sunday at 3 AM UTC (download + mapping, ~5-6 minutes with ProPublica)

├── requirements.txt          # Python dependencies

└── README.md                 # This file**To enable a schedule:**

```1. Open Dagster UI → **Overview** → **Schedules**

2. Find the schedule (e.g., `weekly_bulk_data_pipeline`)

---3. Toggle **Start Schedule**

4. Verify it shows as **RUNNING**

## 💾 Data Sources & File Formats

Once enabled, the pipeline will automatically refresh your data every Sunday morning.

### Current Data Sources

## Project Structure

**1. Congress Legislators** (GitHub - unitedstates/congress-legislators)

- **File**: `legislators-current.yaml` (~1MB)```

- **Contains**: All 538 current members with FEC candidate IDs, bio, termslegal-tender/

- **Update frequency**: Weekly (GitHub updates as changes occur)├── data/                     # Local data repository (mounted in Docker)

- **Our sync**: Weekly Sunday 2 AM UTC│   ├── legislators/          # GitHub legislators data

│   │   ├── current.yaml      # Current members with FEC IDs

**2. FEC Bulk Data** (Federal Election Commission)│   │   └── metadata.json

- **Files**: `webl` (candidate financial summaries) (~2-3MB per cycle)│   ├── fec/                  # FEC bulk data

- **Cycles tracked**: 2020, 2022, 2024, 2026 (8 years)│   │   ├── 2024/             # 2024 election cycle

- **Contains**: Aggregated totals per candidate (raised, spent, cash, debts)│   │   │   ├── candidates.zip

- **Update frequency**: Monthly (~15th of each month)│   │   │   ├── committees.zip

- **Our sync**: Weekly Sunday 2 AM UTC (checks Last-Modified headers)│   │   │   ├── linkages.zip

│   │   │   ├── summaries/

### Future Data Sources (Planned)│   │   │   └── transactions/

│   │   └── 2026/             # 2026 election cycle

**3. Individual Contributions** (`indiv` files)│   └── congress_api/         # ProPublica API cache

- **Size**: ~1.5GB per cycle├── src/

- **Contains**: Donor names, employers, occupations, amounts, dates│   ├── __init__.py           # Dagster definitions (assets, jobs, schedules)

- **Use case**: "Which industries fund this person?"│   ├── assets/               # Data assets

- **Implementation**: Phase 1 (next priority)│   │   ├── data_sync.py      # Downloads & syncs external data

│   │   ├── member_mapping.py # Builds member→FEC mapping

**4. Committee Transfers** (`pas2` files)│   │   ├── congress.py       # congress_members asset

- **Size**: ~700MB per cycle│   │   └── donors.py         # member_donor_data asset

- **Contains**: PAC→Candidate transfers, party transfers│   ├── jobs/                 # Asset jobs

- **Use case**: "Corporate PAC influence"│   │   └── asset_jobs.py     # All job definitions

- **Implementation**: Phase 2│   ├── schedules/            # Automated schedules

│   │   └── __init__.py       # Weekly Sunday schedules

**5. Independent Expenditures** (`oppexp` files)│   ├── data/                 # Data repository management

- **Size**: ~300MB per cycle│   │   └── repository.py     # DataRepository class

- **Contains**: Super PAC spending FOR/AGAINST candidates│   ├── resources/            # Shared resources

- **Use case**: "Dark money influence"│   │   └── mongo.py          # MongoDB resource

- **Implementation**: Phase 3│   ├── api/                  # API clients

│   │   ├── congress_legislators.py  # GitHub legislators API

**6. Lobbying Disclosures** (Senate LDA API)│   │   ├── fec_bulk_data.py         # FEC bulk data API

- **Contains**: Lobbyist filings, clients, bills lobbied, issues│   │   ├── congress_api.py          # ProPublica Congress API

- **Use case**: "Corporate lobbying activity"│   │   ├── election_api.py          # OpenFEC API

- **Implementation**: Phase 4│   │   └── lobbying_api.py          # Senate LDA API

│   └── utils/                # Utility functions

**7. Voting Records** (Congress.gov API)├── inspect_data.py           # Repository inspection tool

- **Contains**: Bill votes, member positions, outcomes├── dagster.yaml              # Dagster instance configuration

- **Use case**: "Correlation analysis (money → votes)"├── workspace.yaml            # Code location configuration

- **Implementation**: Phase 5├── docker-compose.yml        # Service definitions

├── Dockerfile                # Multi-stage container build

### FEC Data: How It Works└── requirements.txt          # Python dependencies

```

**Election Cycles** (2-year periods named by election year)

```## Data Repository

2020 Cycle: Jan 2019 - Dec 2020

2022 Cycle: Jan 2021 - Dec 2022All downloaded data is organized in the `data/` directory with a clean structure:

2024 Cycle: Jan 2023 - Dec 2024

2026 Cycle: Jan 2025 - Dec 2026### Directory Layout

```- **`data/legislators/`**: Congress members data from GitHub (1MB)

- **`data/fec/2024/`**: 2024 election cycle FEC bulk data

**Why Track 4 Cycles?**- **`data/fec/2026/`**: 2026 election cycle FEC bulk data

- **Senate has 3 classes** with staggered 6-year terms:- **`data/congress_api/`**: Cached ProPublica API responses

  - Class 1: Elected 2024 (next: 2030)

  - Class 2: Elected 2026 (next: 2032)### File Naming

  - Class 3: Elected 2022 (next: 2028) ← Would be missing with only 2024/2026!Friendly names instead of cryptic FEC codes:

- **House**: All 435 seats up every 2 years- `candidates.zip` (was `cn24.zip`)

- **Result**: 4 cycles = complete coverage of all 538 members- `committees.zip` (was `cm24.zip`)

- `linkages.zip` (was `ccl24.zip`)

**Filing Schedule**- `independent_expenditures.zip` (was `oppexp24.zip`)

- **Quarterly reports**: Q1 (Apr 15), Q2 (Jul 15), Q3 (Oct 15), Year-End (Jan 31)

- **Monthly reports**: High-volume committees (>$50K/month)### Inspection Tools

- **Election reports**: Pre-General (12 days before), Post-General (30 days after)

- **FEC publishes bulk files**: Monthly (~15th)**`inspect_data.py`** - View repository contents and stats:

```bash

**Update Strategy**python3 inspect_data.py              # Overview of downloaded files

- **Smart caching**: Check `Last-Modified` headers before downloadingpython3 inspect_data.py --metadata   # Detailed metadata

- **Weekly sync**: Runs every Sunday 2 AM UTCpython3 inspect_data.py --json       # JSON output

- **Efficient**: Only downloads if remote files are newer than local cache```



---**`test_data_repository.py`** - Test downloads and structure:

```bash

## 🔧 Working with the Pipelinepython3 test_data_repository.py      # Test structure + optional downloads

```

### Available Assets

## Data Sync Strategy

**Current Pipeline:**

1. **`data_sync`**: Downloads legislators + FEC bulk data (~10MB, ~10 seconds)### Smart Downloads

2. **`member_fec_mapping`**: Builds member→FEC mapping (538 profiles, ~3 seconds)The `data_sync` asset checks remote `Last-Modified` headers before downloading:

3. **`member_financial_summary`**: Aggregates financial data (4 cycles, ~5 seconds)1. If local file doesn't exist → download

2. Check remote `Last-Modified` timestamp

### Available Jobs3. Compare to local file modification time

4. Only download if remote is newer

**Current Jobs:**5. Fallback to 7-day age check if remote check fails

- **`data_sync_job`**: Download fresh data

- **`member_fec_mapping_job`**: Build member mappings### Configuration

- **`member_financial_summary_job`**: Aggregate financial summaries```python

- **`bulk_data_pipeline_job`**: Full pipeline (all 3 assets in sequence) ⭐ **Use this**DataSyncConfig(

    force_refresh=False,           # Force re-download

### Running Jobs    cycles=["2024", "2026"],       # FEC cycles to sync

    sync_legislators=True,         # Legislators file

**Via Dagster UI** (Recommended):    sync_fec_core=True,            # Core files (~4MB)

1. Open http://localhost:3000    sync_fec_summaries=False,      # Summaries (~7MB)

2. Navigate to **"Jobs"** tab    sync_fec_transactions=False,   # Transactions (~4GB)

3. Select **`bulk_data_pipeline_job`**    check_remote_modified=True,    # Check Last-Modified headers

4. Click **"Launch Run"**)

5. Monitor progress in real-time```



**Via Command Line**:### Presets

```bash- **Minimal** (fast, ~4MB): legislators + core FEC files

# Run full pipeline- **Standard** (recommended, ~11MB): + summaries

docker compose exec dagster-webserver dagster job execute -m src -j bulk_data_pipeline_job- **Complete** (large, ~4GB): + transaction files



# Or materialize specific assets## Development

docker compose exec dagster-webserver dagster asset materialize -m src --select data_sync

```### Adding New Assets



### Schedules1. **Create an asset file** in `src/assets/` (e.g., `bills.py`):

   ```python

**Available Schedule:**   from dagster import asset, AssetExecutionContext, Output, MetadataValue

- **`weekly_bulk_data_pipeline`**: Runs complete pipeline every Sunday at 2 AM UTC   from src.resources.mongo import MongoDBResource

   from src.api.congress_api import get_bills

**To Enable:**

1. Open Dagster UI → **Overview** → **Schedules**   @asset(

2. Find `weekly_bulk_data_pipeline`       name="congress_bills",

3. Toggle **Start Schedule**       description="Current bills from Congress.gov API",

4. Verify status shows **RUNNING**       group_name="congress",

       compute_kind="api",

Once enabled, your data automatically refreshes every Sunday morning! 🎉   )

   def congress_bills_asset(

---       context: AssetExecutionContext,

       mongo: MongoDBResource

## 📊 Current Data Schema   ) -> Output[list]:

       """Fetch and persist current bills."""

### MongoDB Collections       bills = get_bills()

       

**1. `member_fec_mapping`** (538 documents)       with mongo.get_client() as client:

```javascript           collection = mongo.get_collection(client, "bills")

{           # Store bills...

  _id: "bioguide_id",              // Primary key (e.g., "G000574")       

  name: "Ruben Gallego",       return Output(

  state: "AZ",           value=bills,

  district: null,                  // Senate = null           metadata={

  chamber: "senate",               "total_bills": len(bills),

  party: "Democrat",               "preview": MetadataValue.json(bills[:3]),

             }

  // FEC identifiers       )

  fec_candidate_id: "S4AZ00139",   // Current campaign committee   ```

  fec_candidate_ids: [             // All committees (House + Senate)

    "H4AZ07043",                   // Historical House committee2. **Export the asset** in `src/assets/__init__.py`:

    "S4AZ00139"                    // Current Senate committee   ```python

  ],   from src.assets.bills import congress_bills_asset

  committee_ids: ["C00123456"],    // PACs, leadership PACs   

     __all__ = [

  // External IDs       "congress_members_asset",

  bioguide: "G000574",       "member_donor_data_asset",

  govtrack: 412612,       "congress_bills_asset",

  opensecrets: "N00039289",   ]

     ```

  // Metadata

  term_start: "2025-01-03",3. **Update definitions** in `src/__init__.py`:

  term_end: "2031-01-03",   ```python

  updated_at: ISODate("2025-10-14")   from src.assets import congress_members_asset, member_donor_data_asset, congress_bills_asset

}   

```   defs = Definitions(

       assets=[congress_members_asset, member_donor_data_asset, congress_bills_asset],

**2. `member_financial_summary`** (538 documents)       resources={"mongo": mongo_resource},

```javascript   )

{   ```

  _id: "bioguide_id",

  name: "Ruben Gallego",4. **Restart services**:

  candidate_ids: ["H4AZ07043", "S4AZ00139"],   ```bash

     docker compose restart dagster-webserver dagster-daemon

  // Per-cycle breakdown (clean, deduplicated within each cycle)   ```

  by_cycle: {

    "2020": {5. **Materialize in UI**: Go to Assets tab → Click your new asset → Materialize

      total_raised: 5123456.78,

      total_spent: 4987654.32,### Asset Dependencies

      cash_on_hand: 135802.46,

      individual_contributions: 3456789.01,To make an asset depend on another:

      debts: 0.00,```python

      num_entries: 2              // Deduped entries@asset

    },def donor_analysis(congress_members, member_donor_data):

    "2022": {...},    # This asset depends on both congress_members and member_donor_data

    "2024": {...},    # Dagster will automatically materialize them first

    "2026": {...}    pass

  },```

  

  // Career totals (sum across all cycles, rounded to cents)### Viewing Data

  career_totals: {

    total_raised: 31121631.69,    // No more .689999998!Access MongoDB via Mongo Express at http://localhost:8081:

    total_spent: 29774555.72,- **Database**: `legal_tender`

    individual_contributions: 22456789.45,- **Collections**: `members`, `bills`, etc.

    cycles_included: ["2020", "2022", "2024", "2026"]- **Credentials**: `ltuser` / `ltpass`

  },

  ## Configuration

  // Quick stats

  latest_cycle: "2026",### Credentials

  cycles_with_data: 4,All services use standardized credentials:

  - **Username**: `ltuser`

  // Metadata- **Password**: `ltpass`

  updated_at: ISODate("2025-10-14")

}### Environment Variables

```Required in `.env`:

- `CONGRESS_API_KEY`: Congress.gov API key

**Key Design Decisions:**- `ELECTION_API_KEY`: OpenFEC API key  

- ✅ **Per-cycle storage**: Enables temporal analysis (election year vs off-year)- `LOBBYING_API_KEY`: Senate LDA API key

- ✅ **Deduplication within cycles**: (CAND_ID, CVG_END_DT, cycle) as unique key- `MONGO_URI`: MongoDB connection string (default: `mongodb://ltuser:ltpass@mongo:27017/admin`)

- ✅ **Career aggregation**: Sum across cycles for rankings and "quick hover" stats

- ✅ **Rounded to cents**: No floating-point errors (`.69` not `.689999998`)## Troubleshooting

- ✅ **Flexible**: Can analyze by cycle OR by career

### Services won't start

---```bash

# Check logs

## 🛠️ Developmentdocker compose logs



### Development Mode# Restart services

docker compose restart

Use `./start.sh -dev` for hot-reload development:

- ✅ Code changes apply instantly (no rebuild needed)# Clean restart

- ✅ Local `src/` folder mounted in containerdocker compose down -v && docker compose up -d

- ✅ Database ports exposed (27017, 5432) for local tools```

- ⚠️ Less isolated (your local code runs in container)

### Permission errors

### Adding New AssetsAll containers run as non-root user `dagster` (UID 1000). If you encounter permission issues, rebuild:

```bash

1. **Create asset file** in `src/assets/` (e.g., `individual_donors.py`):docker compose build --no-cache

   ```pythondocker compose up -d

   from dagster import asset, AssetExecutionContext```

   from src.resources.mongo import MongoDBResource

   ### Database connection issues

   @asset(```bash

       name="individual_donors",# Check PostgreSQL

       description="Parse FEC indiv files for donor details",docker compose exec dagster-postgres pg_isready -U ltuser -d dagster

       deps=["member_fec_mapping"],  # Dependency

       group_name="finance",# Check MongoDB

   )docker compose exec mongo mongosh --eval "db.adminCommand('ping')"

   def individual_donors_asset(```

       context: AssetExecutionContext,

       mongo: MongoDBResource---
   ):
       # Your logic here
       pass
   ```

2. **Export in** `src/assets/__init__.py`:
   ```python
   from src.assets.individual_donors import individual_donors_asset
   
   __all__ = [..., "individual_donors_asset"]
   ```

3. **Register in** `src/__init__.py`:
   ```python
   from src.assets import individual_donors_asset
   
   defs = Definitions(
       assets=[..., individual_donors_asset],
       ...
   )
   ```

4. **Restart**:
   ```bash
   docker compose restart dagster-webserver dagster-daemon
   ```

### Viewing Data

**Mongo Express** (http://localhost:8081):
- Database: `legal_tender`
- Collections: `member_fec_mapping`, `member_financial_summary`
- Credentials: `ltuser` / `ltpass`

---

## 📝 Configuration

### Environment Variables

Required in `.env`:
```bash
# API Keys
CONGRESS_API_KEY=your_congress_gov_key
ELECTION_API_KEY=your_openfec_key
LOBBYING_API_KEY=your_senate_lda_key

# Database
MONGO_URI=mongodb://ltuser:ltpass@mongo:27017/admin
```

### Service Credentials

All services use standardized credentials:
- **Username**: `ltuser`
- **Password**: `ltpass`

---

## 🐛 Troubleshooting

### Services won't start
```bash
# Check logs
docker compose logs

# Restart specific service
docker compose restart dagster-webserver

# Nuclear option (wipes data!)
./start.sh -v
```

### Permission errors
Containers run as non-root user `dagster` (UID 1000). If you see permission issues:
```bash
docker compose build --no-cache
docker compose up -d
```

### Database connection issues
```bash
# Check MongoDB
docker compose exec mongo mongosh --eval "db.adminCommand('ping')"

# Check PostgreSQL
docker compose exec dagster-postgres pg_isready -U ltuser -d dagster
```

---

## 📈 Roadmap

### Phase 1: Individual Donors (Next Up)
- [ ] Download FEC `indiv` files (1.5GB per cycle)
- [ ] Parse donor records (name, employer, amount, date)
- [ ] Link donors to members via committee IDs
- [ ] Aggregate by industry/sector
- [ ] Enable "which industries fund X?" queries

### Phase 2: PAC Money
- [ ] Download FEC `pas2` files (700MB per cycle)
- [ ] Track PAC→Candidate transfers
- [ ] Identify corporate PACs
- [ ] Separate individual vs organizational money

### Phase 3: Dark Money
- [ ] Download FEC `oppexp` files (300MB per cycle)
- [ ] Track Super PAC spending FOR/AGAINST
- [ ] Identify largest independent expenditures
- [ ] Compare to candidate's own spending

### Phase 4: Lobbying
- [ ] Integrate Senate LDA API
- [ ] Track corporate lobbying activity
- [ ] Link lobbyists to bills
- [ ] Connect donors to lobbying firms

### Phase 5: Voting Records
- [ ] Integrate Congress.gov voting API
- [ ] Track member votes on bills
- [ ] Correlate votes with donor industries
- [ ] Calculate donor influence scores

### Phase 6: AI Analysis
- [ ] Profile donors by policy stances
- [ ] Analyze bill text for policy alignment
- [ ] Score bills for donor alignment
- [ ] Predict votes based on donor influence
- [ ] Generate influence reports

---

## 🤝 Contributing

This project is under active development. Key areas for contribution:
- 📊 **Data Engineering**: New data sources, parsers, ETL pipelines
- 🔬 **Analysis**: Statistical models, correlation analysis
- 🤖 **AI/ML**: NLP for bill analysis, donor profiling
- 🎨 **Visualization**: Dashboards, influence maps
- 📖 **Documentation**: Guides, tutorials, data dictionaries

---

## 📜 License

[Add your license here]

---

## 🙏 Acknowledgments

**Data Sources:**
- [Federal Election Commission (FEC)](https://www.fec.gov/) - Campaign finance data
- [unitedstates/congress-legislators](https://github.com/unitedstates/congress-legislators) - Legislator data
- [ProPublica Congress API](https://projects.propublica.org/api-docs/congress-api/) - Congressional data
- [Senate LDA](https://lda.senate.gov/) - Lobbying disclosures

**Built With:**
- [Dagster](https://dagster.io/) - Data orchestration
- [MongoDB](https://www.mongodb.com/) - Document storage
- [Docker](https://www.docker.com/) - Containerization

---

**Questions?** Open an issue or check existing documentation in the codebase.

**Status**: 🚧 Active Development | Last Updated: October 2025
