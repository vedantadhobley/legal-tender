# Legal Tender# Legal Tender 💰🏛️# Legal Tender



**Follow the money. Map the influence. Expose the connections.**



Legal Tender is a data pipeline and analysis system that traces financial influence in US politics by connecting campaign donations to voting behavior through AI-powered bill analysis.> **Track the money. Follow the influence. Expose the connections.**> **Latest Update**: Implemented organized data repository with smart downloads that check `Last-Modified` headers. Downloads are now independent assets with weekly scheduling. See [Data Repository](#data-repository) and [Data Sync Strategy](#data-sync-strategy) sections below.



---



## The Big PictureA comprehensive system for analyzing financial influence in US politics by connecting campaign finance, lobbying activity, and voting records.## Project Overview



### What We're Building



A system that answers one fundamental question: **"Does money influence how politicians vote?"**---Legal Tender analyzes the influence of donors on US politicians by orchestrating data collection, enrichment, and AI-driven analysis. The project aims to:



Here's how:



1. **Track the Money**: Collect campaign finance data (who donated how much to which politicians)## 🎯 Project Vision1. Collect and keep up-to-date a list of all current US Congress members (House & Senate).

2. **Profile the Donors**: Use AI to generate policy preferences for each donor (what they want/don't want in legislation)

3. **Analyze the Bills**: Use AI to score each bill against every donor's preferences2. Gather and update donor data for each politician (amounts, names, organizations) using the Election (FEC) API.

4. **Map the Influence**: Connect donations → politicians → votes to quantify donor influence

Legal Tender reveals the hidden connections between money and power in Congress by:3. Profile each unique donor to determine which policies they support or oppose (using AI/NLP and web research).

### The Core Insight

4. Fetch and update upcoming bills and voting data from the Congress.gov API.

Every donor has policy goals. When they donate to a politician, they're (theoretically) trying to influence policy outcomes. By using AI to:

- **Profile donors** (what policies do they support/oppose?)1. **Tracking Campaign Finance**: Who donates to politicians? How much? From which industries?5. Use AI to compare each bill with donor policy stances, scoring each bill for each donor (+1 to -1 scale).

- **Analyze bills** (does this bill align with donor X's interests?)

- **Track votes** (did the politician vote in line with their donors' interests?)2. **Monitoring Lobbying**: Which corporations and interest groups are lobbying which members on which bills?6. Aggregate scores and donation amounts to predict how politicians may vote and quantify donor influence.



We can create an **influence score** that shows how strongly donations correlate with voting behavior.3. **Analyzing Votes**: How do donations and lobbying correlate with voting behavior?7. Store all data in MongoDB for auditability, traceability, and further analysis.



### Example Flow4. **Scoring Influence**: Quantify donor influence on policy decisions using AI-driven analysis.



```mermaid## Data Requirements

graph LR

    A[Pharmaceutical Company] -->|$500K donation| B[Senator X]### The Goal

    C[Healthcare Bill] -->|AI Analysis| D[Scores +0.8 for pharma interests]

    B -->|Votes YES| CBuild a transparent, auditable system that answers questions like:1. **Current Politicians**

    D -->|Maps to| A

    - "Does this senator vote in favor of their top donors?"	- List of all current US Congress members (House & Senate).

    style A fill:#f96,stroke:#333,stroke-width:2px

    style B fill:#69f,stroke:#333,stroke-width:2px- "Which industries have the most influence on this committee?"	- Source: [Congress.gov API](https://api.congress.gov/)

    style C fill:#9f6,stroke:#333,stroke-width:2px

    style D fill:#ff9,stroke:#333,stroke-width:2px- "Are lobbying dollars predictive of bill outcomes?"

```

2. **Donor Data**

**Result**: We can say "Senator X received $500K from pharma and voted in favor of a bill that scores +0.8 for pharma interests."

---	- List of donors for each politician, including donation amounts.

---

	- Source: [Election (FEC) API](https://api.open.fec.gov/developers/)

## System Architecture

## 📊 Current Status (October 2025)

### High-Level Overview

3. **Upcoming Bills**

```mermaid

graph TB### ✅ What's Working	- Data on upcoming bills, including text, summaries, and voting records.

    subgraph "Data Collection"

        A[FEC Campaign Finance Data]	- Source: [Congress.gov API](https://api.congress.gov/)

        B[Congress Member Info]

        C[Bill Text & Votes]**Campaign Finance Pipeline** (Bulk FEC Data)

    end

    - ✅ **538 members** with validated FEC candidate IDs4. **Lobbying Data**

    subgraph "Storage"

        D[(MongoDB)]- ✅ **4 election cycles** tracked (2020, 2022, 2024, 2026) = 8 years of data	- Federal lobbying filings and clients.

    end

    - ✅ **Per-cycle financial summaries**: total raised, spent, cash on hand, debts	- Source: [Senate LDA API](https://lda.senate.gov/api/redoc/v1/)

    subgraph "AI Analysis"

        E[Donor Profiler]- ✅ **Career totals**: aggregated across all cycles for rankings

        F[Bill Analyzer]

        G[Influence Scorer]- ✅ **Smart downloads**: Only fetches data when remote files are updated## Orchestration & Automation

    end

    - ✅ **Weekly automation**: Scheduled pipeline runs every Sunday at 2 AM UTC

    subgraph "Pipeline Orchestration"

        H[Dagster]- **Dagster** is used to orchestrate and schedule all data fetches, updates, and AI analysis flows.

    end

    **Data Coverage**- Data syncs (e.g., for politicians, bills, donors) run on a daily schedule, ensuring MongoDB always reflects the latest state.

    A --> H

    B --> H- 🏛️ All current House members (435)- Each entity (politician, donor, bill) is upserted by its unique ID for reliability and auditability.

    C --> H

    H --> D- 🏛️ All current Senators (100) including Class 3 (not running until 2028)- Audit fields (e.g., last_updated) and change logs are maintained for traceability.

    D --> E

    D --> F- 💰 Financial summaries from FEC webl files (~2-3MB per cycle)

    E --> G

    F --> G- 🗂️ Member→FEC mapping with validated candidate and committee IDs## AI/NLP Workflow

    G --> D

    

    style H fill:#69f,stroke:#333,stroke-width:3px

    style D fill:#f96,stroke:#333,stroke-width:3px**Architecture**1. For each unique donor, use AI/NLP to generate a profile of policy stances (FOR/AGAINST tables) using web research and public data.

```

- 🐳 Fully Dockerized with Dagster orchestration2. For each new or updated bill, use AI/NLP to extract key policy areas and compare them to donor stances.

### Technical Stack

- 🗄️ MongoDB for application data, PostgreSQL for Dagster metadata3. Score each bill for each donor (+1 to -1) based on alignment.

```mermaid

graph LR- 📅 Automated weekly schedules (deactivated by default for manual control)4. Aggregate scores and donation amounts to predict politician voting behavior and donor influence.

    A[Docker Compose] --> B[Dagster Orchestration]

    A --> C[MongoDB Storage]- 🔍 Clean data repository structure with smart caching

    A --> D[PostgreSQL Metadata]

    ## Next Steps

    B --> E[Python Pipeline]

    E --> F[FEC Data Parser]### 🚧 What's Next

    E --> G[AI Models]

    E --> H[Analysis Engine]1. Implement Dagster jobs for:

    

    style A fill:#2496ed,color:#fff**Phase 1: Individual Donor Data** (Next Priority)	- Fetching/updating Congress members

    style B fill:#654ff0,color:#fff

    style C fill:#47a248,color:#fff- 📥 Download FEC `indiv` files (~1.5GB per cycle)	- Fetching/updating donor data

    style D fill:#336791,color:#fff

```- 👤 Parse individual contribution records (donor names, employers, amounts, dates)	- Fetching/updating bills and votes



**Core Technologies**:- 🏢 Link donors to industries/organizations	- AI/NLP donor and bill analysis

- **Dagster**: Orchestrates data pipelines, schedules jobs, monitors runs

- **MongoDB**: Stores members, donors, bills, financial data, analysis results- 💵 Enable "which industries fund this person?" analysis2. Store and audit all data in MongoDB.

- **PostgreSQL**: Stores Dagster metadata (run history, logs, events)

- **Docker**: Containerizes everything for consistent deployment3. Build scoring, prediction, and visualization modules.

- **Python 3.11**: All data processing and AI integration

**Phase 2: PAC & Transfer Data**4. Document and test all jobs for reliability.

---

- 📥 Download FEC `pas2` files (~700MB per cycle)

## Current Status (October 2025)

- 🔗 Track PAC→Candidate transfers

### ✅ Phase 0: Foundation (COMPLETE)

- 🎯 Identify corporate PAC influence## Technology Stack

**What's Working**:

- Complete campaign finance data pipeline (FEC bulk data)- 📊 Separate individual vs organizational money

- 538 members with validated FEC candidate IDs

- 4 election cycles tracked (2020, 2022, 2024, 2026) = 8 years of financial data- **Orchestration**: Dagster (workflow scheduling and monitoring)

- Per-cycle financial summaries (total raised, spent, cash on hand, debts)

- Career totals aggregated across all cycles**Phase 3: Independent Expenditures (Dark Money)**- **Data Storage**: MongoDB (application data), PostgreSQL (Dagster metadata)

- Smart caching (only downloads when remote files update)

- Weekly automation (scheduled for Sunday 2 AM UTC)- 📥 Download FEC `oppexp` files (~300MB per cycle)- **Containerization**: Docker & Docker Compose



**MongoDB Collections**:- 💸 Track Super PAC spending FOR/AGAINST candidates- **Language**: Python 3.11

- `member_fec_mapping`: 538 members with FEC IDs, bio info, external identifiers

- `member_financial_summary`: Per-cycle financial data with career totals- 🕵️ Uncover "dark money" influence (often exceeds candidate's own spending)



**Data Coverage**:## Architecture

- All 435 House members

- All 100 Senators (including all 3 election classes)**Phase 4: Lobbying Data Integration**

- Financial summaries from webl files (~2-3MB per cycle)

- 📥 Fetch Senate LDA API lobbying disclosures```

---

- 🏢 Link corporations to lobbyists to bills┌─────────────────────────────────────────────────────────────┐

## The Pipeline: 6 Phases

- 📋 Track which bills are being lobbied on│               Dagster Webserver (Port 3000)                  │

### Phase 1: Individual Donor Data (NEXT)

- 🔗 Connect donors → lobbying → votes│                     User Interface & API                     │

**Goal**: Build a master donor list with contribution details

└────────────┬───────────────────────────┬────────────────────┘

**Tasks**:

- Download FEC `indiv` files (~1.5GB per cycle, 4 cycles = 6GB)**Phase 5: Bill Voting Records**             │                           │

- Parse individual contribution records (name, employer, occupation, amount, date)

- Link donors to members via committee IDs- 📥 Fetch Congress.gov API voting data   ┌─────────▼─────────┐       ┌────────▼──────────┐

- Deduplicate donors across cycles (same person/org = one entity)

- Create master donor list with aggregated contribution totals- 🗳️ Track how members vote on bills   │  Dagster Daemon   │       │  PostgreSQL       │



**Output**: - 🧮 Correlate donations/lobbying with votes   │  - Schedules      │       │  - Run Storage    │

- `donors` collection: Master list of unique donors

- `contributions` collection: Transaction-level donation records- 🎯 Score donor influence on policy outcomes   │  - Sensors        │       │  - Event Logs     │

- Each politician links to their donors with amounts

   │  - Run Queues     │       │  - Asset Metadata │

---

**Phase 6: AI/NLP Analysis**   └─────────┬─────────┘       └───────────────────┘

### Phase 2: AI Donor Profiling (THE CORE INNOVATION)

- 🤖 Profile donors by policy stances (FOR/AGAINST)             │

**Goal**: Use AI to generate policy preference profiles for each donor

- 📄 Analyze bill text for policy alignment   ┌─────────▼─────────┐       ┌───────────────────┐

**How It Works**:

- 📊 Score bills for each donor (+1 to -1 scale)   │    MongoDB        │       │  Mongo Express    │

```mermaid

graph TD- 🎯 Predict voting behavior based on donor influence   │  - Members        │◄──────┤  (Port 8081)      │

    A[Donor: Exxon Mobil] --> B[Research Context]

    B --> C[Industry: Oil & Gas]   │  - Bills          │       │  Admin UI         │

    B --> D[Public Statements]

    B --> E[Lobbying Records]---   │  - Donations      │       └───────────────────┘

    B --> F[Past Political Activity]

       └───────────────────┘

    C --> G[AI Model]

    D --> G## 🏗️ Architecture```

    E --> G

    F --> G

    

    G --> H[Policy Profile]### System Overview### Data Pipeline Flow

    H --> I[PROS: Policies They Support]

    H --> J[CONS: Policies They Oppose]

    

    I --> K[+ Fossil fuel subsidies<br/>+ Deregulation<br/>+ Tax breaks for energy companies]``````

    J --> L[- Carbon taxes<br/>- Renewable energy mandates<br/>- Environmental regulations]

    ┌─────────────────────────────────────────────────────────────┐Weekly Schedule (Sunday 2 AM UTC)

    style A fill:#f96,stroke:#333,stroke-width:2px

    style G fill:#69f,stroke:#333,stroke-width:2px│               Dagster Webserver (Port 3000)                  │         │

    style H fill:#9f6,stroke:#333,stroke-width:2px

```│                     Orchestration & Monitoring               │         ▼



**Tasks**:└────────────┬───────────────────────────┬────────────────────┘   data_sync_asset

- For each donor (company, individual, PAC):

  - Research their industry/sector             │                           │   • Check Last-Modified headers

  - Identify public policy positions

  - Find lobbying records (if available)   ┌─────────▼─────────┐       ┌────────▼──────────┐   • Download if remote newer

  - Analyze past political activity

- Use AI/NLP to generate:   │  Dagster Daemon   │       │  PostgreSQL       │   • Legislators file (1MB)

  - **PROS list**: Policies they would support (scored +1)

  - **CONS list**: Policies they would oppose (scored -1)   │  - Schedules      │       │  - Run History    │   • FEC bulk data (~4MB)

- Store profiles in MongoDB

   │  - Sensors        │       │  - Event Logs     │         │

**Output**:

- `donor_profiles` collection with AI-generated policy preferences   │  - Run Queues     │       │  - Metadata       │         │ Dependencies

- Each profile includes confidence scores and source citations

   └─────────┬─────────┘       └───────────────────┘         ▼

**Example Profile**:

```json             │   member_fec_mapping_asset

{

  "donor_id": "exxon_mobil",   ┌─────────▼─────────┐       ┌───────────────────┐   • Load cached data

  "name": "Exxon Mobil Corporation",

  "industry": "Oil & Gas",   │    MongoDB        │       │  Mongo Express    │   • Build member→FEC mapping

  "profile": {

    "pros": [   │  - Members        │◄──────┤  (Port 8081)      │   • Extract committee IDs

      {"policy": "Fossil fuel subsidies", "score": 1.0},

      {"policy": "Energy deregulation", "score": 0.9},   │  - Financial Data │       │  Admin UI         │   • Validate FEC IDs

      {"policy": "Tax breaks for oil companies", "score": 1.0}

    ],   │  - Mappings       │       └───────────────────┘         │

    "cons": [

      {"policy": "Carbon pricing", "score": -1.0},   └───────────────────┘         ▼

      {"policy": "Renewable energy mandates", "score": -0.8},

      {"policy": "EPA regulations", "score": -0.9}```     MongoDB

    ]

  },   Collection: member_fec_mapping

  "confidence": 0.85,

  "sources": ["lobbying records", "public statements", "industry analysis"]### Data Pipeline Flow```

}

```



---```## Quick Start



### Phase 3: Bill Analysis & Scoring (THE MAGIC)Weekly Schedule (Sunday 2 AM UTC)



**Goal**: Use AI to score bills against donor preferences         │### Prerequisites



**How It Works**:         ▼- Docker and Docker Compose



```mermaid   ┌──────────────────────────────────┐- `.env` file with API keys (see `.env.example`)

graph TD

    A[Bill: Clean Energy Act] --> B[AI Bill Analyzer]   │      data_sync_asset             │

    B --> C[Extract Key Provisions]

       │  • Check remote Last-Modified    │### Start Services

    C --> D[Provision 1: Carbon Tax]

    C --> E[Provision 2: Renewable Mandates]   │  • Download if updated           │

    C --> F[Provision 3: Fossil Fuel Phase-Out]

       │  • Legislators (1MB)             │**Production Mode** (default - isolated, secure):

    G[Donor Profile: Exxon Mobil] --> H[Policy Preferences]

    H --> I[CONS: Carbon taxes -1.0]   │  • FEC bulk data (~10MB)         │```bash

    H --> J[CONS: Renewable mandates -0.8]

    H --> K[CONS: Fossil fuel regulations -0.9]   └──────────┬───────────────────────┘./start.sh

    

    D --> L[Compare]              │# or manually: docker compose up -d

    I --> L

    E --> L              │ Dependencies```

    J --> L

    F --> L              ▼

    K --> L

       ┌──────────────────────────────────┐**Development Mode** (hot-reload, code changes apply instantly):

    L --> M[Bill Score for Exxon: -0.9]

    M --> N[HIGH NEGATIVE ALIGNMENT]   │   member_fec_mapping_asset       │```bash

    

    style A fill:#9f6,stroke:#333,stroke-width:2px   │  • Build member→FEC mapping      │./start.sh -dev

    style B fill:#69f,stroke:#333,stroke-width:2px

    style M fill:#f96,stroke:#333,stroke-width:2px   │  • Validate candidate IDs        │# or manually: docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d

```

   │  • Extract committee IDs         │```

**Tasks**:

- Fetch bills from Congress.gov API   └──────────┬───────────────────────┘

- Use AI/NLP to:

  - Extract key provisions from bill text              │**Wipe & Restart** (clear all data):

  - Identify policy areas (healthcare, energy, taxes, etc.)

  - Compare provisions to donor PROS/CONS lists              │ Dependencies```bash

  - Generate alignment score (-1 to +1) for each donor

- Store scores in MongoDB              ▼./start.sh -v           # Production mode, wipe volumes



**Scoring Logic**:   ┌──────────────────────────────────┐./start.sh -dev -v      # Dev mode, wipe volumes

- **+1.0**: Bill strongly aligns with donor's interests (all PROS, no CONS)

- **0.0**: Bill is neutral to donor's interests   │  member_financial_summary_asset  │```

- **-1.0**: Bill strongly opposes donor's interests (all CONS, no PROS)

   │  • Parse webl files (4 cycles)   │

**Output**:

- `bills` collection: Bill text, provisions, metadata   │  • Deduplicate by (ID,date,cycle)│### Access Applications

- `bill_scores` collection: Score for every donor × bill combination

   │  • Aggregate per-cycle totals    │- **Dagster UI**: http://localhost:3000 (job management and monitoring)

**Example**:

```json   │  • Compute career totals         │- **Mongo Express**: http://localhost:8081 (database UI, credentials: `ltuser`/`ltpass`)

{

  "bill_id": "hr1234_118",   └──────────┬───────────────────────┘

  "title": "Clean Energy Act",

  "donor_scores": [              │### Development vs Production Mode

    {"donor_id": "exxon_mobil", "score": -0.9, "reasoning": "Contains carbon tax, renewable mandates"},

    {"donor_id": "tesla", "score": 0.8, "reasoning": "Supports EV infrastructure, renewable energy"},              ▼

    {"donor_id": "sierra_club", "score": 1.0, "reasoning": "Aligns with environmental goals"}

  ]          MongoDB**Use Production Mode** (`./start.sh`) when:

}

```   member_financial_summary collection- Running in production/staging environments



---   {- You want isolated, secure containers



### Phase 4: Voting Records & Correlation     by_cycle: {2020: {...}, 2024: {...}},- Code changes are infrequent



**Goal**: Track how politicians vote and correlate with donor influence     career_totals: {...},- **Trade-off**: Must rebuild after each code change (~30-60s)



**Tasks**:     latest_cycle: "2026",

- Fetch voting records from Congress.gov API

- For each vote:     cycles_with_data: 3**Use Development Mode** (`./start.sh -dev`) when:

  - Get the bill score for each donor

  - Get the donation amount from that donor to the politician   }- Actively developing and testing jobs

  - Calculate weighted influence score

- Aggregate across all votes to create politician influence scores```- Making frequent code changes



**Influence Calculation**:- Debugging issues locally

```

For each politician:---- **Benefits**: Code changes apply instantly (no rebuild), database ports exposed for local tools

  For each vote:

    For each donor to that politician:- **Trade-off**: Less isolated (your local code folder is mounted in container)

      weighted_influence += (donation_amount × bill_score × vote_direction)

```## 🚀 Quick Start



**Output**:## Working with Data Assets

- `votes` collection: How each member voted on each bill

- `influence_scores` collection: Correlation between donations and votes### Prerequisites



**Example Analysis**:- Docker & Docker Compose### Available Assets

```json

{- `.env` file with API keys (copy from `.env.example`)

  "politician_id": "senator_x",

  "name": "Senator X",**Current Pipeline (Bulk Data Approach):**

  "total_donations": 5000000,

  "top_donor_influence": [### Start Services- **`data_sync`**: Downloads legislators file + FEC bulk data (~4MB). Smart caching with Last-Modified headers - only downloads if remote files are newer than local cache.

    {

      "donor": "Exxon Mobil",- **`member_fec_mapping`**: Builds complete member profiles (~538 docs in MongoDB). For each member, creates:

      "donated": 500000,

      "bills_scored": 12,```bash  - Validated FEC candidate IDs (their campaign committees)

      "alignment_rate": 0.92,

      "influence_score": 0.87# Production mode (isolated, secure)  - Committee IDs (PACs and other committees they control)

    }

  ]./start.sh  - Bio info (name, party, state, district, term dates)

}

```  - External IDs (bioguide, govtrack, opensecrets, etc.)



---# Development mode (hot-reload, code changes apply instantly)  - Optional: Photo URL, social media, office contact (via ProPublica API)



### Phase 5: Lobbying Integration./start.sh -dev  



**Goal**: Add lobbying data to the influence map  **Output**: MongoDB collection `member_fec_mapping` - the foundation for connecting members to financial data.



**Tasks**:# Wipe & restart (clear all data)

- Fetch lobbying disclosures from Senate LDA API

- Link lobbyists to donors (same companies)./start.sh -v           # Production**Legacy Assets (API-based, deprecated):**

- Track which bills are being lobbied

- Correlate lobbying activity with votes./start.sh -dev -v      # Development- **`congress_members`**: Fetches members from ProPublica Congress API



**Output**:```- **`member_donor_data`**: Fetches donor data from OpenFEC API (depends on congress_members)

- `lobbying` collection: Which companies lobby which members on which bills

- Enhanced influence scores that include lobbying pressure



---### Access Applications### Materializing Assets (Refreshing Data)



### Phase 6: Visualization & API- **Dagster UI**: http://localhost:3000 (orchestration & monitoring)



**Goal**: Make the data accessible and understandable- **Mongo Express**: http://localhost:8081 (database admin, `ltuser`/`ltpass`)**Via Dagster UI** (Recommended):



**Tasks**:1. Open http://localhost:3000

- Build REST API for querying influence data

- Create visualization dashboards:---2. Navigate to **"Assets"** tab

  - "Which industries fund this politician?"

  - "How does this politician vote relative to their donors?"3. Click on an asset to view details

  - "Which bills had the strongest donor influence?"

  - "Network graph: donors → politicians → votes"## 📁 Project Structure4. Click **"Materialize"** to refresh the data

- Generate automated reports

5. View metadata, lineage graph, and run history

---

```

## Quick Start

legal-tender/**Via Command Line:**

### Prerequisites

├── data/                     # Local data repository (mounted in Docker)```bash

- Docker & Docker Compose

- `.env` file with API keys (copy from `.env.example`)│   ├── legislators/          # Congress members with FEC IDs# Materialize a specific asset



### Running the System│   ├── fec/                  # FEC bulk data by cycledocker compose exec dagster-webserver dagster asset materialize --select congress_members -m src



**Production Mode** (isolated, secure):│   │   ├── 2020/

```bash

./start.sh│   │   ├── 2022/# Materialize all assets

```

│   │   ├── 2024/docker compose exec dagster-webserver dagster asset materialize --select "*" -m src

**Development Mode** (hot-reload, code changes apply instantly):

```bash│   │   └── 2026/

./start.sh -dev

```│   └── congress_api/         # API response cache# List all assets



**Wipe & Restart** (clear all data):│docker compose exec dagster-webserver dagster asset list -m src

```bash

./start.sh -v├── src/```

```

│   ├── __init__.py           # Dagster definitions (assets, jobs, schedules)

### Access Points

│   │### Available Jobs

- **Dagster UI**: http://localhost:3000 (pipeline orchestration)

- **Mongo Express**: http://localhost:8081 (database admin)│   ├── assets/               # Data pipeline assets

  - Username: `ltuser`

  - Password: `ltpass`│   │   ├── data_sync.py      # Downloads legislators + FEC bulk data**Current Jobs (Bulk Data Approach - Recommended):**



---│   │   ├── member_mapping.py # Builds member→FEC ID mapping- **`data_sync_job`**: Downloads legislators file + FEC bulk data (~4MB). Smart caching checks remote Last-Modified headers.



## Current Pipeline│   │   └── financial_summary.py  # Aggregates financial summaries- **`member_fec_mapping_job`**: Builds complete member→FEC mapping (~538 profiles) with validated FEC IDs and committee IDs. Takes ~30 seconds (or ~5 min with ProPublica enhancement).



### Available Assets│   │- **`bulk_data_pipeline_job`**: Complete pipeline - runs data_sync + member_fec_mapping in sequence. **Use this for full refresh.**



1. **`data_sync`**: Downloads legislators + FEC bulk data (~10MB, ~10 sec)│   ├── jobs/                 # Job definitions

2. **`member_fec_mapping`**: Builds member→FEC mapping (538 profiles, ~3 sec)

3. **`member_financial_summary`**: Aggregates financial data (4 cycles, ~5 sec)│   │   └── asset_jobs.py     # Materialization jobs**Legacy Jobs (API-based, deprecated):**



### Running the Pipeline│   │- **`congress_pipeline`**: Fetches members from ProPublica API



**Via Dagster UI** (recommended):│   ├── schedules/            # Automated schedules- **`donor_pipeline`**: Fetches donor data from OpenFEC API

1. Open http://localhost:3000

2. Go to **Jobs** → **`bulk_data_pipeline_job`**│   │   └── __init__.py       # Weekly pipeline schedule- **`full_pipeline`**: Legacy pipeline combining congress + donor jobs

3. Click **Launch Run**

│   │

**Via CLI**:

```bash│   ├── data/                 # Data repository management**Via Command Line:**

docker compose exec dagster-webserver dagster job execute -m src -j bulk_data_pipeline_job

```│   │   └── repository.py     # Smart caching, downloads```bash



### Scheduling│   │# Run the complete bulk data pipeline (recommended)



Enable the weekly schedule in Dagster UI:│   ├── resources/            # Shared resourcesdocker compose exec dagster-webserver dagster job execute -m src -j bulk_data_pipeline_job

1. **Overview** → **Schedules**

2. Find `weekly_bulk_data_pipeline`│   │   └── mongo.py          # MongoDB resource

3. Toggle **Start Schedule**

│   │# Or run individual jobs:

Runs every Sunday at 2 AM UTC automatically.

│   ├── api/                  # API clientsdocker compose exec dagster-webserver dagster job execute -m src -j data_sync_job

---

│   │   ├── congress_legislators.py  # GitHub legislators APIdocker compose exec dagster-webserver dagster job execute -m src -j member_fec_mapping_job

## Data Schema

│   │   ├── fec_bulk_data.py         # FEC bulk downloads

### Current Collections

│   │   ├── congress_api.py          # ProPublica Congress API# List all jobs

**`member_fec_mapping`** (538 documents)

```javascript│   │   ├── election_api.py          # OpenFEC APIdocker compose exec dagster-webserver dagster job list -m src

{

  _id: "bioguide_id",│   │   └── lobbying_api.py          # Senate LDA API```

  name: "Ruben Gallego",

  state: "AZ",│   │

  chamber: "senate",

  party: "Democrat",│   └── utils/                # Utilities### Schedules

  fec_candidate_ids: ["H4AZ07043", "S4AZ00139"],

  committee_ids: ["C00123456"],│

  term_start: "2025-01-03",

  term_end: "2031-01-03"├── docker-compose.yml        # Production services**Weekly Sunday Schedules** (must be manually enabled in Dagster UI):

}

```├── docker-compose.dev.yml    # Development overrides- **`weekly_data_sync`**: Downloads fresh data every Sunday at 2 AM UTC (~4MB download, ~30 seconds)



**`member_financial_summary`** (538 documents)├── Dockerfile                # Multi-stage build- **`weekly_bulk_data_pipeline`**: Complete pipeline every Sunday at 3 AM UTC (download + mapping, ~5-6 minutes with ProPublica)

```javascript

{├── requirements.txt          # Python dependencies

  _id: "bioguide_id",

  name: "Ruben Gallego",└── README.md                 # This file**To enable a schedule:**

  by_cycle: {

    "2020": {total_raised: 5123456.78, total_spent: 4987654.32, ...},```1. Open Dagster UI → **Overview** → **Schedules**

    "2022": {...},

    "2024": {...},2. Find the schedule (e.g., `weekly_bulk_data_pipeline`)

    "2026": {...}

  },---3. Toggle **Start Schedule**

  career_totals: {

    total_raised: 31121631.69,4. Verify it shows as **RUNNING**

    total_spent: 29774555.72,

    cycles_included: ["2020", "2022", "2024", "2026"]## 💾 Data Sources & File Formats

  },

  latest_cycle: "2026"Once enabled, the pipeline will automatically refresh your data every Sunday morning.

}

```### Current Data Sources



### Future Collections## Project Structure



**`donors`** (master list)**1. Congress Legislators** (GitHub - unitedstates/congress-legislators)

```javascript

{- **File**: `legislators-current.yaml` (~1MB)```

  _id: "donor_id",

  name: "Exxon Mobil Corporation",- **Contains**: All 538 current members with FEC candidate IDs, bio, termslegal-tender/

  type: "corporation",

  industry: "Oil & Gas",- **Update frequency**: Weekly (GitHub updates as changes occur)├── data/                     # Local data repository (mounted in Docker)

  total_contributed: 15000000,

  politicians_funded: ["senator_x", "rep_y"],- **Our sync**: Weekly Sunday 2 AM UTC│   ├── legislators/          # GitHub legislators data

  cycles_active: ["2020", "2022", "2024"]

}│   │   ├── current.yaml      # Current members with FEC IDs

```

**2. FEC Bulk Data** (Federal Election Commission)│   │   └── metadata.json

**`donor_profiles`** (AI-generated)

```javascript- **Files**: `webl` (candidate financial summaries) (~2-3MB per cycle)│   ├── fec/                  # FEC bulk data

{

  _id: "donor_id",- **Cycles tracked**: 2020, 2022, 2024, 2026 (8 years)│   │   ├── 2024/             # 2024 election cycle

  pros: [

    {policy: "Fossil fuel subsidies", score: 1.0},- **Contains**: Aggregated totals per candidate (raised, spent, cash, debts)│   │   │   ├── candidates.zip

    {policy: "Energy deregulation", score: 0.9}

  ],- **Update frequency**: Monthly (~15th of each month)│   │   │   ├── committees.zip

  cons: [

    {policy: "Carbon pricing", score: -1.0},- **Our sync**: Weekly Sunday 2 AM UTC (checks Last-Modified headers)│   │   │   ├── linkages.zip

    {policy: "Renewable mandates", score: -0.8}

  ],│   │   │   ├── summaries/

  confidence: 0.85,

  generated_at: "2025-10-14",### Future Data Sources (Planned)│   │   │   └── transactions/

  model: "gpt-4"

}│   │   └── 2026/             # 2026 election cycle

```

**3. Individual Contributions** (`indiv` files)│   └── congress_api/         # ProPublica API cache

**`bills`** (Congress.gov)

```javascript- **Size**: ~1.5GB per cycle├── src/

{

  _id: "bill_id",- **Contains**: Donor names, employers, occupations, amounts, dates│   ├── __init__.py           # Dagster definitions (assets, jobs, schedules)

  title: "Clean Energy Act",

  congress: 118,- **Use case**: "Which industries fund this person?"│   ├── assets/               # Data assets

  type: "hr",

  number: 1234,- **Implementation**: Phase 1 (next priority)│   │   ├── data_sync.py      # Downloads & syncs external data

  provisions: ["carbon tax", "renewable mandates"],

  introduced_date: "2024-01-15"│   │   ├── member_mapping.py # Builds member→FEC mapping

}

```**4. Committee Transfers** (`pas2` files)│   │   ├── congress.py       # congress_members asset



**`bill_scores`** (AI analysis)- **Size**: ~700MB per cycle│   │   └── donors.py         # member_donor_data asset

```javascript

{- **Contains**: PAC→Candidate transfers, party transfers│   ├── jobs/                 # Asset jobs

  bill_id: "hr1234_118",

  donor_scores: [- **Use case**: "Corporate PAC influence"│   │   └── asset_jobs.py     # All job definitions

    {donor_id: "exxon_mobil", score: -0.9},

    {donor_id: "tesla", score: 0.8},- **Implementation**: Phase 2│   ├── schedules/            # Automated schedules

    {donor_id: "sierra_club", score: 1.0}

  ],│   │   └── __init__.py       # Weekly Sunday schedules

  analyzed_at: "2025-10-14"

}**5. Independent Expenditures** (`oppexp` files)│   ├── data/                 # Data repository management

```

- **Size**: ~300MB per cycle│   │   └── repository.py     # DataRepository class

**`votes`** (Congress.gov)

```javascript- **Contains**: Super PAC spending FOR/AGAINST candidates│   ├── resources/            # Shared resources

{

  bill_id: "hr1234_118",- **Use case**: "Dark money influence"│   │   └── mongo.py          # MongoDB resource

  vote_date: "2024-03-20",

  members: [- **Implementation**: Phase 3│   ├── api/                  # API clients

    {bioguide_id: "senator_x", position: "Yea"},

    {bioguide_id: "senator_y", position: "Nay"}│   │   ├── congress_legislators.py  # GitHub legislators API

  ],

  result: "Passed"**6. Lobbying Disclosures** (Senate LDA API)│   │   ├── fec_bulk_data.py         # FEC bulk data API

}

```- **Contains**: Lobbyist filings, clients, bills lobbied, issues│   │   ├── congress_api.py          # ProPublica Congress API



**`influence_scores`** (calculated)- **Use case**: "Corporate lobbying activity"│   │   ├── election_api.py          # OpenFEC API

```javascript

{- **Implementation**: Phase 4│   │   └── lobbying_api.py          # Senate LDA API

  politician_id: "senator_x",

  total_donations: 5000000,│   └── utils/                # Utility functions

  donor_alignment: [

    {**7. Voting Records** (Congress.gov API)├── inspect_data.py           # Repository inspection tool

      donor_id: "exxon_mobil",

      donated: 500000,- **Contains**: Bill votes, member positions, outcomes├── dagster.yaml              # Dagster instance configuration

      bills_scored: 12,

      votes_aligned: 11,- **Use case**: "Correlation analysis (money → votes)"├── workspace.yaml            # Code location configuration

      alignment_rate: 0.92,

      influence_score: 0.87- **Implementation**: Phase 5├── docker-compose.yml        # Service definitions

    }

  ],├── Dockerfile                # Multi-stage container build

  overall_influence: 0.78

}### FEC Data: How It Works└── requirements.txt          # Python dependencies

```

```

---

**Election Cycles** (2-year periods named by election year)

## Development

```## Data Repository

### Project Structure

2020 Cycle: Jan 2019 - Dec 2020

```

legal-tender/2022 Cycle: Jan 2021 - Dec 2022All downloaded data is organized in the `data/` directory with a clean structure:

├── src/

│   ├── __init__.py              # Dagster definitions2024 Cycle: Jan 2023 - Dec 2024

│   ├── assets/                  # Data pipeline assets

│   │   ├── data_sync.py2026 Cycle: Jan 2025 - Dec 2026### Directory Layout

│   │   ├── member_mapping.py

│   │   └── financial_summary.py```- **`data/legislators/`**: Congress members data from GitHub (1MB)

│   ├── jobs/                    # Job definitions

│   ├── schedules/               # Automated schedules- **`data/fec/2024/`**: 2024 election cycle FEC bulk data

│   ├── api/                     # External API clients

│   ├── data/                    # Data repository management**Why Track 4 Cycles?**- **`data/fec/2026/`**: 2026 election cycle FEC bulk data

│   └── resources/               # Shared resources (MongoDB, etc)

├── data/                        # Local data cache- **Senate has 3 classes** with staggered 6-year terms:- **`data/congress_api/`**: Cached ProPublica API responses

├── docker-compose.yml

├── Dockerfile  - Class 1: Elected 2024 (next: 2030)

└── README.md

```  - Class 2: Elected 2026 (next: 2032)### File Naming



### Adding New Assets  - Class 3: Elected 2022 (next: 2028) ← Would be missing with only 2024/2026!Friendly names instead of cryptic FEC codes:



1. Create asset file in `src/assets/`- **House**: All 435 seats up every 2 years- `candidates.zip` (was `cn24.zip`)

2. Export in `src/assets/__init__.py`

3. Register in `src/__init__.py` Definitions- **Result**: 4 cycles = complete coverage of all 538 members- `committees.zip` (was `cm24.zip`)

4. Restart: `docker compose restart dagster-webserver dagster-daemon`

- `linkages.zip` (was `ccl24.zip`)

### Environment Variables

**Filing Schedule**- `independent_expenditures.zip` (was `oppexp24.zip`)

Required in `.env`:

```bash- **Quarterly reports**: Q1 (Apr 15), Q2 (Jul 15), Q3 (Oct 15), Year-End (Jan 31)

CONGRESS_API_KEY=your_key

ELECTION_API_KEY=your_key- **Monthly reports**: High-volume committees (>$50K/month)### Inspection Tools

LOBBYING_API_KEY=your_key

MONGO_URI=mongodb://ltuser:ltpass@mongo:27017/admin- **Election reports**: Pre-General (12 days before), Post-General (30 days after)

```

- **FEC publishes bulk files**: Monthly (~15th)**`inspect_data.py`** - View repository contents and stats:

---

```bash

## Roadmap

**Update Strategy**python3 inspect_data.py              # Overview of downloaded files

### Immediate Next Steps

- **Smart caching**: Check `Last-Modified` headers before downloadingpython3 inspect_data.py --metadata   # Detailed metadata

- [ ] **Phase 1**: Download and parse FEC `indiv` files

- [ ] Build master donor list- **Weekly sync**: Runs every Sunday 2 AM UTCpython3 inspect_data.py --json       # JSON output

- [ ] Link donors to politicians

- **Efficient**: Only downloads if remote files are newer than local cache```

### Core Innovation



- [ ] **Phase 2**: AI donor profiling system

- [ ] Generate PROS/CONS for each donor---**`test_data_repository.py`** - Test downloads and structure:

- [ ] **Phase 3**: AI bill analysis system

- [ ] Score bills against donor preferences```bash



### Analysis & Correlation## 🔧 Working with the Pipelinepython3 test_data_repository.py      # Test structure + optional downloads



- [ ] **Phase 4**: Voting records integration```

- [ ] Calculate influence scores

- [ ] **Phase 5**: Lobbying data integration### Available Assets



### Presentation## Data Sync Strategy



- [ ] **Phase 6**: Build REST API**Current Pipeline:**

- [ ] Create visualization dashboards

- [ ] Generate automated reports1. **`data_sync`**: Downloads legislators + FEC bulk data (~10MB, ~10 seconds)### Smart Downloads



---2. **`member_fec_mapping`**: Builds member→FEC mapping (538 profiles, ~3 seconds)The `data_sync` asset checks remote `Last-Modified` headers before downloading:



## Contributing3. **`member_financial_summary`**: Aggregates financial data (4 cycles, ~5 seconds)1. If local file doesn't exist → download



This is an active research project. Areas for contribution:2. Check remote `Last-Modified` timestamp



- **Data Engineering**: Parsers, ETL pipelines, data quality### Available Jobs3. Compare to local file modification time

- **AI/ML**: Donor profiling models, bill analysis NLP

- **Analysis**: Statistical correlation, influence scoring algorithms4. Only download if remote is newer

- **Visualization**: Dashboards, network graphs, reports

**Current Jobs:**5. Fallback to 7-day age check if remote check fails

---

- **`data_sync_job`**: Download fresh data

## License

- **`member_fec_mapping_job`**: Build member mappings### Configuration

[Add your license]

- **`member_financial_summary_job`**: Aggregate financial summaries```python

---

- **`bulk_data_pipeline_job`**: Full pipeline (all 3 assets in sequence) ⭐ **Use this**DataSyncConfig(

## Acknowledgments

    force_refresh=False,           # Force re-download

**Data Sources**:

- [Federal Election Commission](https://www.fec.gov/) - Campaign finance data### Running Jobs    cycles=["2024", "2026"],       # FEC cycles to sync

- [unitedstates/congress-legislators](https://github.com/unitedstates/congress-legislators) - Legislator data

- [Congress.gov API](https://api.congress.gov/) - Bills and votes    sync_legislators=True,         # Legislators file

- [Senate LDA](https://lda.senate.gov/) - Lobbying disclosures

**Via Dagster UI** (Recommended):    sync_fec_core=True,            # Core files (~4MB)

**Built With**:

- [Dagster](https://dagster.io/) - Data orchestration1. Open http://localhost:3000    sync_fec_summaries=False,      # Summaries (~7MB)

- [MongoDB](https://www.mongodb.com/) - Document storage

- [Docker](https://www.docker.com/) - Containerization2. Navigate to **"Jobs"** tab    sync_fec_transactions=False,   # Transactions (~4GB)



---3. Select **`bulk_data_pipeline_job`**    check_remote_modified=True,    # Check Last-Modified headers



**Status**: Phase 0 Complete | Phase 1 Starting Soon | Last Updated: October 20254. Click **"Launch Run"**)


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
