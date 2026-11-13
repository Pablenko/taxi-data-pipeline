# Data Engineering Practical Assessment: Medallion Pipeline on Azure

_Oct 9, 2026 · @Anurag Dhuria_ — transcribed from `task1.jpeg`–`task6.jpeg`

## Overview

Build a small, production-style batch pipeline on Azure. Azure Data Factory ingests public taxi trip data, and Azure Databricks refines it through bronze, silver and gold layers.

| Item | Detail |
|---|---|
| Expected effort | 4 to 6 hours of hands-on work |
| Submission window | 5 calendar days from receipt |
| Follow-up | 60-minute review session: you walk through the solution and make a small change live |
| What we assess | Design judgement, correctness, incremental behaviour, data quality, governance, and how clearly you explain trade-offs |

Sound engineering decisions matter more than volume of code. A small solution that reruns safely and is well explained beats a large one that does not.

## Scenario

A city mobility analytics team needs a trusted view of yellow taxi activity to report revenue and demand by zone. The source publishes one Parquet file per month. Files can be republished with corrections, and the file structure changes over time. Analysts will query the gold layer from Power BI, so it must be stable, documented and cheap to query.

Your pipeline must:

1. Load a three-month history.
2. Load one further month incrementally, without reprocessing the history.
3. Survive a re-run of any month without creating duplicates.

### Target flow: Data Factory lands raw files, Databricks refines them in three layers

```
TLC public files ──► Azure Data Factory ──► ADLS Gen2 raw zone
(Monthly Parquet,    (Parameterised copy,   (Partitioned by month)
 HTTPS)               audit)                        │
                                                    │ new files picked up incrementally
┌─ Azure Databricks with Unity Catalog ─────────────┼──────────────────────────┐
│                                                   ▼                          │
│  Bronze ──────────────────► Silver ──────────────────► Gold                  │
│  (As received,              (Typed, cleaned,           (Revenue and demand   │
│   append only)               keyed)                     marts)               │
│                               │ failed rows                                  │
│                               ▼                                              │
│                             Quarantine                                       │
│                             (Rows that fail a rule)                          │
└──────────────────────────────────────────────────────────────────────────────┘
```

_Target flow: 3 Azure services, 3 layers, 1 quarantine._

Data Factory also triggers the Databricks processing after each landing. Analysts read only the gold schema.

## Dataset

Use the NYC Taxi and Limousine Commission (TLC) yellow taxi trip records, published as one Parquet file per month on the [TLC Trip Record Data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

| Load | Months | Purpose |
|---|---|---|
| Initial | 2024-10, 2024-11, 2024-12 | History load |
| Incremental | 2025-01 | A new month arrives |
| Re-run | 2024-12 again | Proves the pipeline is idempotent |

- Trip files: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`
- Zone lookup (reference data): `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`
- Column definitions: [Yellow Trips Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)

The data is real and imperfect. Expect records that break business rules, and expect the file structure to differ between months. Finding and handling both is part of the exercise.

## Environment and constraints

Use your own Azure subscription. A free trial or pay-as-you-go account is enough.

Required services:

- Azure Data Factory for ingestion and orchestration
- Azure Data Lake Storage Gen2 for the raw landing zone
- Azure Databricks with Unity Catalog for bronze, silver and gold
- Azure Key Vault or a Databricks secret scope for any secrets

Constraints:

- Data Factory must copy the files from the public URL into ADLS. Do not download files from inside a notebook.
- No storage keys, tokens or passwords in code or pipeline JSON. Prefer managed identity.
- Every table is a Delta table registered in Unity Catalog, under one catalog with `bronze`, `silver` and `gold` schemas.
- Keep compute small: single-node or serverless, with auto-termination. The exercise should stay well within a free trial credit.
- PySpark and SQL only. Delta Live Tables (Lakeflow declarative pipelines) or plain notebooks and jobs are both acceptable. Explain your choice.
- Do not use any employer or client environment, data or code.

## Requirements

Each part lists what must work. How you build it is your decision, and the README should record your reasoning.

### 1. Ingestion with Data Factory

- One parameterised pipeline loads any month from a year-month parameter. No per-month pipelines and no hard-coded URLs.
- A control table or config file decides which datasets and months to load.
- Files land in ADLS under a partitioned raw path, for example `raw/tlc/yellow/year=2024/month=10/`.
- The zone lookup file is ingested by the same framework.
- The pipeline triggers the Databricks processing and passes its parameters through.
- A failed download fails the run clearly, retries sensibly, and writes an audit record with run id, file, size or row count, status and timestamps.

### 2. Bronze

- Raw data is loaded as received into an append-only Delta table.
- Each row carries ingestion metadata: source file, load timestamp and pipeline run id.
- File discovery is incremental (Auto Loader, COPY INTO or equivalent). A file already loaded is not loaded twice.
- Structural differences between monthly files are handled deliberately, not by accident.

### 3. Silver

- Trips are typed, cleaned and conformed at one row per trip, with a documented business key.
- At least five data quality rules, each with a stated reason.
- Rows that fail a rule go to a quarantine table with the rule that failed. They are not silently dropped.
- Trips are enriched with pickup and drop-off borough and zone names.
- Loads are incremental upserts. Re-running a month leaves row counts unchanged.

### 4. Gold

| Table | Grain | Measures |
|---|---|---|
| `daily_zone_revenue` | Pickup date, pickup zone | Trips, total fare, total tips, average distance |
| `hourly_demand` | Pickup hour, day of week, borough | Trip count |
| `data_quality_summary` | Month, rule | Rows received, passed, quarantined |

Gold must refresh incrementally from changes in silver, not by full rebuild.

### 5. Governance and operations

- Table and column comments on every gold table.
- Read access on gold only for an `analysts` group. Include the grant statements.
- A reconciliation check per month showing that raw file rows equal bronze rows, and bronze rows equal silver plus quarantine.
- A table layout choice for gold (partitioning, clustering or neither) with the reason.

## Stretch goals (optional)

Pick any that show your strengths:

- Deployment to a second environment with a Databricks Asset Bundle or an Azure DevOps pipeline
- Unit tests for the transformation logic
- Alerting on failure or on a data quality threshold
- A short note on how you would move this design to streaming, or to Microsoft Fabric

## Deliverables

Share a private Git repository (GitHub or Azure DevOps) with the reviewers. It should contain:

- Data Factory pipeline, dataset, linked service and trigger definitions, as JSON or an ARM or Bicep export
- Databricks notebooks or Python and SQL files
- A `README.md` covering the architecture diagram, how to deploy and run, design decisions and trade-offs, assumptions, known limitations, what you would change for production, and the approximate cost you incurred
- Evidence of three runs (initial, incremental, re-run): Data Factory monitor screenshots, the Databricks job or pipeline run, and row counts per layer per month
- A Unity Catalog lineage screenshot for one gold table

Remove secrets and subscription identifiers before sharing.

## Rules and review session

- AI coding assistants are allowed. Say in the README where you used them. You must be able to explain and change every line.
- If a requirement is unclear, state your assumption in the README and carry on. Questions to your recruiter contact are also welcome.
- Partial submissions are fine. Tell us what you skipped and why.
- Keep your Azure environment available until the review session, then delete the resource group.

The review session runs 60 minutes:

1. Walkthrough of your architecture and one run (15 minutes).
2. A small change made live in your environment, such as a new quality rule or a new gold measure (20 minutes).
3. Questions on your design decisions and how the solution scales (15 minutes).
4. Your questions for us (10 minutes).
