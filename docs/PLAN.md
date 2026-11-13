# Solution plan (draft)

Working plan for [TASK.md](TASK.md). Decisions agreed: plain Python wheel + Databricks Job, a new folder per ingest, Bicep, stretch goals = unit tests + Asset Bundle dev/prod + alerting.

## 1. Architecture

```
TLC CloudFront (HTTPS)
   │  ADF Copy (anonymous HTTP → ADLS via ADF managed identity)
   ▼
ADLS Gen2  raw/tlc/yellow/year=YYYY/month=MM/ingest_id=<runId>/yellow_tripdata_YYYY-MM.parquet
           raw/tlc/reference/taxi_zone_lookup/ingest_id=<runId>/taxi_zone_lookup.csv
           raw/_control/load_config.json      (what to load)
           raw/_audit/…json                   (one record per file per run)
   │  ADF → Databricks Job (run_id, dataset, year_month passed as job parameters)
   ▼
Unity Catalog: catalog `taxi`
   bronze.yellow_trips_raw        append-only, Auto Loader (availableNow), + _source_file, _load_ts, _pipeline_run_id
   bronze.taxi_zone_lookup_raw
   bronze.ingestion_audit         (audit JSON → Delta)
   silver.yellow_trips            typed, keyed, enriched, MERGE upserts, CDF enabled
   silver.yellow_trips_quarantine failed rows + failed_rules array
   silver.taxi_zone               SCD1 lookup
   gold.daily_zone_revenue
   gold.hourly_demand
   gold.data_quality_summary
   gold.reconciliation (or a view)  raw = bronze = silver + quarantine, per month
```

**Identity, with no secrets anywhere:**
- ADF system MI → ADLS (Storage Blob Data Contributor) and → Databricks (linked service with MSI auth, workspace Contributor role).
- Databricks Access Connector MI → ADLS (UC storage credential + external location on `raw`).
- The TLC source is anonymous. Key Vault is still deployed and linked to ADF to show the pattern, but it holds no secrets because none are needed. The README explains this.

## 2. Ingestion (ADF)

- `pl_master`: Lookup `load_config.json` → ForEach (sequential) over entries → Execute `pl_ingest_file` → after all succeed, trigger the Databricks job once with the list of loaded months.
- `pl_ingest_file`: parameters `dataset`, `year_month`, `run_id`.
  - The URL and sink path are built from config templates plus `year_month`. No URLs are hard-coded in the pipeline.
  - Copy activity: retry 3 × 60 s and a timeout. The HTTP 404 for a month that isn't published yet fails clearly.
  - Audit on both success and failure: run id, file, bytes, rows copied, status, start/end, error message. Written as a small JSON file to `raw/_audit/` with a Copy activity (dummy source plus additional columns), so the audit trail survives even if Databricks is down.
- The trips and the zone lookup go through the same pipeline. The config holds `url_template`, `sink_path_template` and `format`.
- Trigger: a schedule trigger (monthly, defined but stopped) plus manual runs for the three demo loads.

**Re-run and republished files (decided):** Every ingest lands in a new `ingest_id=` folder. Bronze stays append-only and every load is kept. Auto Loader never reads the same file twice. Silver processes only the newest ingest per month, so a corrected file replaces the month's rows in silver. The alternative was to overwrite the same path and rely on `cloudFiles.allowOverwrites`. It was rejected because bronze would lose history and the behaviour would be implicit.

## 3. Bronze

- Auto Loader with `trigger(availableNow=True)` and a checkpoint per dataset. This gives incremental file discovery with exactly-once file processing.
- **Schema drift is handled deliberately:**
  - `schemaEvolutionMode = addNewColumns`, with a schema location tracked in the checkpoint.
  - `cloudFiles.schemaHints` pins key types.
  - Unexpected columns or types go to `_rescued_data`. Nothing is dropped silently.
  - Known drift in our window: 2025-01 adds `cbd_congestion_fee`, and some int and double types vary between months. I'll profile the four files first and document what I find.
- Bronze rows are as received. Only the metadata columns are added, plus `_source_year_month` parsed from the path.

## 4. Silver

- **Conform:** snake_case names; cast to fixed types (timestamps, decimal(10,2) for money, int codes); `Airport_fee`/`airport_fee` coalesced; `cbd_congestion_fee` defaults to null before 2025-01.
- **Business key:** The source has no trip id. `trip_key = sha2(vendor_id, pickup_ts, dropoff_ts, pu_location_id, do_location_id, fare_amount, total_amount, payment_type)`. Exact duplicates within a load are a DQ failure, not a new trip.
- **DQ rules (≥5, each with a reason), stored as a rules table/dict** so a live "add a rule" change is one line:
  1. `pickup_in_file_month`: the pickup timestamp falls within the file's month, give or take a small tolerance. Files contain stray rows from 2002 or 2026, which would corrupt the daily aggregates.
  2. `dropoff_after_pickup`: a trip can't have zero or negative duration.
  3. `non_negative_amounts`: fare and total ≥ 0. Negative values are voids or reversals, not revenue.
  4. `valid_distance`: 0 < distance < 200 miles. Zero-distance or absurd trips distort the average distance.
  5. `known_locations`: PU and DO IDs exist in the zone lookup.
  6. `valid_payment_type`: payment type is in the data dictionary code set.
  7. `duplicate_trip_key`: keeps the 1:1 grain.
  - A row can fail several rules. It goes to quarantine once, with a `failed_rules` array, so bronze = silver + quarantine holds exactly.
- **Enrich:** join the zone lookup twice, for pickup and drop-off borough and zone.
- **Load:** `MERGE` on `trip_key`, scoped to the month being loaded, plus `WHEN NOT MATCHED BY SOURCE AND source_year_month = X THEN DELETE`. A re-run leaves counts unchanged, and a corrected file removes rows that no longer exist. Quarantine uses the same replace-by-month pattern.
- Change Data Feed is enabled on silver, so gold can read only the changes.

## 5. Gold (incremental from silver CDF)

- Read the silver CDF since the last processed version, which is stored in a small `gold._watermark` table. From it, collect the **affected keys**: pickup dates and zones, or hours, days of week and boroughs, or months. Recompute only those slices from silver, then `MERGE` them into gold. This is correct for updates and deletes, and it avoids a full rebuild.
- `hourly_demand` is not date-scoped (hour × day of week × borough across all history), so any change touches its slices. It is recomputed only for the affected (hour, day of week, borough) keys. This is cheap and still incremental.
- `data_quality_summary` is recomputed for the affected months from silver and quarantine.
- **Layout:** liquid clustering on `pickup_date` for `daily_zone_revenue`. The tables are tiny (about 8k rows per month), so partitioning would only create small files, and clustering speeds up Power BI date filters. The other two tables use neither, because they're too small to benefit. Each choice is justified in the README.
- Every gold table and column gets a comment. Power BI can read `COMMENT` metadata.

## 6. Governance and operations

- `GRANT USE CATALOG ON CATALOG taxi TO analysts; GRANT USE SCHEMA, SELECT ON SCHEMA taxi.gold TO analysts;` Nothing is granted on bronze or silver.
- Reconciliation per month: `raw rows` (ADF audit rowsCopied) = bronze rows (for that ingest) = silver + quarantine. Written to `gold.reconciliation`. The job fails if a check fails.
- The job runs on serverless, or a single-node cluster with 10-minute auto-termination.

## 7. Orchestration choice (decided)

**A plain Python package, built as a wheel, run by one Databricks Job.** There are no notebooks and no DLT.
- The `taxi` package (`databricks/src/taxi/`) exposes entry points `bronze`, `silver`, `gold` and `reconcile`. Each one is a `python_wheel_task` in the job, running on serverless or a single-node cluster. Job parameters (`run_id`, `year_months`) arrive as CLI args.
- The transforms are pure functions (DataFrame in, DataFrame out). They're kept apart from the I/O (read/MERGE), so pytest can test them with local PySpark.
- The job and wheel are deployed by a Databricks Asset Bundle with `dev` and `prod` targets. Each target gets its own catalog (`taxi_dev` / `taxi_prod`), and the catalog name is a bundle variable.
- Why not DLT: the core of the assessment is explicit incremental logic (MERGE, delete-by-month, CDF watermark), and a live change in the review is a one-line edit in a `.py` file. The README mentions DLT expectations with APPLY CHANGES as an alternative.

## 8. Repo layout

```
infra/            main.bicep (+ modules), parameters.example.json
adf/              linkedService/ dataset/ pipeline/ trigger/  (ADF git-format JSON)
config/           load_config.json
databricks/
  databricks.yml  Asset Bundle: wheel artifact + job + targets dev/prod
  pyproject.toml  package `taxi`, entry points
  src/taxi/       bronze.py silver.py gold.py dq_rules.py reconcile.py common.py
  sql/            00_setup_catalog.sql 10_grants.sql 20_comments.sql
  tests/          pytest + local pyspark: DQ rules, trip_key, conform, gold slices
docs/             TASK.md PLAN.md evidence/ (screenshots, row counts)
README.md
```

## 8a. Alerting (stretch)

- ADF: an Azure Monitor alert rule on `PipelineFailedRuns > 0` sends to an action group (email).
- Databricks: job `email_notifications.on_failure`. The `reconcile` task also fails the job when any month's quarantine rate is above a configured threshold (default 5%) or a reconciliation check fails, so both kinds of problem reach the same alert.

## 9. Build order (aim: ~6 h)

1. Profile the four files locally with pyspark/duckdb: schema diffs and DQ issues. Outcome: the rules and the drift list. (30 min)
2. Bicep: RG, ADLS (HNS), ADF, Key Vault, Databricks (premium), access connector, role assignments. Then the UC catalog, schemas and external location. (60 min)
3. ADF config, pipelines and audit; test with 2024-10. (60 min)
4. Bronze → silver → gold code, unit tests for rules and key. (2 h)
5. Job wiring from ADF; the three demo runs; reconciliation; screenshots; lineage. (45 min)
6. README: decisions, assumptions, limitations, production changes, cost, AI usage. (45 min)

## Open questions / assumptions

- Azure subscription and region: does a trial have enough vCPU quota for a 4-core single node, or does the workspace allow serverless jobs?
- Should the `analysts` group be a Databricks account group (created by us)?
- Gold "pickup zone" grain uses `pu_location_id` plus zone name and borough. Trips with unknown zones are excluded by DQ.
