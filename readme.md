# SocialPulse Analytics Platform

A production-grade, end-to-end **Azure Databricks Lakehouse** project implementing a complete data engineering pipeline for social media clickstream analytics. Built as both a real-world portfolio project and preparation for the **Databricks Certified Data Engineer Professional** exam.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Phases](#phases)
- [Key Design Decisions](#key-design-decisions)
- [Setup & Prerequisites](#setup--prerequisites)
- [How to Run](#how-to-run)
- [Data Model](#data-model)
- [Security & Governance](#security--governance)
- [Observability](#observability)
- [CI/CD](#cicd)
- [Exam Coverage](#exam-coverage)

---

## Project Overview

SocialPulse Analytics Platform simulates a real-world social media data platform that:

- Ingests **real-time clickstream events** (streaming) and **batch user/ad data**
- Processes data through a **Medallion Architecture** (Bronze → Silver → Gold)
- Applies **dynamic rule-based data quality** engine
- Implements **SCD Type 2** for historical tracking
- Provides **Gold layer business metrics** for Power BI dashboards
- Enforces **Unity Catalog governance** with row filters and column masks
- Monitors pipeline health via **system tables and DLT event logs**
- Shares data externally via **Delta Sharing**
- Deploys infrastructure as code via **Databricks Asset Bundles (DABs)**

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│   App Events (JSON)  │  User Profiles (CSV)  │  Ad Campaigns    │
└──────────┬───────────────────┬───────────────────┬──────────────┘
           │                   │                   │
           ▼                   ▼                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ADLS Gen2 — Landing Zone                     │
│   /events/batch_*/   │   /users/   │   /ads/                    │
└──────────┬───────────────────┬───────────────────┬──────────────┘
           │                   │                   │
           ▼                   ▼                   ▼
┌─────────────────────────────────────────────────────────────────┐
│              Azure Data Factory — Orchestration                 │
│  Storage Event Trigger → DLT Pipeline → Gold Notebooks → Alert  │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│            Azure Databricks + Delta Live Tables                 │
│                                                                 │
│  BRONZE (raw)        SILVER (clean)       GOLD (business)       │
│  ─────────────       ────────────────     ──────────────────    │
│  raw_events     →    clean_events    →    user_session_summary  │
│  raw_users      →    clean_users     →    content_engagement    │
│  raw_ads        →    clean_ads       →    ad_performance        │
│                                      →    hourly_active_users   │
│                                      →    anomaly_flags         │
└──────────────────────────┬──────────────────────────────────────┘
                           │
           ┌───────────────┼───────────────┐
           ▼               ▼               ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ Delta Sharing│  │ Observability│  │   Security   │
│ Power BI     │  │ System Tables│  │  UC Masks    │
│ Partners     │  │  Event Logs  │  │  Row Filters │
└──────────────┘  └──────────────┘  └──────────────┘
```

---

## Tech Stack

| Component | Technology |
|---|---|
| Cloud Platform | Microsoft Azure |
| Storage | Azure Data Lake Storage Gen2 |
| Compute | Azure Databricks (Serverless) |
| Pipeline | Delta Live Tables (Declarative Pipelines) |
| Orchestration | Azure Data Factory |
| Data Format | Delta Lake |
| Governance | Unity Catalog |
| Serving | Delta Sharing |
| Dashboarding | Power BI |
| CI/CD | Databricks Asset Bundles + GitHub Actions |
| Language | Python, PySpark, SQL |
| Testing | pytest, assertDataFrameEqual |

---

## Project Structure

```
SocialPulse/
│
├── 00_setup/
│   ├── common_functions.py        ← get_df(), upsert_target_table()
│   ├── common_variables.py        ← catalog, schema, table names
│   └── setup_project_environment  ← UC catalog, schemas, config tables
│
├── 01_data_simulator/
│   └── 01_data_simulator.py       ← Faker-based event/user/ad generator
│
├── 02_bronze/
│   ├── 02a_bronze_events.py       ← DLT streaming, Autoloader (events)
│   ├── 02b_bronze_users.py        ← DLT batch, Autoloader (users)
│   └── 02c_bronze_ads.py          ← DLT batch, Autoloader (ads)
│
├── 03_silver/
│   ├── 03a_silver_events.py       ← DLT view + streaming table
│   ├── 03b_silver_users.py        ← DLT SCD2 via create_auto_cdc_flow
│   └── 03c_silver_ads.py          ← DLT SCD2 via create_auto_cdc_flow
│
├── 04_gold/
│   ├── 01_user_session_summary.py ← Standard notebook, MERGE
│   ├── 02_content_engagement.py   ← Standard notebook, MERGE
│   ├── 03_ad_performance.py       ← Standard notebook, MERGE
│   ├── 04_hourly_active_users.py  ← Standard notebook, MERGE
│   └── 05_anomaly_flags.py        ← Standard notebook, APPEND
│
├── 05_observability/
│   ├── 01_vw_pipeline_health.py   ← system.lakeflow queries
│   ├── 02_vw_data_quality.py      ← DLT event_log expectations
│   ├── 03_vw_pipeline_cost.py     ← system.billing.usage
│   └── 04_vw_table_growth.py      ← DESCRIBE DETAIL per table
│
├── 06_security/
│   ├── 01_column_masks.py         ← UC masking functions (email, ip, name)
│   ├── 02_row_filters.py          ← UC row filters (country + tier)
│   └── 03_gdpr_purge.py           ← Deletion vectors + VACUUM
│
├── 07_delta_sharing/
│   ├── 01_create_share.py         ← Gold tables share
│   ├── 02_create_ops_share.py     ← Observability share
│   └── 03_lakehouse_federation.py ← Foreign catalog setup
│
├── 08_cicd/
│   ├── databricks.yml             ← DABs bundle config
│   ├── variables/
│   │   ├── dev.yml
│   │   └── prod.yml
│   └── .github/workflows/
│       └── deploy.yml             ← GitHub Actions CI/CD
│
├── tests/
│   ├── test_common_functions.py   ← pytest unit tests
│   └── test_silver_rules.py       ← rule engine tests
│
└── powerbi/
    ├── measures/
    │   ├── platform_health.dax
    │   ├── content_performance.dax
    │   ├── ad_performance.dax
    │   └── observability.dax
    └── SocialPulse.pbix
```

---

## Phases

| Phase | Description | Status |
|---|---|---|
| 1 | Azure Setup (ADLS, Databricks, ADF, Unity Catalog) | ✅ Complete |
| 2 | Data Simulator (Faker-based clickstream generator) | ✅ Complete |
| 3 | Bronze Layer (DLT + Autoloader + schema inference) | ✅ Complete |
| 4 | Silver Layer (DLT + SCD2 + dynamic rule engine) | ✅ Complete |
| 5 | Gold Layer (Hybrid DLT + standard notebooks) | ✅ Complete |
| 6 | Observability (System tables + DLT event logs) | ✅ Complete |
| 7 | Security & Compliance (UC masks + row filters + GDPR) | ✅ Complete |
| 8 | Delta Sharing (D2O for Power BI + Lakehouse Federation) | ✅ Complete |
| 9 | CI/CD (DABs + GitHub Actions + unit tests) | ✅ Complete |
| 10 | Power BI Dashboards | 🔄 In Progress |

---

## Key Design Decisions

### Config-Driven Architecture
All pipeline configuration stored in Unity Catalog config schema:

```
socialpulse_catalog.config
├── rule_config          ← data quality rules per entity
├── project_properties   ← paths, schemas, field definitions
├── watermark            ← incremental processing watermarks
├── pipeline_config      ← pipeline names for observability
└── obs_*                ← observability Delta tables
```

### Dynamic Rule Engine
Silver layer applies validation rules dynamically from `rule_config` table:
- `expect_all()` → warn on violation, keep record
- `expect_all_or_drop()` → drop invalid records
- `expect_all_or_fail()` → fail pipeline on violation

### Hybrid Pipeline Architecture
- **DLT Pipelines** → Bronze ingestion + Silver transformation
- **Standard Notebooks** → Gold aggregations (enables complex logic)
- **ADF** → Orchestrates both, handles dependencies

### Incremental Processing
Gold notebooks use watermark-based incremental reads:
- Watermark stored in `config.watermark` table
- 30-minute buffer handles late-arriving data
- MERGE ensures idempotency on rerun

### SCD Type 2
Users and Ads tracked historically via `create_auto_cdc_flow`:
- `sequence_by = updated_at` (source system timestamp)
- `except_column_list` excludes audit columns from change detection
- Prevents unnecessary SCD2 versions on pipeline reruns

---

## Setup & Prerequisites

### Azure Resources Required
```
✅ Azure Subscription (Pay-as-you-go recommended)
✅ Azure Data Lake Storage Gen2
✅ Azure Databricks (Premium tier for Unity Catalog)
✅ Azure Data Factory
✅ Azure Databricks Access Connector (Managed Identity)
```

### Databricks Setup
```
1. Create Unity Catalog Metastore
2. Create Storage Credential (Managed Identity)
3. Create External Locations:
   - landing, bronze, silver, gold containers
4. Create Catalog: socialpulse_catalog
5. Create Schemas: bronze, silver, gold, config, security
6. Run: 00_setup/setup_project_environment
```

### Python Dependencies
```bash
pip install faker
pip install pytest
pip install databricks-sdk
```

---

## How to Run

### 1. Generate Data
```
Run: 01_data_simulator/01_data_simulator.py
→ Generates events, users, ads in ADLS landing zone
```

### 2. Run Bronze Pipeline
```
Databricks → Pipelines → SocialPulse Bronze Streaming → Start
Databricks → Pipelines → SocialPulse Bronze Batch → Start
```

### 3. Run Silver Pipeline
```
Databricks → Pipelines → SocialPulse Silver Streaming → Start
Databricks → Pipelines → SocialPulse Silver Batch → Start
```

### 4. Run Gold Notebooks (via ADF or manually)
```
Run in order:
1. 04_gold/01_user_session_summary.py
2. 04_gold/02_content_engagement.py
3. 04_gold/03_ad_performance.py
4. 04_gold/04_hourly_active_users.py
5. 04_gold/05_anomaly_flags.py
```

### 5. Run Observability
```
Run: 05_observability/01_vw_pipeline_health.py
Run: 05_observability/02_vw_data_quality.py
```

### 6. Deploy via DABs
```bash
databricks bundle deploy --target dev
databricks bundle deploy --target prod
```

---

## Data Model

### Bronze Layer (Raw)
| Table | Source | Ingestion | Mode |
|---|---|---|---|
| raw_events | ADLS /landing/events/ | Autoloader + DLT | Streaming |
| raw_users | ADLS /landing/users/ | Autoloader + DLT | Triggered |
| raw_ads | ADLS /landing/ads/ | Autoloader + DLT | Triggered |

### Silver Layer (Cleaned)
| Table | Pattern | Key |
|---|---|---|
| clean_events | Streaming append | event_id |
| clean_users | SCD2 via CDC flow | user_id |
| clean_ads | SCD2 via CDC flow | ad_id |

### Gold Layer (Business)
| Table | Grain | Write Mode |
|---|---|---|
| user_session_summary | Per user | MERGE on user_id |
| content_engagement | Per content | MERGE on content_id |
| ad_performance | Per ad | MERGE on ad_id |
| hourly_active_users | Per hour/platform/country | MERGE on composite key |
| anomaly_flags | Per detection event | APPEND |

---

## Security & Governance

### Unity Catalog Groups
```
socialpulse_admin          → full access all layers
socialpulse_analyst_UK     → UK data only
socialpulse_analyst_US     → US data only
socialpulse_analyst_free_tiers     → free tier users
socialpulse_analyst_premium_tiers  → premium tier users
socialpulse_analyst_business_tiers → business tier users
socialpulse_analyst_all_tiers      → all tiers
```

### Column Masks (PII Protection)
| Column | Table | Masking |
|---|---|---|
| email | clean_users | Partial (j***@gmail.com) for analysts |
| full_name | clean_users | Hidden for non-admin |
| username | clean_users | Partial masking |
| ip_address | clean_events | Hidden for non-admin |

### Row Filters
Applied on `clean_users` and `clean_events`:
- Country-based access (UK analysts see UK data only)
- Tier-based access (free tier analysts see free users only)
- Admin bypasses all filters

### GDPR Compliance
- **Deletion Vectors** enabled on Silver tables for fast deletes
- **GDPR purge notebook** deletes user data across all layers
- **Deletion order**: Gold → Silver events → Silver users
- **Landing archive** purged for complete data removal
- **VACUUM** run after deletion for physical removal

---

## Observability

### Pipeline Health
Sourced from `system.lakeflow.pipeline_update_timeline`:
- Run history per pipeline
- Success/failure tracking
- Duration metrics

### Data Quality
Sourced from DLT event log (publish to event log enabled):
- Per-expectation pass/fail counts
- Pass percentage per rule
- Records dropped by entity

### Cost Tracking
Sourced from `system.billing.usage`:
- DBU consumption per pipeline
- Estimated cost per day

### Table Growth
Sourced from `DESCRIBE DETAIL` per table:
- Number of files
- Size in bytes
- Last modified timestamp

---

## CI/CD

### Databricks Asset Bundles
All Databricks resources defined as code in `databricks.yml`:
- DLT pipelines
- Workflows/Jobs
- Cluster configurations
- Environment-specific variables

### GitHub Actions Pipeline
```
PR Merged to main
      ↓
Install Databricks CLI
      ↓
Run pytest unit tests
      ↓
databricks bundle deploy --target dev
      ↓
Manual approval
      ↓
databricks bundle deploy --target prod
```

### Environment Differences (dev vs prod)
| Setting | Dev | Prod |
|---|---|---|
| Catalog | socialpulse_catalog | socialpulse_prod_catalog |
| Cluster size | Small (single node) | Large (multi node) |
| Pipeline mode | Triggered | Continuous (events) |
| Storage account | deasocialpulsedl | socialpulseprod |

---

## Exam Coverage

This project covers the following **Databricks Certified Data Engineer Professional** exam sections:

| Section | Weight | Coverage |
|---|---|---|
| Python & SQL Development | 22% | DLT, UDFs, assertDataFrameEqual |
| Data Ingestion | 7% | Autoloader, cloudFiles, schema evolution |
| Data Transformation | 10% | Window functions, MERGE, SCD2 |
| Data Sharing & Federation | 5% | Delta Sharing, Lakehouse Federation |
| Monitoring & Alerting | 10% | System tables, DLT event logs, SQL Alerts |
| Cost & Performance | 13% | Liquid Clustering, Serverless, OPTIMIZE |
| Security & Compliance | 10% | UC masks, row filters, deletion vectors |
| Data Governance | 7% | Unity Catalog, lineage, audit |
| Debugging & Deploying | 10% | DABs, Git integration, job repair |
| Data Modelling | 6% | Medallion, Delta Lake, SCD2 |
| Data Sharing | 5% | Delta Sharing, D2O, D2D |

---

## Author

**Guruprasad Kulkarni**  


---

## License

This project is for educational and portfolio purposes.