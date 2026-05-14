# Snowflake Data Pipeline: Dynamic Tables vs Streams & Tasks

A comprehensive Snowflake project demonstrating two approaches to building **real-time, multi-layered data pipelines** — using **Dynamic Tables** (modern, declarative) and **Streams & Tasks** (legacy, imperative). Both pipelines follow a medallion architecture (Source → Raw → Clean → Consumption) and culminate in a star schema with dimension and fact tables.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Pipeline 1: Dynamic Tables (DT Series)](#pipeline-1-dynamic-tables-dt-series)
- [Pipeline 2: Streams & Tasks (Notebook Series)](#pipeline-2-streams--tasks-notebook-series)
- [Pipeline 3: End-to-End Dynamic Tables (E2E Series)](#pipeline-3-end-to-end-dynamic-tables-e2e-series)
- [File Reference](#file-reference)
- [Data Model](#data-model)
- [Prerequisites](#prerequisites)
- [Getting Started](#getting-started)
- [Key Concepts Demonstrated](#key-concepts-demonstrated)
- [Important Notes](#important-notes)

---

## Project Overview

This project provides hands-on implementations of Snowflake data pipelines using two distinct paradigms:

| Approach | Files | Database | Key Features |
|---|---|---|---|
| **Dynamic Tables** | `DT01`–`DT08` | `DT_DB` | Declarative, auto-refreshing, minimal orchestration |
| **Streams & Tasks** | `01`–`06` notebooks | `LEGACY_DB` | Imperative, CDC-based, full task DAG orchestration |
| **E2E Dynamic Tables** | `E2E01`–`E2E04` | `DT_DBV2` | Complete star schema pipeline using Dynamic Tables |

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    DATA PIPELINE FLOW                     │
├─────────────────────────────────────────────────────────┤
│                                                          │
│   CSV Files ──► Internal Stage ──► Raw Layer             │
│                     (COPY INTO)       │                   │
│                                       ▼                   │
│                                  Clean Layer              │
│                              (Dedup, Filter,              │
│                               Transform)                  │
│                                       │                   │
│                                       ▼                   │
│                             Consumption Layer             │
│                            (Star Schema: DIM              │
│                             + FACT tables)                │
└─────────────────────────────────────────────────────────┘
```

### Layer Details

| Layer | Schema | Purpose |
|---|---|---|
| **Source** | `source_sch` / `source` | Internal stage + file format definitions |
| **Raw** | `raw_sch` / `raw` | Landing zone for CSV data via COPY INTO tasks |
| **Clean** | `clean_sch` / `clean` | Deduplication, filtering (e.g., active employees only), joins, and transformations |
| **Consumption** | `consumption_sch` / `consumption` | Dimensional model — Customer DIM, Date DIM, Priority DIM, Order FACT |

---

## Pipeline 1: Dynamic Tables (DT Series)

Uses **Dynamic Tables** with configurable `target_lag` for automatic, incremental refresh.

### Files (Execute in Order)

| File | Description |
|---|---|
| `DT01.Setup.sql` | Creates database `DT_DB`, schemas (`source_sch`, `raw_sch`, `clean_sch`, `consumption_sch`), CSV file format, and internal stage |
| `DT02.RawLayer.sql` | Creates `employee_raw` table, a dedicated warehouse (`dt_01_wh`), and a scheduled task to COPY data from stage every 2 minutes |
| `DT03.CleanLayer.sql` | Creates `employees_clean_dt` Dynamic Table — filters active employees, renames columns, 5-minute target lag |
| `DT04.DeleteUpdateOperations.sql` | Demonstrates how Dynamic Tables react to DELETE and UPDATE operations on source data |
| `DT05.DTonStages&MV.sql` | Explores limitations — Dynamic Tables cannot be created directly on stages or on views backed by stages; materialized views also unsupported on stages |
| `DT06MultipleRawSource.sql` | Adds a second data source (`emp_leave_raw`), a child task for leave data ingestion, and a new warehouse (`dt_emp_leave_wh`) |
| `DT07.CleanLayer.sql` | Creates `emp_leave_clean_dt` (employee + leave join), aggregates like `leave_by_cat_dt`, `leave_by_category`, and `total_leave_dt` |
| `DT08.DownstreamLayer.sql` | Demonstrates chaining Dynamic Tables using `target_lag = 'downstream'` to create hierarchical refresh dependencies |

### Datasets

- **Employees**: `emp_id`, `first_name`, `last_name`, `date_of_birth`, `date_of_joining`, `email_address`, `department`, `designation`, `level`, `office_location`, `active`
- **Employee Leaves**: `emp_id`, `leave_type`, `leave_applied_date`, `leave_start_date`, `leave_end_date`, `leave_days`, `status`

---

## Pipeline 2: Streams & Tasks (Notebook Series)

Uses **Streams** (change data capture) and **Tasks** (scheduled + DAG orchestration) with **MERGE** statements for upserts.

### Notebooks (Execute in Order)

| Notebook | Description |
|---|---|
| `01.database_schema_stage.ipynb` | Creates database `LEGACY_DB`, schemas, file format, and internal stage |
| `02.raw_layer.ipynb` | Creates `customers_raw` and `orders_raw` tables with append-only streams; root task with child copy tasks scheduled every 1 minute |
| `03.clean_layer.ipynb` | Creates `customers_clean` and `orders_clean` tables with streams; MERGE tasks for deduplication using `ROW_NUMBER()` |
| `04.consumption_layer.ipynb` | Creates star schema — `customer_dim`, `date_dim`, `priority_dim`, `order_fact` — populated via stream-driven MERGE tasks |
| `05.ResumeStreamsTasks.ipynb` | Resumes all tasks in correct order (children first, then parents); also provides suspend commands |
| `06.DataValidationStreamsTasks.ipynb` | Validation queries to check row counts and data integrity across all layers |

### Task DAG Structure

```
root_task (1 min schedule)
├── copy_to_customer_raw_task
│   └── populate_clean_customer_task (when stream has data)
│       └── populate_customer_dim_task (when stream has data)
│           └── populate_order_fact_task (when all dim streams have data)
└── copy_to_order_raw_task
    └── populate_clean_order_task (when stream has data)
        ├── populate_date_dim_task (when stream has data)
        │   └── populate_order_fact_task
        └── populate_priority_dim_task (when stream has data)
            └── populate_order_fact_task
```

### Datasets

- **Customers**: `cust_key`, `name`, `address`, `nation_name`, `phone`, `acct_bal`, `mkt_segment`
- **Orders**: `order_key`, `cust_key`, `order_status`, `total_price`, `order_date`, `order_priority`, `clerk`, `ship_priority`

---

## Pipeline 3: End-to-End Dynamic Tables (E2E Series)

A complete star schema pipeline built entirely with Dynamic Tables, using the same Customer/Order datasets.

### Files (Execute in Order)

| File | Description |
|---|---|
| `E2E01.Setup.sql` | Creates database `DT_DBV2`, schemas (`source`, `raw`, `clean`, `consumption`), file format, and stage |
| `E2E02.RawLayer.sql` | Creates `customer_raw` and `order_raw` tables with scheduled COPY tasks (2-minute cadence) using dedicated `dt_task_load_wh` |
| `E2E03.CleanLayer.sql` | Creates `customer_clean_dt` and `order_clean_dt` Dynamic Tables with `ROW_NUMBER()` deduplication and 2-minute target lag |
| `E2E04.ConsumptionLayer.sql` | Creates full star schema using Dynamic Tables — `customer_dim_dt`, `date_dim_dt`, `priority_dim_dt` (with `DOWNSTREAM` lag), and `order_fact_dt` (3-minute lag) with sequence-based surrogate keys |

### Star Schema

```
           ┌──────────────┐
           │ customer_dim │
           │   (DT, SEQ)  │
           └──────┬───────┘
                  │
┌─────────┐  ┌───┴───────┐  ┌──────────────┐
│ date_dim │──│ order_fact │──│ priority_dim │
│ (DT,SEQ) │  │  (DT,SEQ) │  │  (DT, SEQ)   │
└─────────┘  └───────────┘  └──────────────┘
```

---

## File Reference

| File | Type | Pipeline |
|---|---|---|
| `DT01.Setup.sql` | SQL Worksheet | Dynamic Tables — Setup |
| `DT02.RawLayer.sql` | SQL Worksheet | Dynamic Tables — Raw |
| `DT03.CleanLayer.sql` | SQL Worksheet | Dynamic Tables — Clean |
| `DT04.DeleteUpdateOperations.sql` | SQL Worksheet | Dynamic Tables — DML Operations |
| `DT05.DTonStages&MV.sql` | SQL Worksheet | Dynamic Tables — Limitations |
| `DT06MultipleRawSource.sql` | SQL Worksheet | Dynamic Tables — Multi-source |
| `DT07.CleanLayer.sql` | SQL Worksheet | Dynamic Tables — Advanced Clean |
| `DT08.DownstreamLayer.sql` | SQL Worksheet | Dynamic Tables — Downstream Lag |
| `E2E01.Setup.sql` | SQL Worksheet | E2E Dynamic Tables — Setup |
| `E2E02.RawLayer.sql` | SQL Worksheet | E2E Dynamic Tables — Raw |
| `E2E03.CleanLayer.sql` | SQL Worksheet | E2E Dynamic Tables — Clean |
| `E2E04.ConsumptionLayer.sql` | SQL Worksheet | E2E Dynamic Tables — Consumption |
| `01.database_schema_stage.ipynb` | Notebook | Streams & Tasks — Setup |
| `02.raw_layer.ipynb` | Notebook | Streams & Tasks — Raw |
| `03.clean_layer.ipynb` | Notebook | Streams & Tasks — Clean |
| `04.consumption_layer.ipynb` | Notebook | Streams & Tasks — Consumption |
| `05.ResumeStreamsTasks.ipynb` | Notebook | Streams & Tasks — Task Management |
| `06.DataValidationStreamsTasks.ipynb` | Notebook | Streams & Tasks — Validation |

---

## Prerequisites

- Snowflake account with `SYSADMIN` and `ACCOUNTADMIN` roles
- `COMPUTE_WH` warehouse (or any X-Small warehouse)
- Ability to execute tasks (`EXECUTE TASK` and `EXECUTE MANAGED TASK` privileges)
- CSV data files uploaded to the respective internal stages:
  - `@dt_db.source_sch.dynamic_tbl_stage/employees/` — Employee data
  - `@dt_db.source_sch.dynamic_tbl_stage/emp_leave_context/` — Employee data (multi-source)
  - `@dt_db.source_sch.dynamic_tbl_stage/emp_leave_context/leave_data/` — Leave data
  - `@dt_dbv2.source.my_stage/customer/` — Customer data
  - `@dt_dbv2.source.my_stage/order/` — Order data
  - `@legacy_db.source_sch.my_stage/customer/` — Customer data (Streams & Tasks)
  - `@legacy_db.source_sch.my_stage/order/` — Order data (Streams & Tasks)

---

## Getting Started

### Pipeline 1: Dynamic Tables

```sql
-- 1. Run setup
-- Execute DT01.Setup.sql

-- 2. Upload CSV files to the stage
PUT file:///path/to/employees.csv @dt_db.source_sch.dynamic_tbl_stage/employees/;

-- 3. Create raw layer and start ingestion
-- Execute DT02.RawLayer.sql

-- 4. Create clean layer Dynamic Tables
-- Execute DT03.CleanLayer.sql

-- 5. Explore additional features
-- Execute DT04 through DT08 in order
```

### Pipeline 2: Streams & Tasks

```sql
-- 1. Run notebooks 01 through 04 in order to create all objects

-- 2. Upload CSV files to the stage
PUT file:///path/to/customer.csv @legacy_db.source_sch.my_stage/customer/;
PUT file:///path/to/order.csv @legacy_db.source_sch.my_stage/order/;

-- 3. Grant task execution privileges (run as ACCOUNTADMIN)
GRANT EXECUTE TASK, EXECUTE MANAGED TASK ON ACCOUNT TO ROLE SYSADMIN;

-- 4. Resume all tasks using notebook 05

-- 5. Validate data using notebook 06

-- 6. Suspend tasks when done (notebook 05)
```

### Pipeline 3: E2E Dynamic Tables

```sql
-- 1. Execute E2E01 through E2E04 in order
-- 2. Upload CSV data to @dt_dbv2.source.my_stage
-- 3. Resume the COPY tasks
-- 4. Dynamic Tables auto-refresh downstream
```

---

## Key Concepts Demonstrated

| Concept | Where |
|---|---|
| **Dynamic Tables** with `target_lag` | `DT03`, `DT07`, `DT08`, `E2E03`, `E2E04` |
| **Downstream lag** (cascading refresh) | `DT08`, `E2E04` |
| **DT limitations** (no stage/MV sources) | `DT05` |
| **DML propagation** through DTs | `DT04` |
| **Streams** (append-only CDC) | Notebooks `02`, `03` |
| **Task DAGs** with dependencies | Notebooks `02`, `03`, `04` |
| **MERGE** (upsert) patterns | Notebooks `03`, `04` |
| **ROW_NUMBER() deduplication** | `DT03`, `E2E03`, Notebook `03` |
| **Star schema** (DIM + FACT) | `E2E04`, Notebook `04` |
| **Sequences** for surrogate keys | `E2E04` |
| **Window functions** in DTs | `DT07` (rolling avg leave) |
| **Multi-source ingestion** | `DT06` |
| **Task resume/suspend ordering** | Notebook `05` |
| **Data validation queries** | Notebook `06` |

---

## Important Notes

- **Task Scheduling**: Tasks run on a 1–2 minute schedule. Remember to **suspend tasks** when not in use to avoid unnecessary credit consumption.
- **Task Resume Order**: Always resume child tasks before parent tasks, and suspend in the reverse order.
- **Dynamic Tables vs Streams & Tasks**: Dynamic Tables provide a simpler, declarative approach with automatic incremental refresh. Streams & Tasks offer more control but require explicit orchestration.
- **Warehouses**: The project creates several dedicated warehouses (`dt_01_wh`, `dt_emp_leave_wh`, `dt_task_load_wh`, `dt_transform_wh`). All are X-Small and auto-suspend after 60 seconds.
- **Data Files**: You must upload your own CSV files to the internal stages before running the pipelines. The schema for each CSV matches the table definitions in the raw layer scripts.

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
