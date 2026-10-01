[한국어](README.md) | [English](README.en.md)
# Incremental POI GeoKG Update Pipeline

A research prototype that updates an existing Neo4j GeoKG by comparing 2024 and 2026 V-World POI snapshots stored in PostgreSQL/PostGIS.

**This repository contains code only.** POI source data, database dumps, credentials, and connection details are not publicly distributed. Reproducing the reported results requires authorized access to equivalent snapshots and an initialized 2024 GeoKG. This repository assumes that both POI snapshots are already loaded into PostgreSQL mart.poi and that a 2024 baseline GeoKG exists in Neo4j. The published code automates change detection, incremental graph updates, and result verification.

## End-to-end research workflow and repository scope

POI snapshots are collected, loaded into a PostgreSQL `raw` schema, and preprocessed into `mart.poi`. This repository automates the subsequent steps: SQL-based change detection (`mart.inc_poi`), batched extraction from PostgreSQL, updates to an existing Neo4j GeoKG, and PostgreSQL–Neo4j consistency verification.

Snapshot acquisition, raw-data ingestion, preprocessing, and initial GeoKG construction are prerequisites **outside this repository's implementation scope**. The integrated pipeline reads incremental rows directly from PostgreSQL; it does not require an intermediate CSV export.

Development began with controlled 2024/2026 samples to test change types, followed by exception-case review and a full-snapshot automation experiment for Seocho-gu, Seoul.

## Workflow

| Module | Files | Role |
| --- | --- | --- |
| 1. Change detection | `create_inc_poi.py`, `sql/create_inc_poi.sql` | Validate the snapshots and create `mart.inc_poi`. |
| 2. Graph update | `apply_inc_poi.py` | Apply incremental POI changes and update `IN` relationships in Neo4j. |
| 3. Verification | `verify_inc_poi.py` | Compare the updated graph with the 2026 snapshot. |
| Orchestration | `run_pipeline.py` | Run modules 1–3 in sequence. |
| Optional reset | `reset_to_t1.py` | Back up the local Neo4j database and restore a private 2024 baseline dump. |

The pipeline does not download POI data, preprocess raw files, or construct the initial GeoKG.

## Change detection

The implementation assumes that `nf_id` is a stable, unique POI identifier. It is the join key, not a watched property.

Watched properties are `poi_nm`, `poi_cl_dc`, `emd_cd`, and `geom`. Geometry is compared in EPSG:5179 using a **0.1 m tolerance**. A smaller positional difference alone does not trigger an update.

| 2024 snapshot | 2026 snapshot | Result |
| --- | --- | --- |
| Absent | Present | `appear` |
| Present | Absent | `disappear` |
| Present | Present, watched property changed | `updated` |
| Present | Present, no watched change | Excluded from `mart.inc_poi` |

The incremental table stores the full source row for each change: 2026 values for `appear` and `updated`, and the last available 2024 values for `disappear`.

## Prerequisites

- Python 3.11, PostgreSQL with PostGIS, and Neo4j. The experiment used Python 3.11.9 and Neo4j Desktop 5.26.16.
- A `mart.poi` table containing both `snapshot = '2024'` and `snapshot = '2026'`. Each snapshot must have unique, non-null `nf_id` values and EPSG:5179 geometry.
- An existing 2024 baseline GeoKG with `:POI2024` nodes, a uniqueness constraint on `:POI2024(nf_id)`, and the required `:admDongList` nodes.
- POI-to-administrative-dong relationships follow `(:POI2024)-[:IN]->(:admDongList)`.
- Database connectivity from the machine running Python. If PostgreSQL is reached through an SSH tunnel, establish it separately before running the pipeline.

The node label `POI2024` is retained after the update. In this prototype, the label names the existing POI node type; it does **not** indicate that the updated graph still contains 2024 data.

## Setup

Install dependencies:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

Create a local `.env` file in the project root using your own connection details:

```dotenv
PG_HOST=127.0.0.1
PG_PORT=<PostgreSQL port>
PG_DATABASE=<database name>
PG_USER=<database user>
PG_PASSWORD=<database password>

NEO4J_URI=neo4j://127.0.0.1:7687
NEO4J_DATABASE=neo4j
NEO4J_USER=<Neo4j user>
NEO4J_PASSWORD=<Neo4j password>

# Required only for the optional local T1 restore:
NEO4J_HOME=<local Neo4j DBMS directory>
NEO4J_JAVA_HOME=<local Java runtime directory>
```

Never commit `.env`.

## Run the pipeline

Run from the project root. Without `--apply`, the command performs checks without changing either database:

```powershell
python run_pipeline.py
```

To run the complete 2024 → 2026 update from the baseline graph:

```powershell
python run_pipeline.py --apply
```

This command creates `mart.inc_poi`, applies its changes to Neo4j, and runs the final verification. **It changes both databases:** module 1 drops and rebuilds `mart.inc_poi`, while module 2 updates the graph. The orchestrator rejects `--apply` unless the graph's `nf_id` set matches the 2024 baseline.

Individual modules can also be run separately:

```powershell
python create_inc_poi.py --apply
python apply_inc_poi.py --apply
python verify_inc_poi.py
```

The verifier is read-only.

## Optional return to T1

`reset_to_t1.py` expects a compatible private dump at:

```text
backups/t1/neo4j_t1_corrected_20261001.dump
```

**That dump is not included in this repository.** Confirm `NEO4J_HOME` and `NEO4J_JAVA_HOME`, then stop the Neo4j Desktop instance before applying the reset:

```powershell
python reset_to_t1.py
python reset_to_t1.py --apply
```

The script backs up the current database before replacing it with T1. Afterward, start Neo4j Desktop and run `python apply_inc_poi.py` **without** `--apply` to check that the graph is back at the 2024 baseline.

## Experimental results

| Measure | Count |
| --- | ---: |
| 2024 POIs | 125,010 |
| 2026 POIs | 105,494 |
| `appear` | 49,315 |
| `disappear` | 68,831 |
| `updated` | 1,456 |
| Total incremental rows | 119,602 |

In the completed local experiment, applying the incremental table to Neo4j took approximately **34 seconds**. The final verifier checked 105,494 target POIs, the full stored attributes of 50,771 appeared or updated POIs, and the absence of 68,831 disappeared POIs. It reported **0 errors**. This runtime is an observation from one environment, not a general performance guarantee.

## Data availability and scope

The POI snapshots and Neo4j dumps are not published. The `.gitignore` excludes `.env`, `.venv/`, `data/`, and `backups/`; when uploading through a browser, select files manually because `.gitignore` does not filter browser uploads.

This fixed-snapshot prototype assumes stable `nf_id` values and an initialized baseline graph. Raw-data ingestion, initial GeoKG construction, entity reconciliation, and continuous streaming CDC are outside its scope.
