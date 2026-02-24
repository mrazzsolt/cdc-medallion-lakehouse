# CDC Medallion Lakehouse (Oracle → Kafka → MinIO(S3) → dbt → DuckDB)

#!!UNDER DEVELOPMENT!!

A modern, containerized reference implementation of a **CDC-driven Medallion Lakehouse** pipeline.

The project demonstrates an end-to-end flow where **dummy valid and invalid records** are generated into an **Oracle source database** using Python scripts, changes are **streamed through Kafka**, landed into a **MinIO S3 bucket (Bronze)**, and then transformed with **dbt** into **DuckDB (Silver tier)**. Invalid records are separated via a **DLQ (Dead Letter Queue) handler**.

---

## Architecture Overview

### Current Data Flow (implemented)
1. **Oracle (Source DB)**  
   - Python scripts insert **dummy “good” and “bad” data** into Oracle tables to simulate real-world data quality issues.
2. **Kafka (Streaming / CDC transport)**  
   - Changes are streamed via Kafka topics.
3. **MinIO (S3-compatible storage) — Bronze tier**  
   - Kafka-delivered events are persisted into an S3 bucket in raw / landing form.
4. **DLQ Handler (Bad data isolation)**  
   - Separates invalid / non-conforming records into a dedicated dead-letter flow for later inspection and remediation.
5. **dbt → DuckDB — Silver tier**  
   - dbt models load and transform data from the S3 Bronze zone into DuckDB as the **Silver layer** (cleaned/structured).

### Roadmap (planned)
- **Airflow orchestration** (scheduling, retries, observability)
- **Gold tier** (business-ready marts/aggregates)
- **Grafana dashboards** (metrics & operational visibility)
- Additional improvements (schema governance, more tests, better validation, etc.)

---

## Tech Stack

- **Python** — data generators, utilities, pipeline helpers
- **Oracle** — source database (dummy data producer target)
- **Kafka** — streaming backbone for CDC/event transport
- **Debezium** - Kafka connector
- **MinIO** — S3-compatible object storage (Bronze)
- **dbt** — transformations / modeling
- **DuckDB** — Silver tier analytical store
- **Docker Compose** — local orchestration

---

## Quick Start (Docker)

### Prerequisites
- Docker + Docker Compose (v2)

### Start the stack
The full environment can be started with:

```bash
docker compose up -d
```

### Stop the stack
```bash
docker compose down
```

> Tip: if you want to reset volumes/state, use `docker compose down -v` (only if you’re okay losing local persisted data).

---

## How to Use (Typical Local Workflow)

1. **Bring up the environment**
   ```bash
   docker compose up -d
   ```

2. **Generate source data in Oracle (good + bad)**
   - The scriőt automatically runs the included Python scripts that insert dummy valid/invalid records into the Oracle source DB.
   - This simulates realistic ingestion conditions and data quality problems.

3. **Observe streaming into Kafka**
   - As changes occur, events flow through Kafka topics.

4. **Verify landing in MinIO (Bronze)**
   - Raw events land in the configured S3 bucket.
   - The DLQ handler isolates invalid records into a separate path/stream.

5. **Run dbt to build Silver in DuckDB**
   - dbt reads from the Bronze zone (MinIO S3) and materializes Silver models into DuckDB.

---

## Medallion Layers in This Project

- **Bronze (MinIO / S3 bucket)**  
  Raw, replayable data as received from the streaming layer.

- **Silver (DuckDB via dbt)**  
  Cleaned, structured, validated datasets suitable for analytics and downstream processing.

- **Gold (planned)**  
  Business-facing aggregates, marts, and domain-specific datasets.

---

## Data Quality & DLQ

A key aspect of this repository is demonstrating **data quality handling** in streaming pipelines:

- **Valid records** proceed through the standard path (Kafka → S3 → dbt → DuckDB).
- **Invalid records** are **diverted** by the **DLQ handler**, enabling:
  - easier debugging
  - reprocessing after fixes
  - auditability and traceability

---

## Project Status

This repository is actively evolving. The current implementation focuses on:
- local reproducibility via Docker Compose
- CDC-like streaming simulation from Oracle
- Bronze landing + DLQ separation
- Silver tier modeling with dbt + DuckDB

Upcoming milestones include orchestration (Airflow), Gold tier, and operational dashboards (Grafana).
