# WRC Decisions Scraping Pipeline

A Scrapy-based pipeline that scrapes decisions and determinations from the Irish
[Workplace Relations Commission](https://www.workplacerelations.ie), stores raw
documents in object storage and searchable metadata in a NoSQL database, then
transforms the raw data into a cleaned, curated zone. Orchestrated with Dagster.

## Architecture at a glance

Scrapy spider ──► MongoDB (metadata) ┐
└─► MinIO (raw documents) ├─ landing zone
│
Transform ────► MongoDB (clean metadata) ┐
└──► MinIO (cleaned docs) ├─ curated zone
┘
Dagster orchestrates: landing_documents ──► cleaned_documents


See `ARCHITECTURE.md` for the design decisions behind it.

## Prerequisites

- Python 3.11+
- Docker and Docker Compose

## 1. Start the storage services

MongoDB (metadata), MinIO (object storage), and PostgreSQL (Dagster run
metadata) all run in containers:

```bash
docker compose up -d
docker compose ps          # confirm mongo, minio, postgres are running
```

- MinIO console: http://localhost:9001 (log in with `MINIO_ROOT_USER` /
  `MINIO_ROOT_PASSWORD` from your `.env`)
- MongoDB: `localhost:27017`

## 2. Set up the Python environment

```bash
python -m venv .venv

# Windows (PowerShell):
.venv\Scripts\Activate.ps1
# macOS / Linux:
source .venv/bin/activate

pip install -r requirements.txt
```

## 3. Configure

Copy `.env.example` to `.env` and set real values (at minimum, replace the
placeholder passwords):

```bash
cp .env.example .env
```

All connection strings, ports, bucket names, and collection names are read from
`.env`. There are no hardcoded values in the code.

## 4. Run the scraper

```bash
scrapy crawl wrc -a start_date=2025-01-01 -a end_date=2025-02-01
```

- `start_date` and `end_date` are `YYYY-MM-DD`. If omitted, the range defaults
  to 2026-01-01 through today.
- The crawl partitions monthly, per adjudicating body. PDF/DOC/DOCX files are
  stored as-is; HTML decision pages are stored as `.html`.
- A SHA-256 hash is computed per document and stored with the metadata.
- The run is **idempotent**: re-running the same range creates no duplicate
  records and re-downloads nothing that hasn't changed.
- Every run ends with a JSON `run_summary` line reporting results found,
  documents scraped, and any failures with their URLs and reasons.

## 5. Run the transformation

```bash
python -m wrc_pipeline.transform --start-date 2025-01-01 --end-date 2025-02-01
```

For each record in the range it: passes PDF/DOC/DOCX through unchanged; strips
HTML pages down to the decision content (removing nav, header, footer, and
scripts); renames every file to `identifier.ext`; recomputes the hash; and
writes to a **separate** curated bucket and collection. The landing zone is
never modified.

## 6. Run the full pipeline via Dagster (recommended)

Point Dagster at its config, then start it:

```bash
# Windows (PowerShell):
$env:DAGSTER_HOME = "$PWD\orchestration"
# macOS / Linux:
export DAGSTER_HOME="$PWD/orchestration"

dagster dev -m orchestration.definitions
```

Open http://localhost:3000 → **Assets** → **Materialize all**, and supply the
run config:

```yaml
ops:
  landing_documents:
    config:
      start_date: "2025-01-01"
      end_date: "2025-02-01"
  cleaned_documents:
    config:
      start_date: "2025-01-01"
      end_date: "2025-02-01"
```

Dagster runs `landing_documents` (the crawl), then `cleaned_documents` (the
transform), enforcing the dependency between them. Run history is persisted in
PostgreSQL.

## Logging

All logs are structured JSON. Each run reports, per partition, the body being
scraped and the number of results found versus scraped; any failed download is
logged with its URL and error; and each run closes with a `run_summary` object.

## Project layout

| Path | Purpose |
|------|---------|
| `wrc_pipeline/spiders/wrc.py` | Scrapy spider: partitioning, pagination, downloads |
| `wrc_pipeline/pipelines.py` | Stores documents in MinIO, metadata in MongoDB |
| `wrc_pipeline/transform.py` | Landing → curated transformation |
| `wrc_pipeline/logging_config.py` | Shared JSON logging setup |
| `wrc_pipeline/settings.py` | Scrapy settings |
| `orchestration/definitions.py` | Dagster assets and job |
| `orchestration/dagster.yaml` | Dagster storage config (PostgreSQL) |
| `docker-compose.yml` | MongoDB, MinIO, PostgreSQL containers |
| `.env.example` | Template for configuration |