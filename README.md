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

## Run with Docker Compose

Docker with Compose v2 is the only prerequisite. Both Python packages, the
Dagster dev runs the webserver and daemon in the single `app` container.
MongoDB, MinIO, and PostgreSQL run as separate storage services.

1. Copy `.env.example` to `.env` and replace the placeholder credentials.
2. Start the stack:

```bash
docker compose up --build -d --wait
docker compose ps
docker compose logs -f app
```

## Run the pipeline

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

## Project layout

| Path | Purpose |
|------|---------|
| `packages/wrc_pipeline/spiders/wrc.py` | Scrapy spider: partitioning, pagination, downloads |
| `packages/wrc_pipeline/pipelines.py` | Stores documents in MinIO, metadata in MongoDB |
| `packages/wrc_pipeline/transform/transform.py` | Landing → curated transformation and CLI entry point |
| `packages/wrc_pipeline/logging_config.py` | Shared JSON logging setup |
| `packages/wrc_pipeline/settings.py` | Scrapy settings |
| `packages/orchestration/definitions.py` | Dagster assets and job |
| `packages/orchestration/dagster.yaml` | Dagster storage config (PostgreSQL) |
| `packages/Dockerfile` | Non-root application image containing both Python packages |
| `docker-compose.yml` | Application and persistent storage services |
| `.env.example` | Template for configuration |
