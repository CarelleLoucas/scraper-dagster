# Architecture

## Date partition size: monthly

The scraper partitions each adjudicating body into calendar-month ranges.
Monthly is a deliberate midpoint. The WRC search returns a fixed page size and
reports a total result count; when a range holds more results than one page
exposes and pagination markup is absent, the spider bisects that range and
retries. Monthly partitions keep most ranges within a single page for the
~500–1000 document target, so bisection is rare, while avoiding the request
explosion that daily partitions would cause. Partitions are independent units
of work, which is what makes the crawl parallelisable later without changing the
extraction logic.

## Retries and rate limiting

Rate limiting is handled by Scrapy AutoThrottle, which adapts the request delay
to observed latency and a target concurrency, with per-domain concurrency caps.
This is faster than a fixed delay and backs off automatically under load,
meeting the goal of scraping quickly without getting blocked. Transient failures
(HTTP 408, 429, and 5xx) are retried up to three times by Scrapy's retry
middleware, and a 30-second download timeout bounds slow requests. A failure
that survives retries is logged with its URL and status code and does not halt
the run; every un-scraped record is accounted for in the end-of-run summary.

## Deduplication strategy

Every document is hashed with SHA-256. For HTML the hash is computed over the
normalised text content, not the raw bytes, because the pages embed
request-specific markup (tokens, timestamps) that would otherwise change the
hash on every fetch and break idempotency. Object storage is content-addressed,
so an identical document maps to the same object and a MinIO PUT is an idempotent
overwrite. Metadata is upserted on the compound key `(identifier, file_hash)`:
re-running a date range neither duplicates records nor re-downloads unchanged
files. When a document genuinely changes, its hash changes, producing a new
version rather than silently overwriting the old one — hash-based change
detection and versioning in a single mechanism. The transformation stage applies
the same key in its curated collection.

## Scaling to 50+ sources

The data model and partitioning already scale well beyond the target volume:
object storage handles millions of files, metadata is indexed, and work is
divided into independent partitions that are streamed one document at a time.
Three changes would take the pipeline to 50+ sources:

1. **Source registry.** Replace the hardcoded body list and in-spider selectors
   with per-source configuration (base URL, selectors, pagination rules, date
   format), read by a base spider with per-source subclasses. Adding a source
   becomes a config entry rather than a code change; the storage and transform
   stages are already source-agnostic.
2. **Partitioned Dagster assets.** Convert the two assets into time-window
   partitioned assets over the existing monthly logic, so Dagster schedules,
   parallelises, and tracks success per partition — a failed month becomes a
   single partition to re-run rather than a whole-pipeline restart.
3. **Distributed crawl execution.** Replace the single crawl subprocess with
   parallel partition runs, or a shared request queue (e.g. `scrapy-redis`) so
   multiple stateless workers drain one frontier.

This is horizontal scaling of one modular codebase — more workers over more
partitions and configuration — rather than a decomposition into microservices,
which would add network and operational cost the problem does not call for. A
stage would only be split into its own service if it developed a distinct
scaling profile (for example, ML enrichment on GPUs) or a separate owning team.