# Architecture

## Date partitioning

The scraper splits searches into monthly date ranges. This keeps most searches small enough to fit within the WRC result limit without creating the large number of requests that daily partitions would require.

If a range returns more results than can be accessed, the spider automatically splits the range into smaller periods and retries.

Monthly partitions are also independent, making them easy to run in parallel later.

## Retries and rate limiting

Scrapy AutoThrottle adjusts request speed based on website response times, while concurrency limits prevent excessive parallel requests.

Temporary failures such as 408, 429, and 5xx responses are retried up to three times. Requests also have a 30-second timeout.

Failures that remain after retries are logged without stopping the full crawl.

## Deduplication

Each document is identified using a SHA-256 hash. For HTML pages, the hash is calculated from normalized text instead of raw HTML so dynamic values such as tokens or timestamps do not create false changes.

Files are stored using their hash, and metadata is upserted using the identifier and file hash.

Re-running the scraper therefore does not create duplicates. If a document changes, its new hash creates a separate version.

## Scaling to 50+ sources

For 50+ sources, I would make three main changes:

1. Source configuration: move URLs, selectors, pagination rules, and date formats into a source registry instead of hardcoding them in spiders.
2. Dagster partitions: run and track each source and month independently so failed partitions can be retried without restarting the full pipeline.
3. Parallel workers: process multiple partitions concurrently and introduce a shared crawl queue such as scrapy-redis if needed.

The main scaling strategy is horizontal: more workers processing independent partitions, while keeping the storage and transformation layers source-agnostic.

I would only introduce additional microservices if a stage developed a clearly different scaling or ownership requirement.