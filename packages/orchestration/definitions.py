"""Dagster orchestration: ingestion (Scrapy) then transformation, as dependent assets."""
import json
import logging
import os
import subprocess
import sys

from dagster import (
    Config,
    Definitions,
    MaterializeResult,
    asset,
    define_asset_job,
)
from dotenv import load_dotenv

load_dotenv()


class DateRangeConfig(Config):
    """Shared run config so both assets scrape/transform the same window."""
    start_date: str  # YYYY-MM-DD
    end_date: str    # YYYY-MM-DD


@asset
def landing_documents(context, config: DateRangeConfig) -> MaterializeResult:
    """Run the Scrapy crawl for the configured range into Mongo + MinIO landing."""
    context.log.info(f"Starting crawl {config.start_date} .. {config.end_date}")
    with subprocess.Popen(
        [
            sys.executable, "-m", "scrapy", "crawl", "wrc",
            "-a", f"start_date={config.start_date}",
            "-a", f"end_date={config.end_date}",
        ],
        env={**os.environ, "SCRAPY_SETTINGS_MODULE": "wrc_pipeline.settings"},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    ) as process:
        for line in process.stdout:
            line = line.rstrip()
            if not line:
                continue
            try:
                record = json.loads(line)
                level = getattr(logging, record.get("level", "INFO"), logging.INFO)
            except (ValueError, AttributeError, TypeError):
                level = logging.INFO
            context.log.log(level, line)
        returncode = process.wait()
    if returncode != 0:
        raise RuntimeError(f"Scrapy crawl failed (exit {returncode})")
    return MaterializeResult(
        metadata={"start_date": config.start_date, "end_date": config.end_date}
    )


@asset(deps=[landing_documents])
def cleaned_documents(context, config: DateRangeConfig) -> MaterializeResult:
    """Clean HTML, rename to identifier.ext, write to the curated bucket + collection."""
    from wrc_pipeline.transform.pipeline import run_transformation

    summary = run_transformation(config.start_date, config.end_date)
    context.log.info(f"Transformation summary: {summary}")
    return MaterializeResult(metadata=summary)


wrc_pipeline_job = define_asset_job(
    name="wrc_pipeline_job",
    selection=["landing_documents", "cleaned_documents"],
)

defs = Definitions(
    assets=[landing_documents, cleaned_documents],
    jobs=[wrc_pipeline_job],
)
