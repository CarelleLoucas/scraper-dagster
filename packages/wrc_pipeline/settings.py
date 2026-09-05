"""Scrapy settings for the WRC pipeline."""
import logging

from dotenv import load_dotenv

load_dotenv()

BOT_NAME = "wrc_pipeline"
SPIDER_MODULES = ["wrc_pipeline.spiders"]
NEWSPIDER_MODULE = "wrc_pipeline.spiders"

ROBOTSTXT_OBEY = True
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"

LOG_ENABLED = False

from wrc_pipeline.logging_config import configure_json_logging  # noqa: E402

configure_json_logging(level=logging.INFO)