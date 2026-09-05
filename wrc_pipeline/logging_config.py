"""JSON structured logging shared by the spider and the transformation script."""
import logging

from pythonjsonlogger import jsonlogger


def configure_json_logging(level: int = logging.INFO) -> None:
    """Route the root logger through a single JSON handler.

    Any fields passed via ``logger.info(msg, extra={...})`` are merged into
    the JSON object, which is how the spider emits partition/body/counts and
    the end-of-run summary required by the brief.
    """
    handler = logging.StreamHandler()
    handler.setFormatter(
        jsonlogger.JsonFormatter(
            "%(asctime)s %(levelname)s %(name)s %(message)s",
            rename_fields={"asctime": "timestamp", "levelname": "level"},
            json_ensure_ascii=False,
        )
    )
    for noisy in ("pymongo", "urllib3", "scrapy.core.scraper"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)