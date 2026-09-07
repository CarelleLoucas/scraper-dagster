"""Transformation stage: clean landing-zone documents into a curated zone.

Given a date range it reads landing metadata from Mongo, pulls each object from
the landing MinIO bucket, and for every record:

* PDF / DOC / DOCX  -> passed through unchanged.
* HTML              -> stripped to the decision content with BeautifulSoup and
                       re-hashed.

Every file is renamed to ``identifier.ext``, written to a NEW bucket, and its
metadata (new path + new hash) is written to a NEW Mongo collection. The landing
zone is read-only here and is never modified.
"""
import argparse
import hashlib
import logging
import os
from datetime import datetime, timezone
from io import BytesIO
from urllib.parse import quote_plus
import re

from bs4 import BeautifulSoup
from dotenv import load_dotenv
from minio import Minio
from pymongo import ASCENDING, MongoClient

from wrc_pipeline.logging_config import configure_json_logging

load_dotenv()
logger = logging.getLogger("wrc.transform")

# Tags that are never part of the decision text.
_STRIP_TAGS = ("nav", "header", "footer", "script", "style", "form", "button")
# Candidate selectors for the decision body, tried in order. 
_CONTENT_SELECTORS = ("div.content", "main", "div[role='main']", "article", "#content", "body")
_PASSTHROUGH = {"pdf", "doc", "docx"}


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Required environment variable {name} is not set")
    return value


def _as_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _mongo_uri() -> str:
    explicit = os.getenv("MONGO_URI")
    if explicit:
        return explicit
    username = quote_plus(_required("MONGO_ROOT_USERNAME"))
    password = quote_plus(_required("MONGO_ROOT_PASSWORD"))
    host = os.getenv("MONGO_HOST", "localhost")
    port = os.getenv("MONGO_PORT", "27017")
    return f"mongodb://{username}:{password}@{host}:{port}/?authSource=admin"


def _mongo_client() -> MongoClient:
    client = MongoClient(_mongo_uri(), serverSelectionTimeoutMS=10_000,
                         appname="wrc-transform")
    client.admin.command("ping")
    return client


def _minio_client() -> Minio:
    endpoint = os.getenv(
        "MINIO_ENDPOINT", f"localhost:{os.getenv('MINIO_API_PORT', '9000')}"
    )
    return Minio(
        endpoint,
        access_key=_required("MINIO_ROOT_USER"),
        secret_key=_required("MINIO_ROOT_PASSWORD"),
        secure=_as_bool(os.getenv("MINIO_SECURE", "false")),
    )


def clean_html(raw: bytes) -> bytes:
    """Return only the decision content, cleaned for data quality.

    Steps: decode as UTF-8, strip chrome (nav/header/footer/scripts/back-link),
    select the decision container, drop empty paragraphs, and replace
    non-breaking spaces with regular spaces.
    """
    text = raw.decode("utf-8", errors="replace")
    soup = BeautifulSoup(text, "lxml")

    # Remove obvious non-content elements wherever they appear.
    for tag in soup(list(_STRIP_TAGS)):
        tag.decompose()
    for anchor in soup.find_all("a"):
        if "return to search" in anchor.get_text(strip=True).casefold():
            anchor.decompose()

    # Select the decision content container.
    node = None
    for selector in _CONTENT_SELECTORS:
        node = soup.select_one(selector)
        if node is not None:
            break
    if node is None:
        node = soup

    # Drop empty paragraphs left behind after stripping.
    for p in node.find_all("p"):
        if not p.get_text(strip=True) and not p.find(("img", "table", "br")):
            p.decompose()

    # Replace non-breaking spaces
    for text_node in node.find_all(string=True):
        cleaned = str(text_node).replace("\xa0", " ")
        if cleaned != str(text_node):
            text_node.replace_with(cleaned)

    return node.encode("utf-8")


_SAFE_NAME = re.compile(r"[^A-Za-z0-9._-]+")

def _curated_object_name(identifier: str, extension: str) -> str:
    """Rename to identifier.ext (brief requirement), sanitized to safe chars."""
    safe = _SAFE_NAME.sub("_", identifier).strip("_")
    return f"{safe}.{extension}"

def _ascii_safe(value: str) -> str:
    """MinIO object metadata (HTTP headers) must be US-ASCII so storage
    doesn't reject the header."""
    return value.encode("ascii", "replace").decode("ascii")


def run_transformation(start_date: str, end_date: str) -> dict:
    """Transform landing documents in [start_date, end_date] into the curated zone.
    """
    src_collection_name = os.getenv("MONGO_COLLECTION", "wrc_decisions")
    dst_collection_name = os.getenv("MONGO_COLLECTION_CLEAN", "wrc_decisions_clean")
    src_bucket = os.getenv("MINIO_BUCKET", "wrc-decisions")
    dst_bucket = os.getenv("MINIO_BUCKET_CLEAN", "wrc-decisions-clean")

    mongo = _minio = None
    processed = transformed_html = passthrough = failed = 0
    failures: list[dict] = []

    try:
        mongo = _mongo_client()
        db = mongo[os.getenv("MONGO_DATABASE", "kedra")]
        src = db[src_collection_name]
        dst = db[dst_collection_name]
        dst.create_index(
            [("identifier", ASCENDING), ("file_hash", ASCENDING)],
            unique=True, name="clean_decision_version",
        )

        _minio = _minio_client()
        if not _minio.bucket_exists(dst_bucket):
            _minio.make_bucket(dst_bucket)

        query = {"published_date": {"$gte": start_date, "$lte": end_date}}
        total = src.count_documents(query)
        logger.info("transform_started", extra={"start_date": start_date, "end_date": end_date, "source_collection": src_collection_name, "matched": total})

        for record in src.find(query):
            identifier = record.get("identifier", "UNKNOWN")
            extension = record.get("file_extension", "bin")
            storage = record.get("storage", {})
            object_name = storage.get("object_name")
            try:
                if not object_name:
                    raise ValueError("record has no landing object_name")

                response = _minio.get_object(src_bucket, object_name)
                try:
                    body = response.read()
                finally:
                    response.close()
                    response.release_conn()

                if extension == "html":
                    body = clean_html(body)
                    transformed_html += 1
                elif extension in _PASSTHROUGH:
                    passthrough += 1
                else:
                    # Unknown types pass through unchanged but are counted.
                    passthrough += 1

                new_hash = hashlib.sha256(body).hexdigest()
                new_object = _curated_object_name(identifier, extension)

                _minio.put_object(
                    dst_bucket, new_object, BytesIO(body), length=len(body),
                    content_type=record.get("content_type")
                    or "application/octet-stream",
                    metadata={"sha256": new_hash, "identifier": _ascii_safe(identifier)},
                )

                now = datetime.now(timezone.utc)
                clean_meta = {
                    k: v for k, v in record.items()
                    if k not in {"_id", "_content", "first_seen_at", "last_seen_at"}
                }
                clean_meta.update({
                    "file_hash": new_hash,
                    "file_path": new_object,
                    "storage": {
                        "provider": "minio",
                        "bucket": dst_bucket,
                        "object_name": new_object,
                    },
                    "source_landing_hash": record.get("file_hash"),
                    "transformed_at": now,
                })
                # Idempotent: same identifier+new_hash won't duplicate on re-run.
                dst.update_one(
                    {"identifier": identifier, "file_hash": new_hash},
                    {"$set": clean_meta, "$setOnInsert": {"first_seen_at": now}},
                    upsert=True,
                )
                processed += 1
                logger.info("document_transformed", extra={"identifier": identifier, "extension": extension, "new_hash": new_hash, "new_object": new_object})
            except Exception as error:  # noqa: BLE001 - log and continue per brief
                failed += 1
                failures.append({
                    "identifier": identifier,
                    "object_name": object_name,
                    "error": repr(error),
                })
                logger.error("document_transform_failed", extra={"identifier": identifier, "object_name": object_name, "error": repr(error)})

        summary = {
            "matched": total,
            "processed": processed,
            "transformed_html": transformed_html,
            "passthrough": passthrough,
            "failed": failed,
            "failures": failures,
        }
        logger.info("transform_summary", extra=summary)
        return summary
    finally:
        if mongo is not None:
            mongo.close()


def main() -> None:
    """Run with python -m wrc_pipeline.transform.transform."""
    configure_json_logging()
    parser = argparse.ArgumentParser(description="Transform WRC landing documents.")
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()
    result = run_transformation(args.start_date, args.end_date)
    print(result)


if __name__ == "__main__":
    main()
