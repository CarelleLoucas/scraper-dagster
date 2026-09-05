import os
from datetime import datetime, timezone
from io import BytesIO
from typing import Any
from urllib.parse import quote_plus

from itemadapter import ItemAdapter
from minio import Minio
from pymongo import ASCENDING, DESCENDING, MongoClient
from scrapy import signals
from scrapy.exceptions import NotConfigured


class MongoMinioPipeline:
    """Store document bodies in MinIO and searchable metadata in MongoDB."""

    def __init__(self) -> None:
        self.mongo_client: MongoClient | None = None
        self.collection = None
        self.minio_client: Minio | None = None
        self.bucket = os.getenv("MINIO_BUCKET", "wrc-decisions")

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.open_spider, signal=signals.spider_opened)
        crawler.signals.connect(pipeline.close_spider, signal=signals.spider_closed)
        return pipeline

    def open_spider(self, spider) -> None:
        self.mongo_client = MongoClient(
            self._mongo_uri(),
            serverSelectionTimeoutMS=10_000,
            appname="wrc-scraper",
        )
        self.mongo_client.admin.command("ping")
        database_name = os.getenv("MONGO_DATABASE", "kedra")
        collection_name = os.getenv("MONGO_COLLECTION", "wrc_decisions")
        self.collection = self.mongo_client[database_name][collection_name]
        self._create_indexes()

        endpoint = os.getenv(
            "MINIO_ENDPOINT", f"localhost:{os.getenv('MINIO_API_PORT', '9000')}"
        )
        self.minio_client = Minio(
            endpoint,
            access_key=self._required("MINIO_ROOT_USER"),
            secret_key=self._required("MINIO_ROOT_PASSWORD"),
            secure=self._as_bool(os.getenv("MINIO_SECURE", "false")),
        )
        if not self.minio_client.bucket_exists(self.bucket):
            self.minio_client.make_bucket(self.bucket)

        spider.logger.info(
            "storage_ready mongo_database=%s mongo_collection=%s minio_bucket=%s",
            database_name,
            collection_name,
            self.bucket,
        )

    def process_item(self, item: Any, spider):
        adapter = ItemAdapter(item)
        content = adapter.get("_content")
        if not isinstance(content, bytes):
            raise ValueError("Scraped item is missing binary _content")
        if self.collection is None or self.minio_client is None:
            raise RuntimeError("Storage pipeline has not been initialized")

        object_name = self._object_name(adapter)
        # MinIO PUT is idempotent for this content-addressed object name.
        self.minio_client.put_object(
            self.bucket,
            object_name,
            BytesIO(content),
            length=len(content),
            content_type=adapter.get("content_type") or "application/octet-stream",
            metadata={
                "sha256": adapter["file_hash"],
                "identifier": adapter["identifier"],
            },
        )

        now = datetime.now(timezone.utc)
        metadata = dict(adapter)
        metadata.pop("_content", None)
        metadata.update(
            {
                "storage": {
                    "provider": "minio",
                    "bucket": self.bucket,
                    "object_name": object_name,
                },
                "last_seen_at": now,
            }
        )
        self.collection.update_one(
            {
                "identifier": metadata["identifier"],
                "file_hash": metadata["file_hash"],
            },
            {
                "$set": metadata,
                "$setOnInsert": {"first_seen_at": now},
            },
            upsert=True,
        )

        adapter.pop("_content", None)
        adapter["storage"] = metadata["storage"]
        spider.logger.info(
            "document_stored identifier=%s bucket=%s object=%s",
            adapter["identifier"],
            self.bucket,
            object_name,
        )
        return item

    def close_spider(self, spider) -> None:
        if self.mongo_client is not None:
            self.mongo_client.close()

    def _create_indexes(self) -> None:
        self.collection.create_index(
            [("identifier", ASCENDING), ("file_hash", ASCENDING)],
            unique=True,
            name="decision_version",
        )
        self.collection.create_index(
            [("body", ASCENDING), ("published_date", DESCENDING)],
            name="body_published_date",
        )
        self.collection.create_index(
            [("source_url", ASCENDING)], name="source_url"
        )

    @staticmethod
    def _object_name(adapter: ItemAdapter) -> str:
        path = adapter["file_path"]
        stem, separator, extension = path.rpartition(".")
        if not separator:
            stem, extension = path, "bin"
        return f"{stem}/{adapter['file_hash']}.{extension}"

    @classmethod
    def _mongo_uri(cls) -> str:
        explicit_uri = os.getenv("MONGO_URI")
        if explicit_uri:
            return explicit_uri
        username = quote_plus(cls._required("MONGO_ROOT_USERNAME"))
        password = quote_plus(cls._required("MONGO_ROOT_PASSWORD"))
        host = os.getenv("MONGO_HOST", "localhost")
        port = os.getenv("MONGO_PORT", "27017")
        return f"mongodb://{username}:{password}@{host}:{port}/?authSource=admin"

    @staticmethod
    def _required(name: str) -> str:
        value = os.getenv(name)
        if not value:
            raise NotConfigured(f"Required environment variable {name} is not set")
        return value

    @staticmethod
    def _as_bool(value: str) -> bool:
        return value.strip().lower() in {"1", "true", "yes", "on"}
