import hashlib
import re
from collections.abc import Iterable, Iterator
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlparse

import scrapy
from scrapy import signals


class WRCSpider(scrapy.Spider):
    """Download WRC decisions by adjudicating body and date range."""

    name = "wrc"
    allowed_domains = ["workplacerelations.ie"]
    search_url = "https://www.workplacerelations.ie/en/search/"

    # body=3 was verified from a browser capture. Recheck the others if the
    # website changes its search form.
    BODIES = {
        "employment_appeals_tribunal": "1",
        "equality_tribunal": "2",
        "labour_court": "3",
        "workplace_relations_commission": "4",
    }

    DATE_PATTERN = re.compile(r"\b\d{1,2}/\d{1,2}/\d{4}\b")
    RESULT_COUNT_PATTERN = re.compile(
        r"Shows\s+\d+\s+to\s+\d+\s+of\s+([\d,]+)\s+results", re.IGNORECASE
    )
    SAFE_FILENAME_PATTERN = re.compile(r"[^A-Za-z0-9._-]+")

    PRIMARY_RESULT_SELECTOR = "//h2/a[contains(@href, '/en/cases/')]"
    FALLBACK_RESULT_SELECTOR = (
        "//a[contains(@href, '/en/cases/') and not(contains("
        "translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', "
        "'abcdefghijklmnopqrstuvwxyz'), 'view page'))]"
    )
    NEXT_PAGE_SELECTORS = (
        "//a[@rel='next']/@href",
        "//a[contains(translate(normalize-space(.), "
        "'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'next')]/@href",
        "//a[contains(@class, 'next')]/@href",
        "//li[contains(@class, 'next')]/a/@href",
    )

    custom_settings = {
        "ITEM_PIPELINES": {"wrc_pipeline.pipelines.MongoMinioPipeline": 300},
        "CONCURRENT_REQUESTS": 16,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 0.5,
        "AUTOTHROTTLE_MAX_DELAY": 10.0,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 4.0,
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 3,
        "RETRY_HTTP_CODES": [408, 429, 500, 502, 503, 504],
        "DOWNLOAD_TIMEOUT": 30,
        "USER_AGENT": "Mozilla/5.0 (compatible; WRCResearchScraper/1.0)",
        "LOG_LEVEL": "INFO",
    }


    def __init__(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        output_dir: str = "data/landing",
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.start_date = (
            self.parse_cli_date(start_date, "start_date")
            if start_date
            else date(2026, 1, 1)
        )

        self.end_date = (
            self.parse_cli_date(end_date, "end_date")
            if end_date
            else date.today()
        )

        if self.start_date > self.end_date:
            raise ValueError("start_date cannot be after end_date")

        self.output_dir = Path(output_dir)

        # Run-level accounting for the end-of-run summary (requirement 10).
        self.stats_found = 0        # results the site reported across partitions
        self.stats_scraped = 0      # documents successfully stored
        self.stats_failed = 0       # documents that failed, with reasons logged
        self.failures: list[dict] = []
        self.counted_partitions: set = set()

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.on_spider_closed, signal=signals.spider_closed)
        return spider

    def on_spider_closed(self, spider, reason):
        self.logger.info("run_summary", extra={
            "results_found": self.stats_found,
            "documents_scraped": self.stats_scraped,
            "documents_failed": self.stats_failed,
            "failures": self.failures,
            "close_reason": reason,
        })

    async def start(self):
        """Create one initial request per body and calendar month."""
        for body_name, body_id in self.BODIES.items():
            for range_start, range_end in self.monthly_partitions(
                self.start_date, self.end_date
            ):
                yield self.create_search_request(
                    body_name,
                    body_id,
                    range_start,
                    range_end,
                    range_start.strftime("%Y-%m"),
                )

    @staticmethod
    def monthly_partitions(start: date, end: date) -> Iterator[tuple[date, date]]:
        """Split an inclusive range into inclusive calendar-month ranges."""
        current = start
        while current <= end:
            next_month = (
                date(current.year + 1, 1, 1)
                if current.month == 12
                else date(current.year, current.month + 1, 1)
            )
            range_end = min(next_month - timedelta(days=1), end)
            yield current, range_end
            current = range_end + timedelta(days=1)

    def create_search_request(
        self,
        body_name: str,
        body_id: str,
        range_start: date,
        range_end: date,
        partition: str,
    ) -> scrapy.Request:
        query = urlencode(
            {
                "decisions": "1",
                "body": body_id,
                "from": self.format_wrc_date(range_start),
                "to": self.format_wrc_date(range_end),
            }
        )
        url = f"{self.search_url}?{query}"
        context = {
            "body_name": body_name,
            "body_id": body_id,
            "range_start": range_start,
            "range_end": range_end,
            "partition": partition,
        }
        self.logger.info(
            "partition_started body=%s partition=%s start=%s end=%s url=%s",
            body_name,
            partition,
            range_start,
            range_end,
            url,
        )
        return scrapy.Request(
            url,
            callback=self.parse_search_results,
            errback=self.search_error,
            cb_kwargs=context,
        )

    def parse_search_results(
        self,
        response: scrapy.http.Response,
        body_name: str,
        body_id: str,
        range_start: date,
        range_end: date,
        partition: str,
    ) -> Iterator[scrapy.Request]:
        entries = self.extract_result_entries(response)
        total = self.extract_total_count(response)
        partition_key = (body_name, partition)
        if total is not None and partition_key not in self.counted_partitions:
            self.counted_partitions.add(partition_key)
            self.stats_found += total
        context = {
            "body_name": body_name,
            "body_id": body_id,
            "range_start": range_start,
            "range_end": range_end,
            "partition": partition,
        }
        self.logger.info(
            "search_page_received body=%s partition=%s range=%s..%s "
            "total=%s page_records=%s",
            body_name,
            partition,
            range_start,
            range_end,
            total,
            len(entries),
        )

        if next_page := self.extract_next_page(response):
            yield from self.document_requests(response, entries, body_name, partition)
            yield response.follow(
                next_page,
                callback=self.parse_search_results,
                errback=self.search_error,
                cb_kwargs=context,
            )
            return

        # Narrow the range when pagination markup is missing, preventing silent
        # data loss without reprocessing the visible subset.
        if total is not None and total > len(entries):
            if range_start == range_end:
                raise RuntimeError(
                    f"Pagination required for {body_name} on {range_start}: "
                    f"the site reports {total} results but exposes {len(entries)}."
                )

            midpoint = range_start + (range_end - range_start) // 2
            self.logger.warning(
                "pagination_not_detected_splitting_range body=%s partition=%s "
                "range=%s..%s total=%s",
                body_name,
                partition,
                range_start,
                range_end,
                total,
            )
            yield self.create_search_request(
                body_name, body_id, range_start, midpoint, partition
            )
            yield self.create_search_request(
                body_name,
                body_id,
                midpoint + timedelta(days=1),
                range_end,
                partition,
            )
            return

        yield from self.document_requests(response, entries, body_name, partition)

    def extract_result_entries(self, response: scrapy.http.Response) -> list[dict]:
        """Extract unique decision links and their search-result metadata."""
        links = response.xpath(self.PRIMARY_RESULT_SELECTOR)
        if not links:
            links = response.xpath(self.FALLBACK_RESULT_SELECTOR)

        entries = []
        seen_urls = set()
        for link in links:
            if not (href := link.attrib.get("href")):
                continue
            document_url = response.urljoin(href)
            if document_url in seen_urls:
                continue
            seen_urls.add(document_url)

            identifier = self.clean_text(link.xpath("string(.)").get())
            container = link.xpath("ancestor::li[1]")
            if not container:
                container = link.xpath("ancestor::div[1]")
            text_parts = [
                cleaned
                for value in container.xpath(".//text()").getall()
                if (cleaned := self.clean_text(value))
            ]
            entries.append(
                {
                    "identifier": identifier,
                    "description": self.extract_description(text_parts, identifier),
                    "published_date": self.find_date(text_parts),
                    "document_url": document_url,
                }
            )
        return entries

    def document_requests(
        self,
        response: scrapy.http.Response,
        entries: Iterable[dict],
        body_name: str,
        partition: str,
    ) -> Iterator[scrapy.Request]:
        for entry in entries:
            metadata = {
                **entry,
                "body": body_name,
                "partition_date": partition,
                "search_url": response.url,
            }
            yield scrapy.Request(
                entry["document_url"],
                callback=self.parse_document,
                errback=self.document_error,
                cb_kwargs={"metadata": metadata},
            )
    def _stable_hash(self, response) -> str:
        """Hash meaningful content, ignoring per-request dynamic markup.

        WRC HTML embeds request-specific tokens/timestamps that change every
        fetch; hashing the raw bytes would break idempotency. For HTML we hash
        the text content of the main container; for binary docs we hash bytes.
        """
        content_type = response.headers.get(b"Content-Type", b"").decode(
            "utf-8", errors="ignore"
        ).lower()
        if "html" in content_type:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.body, "lxml")
            for tag in soup(["script", "style", "meta", "link", "input"]):
                tag.decompose()
            # Normalized text is stable across requests.
            text = " ".join(soup.get_text().split())
            return hashlib.sha256(text.encode("utf-8")).hexdigest()
        return hashlib.sha256(response.body).hexdigest()

    def parse_document(
        self, response: scrapy.http.Response, metadata: dict
    ) -> Iterator[dict]:
        content_type = response.headers.get(b"Content-Type", b"").decode(
            "utf-8", errors="ignore"
        ).lower()
        extension = self.detect_extension(response.url, content_type)
        # file_hash = hashlib.sha256(response.body).hexdigest()
        file_hash = self._stable_hash(response)
        filename = self.safe_filename(metadata["identifier"])
        file_path = (
            f"{metadata['body']}/{metadata['partition_date']}/"
            f"{filename}.{extension}"
        )
        self.logger.info(
            "document_scraped identifier=%s body=%s partition=%s type=%s "
            "bytes=%s hash=%s",
            metadata["identifier"],
            metadata["body"],
            metadata["partition_date"],
            extension,
            len(response.body),
            file_hash,
        )
        self.stats_scraped += 1
        yield {
            **metadata,
            "source_url": response.url,
            "http_status": response.status,
            "content_type": content_type,
            "file_extension": extension,
            "file_path": file_path,
            "file_hash": file_hash,
            "_content": response.body,  # Storage pipeline should remove this.
        }

    @classmethod
    def extract_total_count(cls, response: scrapy.http.Response) -> int | None:
        text = " ".join(response.xpath("//text()").getall())
        match = cls.RESULT_COUNT_PATTERN.search(text)
        return int(match.group(1).replace(",", "")) if match else None

    @classmethod
    def extract_next_page(cls, response: scrapy.http.Response) -> str | None:
        for selector in cls.NEXT_PAGE_SELECTORS:
            if href := response.xpath(selector).get():
                return href
        return None

    @classmethod
    def find_date(cls, text_parts: Iterable[str]) -> str | None:
        for value in text_parts:
            if not (match := cls.DATE_PATTERN.search(value)):
                continue
            try:
                return datetime.strptime(match.group(), "%d/%m/%Y").date().isoformat()
            except ValueError:
                continue
        return None

    @classmethod
    def extract_description(
        cls, text_parts: Iterable[str], identifier: str
    ) -> str | None:
        ignored = {identifier.casefold(), "view page"}
        candidates = [
            text
            for text in text_parts
            if text.casefold() not in ignored
            and not text.casefold().startswith("ref no:")
            and not cls.DATE_PATTERN.fullmatch(text)
        ]
        return max(candidates, key=len, default=None)

    @staticmethod
    def detect_extension(url: str, content_type: str) -> str:
        mime_types = {
            "application/pdf": "pdf",
            "application/msword": "doc",
            "application/vnd.ms-word": "doc",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
            "text/html": "html",
        }
        for mime_type, extension in mime_types.items():
            if mime_type in content_type:
                return extension

        extension = Path(urlparse(url).path).suffix.lower().lstrip(".")
        if extension in {"pdf", "doc", "docx", "html", "htm"}:
            return "html" if extension == "htm" else extension
        return "bin"

    def search_error(self, failure: Any) -> None:
        self.log_failure("search_failed", failure)

    def document_error(self, failure: Any) -> None:
        self.log_failure("document_download_failed", failure)

    # def log_failure(self, event: str, failure: Any) -> None:
    #     response = getattr(failure.value, "response", None)
    #     status = response.status if response is not None else None
    #     self.logger.error(
    #         "%s url=%s status=%s error=%r",
    #         event,
    #         failure.request.url,
    #         status,
    #         failure.value,
    #     )

    def log_failure(self, event: str, failure: Any) -> None:
        response = getattr(failure.value, "response", None)
        status = response.status if response is not None else None
        self.stats_failed += 1
        self.failures.append({
            "event": event,
            "url": failure.request.url,
            "status": status,
            "error": repr(failure.value),
        })
        self.logger.error(
            "%s url=%s status=%s error=%r",
            event,
            failure.request.url,
            status,
            failure.value,
        )

    @staticmethod
    def parse_cli_date(value: str, argument_name: str) -> date:
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError as error:
            raise ValueError(f"{argument_name} must use YYYY-MM-DD format") from error

    @staticmethod
    def format_wrc_date(value: date) -> str:
        return f"{value.day}/{value.month}/{value.year}"

    @staticmethod
    def clean_text(value: str | None) -> str:
        return " ".join(value.split()) if value else ""

    @classmethod
    def safe_filename(cls, value: str) -> str:
        return cls.SAFE_FILENAME_PATTERN.sub("_", value).strip("_")


from scrapy.crawler import CrawlerProcess

if __name__ == "__main__":
    process = CrawlerProcess()

    process.crawl(
        WRCSpider,
        output_dir="data/landing",
    )

    process.start()
