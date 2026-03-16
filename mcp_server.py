from __future__ import annotations

import argparse
import json
import re
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pprint import pformat
from textwrap import dedent
from typing import Any, Literal
from urllib.parse import urljoin

import scraper_rs
from fastmcp import FastMCP
from pydantic import BaseModel, Field, model_validator
from rnet import Emulation
from silkworm import Engine, HTMLResponse, Request, Spider
from silkworm.cdp import CDPClient
from silkworm.http import HttpClient

SERVER_VERSION = "0.1.0"
DEFAULT_HTML_MAX_SIZE_BYTES = 5_000_000
DEFAULT_TEXT_PREVIEW_CHARS = 1_200
DEFAULT_HTML_PREVIEW_CHARS = 1_500

SERVER_INSTRUCTIONS = dedent(
    """
    Use this server to build, debug, and validate scrapers with silkworm-rs and scraper-rs.

    Recommended workflow:
    1. Fetch a page with `silkworm_fetch` or `silkworm_fetch_cdp`, or save raw HTML with `store_html_document`.
    2. Reuse the returned `document_handle` instead of resending large HTML blobs.
    3. Inspect the page with `inspect_document`, then iterate on selectors with `query_selector`,
       `compare_selectors`, `extract_links`, and `prettify_document`.
    4. When selectors are stable, either run an ad hoc crawl with `run_crawl_blueprint`
       or generate production starter code with `generate_spider_template`.

    This server is optimized for LLM-guided scraper development and returns structured results
    that mirror how silkworm and scraper-rs actually behave.
    """
).strip()

mcp = FastMCP(
    "silkworm-mcp",
    instructions=SERVER_INSTRUCTIONS,
    version=SERVER_VERSION,
    website_url="https://github.com/BitingSnakes/silkworm",
)


class SelectorMode(str, Enum):
    css = "css"
    xpath = "xpath"


class FieldExtractor(str, Enum):
    text = "text"
    html = "html"
    attr = "attr"


class StoredDocumentInfo(BaseModel):
    handle: str
    label: str | None = None
    source_url: str | None = None
    stored_at: str
    fetched_via: str | None = None
    status: int | None = None
    html_chars: int


class DocumentSummary(BaseModel):
    handle: str | None = None
    label: str | None = None
    source_url: str | None = None
    fetched_via: str | None = None
    status: int | None = None
    html_chars: int
    text_chars: int
    title: str | None = None
    link_count: int
    image_count: int
    form_count: int
    heading_preview: list[str] = Field(default_factory=list)
    text_preview: str


class SelectorMatch(BaseModel):
    index: int
    tag: str
    text: str
    html: str | None = None
    attrs: dict[str, str] = Field(default_factory=dict)


class SelectorQueryResult(BaseModel):
    document_handle: str | None = None
    source_url: str | None = None
    mode: SelectorMode
    query: str
    total_matches: int
    returned_matches: int
    omitted_matches: int
    matches: list[SelectorMatch] = Field(default_factory=list)


class SelectorComparisonEntry(BaseModel):
    query: str
    total_matches: int
    preview: list[SelectorMatch] = Field(default_factory=list)


class SelectorComparisonResult(BaseModel):
    document_handle: str | None = None
    source_url: str | None = None
    mode: SelectorMode
    comparisons: list[SelectorComparisonEntry] = Field(default_factory=list)


class LinkMatch(BaseModel):
    index: int
    text: str
    raw_url: str
    absolute_url: str
    attrs: dict[str, str] = Field(default_factory=dict)


class LinkExtractionResult(BaseModel):
    document_handle: str | None = None
    source_url: str | None = None
    mode: SelectorMode
    query: str
    attribute: str
    total_matches: int
    links: list[LinkMatch] = Field(default_factory=list)


class PrettifyResult(BaseModel):
    document_handle: str | None = None
    source_url: str | None = None
    output_chars: int
    truncated_output: bool
    prettified_html: str


class DeleteDocumentResult(BaseModel):
    handle: str
    deleted: bool


class ClearDocumentsResult(BaseModel):
    deleted_count: int


class FetchResult(BaseModel):
    url: str
    final_url: str
    method: str
    status: int | None = None
    is_html: bool
    emulation: str | None = None
    via: Literal["http", "cdp"]
    headers: dict[str, str] = Field(default_factory=dict)
    body_chars: int
    body_preview: str
    document_handle: str | None = None
    summary: DocumentSummary | None = None


class CrawlFieldSpec(BaseModel):
    name: str = Field(description="Output field name.")
    css: str | None = Field(
        default=None,
        description="CSS selector relative to the current scope.",
    )
    xpath: str | None = Field(
        default=None,
        description="XPath selector relative to the current scope.",
    )
    extractor: FieldExtractor = Field(
        default=FieldExtractor.text,
        description="How to extract data from the selected node(s).",
    )
    attr_name: str | None = Field(
        default=None,
        description="Attribute name to read when extractor='attr'.",
    )
    all_matches: bool = Field(
        default=False,
        description="Return all matches instead of the first match.",
    )
    join_with: str | None = Field(
        default=None,
        description="If set together with all_matches=true, join results into one string.",
    )
    absolute_url: bool = Field(
        default=False,
        description="Resolve extracted URLs relative to the page URL.",
    )
    strip: bool = Field(
        default=True,
        description="Trim surrounding whitespace from extracted text.",
    )
    default: Any = Field(
        default=None,
        description="Fallback value when nothing matches.",
    )

    @model_validator(mode="after")
    def validate_query(self) -> "CrawlFieldSpec":
        if bool(self.css) == bool(self.xpath):
            raise ValueError(
                f"Field '{self.name}' must define exactly one of 'css' or 'xpath'."
            )
        if self.extractor == FieldExtractor.attr and not self.attr_name:
            raise ValueError(
                f"Field '{self.name}' uses extractor='attr' but no attr_name was provided."
            )
        if self.extractor != FieldExtractor.attr and self.attr_name is not None:
            raise ValueError(
                f"Field '{self.name}' set attr_name but extractor is not 'attr'."
            )
        return self


class CrawlBlueprint(BaseModel):
    spider_name: str = Field(
        default="mcp_spider",
        description="Spider name used for logging and generated templates.",
    )
    start_urls: list[str] = Field(
        min_length=1,
        description="Entry pages for the crawl.",
    )
    request_headers: dict[str, str] = Field(
        default_factory=dict,
        description="Headers attached to start and follow-up requests.",
    )
    item_selector: str | None = Field(
        default=None,
        description="CSS selector for per-item containers.",
    )
    item_xpath: str | None = Field(
        default=None,
        description="XPath for per-item containers.",
    )
    fields: list[CrawlFieldSpec] = Field(
        min_length=1,
        description="Fields to extract from each item scope.",
    )
    pagination_selector: str | None = Field(
        default=None,
        description="CSS selector for the next page link.",
    )
    pagination_xpath: str | None = Field(
        default=None,
        description="XPath for the next page link.",
    )
    follow_links_selector: str | None = Field(
        default=None,
        description="CSS selector for detail links to follow on each page.",
    )
    follow_links_xpath: str | None = Field(
        default=None,
        description="XPath selector for detail links to follow on each page.",
    )
    follow_link_limit_per_page: int = Field(
        default=20,
        ge=0,
        le=500,
        description="Max followed detail links per page.",
    )
    max_requests: int = Field(
        default=50,
        ge=1,
        le=1000,
        description="Stop scheduling new requests after this many total requests.",
    )
    max_items: int = Field(
        default=100,
        ge=1,
        le=5000,
        description="Stop yielding items after this many extracted records.",
    )
    concurrency: int = Field(
        default=8,
        ge=1,
        le=128,
        description="silkworm Engine concurrency.",
    )
    max_pending_requests: int | None = Field(
        default=None,
        ge=1,
        le=10_000,
        description="Optional Engine queue cap.",
    )
    request_timeout_seconds: float | None = Field(
        default=20.0,
        ge=0.1,
        le=300,
        description="Per-request timeout in seconds.",
    )
    html_max_size_bytes: int = Field(
        default=DEFAULT_HTML_MAX_SIZE_BYTES,
        ge=1_024,
        le=50_000_000,
        description="Maximum parsed HTML payload size.",
    )
    keep_alive: bool = Field(
        default=False,
        description="Enable keep-alive where the underlying client supports it.",
    )
    log_stats_interval: float | None = Field(
        default=None,
        ge=0.1,
        le=300,
        description="Optional silkworm periodic stats logging interval.",
    )
    include_source_url: bool = Field(
        default=True,
        description="Include _source_url on emitted items.",
    )

    @model_validator(mode="after")
    def validate_blueprint(self) -> "CrawlBlueprint":
        _validate_optional_query_pair(
            self.item_selector,
            self.item_xpath,
            "item_selector/item_xpath",
        )
        _validate_optional_query_pair(
            self.pagination_selector,
            self.pagination_xpath,
            "pagination_selector/pagination_xpath",
        )
        _validate_optional_query_pair(
            self.follow_links_selector,
            self.follow_links_xpath,
            "follow_links_selector/follow_links_xpath",
        )
        return self


class CrawlRunResult(BaseModel):
    spider_name: str
    scheduled_requests: int
    emitted_items: int
    max_requests: int
    max_items: int
    items: list[dict[str, Any]] = Field(default_factory=list)


class SpiderTemplateResult(BaseModel):
    class_name: str
    spider_name: str
    code: str


@dataclass(slots=True)
class StoredDocument:
    handle: str
    html: str
    label: str | None = None
    source_url: str | None = None
    stored_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    fetched_via: str | None = None
    status: int | None = None
    headers: dict[str, str] = field(default_factory=dict)

    def info(self) -> StoredDocumentInfo:
        return StoredDocumentInfo(
            handle=self.handle,
            label=self.label,
            source_url=self.source_url,
            stored_at=self.stored_at.isoformat(),
            fetched_via=self.fetched_via,
            status=self.status,
            html_chars=len(self.html),
        )


class DocumentStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._documents: dict[str, StoredDocument] = {}

    def add(
        self,
        html: str,
        *,
        label: str | None = None,
        source_url: str | None = None,
        fetched_via: str | None = None,
        status: int | None = None,
        headers: dict[str, str] | None = None,
    ) -> StoredDocument:
        handle = uuid.uuid4().hex[:12]
        document = StoredDocument(
            handle=handle,
            html=html,
            label=label,
            source_url=source_url,
            fetched_via=fetched_via,
            status=status,
            headers=dict(headers or {}),
        )
        with self._lock:
            self._documents[handle] = document
        return document

    def get(self, handle: str) -> StoredDocument:
        with self._lock:
            document = self._documents.get(handle)
        if document is None:
            raise ValueError(f"Unknown document handle: {handle}")
        return document

    def list(self) -> list[StoredDocument]:
        with self._lock:
            documents = list(self._documents.values())
        documents.sort(key=lambda doc: doc.stored_at, reverse=True)
        return documents

    def delete(self, handle: str) -> bool:
        with self._lock:
            return self._documents.pop(handle, None) is not None

    def clear(self) -> int:
        with self._lock:
            deleted = len(self._documents)
            self._documents.clear()
        return deleted


DOCUMENT_STORE = DocumentStore()
EMULATION_NAMES = sorted(name for name in dir(Emulation) if not name.startswith("_"))


def _validate_optional_query_pair(
    css: str | None,
    xpath: str | None,
    label: str,
) -> None:
    if css and xpath:
        raise ValueError(f"{label} accepts either CSS or XPath, not both at once.")


def _clip(value: str | None, limit: int) -> str:
    if not value:
        return ""
    text = value.strip()
    if len(text) <= limit:
        return text
    return f"{text[:limit].rstrip()}..."


def _normalize_identifier(name: str, default: str = "GeneratedSpider") -> str:
    candidate = re.sub(r"[^0-9a-zA-Z_]", "_", name).strip("_") or default
    if candidate[0].isdigit():
        candidate = f"_{candidate}"
    return candidate


def _resolve_document_input(
    *,
    document_handle: str | None = None,
    html: str | None = None,
) -> tuple[str, StoredDocument | None]:
    if bool(document_handle) == bool(html):
        raise ValueError("Provide exactly one of 'document_handle' or 'html'.")
    if document_handle is not None:
        document = DOCUMENT_STORE.get(document_handle)
        return document.html, document
    assert html is not None
    return html, None


def _document_base_url(
    document: StoredDocument | None,
    override: str | None = None,
) -> str | None:
    if override:
        return override
    if document is not None:
        return document.source_url
    return None


def _normalize_emulation(name: str) -> Emulation:
    try:
        return getattr(Emulation, name)
    except AttributeError as exc:
        choices = ", ".join(EMULATION_NAMES)
        raise ValueError(
            f"Unknown emulation '{name}'. Available values: {choices}"
        ) from exc


def _serialize_element(
    element: Any,
    *,
    index: int,
    include_html: bool,
    text_chars: int,
    html_chars: int,
) -> SelectorMatch:
    return SelectorMatch(
        index=index,
        tag=str(getattr(element, "tag", "")),
        text=_clip(str(getattr(element, "text", "")), text_chars),
        html=_clip(str(getattr(element, "html", "")), html_chars)
        if include_html
        else None,
        attrs=dict(getattr(element, "attrs", {}) or {}),
    )


def _query_document(
    document: scraper_rs.Document,
    *,
    query: str,
    mode: SelectorMode,
) -> list[Any]:
    if mode == SelectorMode.css:
        return document.select(query)
    return document.xpath(query)


def _query_document_first(
    document: scraper_rs.Document,
    *,
    query: str,
    mode: SelectorMode,
) -> Any | None:
    if mode == SelectorMode.css:
        return document.select_first(query)
    return document.xpath_first(query)


async def _query_scope(scope: Any, *, query: str, mode: SelectorMode) -> list[Any]:
    if mode == SelectorMode.css:
        return await scope.select(query)
    return await scope.xpath(query)


async def _query_scope_first(
    scope: Any, *, query: str, mode: SelectorMode
) -> Any | None:
    if mode == SelectorMode.css:
        return await scope.select_first(query)
    return await scope.xpath_first(query)


def _build_summary(
    html: str,
    *,
    handle: str | None = None,
    label: str | None = None,
    source_url: str | None = None,
    fetched_via: str | None = None,
    status: int | None = None,
    max_size_bytes: int = DEFAULT_HTML_MAX_SIZE_BYTES,
    truncate_on_limit: bool = False,
) -> DocumentSummary:
    document = scraper_rs.Document(
        html,
        max_size_bytes=max_size_bytes,
        truncate_on_limit=truncate_on_limit,
    )
    try:
        title_node = document.select_first("title")
        headings = [
            _clip(element.text, 120)
            for element in document.select("h1, h2, h3")[:5]
            if getattr(element, "text", "").strip()
        ]
        return DocumentSummary(
            handle=handle,
            label=label,
            source_url=source_url,
            fetched_via=fetched_via,
            status=status,
            html_chars=len(html),
            text_chars=len(document.text),
            title=_clip(title_node.text, 240) if title_node else None,
            link_count=len(document.select("a[href]")),
            image_count=len(document.select("img[src]")),
            form_count=len(document.select("form")),
            heading_preview=headings,
            text_preview=_clip(document.text, DEFAULT_TEXT_PREVIEW_CHARS),
        )
    finally:
        document.close()


def _store_document(
    html: str,
    *,
    label: str | None = None,
    source_url: str | None = None,
    fetched_via: str | None = None,
    status: int | None = None,
    headers: dict[str, str] | None = None,
) -> StoredDocument:
    return DOCUMENT_STORE.add(
        html,
        label=label,
        source_url=source_url,
        fetched_via=fetched_via,
        status=status,
        headers=headers,
    )


def _query_pair_to_mode_and_query(
    *,
    css: str | None,
    xpath: str | None,
) -> tuple[SelectorMode, str] | None:
    if css:
        return SelectorMode.css, css
    if xpath:
        return SelectorMode.xpath, xpath
    return None


async def _extract_field_value(
    scope: Any,
    field_spec: CrawlFieldSpec,
    *,
    base_url: str,
) -> Any:
    mode, query = _query_pair_to_mode_and_query(
        css=field_spec.css,
        xpath=field_spec.xpath,
    ) or (None, None)
    if mode is None or query is None:
        return field_spec.default

    nodes = (
        await _query_scope(scope, query=query, mode=mode)
        if field_spec.all_matches
        else []
    )
    if not field_spec.all_matches:
        node = await _query_scope_first(scope, query=query, mode=mode)
        nodes = [node] if node is not None else []

    values: list[Any] = []
    for node in nodes:
        if field_spec.extractor == FieldExtractor.text:
            value = getattr(node, "text", "")
        elif field_spec.extractor == FieldExtractor.html:
            value = getattr(node, "html", "")
        else:
            value = node.attr(field_spec.attr_name or "")

        if value is None:
            continue
        if isinstance(value, str) and field_spec.strip:
            value = value.strip()
        if field_spec.absolute_url and isinstance(value, str):
            value = urljoin(base_url, value)
        values.append(value)

    if not values:
        return field_spec.default

    if field_spec.all_matches:
        if field_spec.join_with is not None:
            return field_spec.join_with.join(str(value) for value in values)
        return values

    return values[0]


async def _extract_item(
    scope: Any,
    fields: list[CrawlFieldSpec],
    *,
    base_url: str,
) -> dict[str, Any]:
    item: dict[str, Any] = {}
    for field_spec in fields:
        item[field_spec.name] = await _extract_field_value(
            scope,
            field_spec,
            base_url=base_url,
        )
    return item


class _CollectItemsPipeline:
    def __init__(self) -> None:
        self.items: list[dict[str, Any]] = []

    async def open(self, spider: Spider) -> None:
        return None

    async def close(self, spider: Spider) -> None:
        return None

    async def process_item(
        self,
        item: dict[str, Any],
        spider: Spider,
    ) -> dict[str, Any]:
        self.items.append(item)
        return item


def _render_spider_template(blueprint: CrawlBlueprint, class_name: str) -> str:
    safe_class_name = _normalize_identifier(class_name)
    blueprint_literal = pformat(
        blueprint.model_dump(mode="python"),
        sort_dicts=False,
        width=88,
    )
    return dedent(
        f"""
        from __future__ import annotations

        from urllib.parse import urljoin

        from silkworm import HTMLResponse, Request, Spider, run_spider


        BLUEPRINT = {blueprint_literal}


        class {safe_class_name}(Spider):
            name = BLUEPRINT["spider_name"]
            start_urls = tuple(BLUEPRINT["start_urls"])

            def __init__(self, **kwargs):
                super().__init__(name=BLUEPRINT["spider_name"], start_urls=BLUEPRINT["start_urls"], **kwargs)
                self.request_headers = dict(BLUEPRINT.get("request_headers") or {{}})
                self._scheduled_requests = 0
                self._emitted_items = 0

            async def start_requests(self):
                for url in self.start_urls:
                    if self._scheduled_requests >= BLUEPRINT["max_requests"]:
                        break
                    self._scheduled_requests += 1
                    yield Request(url=url, headers=dict(self.request_headers), callback=self.parse)

            async def _query(self, scope, query: str, mode: str, first: bool = False):
                if mode == "css":
                    return await (scope.select_first(query) if first else scope.select(query))
                return await (scope.xpath_first(query) if first else scope.xpath(query))

            async def _extract_field(self, scope, field: dict[str, object], base_url: str):
                mode = "css" if field.get("css") else "xpath"
                query = field.get("css") or field.get("xpath")
                if not query:
                    return field.get("default")

                all_matches = bool(field.get("all_matches"))
                if all_matches:
                    nodes = await self._query(scope, query, mode, first=False)
                else:
                    first = await self._query(scope, query, mode, first=True)
                    nodes = [first] if first is not None else []

                values = []
                for node in nodes:
                    extractor = field.get("extractor", "text")
                    if extractor == "text":
                        value = getattr(node, "text", "")
                    elif extractor == "html":
                        value = getattr(node, "html", "")
                    else:
                        attr_name = str(field.get("attr_name") or "")
                        value = node.attr(attr_name)

                    if value is None:
                        continue
                    if field.get("strip", True) and isinstance(value, str):
                        value = value.strip()
                    if field.get("absolute_url") and isinstance(value, str):
                        value = urljoin(base_url, value)
                    values.append(value)

                if not values:
                    return field.get("default")
                if all_matches:
                    join_with = field.get("join_with")
                    if join_with is not None:
                        return str(join_with).join(str(value) for value in values)
                    return values
                return values[0]

            async def _extract_item(self, scope, base_url: str):
                item = {{}}
                for field in BLUEPRINT["fields"]:
                    item[field["name"]] = await self._extract_field(scope, field, base_url)
                if BLUEPRINT.get("include_source_url", True):
                    item["_source_url"] = base_url
                return item

            async def _build_follow_requests(self, response: HTMLResponse):
                requests = []

                pagination_query = BLUEPRINT.get("pagination_selector") or BLUEPRINT.get("pagination_xpath")
                if pagination_query:
                    pagination_mode = "css" if BLUEPRINT.get("pagination_selector") else "xpath"
                    next_link = await self._query(response, pagination_query, pagination_mode, first=True)
                    if next_link is not None:
                        href = next_link.attr("href")
                        if href:
                            requests.append(response.follow(href, callback=self.parse, headers=dict(self.request_headers)))

                follow_query = BLUEPRINT.get("follow_links_selector") or BLUEPRINT.get("follow_links_xpath")
                if follow_query:
                    follow_mode = "css" if BLUEPRINT.get("follow_links_selector") else "xpath"
                    nodes = await self._query(response, follow_query, follow_mode, first=False)
                    for node in nodes[: BLUEPRINT.get("follow_link_limit_per_page", 20)]:
                        href = node.attr("href")
                        if href:
                            requests.append(response.follow(href, callback=self.parse, headers=dict(self.request_headers)))

                return requests

            async def parse(self, response: HTMLResponse):
                if self._emitted_items >= BLUEPRINT["max_items"]:
                    return

                item_query = BLUEPRINT.get("item_selector") or BLUEPRINT.get("item_xpath")
                if item_query:
                    item_mode = "css" if BLUEPRINT.get("item_selector") else "xpath"
                    scopes = await self._query(response, item_query, item_mode, first=False)
                else:
                    scopes = [response]

                for scope in scopes:
                    if self._emitted_items >= BLUEPRINT["max_items"]:
                        break
                    self._emitted_items += 1
                    yield await self._extract_item(scope, response.url)

                if self._emitted_items >= BLUEPRINT["max_items"]:
                    return

                for request in await self._build_follow_requests(response):
                    if self._scheduled_requests >= BLUEPRINT["max_requests"]:
                        break
                    self._scheduled_requests += 1
                    yield request


        if __name__ == "__main__":
            run_spider(
                {safe_class_name},
                concurrency=BLUEPRINT["concurrency"],
                request_timeout=BLUEPRINT.get("request_timeout_seconds"),
                max_pending_requests=BLUEPRINT.get("max_pending_requests"),
                html_max_size_bytes=BLUEPRINT["html_max_size_bytes"],
                log_stats_interval=BLUEPRINT.get("log_stats_interval"),
                keep_alive=BLUEPRINT["keep_alive"],
            )
        """
    ).strip()


@mcp.tool(tags={"documents", "selectors"})
def store_html_document(
    html: str,
    source_url: str | None = None,
    label: str | None = None,
    max_size_bytes: int = DEFAULT_HTML_MAX_SIZE_BYTES,
    truncate_on_limit: bool = False,
) -> DocumentSummary:
    """Store raw HTML in the server's in-memory document cache and return a scraper_rs summary."""
    document = _store_document(
        html, label=label, source_url=source_url, fetched_via="manual"
    )
    return _build_summary(
        html,
        handle=document.handle,
        label=document.label,
        source_url=document.source_url,
        fetched_via=document.fetched_via,
        status=document.status,
        max_size_bytes=max_size_bytes,
        truncate_on_limit=truncate_on_limit,
    )


@mcp.tool(tags={"documents"})
def list_documents() -> list[StoredDocumentInfo]:
    """List cached documents that can be reused by handle in later tool calls."""
    return [document.info() for document in DOCUMENT_STORE.list()]


@mcp.tool(tags={"documents"})
def delete_document(handle: str) -> DeleteDocumentResult:
    """Delete a cached document handle."""
    return DeleteDocumentResult(handle=handle, deleted=DOCUMENT_STORE.delete(handle))


@mcp.tool(tags={"documents"})
def clear_documents() -> ClearDocumentsResult:
    """Clear every cached document from the in-memory store."""
    return ClearDocumentsResult(deleted_count=DOCUMENT_STORE.clear())


@mcp.tool(tags={"inspect", "selectors"})
def inspect_document(
    document_handle: str | None = None,
    html: str | None = None,
    source_url: str | None = None,
    max_size_bytes: int = DEFAULT_HTML_MAX_SIZE_BYTES,
    truncate_on_limit: bool = False,
) -> DocumentSummary:
    """Build a high-level summary for stored HTML or an inline HTML snippet."""
    resolved_html, stored_document = _resolve_document_input(
        document_handle=document_handle,
        html=html,
    )
    return _build_summary(
        resolved_html,
        handle=stored_document.handle if stored_document else None,
        label=stored_document.label if stored_document else None,
        source_url=_document_base_url(stored_document, source_url),
        fetched_via=stored_document.fetched_via if stored_document else None,
        status=stored_document.status if stored_document else None,
        max_size_bytes=max_size_bytes,
        truncate_on_limit=truncate_on_limit,
    )


@mcp.tool(tags={"inspect", "selectors"})
def prettify_document(
    document_handle: str | None = None,
    html: str | None = None,
    max_size_bytes: int = DEFAULT_HTML_MAX_SIZE_BYTES,
    truncate_on_limit: bool = False,
    max_output_chars: int = 20_000,
) -> PrettifyResult:
    """Return prettified HTML for visual inspection."""
    resolved_html, stored_document = _resolve_document_input(
        document_handle=document_handle,
        html=html,
    )
    prettified = scraper_rs.prettify(
        resolved_html,
        max_size_bytes=max_size_bytes,
        truncate_on_limit=truncate_on_limit,
    )
    truncated = len(prettified) > max_output_chars
    output = prettified[:max_output_chars] if truncated else prettified
    return PrettifyResult(
        document_handle=stored_document.handle if stored_document else None,
        source_url=stored_document.source_url if stored_document else None,
        output_chars=len(output),
        truncated_output=truncated,
        prettified_html=output,
    )


@mcp.tool(tags={"selectors"})
def query_selector(
    query: str,
    mode: SelectorMode = SelectorMode.css,
    document_handle: str | None = None,
    html: str | None = None,
    limit: int = 20,
    include_html: bool = True,
    text_chars: int = 300,
    html_chars: int = 700,
    source_url: str | None = None,
    max_size_bytes: int = DEFAULT_HTML_MAX_SIZE_BYTES,
    truncate_on_limit: bool = False,
) -> SelectorQueryResult:
    """Run a CSS or XPath query with scraper_rs and return structured match previews."""
    resolved_html, stored_document = _resolve_document_input(
        document_handle=document_handle,
        html=html,
    )
    document = scraper_rs.Document(
        resolved_html,
        max_size_bytes=max_size_bytes,
        truncate_on_limit=truncate_on_limit,
    )
    try:
        matches = _query_document(document, query=query, mode=mode)
        limited_matches = matches[:limit]
        return SelectorQueryResult(
            document_handle=stored_document.handle if stored_document else None,
            source_url=_document_base_url(stored_document, source_url),
            mode=mode,
            query=query,
            total_matches=len(matches),
            returned_matches=len(limited_matches),
            omitted_matches=max(0, len(matches) - len(limited_matches)),
            matches=[
                _serialize_element(
                    match,
                    index=index,
                    include_html=include_html,
                    text_chars=text_chars,
                    html_chars=html_chars,
                )
                for index, match in enumerate(limited_matches)
            ],
        )
    finally:
        document.close()


@mcp.tool(tags={"selectors"})
def compare_selectors(
    selectors: list[str],
    mode: SelectorMode = SelectorMode.css,
    document_handle: str | None = None,
    html: str | None = None,
    preview_limit: int = 3,
    include_html: bool = False,
    text_chars: int = 180,
    html_chars: int = 400,
    source_url: str | None = None,
    max_size_bytes: int = DEFAULT_HTML_MAX_SIZE_BYTES,
    truncate_on_limit: bool = False,
) -> SelectorComparisonResult:
    """Compare multiple selectors against the same document to see which one is the most stable."""
    resolved_html, stored_document = _resolve_document_input(
        document_handle=document_handle,
        html=html,
    )
    document = scraper_rs.Document(
        resolved_html,
        max_size_bytes=max_size_bytes,
        truncate_on_limit=truncate_on_limit,
    )
    try:
        comparisons: list[SelectorComparisonEntry] = []
        for query in selectors:
            matches = _query_document(document, query=query, mode=mode)
            preview = [
                _serialize_element(
                    match,
                    index=index,
                    include_html=include_html,
                    text_chars=text_chars,
                    html_chars=html_chars,
                )
                for index, match in enumerate(matches[:preview_limit])
            ]
            comparisons.append(
                SelectorComparisonEntry(
                    query=query,
                    total_matches=len(matches),
                    preview=preview,
                )
            )
        return SelectorComparisonResult(
            document_handle=stored_document.handle if stored_document else None,
            source_url=_document_base_url(stored_document, source_url),
            mode=mode,
            comparisons=comparisons,
        )
    finally:
        document.close()


@mcp.tool(tags={"selectors", "links"})
def extract_links(
    document_handle: str | None = None,
    html: str | None = None,
    query: str = "a[href]",
    mode: SelectorMode = SelectorMode.css,
    attribute: str = "href",
    limit: int = 100,
    source_url: str | None = None,
    max_size_bytes: int = DEFAULT_HTML_MAX_SIZE_BYTES,
    truncate_on_limit: bool = False,
) -> LinkExtractionResult:
    """Extract and absolutize link-like attributes from matching elements."""
    resolved_html, stored_document = _resolve_document_input(
        document_handle=document_handle,
        html=html,
    )
    base_url = _document_base_url(stored_document, source_url)
    document = scraper_rs.Document(
        resolved_html,
        max_size_bytes=max_size_bytes,
        truncate_on_limit=truncate_on_limit,
    )
    try:
        matches = _query_document(document, query=query, mode=mode)
        links: list[LinkMatch] = []
        for index, match in enumerate(matches[:limit]):
            raw_url = match.attr(attribute)
            if not raw_url:
                continue
            absolute_url = urljoin(base_url, raw_url) if base_url else raw_url
            links.append(
                LinkMatch(
                    index=index,
                    text=_clip(match.text, 200),
                    raw_url=raw_url,
                    absolute_url=absolute_url,
                    attrs=dict(match.attrs or {}),
                )
            )
        return LinkExtractionResult(
            document_handle=stored_document.handle if stored_document else None,
            source_url=base_url,
            mode=mode,
            query=query,
            attribute=attribute,
            total_matches=len(matches),
            links=links,
        )
    finally:
        document.close()


@mcp.tool(tags={"fetch", "silkworm"})
async def silkworm_fetch(
    url: str,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    params: dict[str, str] | None = None,
    body_text: str | None = None,
    body_json: dict[str, Any] | list[Any] | None = None,
    emulation: str = "Firefox139",
    timeout_seconds: float | None = 20.0,
    keep_alive: bool = False,
    store_document: bool = True,
    label: str | None = None,
    body_preview_chars: int = DEFAULT_HTML_PREVIEW_CHARS,
) -> FetchResult:
    """Fetch a page through silkworm's HttpClient and optionally cache the HTML for later selector work."""
    if body_text is not None and body_json is not None:
        raise ValueError("Provide only one of 'body_text' or 'body_json'.")

    client = HttpClient(
        emulation=_normalize_emulation(emulation),
        timeout=timeout_seconds,
        keep_alive=keep_alive,
    )
    request = Request(
        url=url,
        method=method,
        headers=dict(headers or {}),
        params=dict(params or {}),
        data=body_text,
        json=body_json,
    )

    try:
        response = await client.fetch(request)
        try:
            body_text_value = response.text
            is_html = isinstance(response, HTMLResponse)
            stored_document: StoredDocument | None = None
            summary: DocumentSummary | None = None
            if is_html and store_document:
                stored_document = _store_document(
                    body_text_value,
                    label=label,
                    source_url=response.url,
                    fetched_via="http",
                    status=response.status,
                    headers=response.headers,
                )
                summary = _build_summary(
                    body_text_value,
                    handle=stored_document.handle,
                    label=stored_document.label,
                    source_url=stored_document.source_url,
                    fetched_via=stored_document.fetched_via,
                    status=stored_document.status,
                )
            elif is_html:
                summary = _build_summary(
                    body_text_value,
                    source_url=response.url,
                    fetched_via="http",
                    status=response.status,
                )

            return FetchResult(
                url=url,
                final_url=response.url,
                method=method.upper(),
                status=response.status,
                is_html=is_html,
                emulation=emulation,
                via="http",
                headers=response.headers,
                body_chars=len(body_text_value),
                body_preview=_clip(body_text_value, body_preview_chars),
                document_handle=stored_document.handle if stored_document else None,
                summary=summary,
            )
        finally:
            response.close()
    finally:
        await client.close()


@mcp.tool(tags={"fetch", "silkworm", "cdp"})
async def silkworm_fetch_cdp(
    url: str,
    ws_endpoint: str = "ws://127.0.0.1:9222",
    timeout_seconds: float | None = 20.0,
    store_document: bool = True,
    label: str | None = None,
    body_preview_chars: int = DEFAULT_HTML_PREVIEW_CHARS,
) -> FetchResult:
    """Fetch rendered HTML through silkworm's CDP client for JavaScript-heavy pages."""
    client = CDPClient(
        ws_endpoint=ws_endpoint,
        timeout=timeout_seconds,
    )
    try:
        await client.connect()
        response = await client.fetch(Request(url=url))
        html = response.text
        stored_document: StoredDocument | None = None
        summary: DocumentSummary | None = None
        if store_document:
            stored_document = _store_document(
                html,
                label=label,
                source_url=response.url,
                fetched_via="cdp",
                status=response.status,
                headers=response.headers,
            )
            summary = _build_summary(
                html,
                handle=stored_document.handle,
                label=stored_document.label,
                source_url=stored_document.source_url,
                fetched_via=stored_document.fetched_via,
                status=stored_document.status,
            )
        else:
            summary = _build_summary(
                html,
                source_url=response.url,
                fetched_via="cdp",
                status=response.status,
            )

        return FetchResult(
            url=url,
            final_url=response.url,
            method="GET",
            status=response.status,
            is_html=True,
            via="cdp",
            headers=response.headers,
            body_chars=len(html),
            body_preview=_clip(html, body_preview_chars),
            document_handle=stored_document.handle if stored_document else None,
            summary=summary,
        )
    finally:
        await client.close()


@mcp.tool(tags={"crawl", "silkworm"})
async def run_crawl_blueprint(blueprint: CrawlBlueprint) -> CrawlRunResult:
    """Run a configurable silkworm spider without writing code, useful for validating a scraping plan."""

    class BlueprintSpider(Spider):
        name = blueprint.spider_name
        start_urls = tuple(blueprint.start_urls)

        def __init__(self) -> None:
            super().__init__(
                name=blueprint.spider_name,
                start_urls=blueprint.start_urls,
            )
            self._scheduled_requests = 0
            self._emitted_items = 0
            self._request_headers = dict(blueprint.request_headers)

        async def start_requests(self):
            for url in self.start_urls:
                if self._scheduled_requests >= blueprint.max_requests:
                    break
                self._scheduled_requests += 1
                yield Request(
                    url=url,
                    headers=dict(self._request_headers),
                    callback=self.parse,
                )

        async def _next_requests(self, response: HTMLResponse) -> list[Request]:
            requests: list[Request] = []

            pagination_query = _query_pair_to_mode_and_query(
                css=blueprint.pagination_selector,
                xpath=blueprint.pagination_xpath,
            )
            if pagination_query is not None:
                pagination_mode, pagination_value = pagination_query
                next_link = await _query_scope_first(
                    response,
                    query=pagination_value,
                    mode=pagination_mode,
                )
                if next_link is not None:
                    href = next_link.attr("href")
                    if href:
                        requests.append(
                            response.follow(
                                href,
                                callback=self.parse,
                                headers=dict(self._request_headers),
                            )
                        )

            follow_query = _query_pair_to_mode_and_query(
                css=blueprint.follow_links_selector,
                xpath=blueprint.follow_links_xpath,
            )
            if follow_query is not None:
                follow_mode, follow_value = follow_query
                nodes = await _query_scope(
                    response,
                    query=follow_value,
                    mode=follow_mode,
                )
                for node in nodes[: blueprint.follow_link_limit_per_page]:
                    href = node.attr("href")
                    if href:
                        requests.append(
                            response.follow(
                                href,
                                callback=self.parse,
                                headers=dict(self._request_headers),
                            )
                        )

            return requests

        async def parse(self, response: HTMLResponse):
            if self._emitted_items >= blueprint.max_items:
                return

            item_query = _query_pair_to_mode_and_query(
                css=blueprint.item_selector,
                xpath=blueprint.item_xpath,
            )
            if item_query is None:
                scopes = [response]
            else:
                item_mode, item_value = item_query
                scopes = await _query_scope(response, query=item_value, mode=item_mode)

            for scope in scopes:
                if self._emitted_items >= blueprint.max_items:
                    break
                item = await _extract_item(
                    scope,
                    blueprint.fields,
                    base_url=response.url,
                )
                if blueprint.include_source_url:
                    item["_source_url"] = response.url
                self._emitted_items += 1
                yield item

            if self._emitted_items >= blueprint.max_items:
                return

            for request in await self._next_requests(response):
                if self._scheduled_requests >= blueprint.max_requests:
                    break
                self._scheduled_requests += 1
                yield request

    spider = BlueprintSpider()
    collector = _CollectItemsPipeline()
    engine = Engine(
        spider,
        concurrency=blueprint.concurrency,
        max_pending_requests=blueprint.max_pending_requests,
        request_timeout=blueprint.request_timeout_seconds,
        html_max_size_bytes=blueprint.html_max_size_bytes,
        item_pipelines=[collector],
        log_stats_interval=blueprint.log_stats_interval,
        keep_alive=blueprint.keep_alive,
    )
    await engine.run()
    return CrawlRunResult(
        spider_name=blueprint.spider_name,
        scheduled_requests=spider._scheduled_requests,
        emitted_items=spider._emitted_items,
        max_requests=blueprint.max_requests,
        max_items=blueprint.max_items,
        items=collector.items,
    )


@mcp.tool(tags={"crawl", "templates", "silkworm"})
def generate_spider_template(
    blueprint: CrawlBlueprint,
    class_name: str = "GeneratedSpider",
) -> SpiderTemplateResult:
    """Generate a production starter spider that mirrors the crawl blueprint."""
    safe_class_name = _normalize_identifier(class_name)
    return SpiderTemplateResult(
        class_name=safe_class_name,
        spider_name=blueprint.spider_name,
        code=_render_spider_template(blueprint, safe_class_name),
    )


@mcp.resource("silkworm://reference/overview")
def reference_overview() -> str:
    return dedent(
        f"""
        # silkworm-mcp

        Version: {SERVER_VERSION}

        Purpose:
        - Expose silkworm fetching/crawling primitives to MCP clients.
        - Expose scraper-rs parsing and selector debugging tools.
        - Help LLMs move from "I need a scraper" to "here is a working spider blueprint or template".

        Suggested workflow:
        1. `silkworm_fetch` or `silkworm_fetch_cdp`
        2. `inspect_document`
        3. `query_selector` and `compare_selectors`
        4. `extract_links` for pagination/detail URL discovery
        5. `run_crawl_blueprint`
        6. `generate_spider_template`

        Document handles:
        - Most tools accept either `document_handle` or raw `html`.
        - Prefer handles so clients do not resend large HTML payloads.
        """
    ).strip()


@mcp.resource("silkworm://reference/silkworm-cheatsheet")
def silkworm_cheatsheet() -> str:
    return dedent(
        """
        # silkworm cheat sheet

        Core types:
        - `Spider`: define `start_requests()` and `parse()`
        - `Request`: url, method, headers, params, data/json, callback
        - `HTMLResponse`: async CSS/XPath selectors plus `follow()` helpers
        - `Engine`: drives the crawl with concurrency, timeouts, and pipelines

        Common patterns:
        - Start pages: yield `Request(url=..., callback=self.parse)`
        - Extract containers: `await response.select("article.card")`
        - Follow pagination: `yield response.follow(next_href, callback=self.parse)`
        - Extract items: yield dicts from `parse()`

        Useful engine knobs:
        - `concurrency`
        - `request_timeout`
        - `max_pending_requests`
        - `html_max_size_bytes`
        - `keep_alive`
        """
    ).strip()


@mcp.resource("silkworm://reference/scraper-rs-cheatsheet")
def scraper_rs_cheatsheet() -> str:
    return dedent(
        """
        # scraper-rs cheat sheet

        Main API:
        - `Document(html)`
        - `document.select(css)`
        - `document.select_first(css)`
        - `document.xpath(expr)`
        - `document.xpath_first(expr)`
        - `element.text`
        - `element.html`
        - `element.attrs`
        - `element.attr("href")`
        - `scraper_rs.prettify(html)`

        Notes:
        - CSS selectors are usually best for HTML-centric extraction.
        - XPath is useful when matching structure or text-heavy relationships.
        - Store HTML once, then compare selector candidates with `compare_selectors`.
        """
    ).strip()


@mcp.resource(
    "silkworm://reference/crawl-blueprint-schema",
    mime_type="application/json",
)
def crawl_blueprint_schema() -> str:
    return json.dumps(CrawlBlueprint.model_json_schema(), indent=2)


@mcp.resource(
    "silkworm://documents",
    mime_type="application/json",
)
def documents_resource() -> str:
    payload = [
        document.info().model_dump(mode="json") for document in DOCUMENT_STORE.list()
    ]
    return json.dumps(payload, indent=2)


@mcp.resource(
    "silkworm://documents/{handle}/summary",
    mime_type="application/json",
)
def document_summary_resource(handle: str) -> str:
    document = DOCUMENT_STORE.get(handle)
    summary = _build_summary(
        document.html,
        handle=document.handle,
        label=document.label,
        source_url=document.source_url,
        fetched_via=document.fetched_via,
        status=document.status,
    )
    return json.dumps(summary.model_dump(mode="json"), indent=2)


@mcp.resource(
    "silkworm://documents/{handle}/html",
    mime_type="text/html",
)
def document_html_resource(handle: str) -> str:
    return DOCUMENT_STORE.get(handle).html


@mcp.prompt(tags={"planning"})
def plan_silkworm_scraper(goal: str, target_url: str | None = None) -> str:
    return dedent(
        f"""
        Build a scraper plan for this goal: {goal}

        Target URL: {target_url or "not provided"}

        Use the silkworm MCP workflow:
        1. Fetch or inspect the target page.
        2. Identify stable item containers and field selectors.
        3. Validate competing selectors with compare_selectors.
        4. Detect pagination and detail links.
        5. Produce a CrawlBlueprint and, if helpful, a spider template.

        Prefer document handles over repeatedly embedding raw HTML.
        """
    ).strip()


@mcp.prompt(tags={"debugging"})
def debug_selector_strategy(
    extraction_goal: str,
    current_selector: str | None = None,
    mode: SelectorMode = SelectorMode.css,
) -> str:
    return dedent(
        f"""
        Debug this selector strategy for a scraper.

        Extraction goal: {extraction_goal}
        Current selector: {current_selector or "not provided"}
        Mode: {mode.value}

        Use these MCP tools in order:
        - inspect_document
        - query_selector
        - compare_selectors
        - extract_links if navigation is involved

        Prefer selectors that are specific enough to avoid false positives but resilient to layout changes.
        """
    ).strip()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the silkworm MCP server.")
    parser.add_argument(
        "--transport",
        choices=["stdio", "http", "sse", "streamable-http"],
        default="stdio",
        help="MCP transport to run.",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host for HTTP-based transports.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port for HTTP-based transports.",
    )
    parser.add_argument(
        "--path",
        default=None,
        help="Optional MCP endpoint path for HTTP-based transports.",
    )
    args = parser.parse_args()

    if args.transport == "stdio":
        mcp.run(transport="stdio")
        return

    mcp.run(
        transport=args.transport,
        host=args.host,
        port=args.port,
        path=args.path,
    )


if __name__ == "__main__":
    main()
