from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

from .constants import (
    DEFAULT_HTML_MAX_SIZE_BYTES,
    CrawlTransport,
    FieldExtractor,
    SelectorMode,
    SpiderTemplateVariant,
)


class StoredDocumentInfo(BaseModel):
    handle: str
    label: str | None = None
    source_url: str | None = None
    stored_at: str
    last_accessed_at: str
    expires_at: str | None = None
    fetched_via: str | None = None
    status: int | None = None
    html_chars: int
    html_bytes: int


class DocumentStoreStats(BaseModel):
    document_count: int
    total_bytes: int
    max_document_count: int
    max_total_bytes: int
    ttl_seconds: float | None = None
    created_documents: int
    evicted_documents: int
    expired_documents: int
    rejected_documents: int


class ServerConfigurationSummary(BaseModel):
    default_transport: Literal["stdio", "http", "sse", "streamable-http"]
    default_host: str
    default_port: int
    default_path: str | None = None
    log_level: str
    mask_error_details: bool
    strict_input_validation: bool
    http_health_path: str
    http_ready_path: str
    readiness_require_cdp: bool
    readiness_cdp_ws_endpoint: str
    readiness_probe_timeout_seconds: float
    document_max_count: int
    document_max_total_bytes: int
    document_ttl_seconds: float | None = None
    document_store_path: str | None = None


class ServerHealthReport(BaseModel):
    status: Literal["ok", "degraded"]
    ready: bool
    checked_at: str
    process_started_at: str
    uptime_seconds: float
    cdp_required: bool
    cdp_ok: bool | None = None
    cdp_error: str | None = None
    document_store: DocumentStoreStats


class ServerStatusResult(BaseModel):
    server_name: str
    server_version: str
    process_started_at: str
    uptime_seconds: float
    configuration: ServerConfigurationSummary
    document_store: DocumentStoreStats
    health: ServerHealthReport | None = None
    documents: list[StoredDocumentInfo] = Field(default_factory=list)


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


class HtmlParseNode(BaseModel):
    node_type: str | None = None
    tag: str | None = None
    namespace: str | None = None
    attrs: dict[str, str] = Field(default_factory=dict)
    children: list["HtmlParseNode"] = Field(default_factory=list)
    text: str | None = None
    quirks_mode: str | None = None
    errors: list[str] = Field(default_factory=list)
    name: str | None = None
    public_id: str | None = None
    system_id: str | None = None
    target: str | None = None
    data: str | None = None


class HtmlParseResult(BaseModel):
    document_handle: str | None = None
    source_url: str | None = None
    parse_mode: Literal["document", "fragment"]
    max_size_bytes: int | None = None
    truncate_on_limit: bool
    root: HtmlParseNode


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
        is_synthetic_source_url = self.name in {"source_url", "_source_url"}
        if is_synthetic_source_url and not self.css and not self.xpath:
            return self
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
    transport: CrawlTransport = Field(
        default=CrawlTransport.http,
        description="Fetch transport used by the runtime crawl and generated spider.",
    )
    cdp_ws_endpoint: str = Field(
        default="ws://127.0.0.1:9222",
        description="CDP WebSocket endpoint used when transport='cdp'.",
    )
    cdp_timeout_seconds: float | None = Field(
        default=None,
        ge=0.1,
        le=300,
        description="Optional CDP command timeout. Falls back to request_timeout_seconds when omitted.",
    )
    user_agents: list[str] = Field(
        default_factory=list,
        description="Optional user-agent pool used by UserAgentMiddleware.",
    )
    default_user_agent: str | None = Field(
        default="silkworm-mcp/0.1.0",
        description="Fallback user-agent for UserAgentMiddleware.",
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
    delay_seconds: float | None = Field(
        default=None,
        ge=0.0,
        le=300,
        description="Fixed delay inserted before requests.",
    )
    delay_min_seconds: float | None = Field(
        default=None,
        ge=0.0,
        le=300,
        description="Minimum randomized request delay.",
    )
    delay_max_seconds: float | None = Field(
        default=None,
        ge=0.0,
        le=300,
        description="Maximum randomized request delay.",
    )
    retry_max_times: int = Field(
        default=3,
        ge=0,
        le=100,
        description="Retry attempts configured through RetryMiddleware.",
    )
    retry_http_codes: list[int] = Field(
        default_factory=list,
        description="HTTP status codes retried immediately.",
    )
    sleep_http_codes: list[int] = Field(
        default_factory=lambda: [403, 429],
        description="HTTP status codes that trigger backoff sleep before retrying.",
    )
    retry_backoff_base: float = Field(
        default=0.5,
        ge=0.0,
        le=300,
        description="Base backoff delay for RetryMiddleware.",
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
    output_jsonl_path: str | None = Field(
        default=None,
        description="Optional JSON Lines output path for generated spider templates.",
    )
    output_use_opendal: bool | None = Field(
        default=None,
        description="Optional JsonLinesPipeline use_opendal flag for generated templates.",
    )
    use_uvloop_runner: bool = Field(
        default=True,
        description="Generate templates with run_spider_uvloop instead of run_spider.",
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
        if self.delay_seconds is not None and (
            self.delay_min_seconds is not None or self.delay_max_seconds is not None
        ):
            raise ValueError(
                "Use either delay_seconds or delay_min_seconds/delay_max_seconds, not both."
            )
        if (self.delay_min_seconds is None) != (self.delay_max_seconds is None):
            raise ValueError(
                "delay_min_seconds and delay_max_seconds must be set together."
            )
        if (
            self.delay_min_seconds is not None
            and self.delay_max_seconds is not None
            and self.delay_min_seconds > self.delay_max_seconds
        ):
            raise ValueError("delay_min_seconds must be <= delay_max_seconds.")
        for index, start_url in enumerate(self.start_urls):
            _validate_http_url(start_url, field_name=f"start_urls[{index}]")
        if self.transport == CrawlTransport.cdp:
            _validate_ws_url(self.cdp_ws_endpoint, field_name="cdp_ws_endpoint")
        return self


class CrawlRunResult(BaseModel):
    spider_name: str
    execution_variant: SpiderTemplateVariant
    scheduled_requests: int
    emitted_items: int
    max_requests: int
    max_items: int
    items: list[dict[str, Any]] = Field(default_factory=list)


class SpiderTemplateResult(BaseModel):
    class_name: str
    spider_name: str
    template_variant: SpiderTemplateVariant
    code: str


class LivePageExtractionResult(BaseModel):
    url: str
    final_url: str
    status: int | None = None
    headers: dict[str, str] = Field(default_factory=dict)
    via: Literal["cdp"] = "cdp"
    document_handle: str | None = None
    summary: DocumentSummary
    scope_mode: SelectorMode | None = None
    scope_query: str | None = None
    total_scopes: int
    returned_items: int
    omitted_items: int
    items: list[dict[str, Any]] = Field(default_factory=list)


class SpiderCodeValidationResult(BaseModel):
    syntax_ok: bool
    spider_classes: list[str] = Field(default_factory=list)
    imports_silkworm: bool = False
    uses_cdp_client: bool = False
    uses_run_spider: bool = False
    uses_run_spider_uvloop: bool = False
    issues: list[str] = Field(default_factory=list)


def _validate_optional_query_pair(
    css: str | None,
    xpath: str | None,
    label: str,
) -> None:
    if css and xpath:
        raise ValueError(f"{label} accepts either CSS or XPath, not both at once.")


def _validate_url(url: str, *, field_name: str, schemes: set[str]) -> None:
    from urllib.parse import urlparse

    parsed = urlparse(url)
    if parsed.scheme.lower() not in schemes or not parsed.netloc:
        allowed = ", ".join(sorted(schemes))
        raise ValueError(
            f"{field_name} must be an absolute URL using one of: {allowed}"
        )


def _validate_http_url(url: str, *, field_name: str) -> None:
    _validate_url(url, field_name=field_name, schemes={"http", "https"})


def _validate_ws_url(url: str, *, field_name: str) -> None:
    _validate_url(url, field_name=field_name, schemes={"ws", "wss"})


HtmlParseNode.model_rebuild()
