from __future__ import annotations

import importlib.metadata
import logging
import re
from datetime import datetime, timezone
from typing import Any, Literal
from urllib.parse import urljoin

import scraper_rs
from fastmcp.exceptions import ToolError
from rnet import Emulation
from silkworm import Request, Spider
from silkworm.cdp import CDPClient
from silkworm.middlewares import (
    DelayMiddleware,
    RetryMiddleware,
    SkipNonHTMLMiddleware,
    UserAgentMiddleware,
)

from .constants import (
    DEFAULT_HTML_MAX_SIZE_BYTES,
    DEFAULT_HTML_PREVIEW_CHARS,
    DEFAULT_TEXT_PREVIEW_CHARS,
    PROCESS_STARTED_AT,
    SERVER_NAME,
    SERVER_VERSION,
    FieldExtractor,
    SelectorMode,
)
from .documents import StoredDocument
from .models import (
    CrawlBlueprint,
    CrawlFieldSpec,
    DocumentSummary,
    FetchResult,
    HtmlParseNode,
    SelectorMatch,
    ServerConfigurationSummary,
    ServerHealthReport,
    ServerStatusResult,
)
from .runtime import DOCUMENT_STORE, EMULATION_NAMES, SERVER_SETTINGS
from .settings import _validate_url


class _FastMCPToolErrorFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if not record.exc_info:
            return True

        exc_type, exc_value, _traceback = record.exc_info
        if not exc_type or not isinstance(exc_value, ToolError):
            return True

        record.msg = f"{record.getMessage()}: {exc_value}"
        record.args = ()
        record.exc_info = None
        record.exc_text = None
        return True


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


_LEADING_DOCTYPE_RE = re.compile(r"^\s*<!DOCTYPE[^>]*>\s*", re.IGNORECASE)


def _strip_leading_doctype(html: str) -> str:
    return _LEADING_DOCTYPE_RE.sub("", html, count=1)


def _is_unsupported_xpath_dtd_error(error: ValueError) -> bool:
    return "DTD is not supported" in str(error)


def _resolve_document_input(
    *,
    document_handle: str | None = None,
    html: str | None = None,
) -> tuple[str, StoredDocument | None]:
    if bool(document_handle) == bool(html):
        raise ToolError("Provide exactly one of 'document_handle' or 'html'.")
    if document_handle is not None:
        try:
            document = DOCUMENT_STORE.get(document_handle)
        except ValueError as exc:
            raise ToolError(
                f"{exc} The handle is looked up in the server-side document cache. "
                "If it came from an earlier server process or expired, fetch/store the HTML again "
                "or pass inline 'html' to this tool."
            ) from None
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


def _parse_html_tree(
    html: str,
    *,
    parse_mode: Literal["document", "fragment"],
    max_size_bytes: int | None,
    truncate_on_limit: bool,
) -> HtmlParseNode:
    function_name = "parse_document" if parse_mode == "document" else "parse_fragment"
    parse_func = getattr(scraper_rs, function_name, None)
    if parse_func is None and hasattr(scraper_rs, "scraper_rs"):
        parse_func = getattr(scraper_rs.scraper_rs, function_name, None)
    if parse_func is None:
        try:
            version = importlib.metadata.version("scraper-rust")
        except importlib.metadata.PackageNotFoundError:
            version = "unknown"
        raise RuntimeError(
            f"scraper-rust {version} does not expose '{function_name}'. "
            "Install a scraper-rust build that includes the new HTML tree parsing APIs."
        )
    parsed = parse_func(
        html,
        max_size_bytes=max_size_bytes,
        truncate_on_limit=truncate_on_limit,
    )
    return HtmlParseNode.model_validate(parsed)


def _normalize_emulation(name: str) -> Emulation:
    try:
        return getattr(Emulation, name)
    except AttributeError as exc:
        choices = ", ".join(EMULATION_NAMES)
        raise ValueError(
            f"Unknown emulation '{name}'. Available values: {choices}"
        ) from exc


def _validate_http_url(url: str, *, field_name: str) -> None:
    _validate_url(url, field_name=field_name, schemes={"http", "https"})


def _validate_ws_url(url: str, *, field_name: str) -> None:
    _validate_url(url, field_name=field_name, schemes={"ws", "wss"})


def _uptime_seconds(now: datetime | None = None) -> float:
    current_time = now or datetime.now(timezone.utc)
    return round((current_time - PROCESS_STARTED_AT).total_seconds(), 3)


def _server_configuration_summary() -> ServerConfigurationSummary:
    return ServerConfigurationSummary(
        default_transport=SERVER_SETTINGS.default_transport,
        default_host=SERVER_SETTINGS.default_host,
        default_port=SERVER_SETTINGS.default_port,
        default_path=SERVER_SETTINGS.default_path,
        log_level=SERVER_SETTINGS.log_level,
        mask_error_details=SERVER_SETTINGS.mask_error_details,
        strict_input_validation=SERVER_SETTINGS.strict_input_validation,
        http_health_path=SERVER_SETTINGS.http_health_path,
        http_ready_path=SERVER_SETTINGS.http_ready_path,
        readiness_require_cdp=SERVER_SETTINGS.readiness_require_cdp,
        readiness_cdp_ws_endpoint=SERVER_SETTINGS.readiness_cdp_ws_endpoint,
        readiness_probe_timeout_seconds=SERVER_SETTINGS.readiness_probe_timeout_seconds,
        document_max_count=SERVER_SETTINGS.document_max_count,
        document_max_total_bytes=SERVER_SETTINGS.document_max_total_bytes,
        document_ttl_seconds=SERVER_SETTINGS.document_ttl_seconds,
        document_store_path=SERVER_SETTINGS.document_store_path,
    )


async def _probe_cdp_endpoint(
    *,
    ws_endpoint: str,
    timeout_seconds: float,
) -> tuple[bool, str | None]:
    client = CDPClient(ws_endpoint=ws_endpoint, timeout=timeout_seconds)
    try:
        await client.connect()
    except Exception as exc:  # pragma: no cover - defensive integration guard
        return False, f"{type(exc).__name__}: {exc}"
    finally:
        try:
            await client.close()
        except Exception:  # pragma: no cover - best-effort cleanup
            pass
    return True, None


async def _build_health_report(
    *,
    require_cdp: bool,
    probe_cdp: bool,
    ws_endpoint: str | None = None,
) -> ServerHealthReport:
    now = datetime.now(timezone.utc)
    cdp_ok: bool | None = None
    cdp_error: str | None = None
    if probe_cdp or require_cdp:
        cdp_ok, cdp_error = await _probe_cdp_endpoint(
            ws_endpoint=ws_endpoint or SERVER_SETTINGS.readiness_cdp_ws_endpoint,
            timeout_seconds=SERVER_SETTINGS.readiness_probe_timeout_seconds,
        )

    ready = not require_cdp or bool(cdp_ok)
    status = "ok" if ready and (cdp_ok is not False) else "degraded"
    return ServerHealthReport(
        status=status,
        ready=ready,
        checked_at=now.isoformat(),
        process_started_at=PROCESS_STARTED_AT.isoformat(),
        uptime_seconds=_uptime_seconds(now),
        cdp_required=require_cdp,
        cdp_ok=cdp_ok,
        cdp_error=cdp_error,
        document_store=DOCUMENT_STORE.stats(),
    )


def _build_server_status(
    *,
    include_documents: bool,
    health: ServerHealthReport | None = None,
) -> ServerStatusResult:
    documents = (
        [
            document.info(SERVER_SETTINGS.document_ttl_seconds)
            for document in DOCUMENT_STORE.list()
        ]
        if include_documents
        else []
    )
    return ServerStatusResult(
        server_name=SERVER_NAME,
        server_version=SERVER_VERSION,
        process_started_at=PROCESS_STARTED_AT.isoformat(),
        uptime_seconds=_uptime_seconds(),
        configuration=_server_configuration_summary(),
        document_store=DOCUMENT_STORE.stats(),
        health=health,
        documents=documents,
    )


def _configure_logging(level_name: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level_name, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    fastmcp_logger = logging.getLogger("fastmcp.server.server")
    if not any(
        isinstance(existing_filter, _FastMCPToolErrorFilter)
        for existing_filter in fastmcp_logger.filters
    ):
        fastmcp_logger.addFilter(_FastMCPToolErrorFilter())


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
    try:
        return document.xpath(query)
    except ValueError as exc:
        sanitized_html = _strip_leading_doctype(document.html)
        if not _is_unsupported_xpath_dtd_error(exc) or sanitized_html == document.html:
            raise
        fallback_document = scraper_rs.Document(sanitized_html)
        try:
            return fallback_document.xpath(query)
        finally:
            fallback_document.close()


def _query_document_first(
    document: scraper_rs.Document,
    *,
    query: str,
    mode: SelectorMode,
) -> Any | None:
    if mode == SelectorMode.css:
        return document.select_first(query)
    try:
        return document.xpath_first(query)
    except ValueError as exc:
        sanitized_html = _strip_leading_doctype(document.html)
        if not _is_unsupported_xpath_dtd_error(exc) or sanitized_html == document.html:
            raise
        fallback_document = scraper_rs.Document(sanitized_html)
        try:
            return fallback_document.xpath_first(query)
        finally:
            fallback_document.close()


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


def _is_synthetic_source_url_field(field_spec: CrawlFieldSpec) -> bool:
    return field_spec.name in {"source_url", "_source_url"}


async def _extract_field_value(
    scope: Any,
    field_spec: CrawlFieldSpec,
    *,
    base_url: str,
) -> Any:
    if _is_synthetic_source_url_field(field_spec) and not (
        field_spec.css or field_spec.xpath
    ):
        return base_url or field_spec.default

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


def _extract_static_field_value(
    scope: Any,
    field_spec: CrawlFieldSpec,
    *,
    base_url: str,
) -> Any:
    if _is_synthetic_source_url_field(field_spec) and not (
        field_spec.css or field_spec.xpath
    ):
        return base_url or field_spec.default

    mode, query = _query_pair_to_mode_and_query(
        css=field_spec.css,
        xpath=field_spec.xpath,
    ) or (None, None)
    if mode is None or query is None:
        return field_spec.default

    if field_spec.all_matches:
        nodes = scope.select(query) if mode == SelectorMode.css else scope.xpath(query)
    else:
        node = (
            scope.select_first(query)
            if mode == SelectorMode.css
            else scope.xpath_first(query)
        )
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


def _extract_static_item(
    scope: Any,
    fields: list[CrawlFieldSpec],
    *,
    base_url: str,
) -> dict[str, Any]:
    item: dict[str, Any] = {}
    for field_spec in fields:
        item[field_spec.name] = _extract_static_field_value(
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


def _build_runtime_request_middlewares(
    blueprint: CrawlBlueprint,
) -> list[UserAgentMiddleware | DelayMiddleware]:
    middlewares: list[UserAgentMiddleware | DelayMiddleware] = [
        UserAgentMiddleware(
            blueprint.user_agents or None,
            default=blueprint.default_user_agent,
        )
    ]
    if any(
        delay is not None
        for delay in (
            blueprint.delay_seconds,
            blueprint.delay_min_seconds,
            blueprint.delay_max_seconds,
        )
    ):
        middlewares.append(
            DelayMiddleware(
                delay=blueprint.delay_seconds,
                min_delay=blueprint.delay_min_seconds,
                max_delay=blueprint.delay_max_seconds,
            )
        )
    return middlewares


def _build_runtime_response_middlewares(
    blueprint: CrawlBlueprint,
) -> list[SkipNonHTMLMiddleware | RetryMiddleware]:
    return [
        SkipNonHTMLMiddleware(),
        RetryMiddleware(
            max_times=blueprint.retry_max_times,
            retry_http_codes=blueprint.retry_http_codes or None,
            backoff_base=blueprint.retry_backoff_base,
            sleep_http_codes=blueprint.sleep_http_codes or None,
        ),
    ]


def _build_cdp_client(blueprint: CrawlBlueprint) -> CDPClient:
    timeout = blueprint.cdp_timeout_seconds
    if timeout is None:
        timeout = blueprint.request_timeout_seconds
    return CDPClient(
        ws_endpoint=blueprint.cdp_ws_endpoint,
        concurrency=blueprint.concurrency,
        timeout=timeout,
        html_max_size_bytes=blueprint.html_max_size_bytes,
    )


async def _fetch_html_via_cdp(
    *,
    url: str,
    ws_endpoint: str,
    timeout_seconds: float,
    headers: dict[str, str] | None = None,
    store_document: bool,
    label: str | None,
    body_preview_chars: int = DEFAULT_HTML_PREVIEW_CHARS,
) -> tuple[FetchResult, str]:
    client = CDPClient(
        ws_endpoint=ws_endpoint,
        timeout=timeout_seconds,
    )
    try:
        await client.connect()
        response = await client.fetch(Request(url=url, headers=dict(headers or {})))
        try:
            html = response.text
            stored_document: StoredDocument | None = None
            summary: DocumentSummary
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

            result = FetchResult(
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
            return result, html
        finally:
            response.close()
    finally:
        await client.close()


def _summarize_selector_matches(
    matches: list[Any],
    *,
    limit: int,
    include_html: bool,
    text_chars: int,
    html_chars: int,
) -> tuple[int, int, int, list[SelectorMatch]]:
    limited_matches = matches[:limit]
    return (
        len(matches),
        len(limited_matches),
        max(0, len(matches) - len(limited_matches)),
        [
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
