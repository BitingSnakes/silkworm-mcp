from __future__ import annotations

import importlib.metadata
import logging
import re
from dataclasses import dataclass, field as dataclass_field
from datetime import datetime, timezone
from typing import Any, Literal
from urllib.parse import urljoin

import scraper_rs
from fastmcp.exceptions import ToolError
from rnet import Emulation
from silkworm import Request, Spider
from silkworm.cdp import CDPClient
from silkworm.exceptions import HttpError
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
    InverseSelectorMatch,
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


def _collapse_whitespace(value: str | None) -> str:
    return _WHITESPACE_RE.sub(" ", value or "").strip()


def _normalize_identifier(name: str, default: str = "GeneratedSpider") -> str:
    candidate = re.sub(r"[^0-9a-zA-Z_]", "_", name).strip("_") or default
    if candidate[0].isdigit():
        candidate = f"_{candidate}"
    return candidate


_LEADING_DOCTYPE_RE = re.compile(r"^\s*<!DOCTYPE[^>]*>\s*", re.IGNORECASE)
_HTML_DOCUMENT_RE = re.compile(r"^\s*(<!DOCTYPE|<html\b)", re.IGNORECASE)
_WHITESPACE_RE = re.compile(r"\s+")
_SIMPLE_CSS_IDENTIFIER_RE = re.compile(r"^-?[_a-zA-Z][-_a-zA-Z0-9]*$")


@dataclass(slots=True)
class _InverseSelectorNode:
    tag: str
    attrs: dict[str, str]
    parent: _InverseSelectorNode | None
    children: list[_InverseSelectorNode] = dataclass_field(default_factory=list)
    text_content: str = ""
    same_tag_index: int = 1
    same_tag_total: int = 1
    order: int = 0


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


def _html_looks_like_document(html: str) -> bool:
    return bool(_HTML_DOCUMENT_RE.match(html))


def _normalize_text_query(value: str, *, case_sensitive: bool) -> str:
    collapsed = _collapse_whitespace(value)
    return collapsed if case_sensitive else collapsed.casefold()


def _build_inverse_selector_nodes(
    html: str,
    *,
    max_size_bytes: int | None,
    truncate_on_limit: bool,
) -> list[_InverseSelectorNode]:
    root = _parse_html_tree(
        html,
        parse_mode="document",
        max_size_bytes=max_size_bytes,
        truncate_on_limit=truncate_on_limit,
    )
    elements: list[_InverseSelectorNode] = []
    roots: list[_InverseSelectorNode] = []

    def visit(
        node: HtmlParseNode,
        parent: _InverseSelectorNode | None,
    ) -> str:
        if node.node_type == "text":
            return node.text or ""

        if node.node_type == "element" and node.tag:
            element = _InverseSelectorNode(
                tag=node.tag,
                attrs=dict(node.attrs or {}),
                parent=parent,
                order=len(elements),
            )
            elements.append(element)
            if parent is None:
                roots.append(element)
            else:
                parent.children.append(element)

            text_parts: list[str] = []
            for child in node.children:
                child_text = visit(child, element)
                if child_text:
                    text_parts.append(child_text)
            element.text_content = _collapse_whitespace(" ".join(text_parts))
            return element.text_content

        text_parts: list[str] = []
        for child in node.children:
            child_text = visit(child, parent)
            if child_text:
                text_parts.append(child_text)
        return _collapse_whitespace(" ".join(text_parts))

    visit(root, None)

    def assign_same_tag_positions(children: list[_InverseSelectorNode]) -> None:
        totals: dict[str, int] = {}
        for child in children:
            totals[child.tag] = totals.get(child.tag, 0) + 1

        seen: dict[str, int] = {}
        for child in children:
            seen[child.tag] = seen.get(child.tag, 0) + 1
            child.same_tag_index = seen[child.tag]
            child.same_tag_total = totals[child.tag]
            assign_same_tag_positions(child.children)

    assign_same_tag_positions(roots)
    return elements


def _node_matches_text(
    node: _InverseSelectorNode,
    *,
    normalized_query: str,
    match_type: Literal["exact", "contains"],
    case_sensitive: bool,
) -> bool:
    normalized_text = _normalize_text_query(
        node.text_content,
        case_sensitive=case_sensitive,
    )
    if not normalized_text:
        return False
    if match_type == "exact":
        return normalized_text == normalized_query
    return normalized_query in normalized_text


def _has_matching_descendant(
    node: _InverseSelectorNode,
    *,
    matched_orders: set[int],
) -> bool:
    for child in node.children:
        if child.order in matched_orders or _has_matching_descendant(
            child,
            matched_orders=matched_orders,
        ):
            return True
    return False


def _node_chain(
    node: _InverseSelectorNode,
    *,
    strip_document_wrappers: bool,
) -> list[_InverseSelectorNode]:
    chain: list[_InverseSelectorNode] = []
    current: _InverseSelectorNode | None = node
    while current is not None:
        chain.append(current)
        current = current.parent
    chain.reverse()

    if (
        strip_document_wrappers
        and len(chain) >= 2
        and chain[0].tag == "html"
        and chain[1].tag == "body"
    ):
        trimmed = chain[2:]
        if trimmed:
            return trimmed
    return chain


def _css_escape_string(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _xpath_literal(value: str) -> str:
    if "'" not in value:
        return f"'{value}'"
    if '"' not in value:
        return f'"{value}"'
    parts = value.split("'")
    rendered_parts: list[str] = []
    for index, part in enumerate(parts):
        if part:
            rendered_parts.append(f"'{part}'")
        if index != len(parts) - 1:
            rendered_parts.append('"\'"')
    return f"concat({', '.join(rendered_parts)})"


def _build_css_segment(
    node: _InverseSelectorNode,
    *,
    id_counts: dict[str, int],
) -> tuple[str, bool]:
    id_value = node.attrs.get("id")
    if id_value:
        if _SIMPLE_CSS_IDENTIFIER_RE.match(id_value):
            return f"{node.tag}#{id_value}", id_counts.get(id_value, 0) == 1
        return (
            f'{node.tag}[id="{_css_escape_string(id_value)}"]',
            id_counts.get(id_value, 0) == 1,
        )

    classes = [
        item
        for item in node.attrs.get("class", "").split()
        if _SIMPLE_CSS_IDENTIFIER_RE.match(item)
    ]
    segment = node.tag
    if classes:
        segment += "".join(f".{item}" for item in classes)
    if node.same_tag_total > 1:
        segment += f":nth-of-type({node.same_tag_index})"
    return segment, False


def _build_inverse_css_selector(
    node: _InverseSelectorNode,
    *,
    id_counts: dict[str, int],
    strip_document_wrappers: bool,
) -> str:
    chain = _node_chain(node, strip_document_wrappers=strip_document_wrappers)
    start_index = 0
    for index, current in enumerate(chain):
        id_value = current.attrs.get("id")
        if id_value and id_counts.get(id_value, 0) == 1:
            start_index = index

    segments = [
        _build_css_segment(current, id_counts=id_counts)[0]
        for current in chain[start_index:]
    ]
    return " > ".join(segments)


def _build_inverse_xpath_selector(
    node: _InverseSelectorNode,
    *,
    id_counts: dict[str, int],
    strip_document_wrappers: bool,
) -> str:
    chain = _node_chain(node, strip_document_wrappers=strip_document_wrappers)
    for index, current in enumerate(chain):
        id_value = current.attrs.get("id")
        if id_value and id_counts.get(id_value, 0) == 1:
            suffix = "".join(
                f"/{child.tag}[{child.same_tag_index}]" for child in chain[index + 1 :]
            )
            return f"//*[@id={_xpath_literal(id_value)}]{suffix}"
    return "//" + "/".join(
        f"{current.tag}[{current.same_tag_index}]" for current in chain
    )


def _find_inverse_selector_matches(
    html: str,
    *,
    text_query: str,
    match_type: Literal["exact", "contains"],
    case_sensitive: bool,
    text_chars: int,
    max_size_bytes: int | None,
    truncate_on_limit: bool,
) -> list[InverseSelectorMatch]:
    normalized_query = _normalize_text_query(
        text_query,
        case_sensitive=case_sensitive,
    )
    if not normalized_query:
        raise ValueError("'text_query' must contain non-whitespace text.")

    nodes = _build_inverse_selector_nodes(
        html,
        max_size_bytes=max_size_bytes,
        truncate_on_limit=truncate_on_limit,
    )
    matching_nodes = [
        node
        for node in nodes
        if _node_matches_text(
            node,
            normalized_query=normalized_query,
            match_type=match_type,
            case_sensitive=case_sensitive,
        )
    ]
    matched_orders = {node.order for node in matching_nodes}
    specific_matches = [
        node
        for node in matching_nodes
        if not _has_matching_descendant(node, matched_orders=matched_orders)
    ]

    id_counts: dict[str, int] = {}
    for node in nodes:
        id_value = node.attrs.get("id")
        if id_value:
            id_counts[id_value] = id_counts.get(id_value, 0) + 1

    strip_document_wrappers = not _html_looks_like_document(html)
    return [
        InverseSelectorMatch(
            index=index,
            tag=node.tag,
            text=_clip(node.text_content, text_chars),
            css=_build_inverse_css_selector(
                node,
                id_counts=id_counts,
                strip_document_wrappers=strip_document_wrappers,
            ),
            xpath=_build_inverse_xpath_selector(
                node,
                id_counts=id_counts,
                strip_document_wrappers=strip_document_wrappers,
            ),
            attrs=dict(node.attrs),
        )
        for index, node in enumerate(specific_matches)
    ]


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


def _format_cdp_fetch_error(url: str, exc: HttpError) -> str:
    detail = str(exc).strip()
    lowered = detail.lower()

    if "cannot find default execution context" in lowered:
        return (
            f"CDP fetch failed for {url}: the browser page lost its execution context "
            "before HTML could be captured. Retry the request; if it persists, restart the CDP browser."
        )

    if "timed out" in lowered or "timeout" in lowered:
        return (
            f"CDP fetch failed for {url}: the browser did not respond in time. "
            "Retry or increase `timeout_seconds`."
        )

    if detail:
        return f"CDP fetch failed for {url}: {_clip(detail, 240)}"
    return f"CDP fetch failed for {url}."


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
        try:
            await client.connect()
            response = await client.fetch(Request(url=url, headers=dict(headers or {})))
        except HttpError as exc:
            raise ToolError(_format_cdp_fetch_error(url, exc)) from None
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
