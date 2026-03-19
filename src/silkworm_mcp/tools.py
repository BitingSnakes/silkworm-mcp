from __future__ import annotations

import ast
from typing import Any
from urllib.parse import urljoin

from fastmcp.exceptions import ToolError
import scraper_rs
from silkworm import HTMLResponse, Request
from silkworm.exceptions import HttpError
from silkworm.http import HttpClient

from .constants import (
    DEFAULT_HTML_MAX_SIZE_BYTES,
    DEFAULT_HTML_PREVIEW_CHARS,
)
from .documents import StoredDocument
from .helpers import (
    _build_health_report,
    _build_server_status,
    _build_summary,
    _clip,
    _document_base_url,
    _extract_static_item,
    _fetch_html_via_cdp,
    _normalize_emulation,
    _parse_html_tree,
    _query_document,
    _query_pair_to_mode_and_query,
    _resolve_document_input,
    _serialize_element,
    _store_document,
    _summarize_selector_matches,
)
from .models import (
    ClearDocumentsResult,
    CrawlFieldSpec,
    DeleteDocumentResult,
    DocumentSummary,
    FetchResult,
    HtmlParseResult,
    LinkExtractionResult,
    LinkMatch,
    LivePageExtractionResult,
    PrettifyResult,
    SelectorComparisonEntry,
    SelectorComparisonResult,
    SelectorMode,
    SelectorQueryResult,
    ServerStatusResult,
    SpiderCodeValidationResult,
    StoredDocumentInfo,
    _validate_http_url,
    _validate_optional_query_pair,
    _validate_ws_url,
)
from .runtime import DOCUMENT_STORE, SERVER_SETTINGS, mcp


def _format_http_fetch_error(url: str, exc: HttpError) -> str:
    detail = str(exc).strip()
    prefix = f"Request to {url} failed:"
    if detail.startswith(prefix):
        detail = detail[len(prefix) :].strip()

    lowered = detail.lower()
    if (
        "certificate_verify_failed" in lowered
        or "certificate verify failed" in lowered
        or "self signed certificate" in lowered
    ):
        return (
            f"Fetch failed for {url}: TLS certificate verification failed. "
            "The remote site's HTTPS certificate could not be verified by this runtime."
        )

    if "timed out" in lowered or "timeout" in lowered:
        return (
            f"Fetch failed for {url}: the request timed out. "
            "Retry or increase `timeout_seconds`."
        )

    if "name or service not known" in lowered or "dns" in lowered:
        return f"Fetch failed for {url}: DNS resolution failed for the target host."

    if "connection refused" in lowered:
        return f"Fetch failed for {url}: the remote host refused the connection."

    if "connection reset" in lowered:
        return f"Fetch failed for {url}: the connection was reset by the remote host."

    if detail:
        return f"Fetch failed for {url}: {_clip(detail, 240)}"
    return f"Fetch failed for {url}."


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
    return [
        document.info(SERVER_SETTINGS.document_ttl_seconds)
        for document in DOCUMENT_STORE.list()
    ]


@mcp.tool(tags={"documents"})
def delete_document(handle: str) -> DeleteDocumentResult:
    """Delete a cached document handle."""
    return DeleteDocumentResult(handle=handle, deleted=DOCUMENT_STORE.delete(handle))


@mcp.tool(tags={"documents"})
def clear_documents() -> ClearDocumentsResult:
    """Clear every cached document from the in-memory store."""
    return ClearDocumentsResult(deleted_count=DOCUMENT_STORE.clear())


@mcp.tool(tags={"diagnostics", "ops"})
async def server_status(
    include_documents: bool = False,
    probe_cdp: bool = False,
) -> ServerStatusResult:
    """Return runtime status, cache metrics, and optional CDP readiness information."""
    health = await _build_health_report(
        require_cdp=SERVER_SETTINGS.readiness_require_cdp,
        probe_cdp=SERVER_SETTINGS.readiness_require_cdp or probe_cdp,
    )
    return _build_server_status(include_documents=include_documents, health=health)


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


@mcp.tool(tags={"inspect", "selectors"})
def parse_html_document(
    document_handle: str | None = None,
    html: str | None = None,
    source_url: str | None = None,
    max_size_bytes: int | None = DEFAULT_HTML_MAX_SIZE_BYTES,
    truncate_on_limit: bool = False,
) -> HtmlParseResult:
    """Parse a full HTML document into a structured node tree using scraper_rs.parse_document."""
    resolved_html, stored_document = _resolve_document_input(
        document_handle=document_handle,
        html=html,
    )
    return HtmlParseResult(
        document_handle=stored_document.handle if stored_document else None,
        source_url=_document_base_url(stored_document, source_url),
        parse_mode="document",
        max_size_bytes=max_size_bytes,
        truncate_on_limit=truncate_on_limit,
        root=_parse_html_tree(
            resolved_html,
            parse_mode="document",
            max_size_bytes=max_size_bytes,
            truncate_on_limit=truncate_on_limit,
        ),
    )


@mcp.tool(tags={"inspect", "selectors"})
def parse_html_fragment(
    document_handle: str | None = None,
    html: str | None = None,
    source_url: str | None = None,
    max_size_bytes: int | None = DEFAULT_HTML_MAX_SIZE_BYTES,
    truncate_on_limit: bool = False,
) -> HtmlParseResult:
    """Parse an HTML fragment into a structured node tree using scraper_rs.parse_fragment."""
    resolved_html, stored_document = _resolve_document_input(
        document_handle=document_handle,
        html=html,
    )
    return HtmlParseResult(
        document_handle=stored_document.handle if stored_document else None,
        source_url=_document_base_url(stored_document, source_url),
        parse_mode="fragment",
        max_size_bytes=max_size_bytes,
        truncate_on_limit=truncate_on_limit,
        root=_parse_html_tree(
            resolved_html,
            parse_mode="fragment",
            max_size_bytes=max_size_bytes,
            truncate_on_limit=truncate_on_limit,
        ),
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
    _validate_http_url(url, field_name="url")
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
        try:
            response = await client.fetch(request)
        except HttpError as exc:
            raise ToolError(_format_http_fetch_error(url, exc)) from None
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
    headers: dict[str, str] | None = None,
    store_document: bool = True,
    label: str | None = None,
    body_preview_chars: int = DEFAULT_HTML_PREVIEW_CHARS,
) -> FetchResult:
    """Fetch rendered HTML through silkworm's CDP client for JavaScript-heavy pages."""
    _validate_http_url(url, field_name="url")
    _validate_ws_url(ws_endpoint, field_name="ws_endpoint")
    result, _html = await _fetch_html_via_cdp(
        url=url,
        ws_endpoint=ws_endpoint,
        timeout_seconds=timeout_seconds,
        headers=headers,
        store_document=store_document,
        label=label,
        body_preview_chars=body_preview_chars,
    )
    return result


@mcp.tool(tags={"fetch", "silkworm", "cdp", "selectors"})
async def query_selector_cdp(
    url: str,
    query: str,
    mode: SelectorMode = SelectorMode.css,
    ws_endpoint: str = "ws://127.0.0.1:9222",
    timeout_seconds: float | None = 20.0,
    headers: dict[str, str] | None = None,
    store_document: bool = True,
    label: str | None = None,
    limit: int = 20,
    include_html: bool = True,
    text_chars: int = 300,
    html_chars: int = 700,
    max_size_bytes: int = DEFAULT_HTML_MAX_SIZE_BYTES,
    truncate_on_limit: bool = False,
) -> SelectorQueryResult:
    """Fetch a rendered page through CDP, then run a CSS or XPath query on the live DOM snapshot."""
    _validate_http_url(url, field_name="url")
    _validate_ws_url(ws_endpoint, field_name="ws_endpoint")
    fetch_result, html = await _fetch_html_via_cdp(
        url=url,
        ws_endpoint=ws_endpoint,
        timeout_seconds=timeout_seconds,
        headers=headers,
        store_document=store_document,
        label=label,
    )
    document = scraper_rs.Document(
        html,
        max_size_bytes=max_size_bytes,
        truncate_on_limit=truncate_on_limit,
    )
    try:
        matches = _query_document(document, query=query, mode=mode)
        total_matches, returned_matches, omitted_matches, serialized_matches = (
            _summarize_selector_matches(
                matches,
                limit=limit,
                include_html=include_html,
                text_chars=text_chars,
                html_chars=html_chars,
            )
        )
        return SelectorQueryResult(
            document_handle=fetch_result.document_handle,
            source_url=fetch_result.final_url,
            mode=mode,
            query=query,
            total_matches=total_matches,
            returned_matches=returned_matches,
            omitted_matches=omitted_matches,
            matches=serialized_matches,
        )
    finally:
        document.close()


@mcp.tool(tags={"fetch", "silkworm", "cdp", "extract"})
async def extract_structured_data_cdp(
    url: str,
    fields: list[CrawlFieldSpec],
    item_selector: str | None = None,
    item_xpath: str | None = None,
    ws_endpoint: str = "ws://127.0.0.1:9222",
    timeout_seconds: float | None = 20.0,
    headers: dict[str, str] | None = None,
    store_document: bool = True,
    label: str | None = None,
    item_limit: int = 25,
    include_source_url: bool = True,
    max_size_bytes: int = DEFAULT_HTML_MAX_SIZE_BYTES,
    truncate_on_limit: bool = False,
) -> LivePageExtractionResult:
    """Fetch a rendered page through CDP and extract structured records from the rendered DOM."""
    _validate_http_url(url, field_name="url")
    _validate_ws_url(ws_endpoint, field_name="ws_endpoint")
    _validate_optional_query_pair(item_selector, item_xpath, "item_selector/item_xpath")
    fetch_result, html = await _fetch_html_via_cdp(
        url=url,
        ws_endpoint=ws_endpoint,
        timeout_seconds=timeout_seconds,
        headers=headers,
        store_document=store_document,
        label=label,
    )
    document = scraper_rs.Document(
        html,
        max_size_bytes=max_size_bytes,
        truncate_on_limit=truncate_on_limit,
    )
    try:
        item_query = _query_pair_to_mode_and_query(css=item_selector, xpath=item_xpath)
        if item_query is None:
            scopes = [document]
            scope_mode: SelectorMode | None = None
            scope_query: str | None = None
        else:
            scope_mode, scope_query = item_query
            scopes = _query_document(document, query=scope_query, mode=scope_mode)

        items: list[dict[str, Any]] = []
        for scope in scopes[:item_limit]:
            item = _extract_static_item(
                scope,
                fields,
                base_url=fetch_result.final_url,
            )
            if include_source_url:
                item["_source_url"] = fetch_result.final_url
            items.append(item)

        return LivePageExtractionResult(
            url=url,
            final_url=fetch_result.final_url,
            status=fetch_result.status,
            headers=fetch_result.headers,
            document_handle=fetch_result.document_handle,
            summary=fetch_result.summary
            or _build_summary(
                html,
                source_url=fetch_result.final_url,
                fetched_via="cdp",
                status=fetch_result.status,
            ),
            scope_mode=scope_mode,
            scope_query=scope_query,
            total_scopes=len(scopes),
            returned_items=len(items),
            omitted_items=max(0, len(scopes) - len(items)),
            items=items,
        )
    finally:
        document.close()


@mcp.tool(tags={"templates", "code", "validation"})
def validate_spider_code(
    code: str,
    expected_class_name: str | None = None,
) -> SpiderCodeValidationResult:
    """Statically validate generated spider code for syntax and common silkworm/CDP wiring."""
    try:
        tree = ast.parse(code)
    except SyntaxError as exc:
        issue = f"Syntax error at line {exc.lineno}, column {exc.offset}: {exc.msg}"
        return SpiderCodeValidationResult(syntax_ok=False, issues=[issue])

    spider_classes: list[str] = []
    imports_silkworm = False
    uses_cdp_client = False
    uses_run_spider = False
    uses_run_spider_uvloop = False

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith("silkworm"):
                    imports_silkworm = True
        elif isinstance(node, ast.ImportFrom):
            if (node.module or "").startswith("silkworm"):
                imports_silkworm = True
        elif isinstance(node, ast.ClassDef):
            base_names = set()
            for base in node.bases:
                if isinstance(base, ast.Name):
                    base_names.add(base.id)
                elif isinstance(base, ast.Attribute):
                    base_names.add(base.attr)
            if "Spider" in base_names:
                spider_classes.append(node.name)
        elif isinstance(node, ast.Call):
            func = node.func
            func_name = (
                func.id
                if isinstance(func, ast.Name)
                else func.attr
                if isinstance(func, ast.Attribute)
                else None
            )
            if func_name == "CDPClient":
                uses_cdp_client = True
            elif func_name == "run_spider":
                uses_run_spider = True
            elif func_name == "run_spider_uvloop":
                uses_run_spider_uvloop = True

    issues: list[str] = []
    if not imports_silkworm:
        issues.append("Code does not import silkworm modules.")
    if not spider_classes:
        issues.append("No class inheriting from Spider was found.")
    if expected_class_name and expected_class_name not in spider_classes:
        issues.append(f"Expected spider class '{expected_class_name}' was not found.")
    if not (uses_run_spider or uses_run_spider_uvloop):
        issues.append("No run_spider/run_spider_uvloop entrypoint call was found.")

    return SpiderCodeValidationResult(
        syntax_ok=True,
        spider_classes=spider_classes,
        imports_silkworm=imports_silkworm,
        uses_cdp_client=uses_cdp_client,
        uses_run_spider=uses_run_spider,
        uses_run_spider_uvloop=uses_run_spider_uvloop,
        issues=issues,
    )
