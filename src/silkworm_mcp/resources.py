from __future__ import annotations

import json
from textwrap import dedent

from starlette.requests import Request as StarletteRequest
from starlette.responses import JSONResponse, Response

from .constants import SERVER_VERSION, SelectorMode
from .helpers import _build_health_report, _build_server_status, _build_summary
from .models import CrawlBlueprint
from .runtime import DOCUMENT_STORE, SERVER_SETTINGS, mcp


@mcp.custom_route(
    SERVER_SETTINGS.http_health_path,
    methods=["GET"],
    include_in_schema=False,
)
async def http_health(_request: StarletteRequest) -> Response:
    health = await _build_health_report(require_cdp=False, probe_cdp=False)
    return JSONResponse(health.model_dump(mode="json"), status_code=200)


@mcp.custom_route(
    SERVER_SETTINGS.http_ready_path,
    methods=["GET"],
    include_in_schema=False,
)
async def http_readiness(_request: StarletteRequest) -> Response:
    health = await _build_health_report(
        require_cdp=SERVER_SETTINGS.readiness_require_cdp,
        probe_cdp=SERVER_SETTINGS.readiness_require_cdp,
    )
    return JSONResponse(
        health.model_dump(mode="json"),
        status_code=200 if health.ready else 503,
    )


@mcp.resource(
    "silkworm://status",
    mime_type="application/json",
)
def status_resource() -> str:
    payload = _build_server_status(include_documents=True)
    return json.dumps(payload.model_dump(mode="json"), indent=2)


@mcp.resource("silkworm://reference/overview")
def reference_overview() -> str:
    return dedent(
        f"""
        # silkworm-mcp

        Version: {SERVER_VERSION}

        Purpose:
        - Expose silkworm fetching/crawling primitives to MCP clients.
        - Expose scraper-rs DOM parsing and selector debugging tools.
        - Help LLMs move from "I need a scraper" to "here is a working spider blueprint or template".

        Suggested workflow:
        1. `server_status` for runtime diagnostics when needed
        2. `silkworm_fetch` or `silkworm_fetch_cdp`
        3. `inspect_document`
        4. `parse_html_document` or `parse_html_fragment`
        5. `query_selector` and `compare_selectors`
        6. `query_selector_cdp` or `extract_structured_data_cdp` for rendered-page debugging
        7. `extract_links` for pagination/detail URL discovery
        8. `run_crawl_blueprint`
        9. `generate_spider_template`
        10. `validate_spider_code`

        Silkworm knowledge resources:
        - `silkworm://reference/silkworm-cheatsheet`
        - `silkworm://reference/silkworm-playbook`
        - `silkworm://reference/template-variants`
        - `silkworm://reference/scraper-rs-cheatsheet`
        - `silkworm://reference/crawl-blueprint-schema`

        Document handles:
        - Most tools accept either `document_handle` or raw `html`.
        - Prefer handles so clients do not resend large HTML payloads.
        - HTTP transports also expose `{SERVER_SETTINGS.http_health_path}` and `{SERVER_SETTINGS.http_ready_path}`.
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
        - `parse()` should usually accept `Response`, then guard with `isinstance(response, HTMLResponse)` before HTML extraction
        - Start pages: yield `Request(url=..., callback=self.parse)`
        - Extract containers: `await response.select("article.card")`
        - Follow pagination/detail pages: `yield response.follow(next_href, callback=self.parse)`
        - Extract items: yield dicts from `parse()`
        - Preserve provenance: include `_source_url` or a page URL field on emitted items
        - JS-rendered pages: set `transport="cdp"` and `cdp_ws_endpoint` in `CrawlBlueprint`

        Middleware heuristics:
        - `UserAgentMiddleware`: use by default
        - `RetryMiddleware`: use for transient failures or anti-bot throttling
        - `DelayMiddleware`: add when a site is rate-sensitive or you need politeness
        - `SkipNonHTMLMiddleware`: use for normal HTML crawls so image/API responses do not hit HTML callbacks
        - `request.meta["allow_non_html"] = True`: opt out per request when intentionally fetching XML or other non-HTML content

        Extraction heuristics:
        - Prefer CSS selectors first; switch to XPath only when structure/text relationships are awkward in CSS
        - Use `all_matches=true` for repeated tags/categories/images
        - Use `extractor="attr"` with `attr_name="href"` or `src` for URLs/assets
        - Set `absolute_url=true` whenever a field should be reusable outside the current page context
        - Use `join_with` only when downstream consumers want a flat string instead of an array

        Crawl design rules:
        - If listing pages already contain the needed fields, do not follow detail pages
        - If a field only exists on details, extract the detail URL first, then follow it
        - Validate selectors on one page before enabling pagination
        - Keep `max_requests`, `max_items`, `concurrency`, and `request_timeout` explicit while prototyping
        - Prefer JSON Lines output for generated starter spiders
        - Use `run_spider_uvloop` when available, otherwise `run_spider`

        Useful engine knobs:
        - `concurrency`
        - `request_timeout`
        - `max_pending_requests`
        - `html_max_size_bytes`
        - `keep_alive`
        """
    ).strip()


@mcp.resource("silkworm://reference/silkworm-playbook")
def silkworm_playbook() -> str:
    return dedent(
        """
        # silkworm scraper playbook

        Goal:
        - Generate scrapers that follow silkworm idioms closely enough to run with minimal manual fixes.

        Decision tree:
        1. Start with `silkworm_fetch` on a representative page.
        2. If the important content is present in the returned HTML, stay on normal HTTP transport.
        3. If content is missing, delayed, or selector results differ after render, switch to `silkworm_fetch_cdp` and set `transport="cdp"` in the blueprint.
        4. Find the smallest stable item container before defining field selectors.
        5. Extract list-page fields first; only add detail-page crawling when essential fields are absent.
        6. Validate pagination separately from item extraction.

        Blueprint design guidance:
        - `item_selector` should point at one logical record card/row/article.
        - `fields` should be relative to the item scope whenever possible.
        - For URLs use `extractor="attr"`, `attr_name="href"`, and usually `absolute_url=true`.
        - For repeated values such as tags, features, or breadcrumbs use `all_matches=true`.
        - Use `pagination_selector` for one "next page" control, not all page links.
        - Use `follow_links_selector` only for detail URLs, and keep `follow_link_limit_per_page` bounded.
        - Keep `include_source_url=true` during development.

        Generated spider expectations:
        - Import `Response` and `HTMLResponse`.
        - Guard non-HTML responses before selector calls.
        - Use `response.follow(...)` instead of hand-rolled `urljoin(...)` for crawl navigation.
        - Add `UserAgentMiddleware` by default.
        - Add `RetryMiddleware` and usually `SkipNonHTMLMiddleware` for HTML crawls.
        - Add `DelayMiddleware` only when the target site needs slower pacing.
        - Output to `JsonLinesPipeline` unless another sink is explicitly required.

        Patterns from the bundled Silkworm examples:
        - Quotes demo: list-page extraction + pagination with `response.follow`
        - Callback pipeline demo: enrich or validate items without custom pipeline classes
        - Sitemap demo: opt into non-HTML responses for XML sitemap requests with `meta={"allow_non_html": True}`
        - Lightpanda/CDP demos: connect a `CDPClient`, swap engine transport, and close the client explicitly
        """
    ).strip()


@mcp.resource("silkworm://reference/template-variants")
def template_variants_reference() -> str:
    return dedent(
        """
        # template and crawl variants

        Use `variant="auto"` by default when:
        - the blueprint already clearly signals the crawl shape
        - you want `run_crawl_blueprint` and `generate_spider_template` to pick the normal Silkworm pattern
        - start URLs, `transport`, and follow-link settings are trustworthy enough for inference

        Choose `variant="list_only"` explicitly when:
        - listing pages already contain the fields you need
        - you want pagination but no detail-page crawling
        - `follow_links_selector` might exist on the page but should not drive runtime behavior

        Choose `variant="list_detail"` explicitly when:
        - listing pages mainly act as navigation to detail pages
        - final items should come from detail responses, not listing scopes
        - you want `_listing_url` preserved on emitted detail items

        Choose `variant="sitemap_xml"` explicitly when:
        - entrypoints are XML sitemaps or sitemap indexes
        - URLs do not obviously contain `sitemap` or end in `.xml`, so auto-inference may miss the intent
        - you want requests to start with `meta={"allow_non_html": True}`

        Choose `variant="cdp_heavy"` explicitly when:
        - pages need rendered DOM state and you want the general parse/follow runtime shape
        - `transport="cdp"` is required regardless of what the start URL looks like
        - you do not want auto-inference to collapse the crawl into a simpler list-only or list-detail assumption

        Inference summary:
        - `auto` picks `sitemap_xml` for sitemap-like start URLs
        - otherwise `auto` picks `cdp_heavy` for `transport="cdp"`
        - otherwise `auto` picks `list_detail` when follow-link selectors are configured
        - otherwise `auto` picks `list_only`
        """
    ).strip()


@mcp.resource("silkworm://reference/scraper-rs-cheatsheet")
def scraper_rs_cheatsheet() -> str:
    return dedent(
        """
        # scraper-rs cheat sheet

        Main API:
        - `Document(html)`
        - `scraper_rs.parse_document(html)`
        - `scraper_rs.parse_fragment(html)`
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
        - `parse_html_document` and `parse_html_fragment` expose parser-level node trees, parser errors, and quirks metadata through MCP.
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
        document.info(SERVER_SETTINGS.document_ttl_seconds).model_dump(mode="json")
        for document in DOCUMENT_STORE.list()
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
        Build a concrete silkworm scraper plan for this goal:
        {goal}

        Target URL: {target_url or "not provided"}

        Work in this order:
        1. Choose the first page to inspect and say whether plain HTTP or CDP should be tried first.
        2. Identify the likely item container, field selectors, pagination selector, and any detail-page links.
        3. Call out which MCP tools should be used to validate each assumption.
        4. Decide whether the crawl should be list-only, list-detail, sitemap-driven, or CDP-heavy.
        5. Produce a draft `CrawlBlueprint` shape with the important fields and runtime limits.
        6. Note whether generating a spider template is appropriate yet or should wait for more selector validation.

        Apply these silkworm rules:
        - Prefer HTTP before CDP; switch only when the rendered DOM materially changes extraction.
        - Keep selectors relative to the item scope instead of page-wide when possible.
        - Use `extractor="attr"` with `attr_name="href"` and usually `absolute_url=true` for navigational links.
        - Use `all_matches=true` for repeated fields such as tags, categories, breadcrumbs, or image lists.
        - Avoid detail-page crawling if listing pages already contain the required fields.
        - Plan for `UserAgentMiddleware`, `RetryMiddleware`, and usually `SkipNonHTMLMiddleware`.
        - Keep `max_requests`, `max_items`, `concurrency`, and `request_timeout_seconds` explicit while prototyping.

        Prefer document handles over repeatedly embedding raw HTML.
        Be specific and operational: name the tools, selectors to verify, and likely blueprint fields.
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
        Debug this selector strategy for a scraper and propose a safer extraction approach.

        Extraction goal: {extraction_goal}
        Current selector: {current_selector or "not provided"}
        Mode: {mode.value}

        Use this workflow:
        1. Inspect the page structure with `inspect_document`.
        2. Use `parse_html_document` or `parse_html_fragment` when the DOM structure is ambiguous.
        3. Test the current selector with `query_selector`.
        4. Compare alternatives with `compare_selectors`.
        5. Use `query_selector_cdp` if the page appears render-dependent.
        6. Use `extract_links` if the extraction goal involves pagination or detail navigation.

        Evaluate the selector strategy against these criteria:
        - Match the intended nodes without obvious false positives.
        - Stay resilient to layout shifts and presentational wrapper changes.
        - Prefer selectors scoped to each item container instead of brittle page-wide selectors.
        - For links or assets, verify whether `attr("href")` or `attr("src")` is the real target value.
        - If the current selector is weak, suggest 2 or 3 stronger alternatives and explain the tradeoffs briefly.
        """
    ).strip()
