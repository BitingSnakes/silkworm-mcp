from __future__ import annotations

import argparse
import xml.etree.ElementTree as ET
from typing import Any

import scraper_rs
from silkworm import Engine, HTMLResponse, Request, Response, Spider
from silkworm.cdp import CDPClient

from .constants import (
    SERVER_NAME,
    SERVER_VERSION,
    CrawlTransport,
    SelectorMode,
    SpiderTemplateVariant,
)
from .documents import DocumentStore
from .helpers import (
    _CollectItemsPipeline,
    _build_cdp_client,
    _build_runtime_request_middlewares,
    _build_runtime_response_middlewares,
    _extract_item,
    _normalize_identifier,
    _query_pair_to_mode_and_query,
    _query_scope,
    _query_scope_first,
)
from .models import CrawlBlueprint, CrawlFieldSpec, CrawlRunResult, SpiderTemplateResult
from .runtime import SERVER_SETTINGS, logger, mcp
from .templates import _infer_spider_template_variant, _render_spider_template
from . import resources as _resources
from . import tools as _tools

clear_documents = _tools.clear_documents
list_documents = _tools.list_documents
parse_html_document = _tools.parse_html_document
parse_html_fragment = _tools.parse_html_fragment
query_selector = _tools.query_selector
server_status = _tools.server_status
silkworm_fetch = _tools.silkworm_fetch
store_html_document = _tools.store_html_document
silkworm_playbook = _resources.silkworm_playbook
template_variants_reference = _resources.template_variants_reference

__all__ = [
    "CDPClient",
    "CrawlBlueprint",
    "CrawlFieldSpec",
    "CrawlRunResult",
    "CrawlTransport",
    "DocumentStore",
    "Engine",
    "HTMLResponse",
    "Request",
    "Response",
    "SelectorMode",
    "SERVER_NAME",
    "SERVER_SETTINGS",
    "SERVER_VERSION",
    "Spider",
    "SpiderTemplateResult",
    "SpiderTemplateVariant",
    "_build_cdp_client",
    "clear_documents",
    "generate_spider_template",
    "list_documents",
    "main",
    "parse_html_document",
    "parse_html_fragment",
    "query_selector",
    "run_crawl_blueprint",
    "scraper_rs",
    "server_status",
    "silkworm_fetch",
    "silkworm_playbook",
    "store_html_document",
    "template_variants_reference",
]


@mcp.tool(tags={"crawl", "silkworm"})
async def run_crawl_blueprint(
    blueprint: CrawlBlueprint,
    variant: SpiderTemplateVariant = SpiderTemplateVariant.auto,
) -> CrawlRunResult:
    """Run a configurable silkworm spider without writing code, useful for validating a scraping plan."""
    resolved_variant = _infer_spider_template_variant(blueprint, variant)

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
                callback = self.parse
                meta: dict[str, Any] = {}
                dont_filter = False
                if resolved_variant == SpiderTemplateVariant.sitemap_xml:
                    callback = self.parse_sitemap
                    meta = {"allow_non_html": True}
                    dont_filter = True
                yield Request(
                    url=url,
                    headers=dict(self._request_headers),
                    callback=callback,
                    meta=meta,
                    dont_filter=dont_filter,
                )

        async def _build_pagination_requests(
            self, response: HTMLResponse
        ) -> list[Request]:
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
            return requests

        async def _build_detail_requests(self, response: HTMLResponse) -> list[Request]:
            requests: list[Request] = []
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
                                callback=self.parse_detail,
                                headers=dict(self._request_headers),
                                meta={"listing_url": response.url},
                            )
                        )
            return requests

        async def _yield_scoped_items(self, response: HTMLResponse):
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

        async def parse(self, response: Response):
            if not isinstance(response, HTMLResponse):
                return

            if resolved_variant == SpiderTemplateVariant.list_detail:
                for request in await self._build_detail_requests(response):
                    if self._scheduled_requests >= blueprint.max_requests:
                        break
                    self._scheduled_requests += 1
                    yield request

                if self._scheduled_requests >= blueprint.max_requests:
                    return

                for request in await self._build_pagination_requests(response):
                    if self._scheduled_requests >= blueprint.max_requests:
                        break
                    self._scheduled_requests += 1
                    yield request
                return

            async for item in self._yield_scoped_items(response):
                yield item

            if self._emitted_items >= blueprint.max_items:
                return

            requests: list[Request]
            if resolved_variant == SpiderTemplateVariant.list_only:
                requests = await self._build_pagination_requests(response)
            else:
                requests = await self._build_pagination_requests(response)
                requests.extend(await self._build_detail_requests(response))

            for request in requests:
                if self._scheduled_requests >= blueprint.max_requests:
                    break
                self._scheduled_requests += 1
                yield request

        async def parse_detail(self, response: Response):
            if not isinstance(response, HTMLResponse):
                return

            if self._emitted_items >= blueprint.max_items:
                return

            item = await _extract_item(
                response,
                blueprint.fields,
                base_url=response.url,
            )
            if blueprint.include_source_url:
                item["_source_url"] = response.url
            listing_url = response.request.meta.get("listing_url")
            if listing_url:
                item["_listing_url"] = listing_url
            self._emitted_items += 1
            yield item

        async def parse_sitemap(self, response: Response):
            if response.status >= 400:
                return

            try:
                root = ET.fromstring(response.text)
            except ET.ParseError:
                return

            namespace = ""
            if root.tag.startswith("{"):
                namespace = root.tag.split("}", 1)[0] + "}"

            sitemap_nodes = root.findall(f".//{namespace}sitemap/{namespace}loc")
            if sitemap_nodes:
                for node in sitemap_nodes:
                    if self._scheduled_requests >= blueprint.max_requests:
                        break
                    if not node.text:
                        continue
                    self._scheduled_requests += 1
                    yield Request(
                        url=node.text.strip(),
                        headers=dict(self._request_headers),
                        callback=self.parse_sitemap,
                        dont_filter=True,
                        meta={"allow_non_html": True},
                    )
                return

            for node in root.findall(f".//{namespace}url/{namespace}loc"):
                if self._scheduled_requests >= blueprint.max_requests:
                    break
                if not node.text:
                    continue
                self._scheduled_requests += 1
                yield Request(
                    url=node.text.strip(),
                    headers=dict(self._request_headers),
                    callback=self.parse_page,
                    dont_filter=True,
                )

        async def parse_page(self, response: Response):
            if not isinstance(response, HTMLResponse):
                return

            async for item in self._yield_scoped_items(response):
                yield item

    spider = BlueprintSpider()
    collector = _CollectItemsPipeline()
    engine = Engine(
        spider,
        concurrency=blueprint.concurrency,
        max_pending_requests=blueprint.max_pending_requests,
        request_timeout=blueprint.request_timeout_seconds,
        html_max_size_bytes=blueprint.html_max_size_bytes,
        request_middlewares=_build_runtime_request_middlewares(blueprint),
        response_middlewares=_build_runtime_response_middlewares(blueprint),
        item_pipelines=[collector],
        log_stats_interval=blueprint.log_stats_interval,
        keep_alive=blueprint.keep_alive,
    )
    cdp_client: CDPClient | None = None
    try:
        if blueprint.transport == CrawlTransport.cdp:
            cdp_client = _build_cdp_client(blueprint)
            await cdp_client.connect()
            await engine.http.close()
            engine.http = cdp_client
        await engine.run()
    finally:
        if cdp_client is not None:
            await cdp_client.close()
    return CrawlRunResult(
        spider_name=blueprint.spider_name,
        execution_variant=resolved_variant,
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
    variant: SpiderTemplateVariant = SpiderTemplateVariant.auto,
) -> SpiderTemplateResult:
    """Generate a production starter spider that mirrors the crawl blueprint."""
    safe_class_name = _normalize_identifier(class_name)
    resolved_variant = _infer_spider_template_variant(blueprint, variant)
    return SpiderTemplateResult(
        class_name=safe_class_name,
        spider_name=blueprint.spider_name,
        template_variant=resolved_variant,
        code=_render_spider_template(blueprint, safe_class_name, resolved_variant),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the silkworm MCP server.")
    parser.add_argument(
        "--transport",
        choices=["stdio", "http", "sse", "streamable-http"],
        default=SERVER_SETTINGS.default_transport,
        help="MCP transport to run.",
    )
    parser.add_argument(
        "--host",
        default=SERVER_SETTINGS.default_host,
        help="Host for HTTP-based transports.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=SERVER_SETTINGS.default_port,
        help="Port for HTTP-based transports.",
    )
    parser.add_argument(
        "--path",
        default=SERVER_SETTINGS.default_path,
        help="Optional MCP endpoint path for HTTP-based transports.",
    )
    args = parser.parse_args()

    from .helpers import _configure_logging

    _configure_logging(SERVER_SETTINGS.log_level)

    logger.info(
        "starting %s version=%s transport=%s host=%s port=%s path=%s",
        SERVER_NAME,
        SERVER_VERSION,
        args.transport,
        args.host,
        args.port,
        args.path,
    )

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
