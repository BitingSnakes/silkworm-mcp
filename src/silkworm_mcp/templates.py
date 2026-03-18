from __future__ import annotations

import json
from textwrap import dedent, indent
from urllib.parse import urlparse

from .constants import CrawlTransport, SpiderTemplateVariant
from .helpers import _normalize_identifier
from .models import CrawlBlueprint


def _infer_spider_template_variant(
    blueprint: CrawlBlueprint,
    requested_variant: SpiderTemplateVariant,
) -> SpiderTemplateVariant:
    if requested_variant != SpiderTemplateVariant.auto:
        return requested_variant

    if any(_looks_like_sitemap_url(url) for url in blueprint.start_urls):
        return SpiderTemplateVariant.sitemap_xml
    if blueprint.transport == CrawlTransport.cdp:
        return SpiderTemplateVariant.cdp_heavy
    if blueprint.follow_links_selector or blueprint.follow_links_xpath:
        return SpiderTemplateVariant.list_detail
    return SpiderTemplateVariant.list_only


def _looks_like_sitemap_url(url: str) -> bool:
    path = urlparse(url).path.lower()
    return path.endswith(".xml") or "sitemap" in path


def _render_template_shared_methods() -> str:
    return dedent(
        """
            async def _query(self, scope, query: str, mode: str, first: bool = False):
                if mode == "css":
                    return await (scope.select_first(query) if first else scope.select(query))
                return await (scope.xpath_first(query) if first else scope.xpath(query))

            async def _extract_field(self, scope, field: dict[str, object], base_url: str):
                mode = "css" if field.get("css") else "xpath"
                query = field.get("css") or field.get("xpath")
                if not query and field.get("name") in {"source_url", "_source_url"}:
                    return base_url or field.get("default")
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
                item = {}
                for field in BLUEPRINT["fields"]:
                    item[field["name"]] = await self._extract_field(scope, field, base_url)
                if BLUEPRINT.get("include_source_url", True):
                    item["_source_url"] = base_url
                return item

            async def _yield_scoped_items(self, response: HTMLResponse):
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
        """
    ).strip()


def _render_template_variant_methods(variant: SpiderTemplateVariant) -> str:
    if variant == SpiderTemplateVariant.list_only:
        return dedent(
            """
                async def _build_pagination_requests(self, response: HTMLResponse):
                    requests = []
                    pagination_query = BLUEPRINT.get("pagination_selector") or BLUEPRINT.get("pagination_xpath")
                    if pagination_query:
                        pagination_mode = "css" if BLUEPRINT.get("pagination_selector") else "xpath"
                        next_link = await self._query(response, pagination_query, pagination_mode, first=True)
                        if next_link is not None:
                            href = next_link.attr("href")
                            if href:
                                requests.append(response.follow(href, callback=self.parse, headers=dict(self.request_headers)))
                    return requests

                async def parse(self, response: Response):
                    if not isinstance(response, HTMLResponse):
                        return

                    if self._emitted_items >= BLUEPRINT["max_items"]:
                        return

                    async for item in self._yield_scoped_items(response):
                        yield item

                    if self._emitted_items >= BLUEPRINT["max_items"]:
                        return

                    for request in await self._build_pagination_requests(response):
                        if self._scheduled_requests >= BLUEPRINT["max_requests"]:
                            break
                        self._scheduled_requests += 1
                        yield request
            """
        ).strip()

    if variant == SpiderTemplateVariant.list_detail:
        return dedent(
            """
                async def _build_listing_requests(self, response: HTMLResponse):
                    requests = []

                    follow_query = BLUEPRINT.get("follow_links_selector") or BLUEPRINT.get("follow_links_xpath")
                    if follow_query:
                        follow_mode = "css" if BLUEPRINT.get("follow_links_selector") else "xpath"
                        nodes = await self._query(response, follow_query, follow_mode, first=False)
                        for node in nodes[: BLUEPRINT.get("follow_link_limit_per_page", 20)]:
                            href = node.attr("href")
                            if href:
                                requests.append(
                                    response.follow(
                                        href,
                                        callback=self.parse_detail,
                                        headers=dict(self.request_headers),
                                        meta={"listing_url": response.url},
                                    )
                                )

                    pagination_query = BLUEPRINT.get("pagination_selector") or BLUEPRINT.get("pagination_xpath")
                    if pagination_query:
                        pagination_mode = "css" if BLUEPRINT.get("pagination_selector") else "xpath"
                        next_link = await self._query(response, pagination_query, pagination_mode, first=True)
                        if next_link is not None:
                            href = next_link.attr("href")
                            if href:
                                requests.append(
                                    response.follow(
                                        href,
                                        callback=self.parse,
                                        headers=dict(self.request_headers),
                                    )
                                )

                    return requests

                async def parse(self, response: Response):
                    if not isinstance(response, HTMLResponse):
                        return

                    for request in await self._build_listing_requests(response):
                        if self._scheduled_requests >= BLUEPRINT["max_requests"]:
                            break
                        self._scheduled_requests += 1
                        yield request

                async def parse_detail(self, response: Response):
                    if not isinstance(response, HTMLResponse):
                        return

                    if self._emitted_items >= BLUEPRINT["max_items"]:
                        return

                    item = await self._extract_item(response, response.url)
                    listing_url = response.request.meta.get("listing_url")
                    if listing_url:
                        item["_listing_url"] = listing_url
                    self._emitted_items += 1
                    yield item
            """
        ).strip()

    if variant == SpiderTemplateVariant.sitemap_xml:
        return dedent(
            """
                async def start_requests(self):
                    for url in self.start_urls:
                        if self._scheduled_requests >= BLUEPRINT["max_requests"]:
                            break
                        self._scheduled_requests += 1
                        yield Request(
                            url=url,
                            headers=dict(self.request_headers),
                            callback=self.parse_sitemap,
                            dont_filter=True,
                            meta={"allow_non_html": True},
                        )

                async def parse_sitemap(self, response: Response):
                    if response.status and response.status >= 400:
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
                            if self._scheduled_requests >= BLUEPRINT["max_requests"]:
                                break
                            if not node.text:
                                continue
                            self._scheduled_requests += 1
                            yield Request(
                                url=node.text.strip(),
                                headers=dict(self.request_headers),
                                callback=self.parse_sitemap,
                                dont_filter=True,
                                meta={"allow_non_html": True},
                            )
                        return

                    for node in root.findall(f".//{namespace}url/{namespace}loc"):
                        if self._scheduled_requests >= BLUEPRINT["max_requests"]:
                            break
                        if not node.text:
                            continue
                        self._scheduled_requests += 1
                        yield Request(
                            url=node.text.strip(),
                            headers=dict(self.request_headers),
                            callback=self.parse_page,
                            dont_filter=True,
                        )

                async def parse_page(self, response: Response):
                    if not isinstance(response, HTMLResponse):
                        return

                    if self._emitted_items >= BLUEPRINT["max_items"]:
                        return

                    async for item in self._yield_scoped_items(response):
                        yield item
            """
        ).strip()

    return dedent(
        """
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

            async def parse(self, response: Response):
                if not isinstance(response, HTMLResponse):
                    return

                if self._emitted_items >= BLUEPRINT["max_items"]:
                    return

                async for item in self._yield_scoped_items(response):
                    yield item

                if self._emitted_items >= BLUEPRINT["max_items"]:
                    return

                for request in await self._build_follow_requests(response):
                    if self._scheduled_requests >= BLUEPRINT["max_requests"]:
                        break
                    self._scheduled_requests += 1
                    yield request
        """
    ).strip()


def _render_template_imports(
    runner_name: str,
    variant: SpiderTemplateVariant,
) -> str:
    lines = [
        "from __future__ import annotations",
        "",
        "import asyncio",
        "import json",
    ]
    if variant == SpiderTemplateVariant.sitemap_xml:
        lines.append("import xml.etree.ElementTree as ET")
    lines.extend(
        [
            "from urllib.parse import urljoin",
            "",
            f"from silkworm import Engine, HTMLResponse, Request, Response, Spider, {runner_name}",
            "from silkworm.cdp import CDPClient",
            "from silkworm.middlewares import (",
            "    DelayMiddleware,",
            "    RetryMiddleware,",
            "    SkipNonHTMLMiddleware,",
            "    UserAgentMiddleware,",
            ")",
            "from silkworm.pipelines import JsonLinesPipeline",
        ]
    )
    return "\n".join(lines)


def _render_spider_template(
    blueprint: CrawlBlueprint,
    class_name: str,
    variant: SpiderTemplateVariant,
) -> str:
    safe_class_name = _normalize_identifier(class_name)
    blueprint_json = indent(
        json.dumps(blueprint.model_dump(mode="json"), indent=2), " " * 4
    )
    runner_name = "run_spider_uvloop" if blueprint.use_uvloop_runner else "run_spider"
    shared_methods = indent(_render_template_shared_methods(), " " * 4)
    variant_methods = indent(_render_template_variant_methods(variant), " " * 4)
    class_sections = [
        f"class {safe_class_name}(Spider):",
        '    name = BLUEPRINT["spider_name"]',
        '    start_urls = tuple(BLUEPRINT["start_urls"])',
        "",
        "    def __init__(self, **kwargs):",
        '        super().__init__(name=BLUEPRINT["spider_name"], start_urls=BLUEPRINT["start_urls"], **kwargs)',
        '        self.request_headers = dict(BLUEPRINT.get("request_headers") or {})',
        "        self._scheduled_requests = 0",
        "        self._emitted_items = 0",
    ]
    if variant != SpiderTemplateVariant.sitemap_xml:
        class_sections.extend(
            [
                "",
                "    async def start_requests(self):",
                "        for url in self.start_urls:",
                '            if self._scheduled_requests >= BLUEPRINT["max_requests"]:',
                "                break",
                "            self._scheduled_requests += 1",
                "            yield Request(url=url, headers=dict(self.request_headers), callback=self.parse)",
            ]
        )
    class_sections.extend(["", shared_methods, "", variant_methods])

    parts = [
        _render_template_imports(runner_name, variant),
        "",
        "BLUEPRINT = json.loads(",
        blueprint_json,
        ")",
        "",
        "\n".join(class_sections),
        "",
        "async def _run_cdp_spider():",
        f"    spider = {safe_class_name}()",
        "    request_mw = [",
        "        UserAgentMiddleware(",
        '            BLUEPRINT.get("user_agents") or None,',
        '            default=BLUEPRINT.get("default_user_agent"),',
        "        )",
        "    ]",
        "    if any(",
        "        value is not None",
        "        for value in (",
        '            BLUEPRINT.get("delay_seconds"),',
        '            BLUEPRINT.get("delay_min_seconds"),',
        '            BLUEPRINT.get("delay_max_seconds"),',
        "        )",
        "    ):",
        "        request_mw.append(",
        "            DelayMiddleware(",
        '                delay=BLUEPRINT.get("delay_seconds"),',
        '                min_delay=BLUEPRINT.get("delay_min_seconds"),',
        '                max_delay=BLUEPRINT.get("delay_max_seconds"),',
        "            )",
        "        )",
        "    response_mw = [",
        "        SkipNonHTMLMiddleware(),",
        "        RetryMiddleware(",
        '            max_times=BLUEPRINT.get("retry_max_times", 3),',
        '            retry_http_codes=BLUEPRINT.get("retry_http_codes") or None,',
        '            backoff_base=BLUEPRINT.get("retry_backoff_base", 0.5),',
        '            sleep_http_codes=BLUEPRINT.get("sleep_http_codes") or None,',
        "        )",
        "    ]",
        "    pipelines = []",
        '    output_path = BLUEPRINT.get("output_jsonl_path")',
        "    if output_path:",
        "        pipelines.append(",
        "            JsonLinesPipeline(",
        "                output_path,",
        '                use_opendal=BLUEPRINT.get("output_use_opendal"),',
        "            )",
        "        )",
        "",
        "    engine = Engine(",
        "        spider,",
        "        request_middlewares=request_mw,",
        "        response_middlewares=response_mw,",
        "        item_pipelines=pipelines,",
        '        concurrency=BLUEPRINT["concurrency"],',
        '        request_timeout=BLUEPRINT.get("request_timeout_seconds"),',
        '        max_pending_requests=BLUEPRINT.get("max_pending_requests"),',
        '        html_max_size_bytes=BLUEPRINT["html_max_size_bytes"],',
        '        log_stats_interval=BLUEPRINT.get("log_stats_interval"),',
        '        keep_alive=BLUEPRINT["keep_alive"],',
        "    )",
        "",
        '    timeout = BLUEPRINT.get("cdp_timeout_seconds")',
        "    if timeout is None:",
        '        timeout = BLUEPRINT.get("request_timeout_seconds")',
        "    cdp_client = CDPClient(",
        '        ws_endpoint=BLUEPRINT.get("cdp_ws_endpoint", "ws://127.0.0.1:9222"),',
        '        concurrency=BLUEPRINT["concurrency"],',
        "        timeout=timeout,",
        '        html_max_size_bytes=BLUEPRINT["html_max_size_bytes"],',
        "    )",
        "    await cdp_client.connect()",
        "    try:",
        "        await engine.http.close()",
        "        engine.http = cdp_client",
        "        await engine.run()",
        "    finally:",
        "        await cdp_client.close()",
        "",
        'if __name__ == "__main__":',
        '    if BLUEPRINT.get("transport") == "cdp":',
        "        asyncio.run(_run_cdp_spider())",
        "        raise SystemExit(0)",
        "",
        "    request_mw = [",
        "        UserAgentMiddleware(",
        '            BLUEPRINT.get("user_agents") or None,',
        '            default=BLUEPRINT.get("default_user_agent"),',
        "        )",
        "    ]",
        "    if any(",
        "        value is not None",
        "        for value in (",
        '            BLUEPRINT.get("delay_seconds"),',
        '            BLUEPRINT.get("delay_min_seconds"),',
        '            BLUEPRINT.get("delay_max_seconds"),',
        "        )",
        "    ):",
        "        request_mw.append(",
        "            DelayMiddleware(",
        '                delay=BLUEPRINT.get("delay_seconds"),',
        '                min_delay=BLUEPRINT.get("delay_min_seconds"),',
        '                max_delay=BLUEPRINT.get("delay_max_seconds"),',
        "            )",
        "        )",
        "    response_mw = [",
        "        SkipNonHTMLMiddleware(),",
        "        RetryMiddleware(",
        '            max_times=BLUEPRINT.get("retry_max_times", 3),',
        '            retry_http_codes=BLUEPRINT.get("retry_http_codes") or None,',
        '            backoff_base=BLUEPRINT.get("retry_backoff_base", 0.5),',
        '            sleep_http_codes=BLUEPRINT.get("sleep_http_codes") or None,',
        "        )",
        "    ]",
        "    pipelines = []",
        '    output_path = BLUEPRINT.get("output_jsonl_path")',
        "    if output_path:",
        "        pipelines.append(",
        "            JsonLinesPipeline(",
        "                output_path,",
        '                use_opendal=BLUEPRINT.get("output_use_opendal"),',
        "            )",
        "        )",
        "",
        f"    {runner_name}(",
        f"        {safe_class_name},",
        "        request_middlewares=request_mw,",
        "        response_middlewares=response_mw,",
        "        item_pipelines=pipelines,",
        '        concurrency=BLUEPRINT["concurrency"],',
        '        request_timeout=BLUEPRINT.get("request_timeout_seconds"),',
        '        max_pending_requests=BLUEPRINT.get("max_pending_requests"),',
        '        html_max_size_bytes=BLUEPRINT["html_max_size_bytes"],',
        '        log_stats_interval=BLUEPRINT.get("log_stats_interval"),',
        '        keep_alive=BLUEPRINT["keep_alive"],',
        "    )",
    ]
    return "\n".join(parts).strip()
