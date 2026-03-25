from __future__ import annotations

import asyncio
import ast
import logging
import sys
import time
import tomllib
from pathlib import Path

import pytest
from fastmcp.exceptions import ToolError
from silkworm import HTMLResponse
from silkworm.exceptions import HttpError
from silkworm.request import Request
from silkworm_mcp import helpers as mcp_helpers
from silkworm_mcp import tools as mcp_tools

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import mcp_server
from mcp_server import (
    CrawlBlueprint,
    CrawlFieldSpec,
    DocumentStore,
    SelectorMode,
    SpiderTemplateVariant,
    clear_documents,
    find_selectors_by_text,
    generate_regex,
    generate_spider_template,
    list_documents,
    parse_html_document,
    parse_html_fragment,
    query_selector,
    run_crawl_blueprint,
    server_status,
    silkworm_fetch,
    silkworm_playbook,
    store_html_document,
    template_variants_reference,
)

SAMPLE_HTML = """
<html>
  <head><title>Catalog</title></head>
  <body>
    <section class="products">
      <article class="product">
        <a class="detail" href="/items/1">
          <h2 class="name">Widget A</h2>
        </a>
        <span class="price">$10</span>
      </article>
      <article class="product">
        <a class="detail" href="/items/2">
          <h2 class="name">Widget B</h2>
        </a>
        <span class="price">$20</span>
      </article>
    </section>
    <a class="next" href="/page/2">Next</a>
  </body>
</html>
""".strip()


@pytest.fixture(autouse=True)
def clear_global_documents() -> None:
    clear_documents()
    yield
    clear_documents()


async def _collect_requests(generator) -> list[Request]:
    return [item async for item in generator]


def test_store_and_list_documents_include_runtime_metadata() -> None:
    summary = store_html_document(
        html=SAMPLE_HTML,
        source_url="https://example.com/catalog",
        label="catalog-demo",
    )

    documents = list_documents()

    assert documents[0].handle == summary.handle
    assert documents[0].label == "catalog-demo"
    assert documents[0].html_bytes >= documents[0].html_chars
    assert documents[0].last_accessed_at
    assert documents[0].expires_at is not None


def test_query_selector_still_returns_expected_matches() -> None:
    summary = store_html_document(
        html=SAMPLE_HTML,
        source_url="https://example.com/catalog",
    )

    result = query_selector(
        document_handle=summary.handle,
        query=".product .name",
        mode=SelectorMode.css,
    )

    assert result.total_matches == 2
    assert result.matches[0].text == "Widget A"


def test_generate_regex_supports_core_grex_options() -> None:
    result = generate_regex(
        test_cases=["aa", "bcbc", "defdefdef"],
        convert_repetitions=True,
        minimum_substring_length=2,
    )

    assert result.pattern == "^(?:aa|(?:bc){2}|(?:def){3})$"
    assert result.convert_repetitions is True
    assert result.minimum_substring_length == 2


def test_find_selectors_by_text_round_trips_css_and_xpath() -> None:
    summary = store_html_document(
        html=SAMPLE_HTML,
        source_url="https://example.com/catalog",
    )

    inverse = find_selectors_by_text(
        document_handle=summary.handle,
        text_query="Widget A",
    )

    assert inverse.total_matches == 1
    assert inverse.matches[0].tag == "h2"
    assert inverse.matches[0].text == "Widget A"

    css_result = query_selector(
        document_handle=summary.handle,
        query=inverse.matches[0].css,
        mode=SelectorMode.css,
    )
    xpath_result = query_selector(
        document_handle=summary.handle,
        query=inverse.matches[0].xpath,
        mode=SelectorMode.xpath,
    )

    assert css_result.total_matches == 1
    assert css_result.matches[0].text == "Widget A"
    assert xpath_result.total_matches == 1
    assert xpath_result.matches[0].text == "Widget A"


def test_find_selectors_by_text_handles_fragments_with_unique_ids() -> None:
    fragment_html = '<div><span id="target">Fragment</span></div>'

    inverse = find_selectors_by_text(
        html=fragment_html,
        text_query="fragment",
        case_sensitive=False,
    )

    assert inverse.total_matches == 1
    assert inverse.matches[0].css == "span#target"
    assert inverse.matches[0].xpath == "//*[@id='target']"

    css_result = query_selector(
        html=fragment_html,
        query=inverse.matches[0].css,
        mode=SelectorMode.css,
    )
    xpath_result = query_selector(
        html=fragment_html,
        query=inverse.matches[0].xpath,
        mode=SelectorMode.xpath,
    )

    assert css_result.total_matches == 1
    assert xpath_result.total_matches == 1


def test_document_store_persists_handles_across_store_instances(
    tmp_path: Path,
) -> None:
    store_path = tmp_path / "documents.sqlite3"
    first_store = DocumentStore(
        max_document_count=16,
        max_total_bytes=1_000_000,
        ttl_seconds=3600,
        store_path=str(store_path),
    )
    second_store = DocumentStore(
        max_document_count=16,
        max_total_bytes=1_000_000,
        ttl_seconds=3600,
        store_path=str(store_path),
    )

    stored = first_store.add(
        SAMPLE_HTML,
        source_url="https://example.com/catalog",
        label="shared-doc",
    )

    loaded = second_store.get(stored.handle)
    listed = second_store.list()

    assert loaded.handle == stored.handle
    assert loaded.html == SAMPLE_HTML
    assert loaded.source_url == "https://example.com/catalog"
    assert listed[0].handle == stored.handle


def test_query_selector_error_mentions_rehydrating_html() -> None:
    with pytest.raises(ToolError) as exc_info:
        query_selector(
            document_handle="missing-handle",
            query=".product .name",
            mode=SelectorMode.css,
        )

    assert "fetch/store the HTML again" in str(exc_info.value)


def test_parse_html_document_returns_structured_tree(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, int | None, bool]] = []

    def fake_parse_document(
        html: str,
        *,
        max_size_bytes: int | None = None,
        truncate_on_limit: bool = False,
    ) -> dict[str, object]:
        calls.append((html, max_size_bytes, truncate_on_limit))
        return {
            "node_type": "document",
            "quirks_mode": "no-quirks",
            "children": [
                {
                    "node_type": "element",
                    "tag": "html",
                    "children": [
                        {
                            "node_type": "element",
                            "tag": "body",
                            "children": [
                                {
                                    "node_type": "element",
                                    "tag": "p",
                                    "attrs": {"class": "lead"},
                                    "children": [
                                        {"node_type": "text", "text": "Hello"}
                                    ],
                                }
                            ],
                        }
                    ],
                }
            ],
            "errors": [],
        }

    monkeypatch.setattr(
        mcp_server.scraper_rs,
        "parse_document",
        fake_parse_document,
        raising=False,
    )
    summary = store_html_document(
        html=SAMPLE_HTML,
        source_url="https://example.com/catalog",
    )

    result = parse_html_document(
        document_handle=summary.handle,
        max_size_bytes=4096,
        truncate_on_limit=True,
    )

    assert calls == [(SAMPLE_HTML, 4096, True)]
    assert result.document_handle == summary.handle
    assert result.source_url == "https://example.com/catalog"
    assert result.parse_mode == "document"
    assert result.root.node_type == "document"
    assert result.root.children[0].tag == "html"
    assert result.root.children[0].children[0].tag == "body"
    assert result.root.children[0].children[0].children[0].attrs["class"] == "lead"
    assert result.root.children[0].children[0].children[0].children[0].text == "Hello"


def test_parse_html_fragment_returns_inline_tree(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, int | None, bool]] = []

    def fake_parse_fragment(
        html: str,
        *,
        max_size_bytes: int | None = None,
        truncate_on_limit: bool = False,
    ) -> dict[str, object]:
        calls.append((html, max_size_bytes, truncate_on_limit))
        return {
            "node_type": "fragment",
            "children": [
                {
                    "node_type": "element",
                    "tag": "div",
                    "attrs": {"data-kind": "card"},
                    "children": [
                        {"node_type": "comment", "data": "note"},
                        {"node_type": "text", "text": "Widget"},
                    ],
                }
            ],
        }

    monkeypatch.setattr(
        mcp_server.scraper_rs,
        "parse_fragment",
        fake_parse_fragment,
        raising=False,
    )

    result = parse_html_fragment(
        html='<div data-kind="card"><!--note-->Widget</div>',
        source_url="https://example.com/fragment",
        max_size_bytes=None,
        truncate_on_limit=False,
    )

    assert calls == [('<div data-kind="card"><!--note-->Widget</div>', None, False)]
    assert result.document_handle is None
    assert result.source_url == "https://example.com/fragment"
    assert result.parse_mode == "fragment"
    assert result.root.node_type == "fragment"
    assert result.root.children[0].tag == "div"
    assert result.root.children[0].children[0].data == "note"
    assert result.root.children[0].children[1].text == "Widget"


def test_document_store_evicts_least_recently_used_document() -> None:
    store = DocumentStore(
        max_document_count=2,
        max_total_bytes=10_000,
        ttl_seconds=None,
    )
    first = store.add("<article>first</article>")
    second = store.add("<article>second</article>")

    store.get(first.handle)
    third = store.add("<article>third</article>")

    handles = {document.handle for document in store.list()}

    assert first.handle in handles
    assert second.handle not in handles
    assert third.handle in handles
    assert store.stats().evicted_documents == 1


def test_document_store_expires_idle_documents() -> None:
    store = DocumentStore(
        max_document_count=2,
        max_total_bytes=10_000,
        ttl_seconds=0.1,
    )
    document = store.add("<p>hello</p>")

    time.sleep(0.12)

    with pytest.raises(ValueError, match="Unknown document handle"):
        store.get(document.handle)
    assert store.stats().expired_documents == 1


def test_server_status_reports_runtime_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(mcp_server.SERVER_SETTINGS, "readiness_require_cdp", False)
    summary = store_html_document(
        html=SAMPLE_HTML,
        source_url="https://example.com/catalog",
    )

    status = asyncio.run(server_status(include_documents=True, probe_cdp=False))

    assert status.server_name == mcp_server.SERVER_NAME
    assert status.server_version == mcp_server.SERVER_VERSION
    assert status.document_store.document_count == 1
    assert status.documents[0].handle == summary.handle
    assert status.health is not None
    assert status.health.ready is True


def test_server_version_matches_pyproject() -> None:
    pyproject_path = Path(__file__).resolve().parents[1] / "pyproject.toml"
    with pyproject_path.open("rb") as pyproject_file:
        pyproject_data = tomllib.load(pyproject_file)

    assert mcp_server.SERVER_VERSION == pyproject_data["project"]["version"]


def test_silkworm_fetch_rejects_non_http_urls() -> None:
    with pytest.raises(ValueError, match="absolute URL"):
        asyncio.run(silkworm_fetch("file:///tmp/not-allowed.html"))


def test_inspect_document_requires_exactly_one_input() -> None:
    with pytest.raises(
        ToolError, match="Provide exactly one of 'document_handle' or 'html'"
    ):
        mcp_tools.inspect_document()


def test_inspect_document_unknown_handle_raises_tool_error() -> None:
    with pytest.raises(ToolError, match="Unknown document handle"):
        mcp_tools.inspect_document(document_handle="missing-handle")


def test_silkworm_fetch_formats_ssl_certificate_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_fetch(self, req: Request):  # noqa: ANN001
        raise HttpError(
            "Request to https://tishreen.news.sy/ failed: "
            "is_connect error: wreq::Error { kind: Request, uri: https://tishreen.news.sy/, "
            "source: Error { kind: Connect, source: Some(Error { code: SSL (1), cause: "
            'Some(Ssl(ErrorStack([Error { reason: "CERTIFICATE_VERIFY_FAILED" }]))) }) } }'
        )

    monkeypatch.setattr(mcp_tools.HttpClient, "fetch", fake_fetch)

    with pytest.raises(ToolError) as exc_info:
        asyncio.run(silkworm_fetch("https://tishreen.news.sy/"))

    message = str(exc_info.value)
    assert message == (
        "Fetch failed for https://tishreen.news.sy/: "
        "TLS certificate verification failed. "
        "The remote site's HTTPS certificate could not be verified by this runtime."
    )
    assert "CERTIFICATE_VERIFY_FAILED" not in message


def test_silkworm_fetch_cdp_formats_execution_context_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeCDPClient:
        async def connect(self) -> None:
            return None

        async def fetch(self, req: Request):  # noqa: ANN001
            raise HttpError(
                "CDP command Runtime.evaluate failed: CDP error: Cannot find default execution context"
            )

        async def close(self) -> None:
            return None

    monkeypatch.setattr(mcp_helpers, "CDPClient", lambda **kwargs: FakeCDPClient())

    with pytest.raises(ToolError) as exc_info:
        asyncio.run(mcp_tools.silkworm_fetch_cdp("https://example.com"))

    assert str(exc_info.value) == (
        "CDP fetch failed for https://example.com: "
        "the browser page lost its execution context before HTML could be captured. "
        "Retry the request; if it persists, restart the CDP browser."
    )


def test_fastmcp_tool_error_filter_suppresses_tracebacks() -> None:
    error = ToolError(
        "Fetch failed for https://tishreen.news.sy/: TLS certificate verification failed."
    )
    record = logging.LogRecord(
        "fastmcp.server.server",
        logging.ERROR,
        __file__,
        1,
        "Error calling tool %r",
        ("silkworm_fetch",),
        (ToolError, error, None),
    )

    allowed = mcp_helpers._FastMCPToolErrorFilter().filter(record)

    assert allowed is True
    assert record.getMessage() == (
        "Error calling tool 'silkworm_fetch': "
        "Fetch failed for https://tishreen.news.sy/: TLS certificate verification failed."
    )
    assert record.exc_info is None
    assert record.exc_text is None


def test_generate_spider_template_closes_cdp_client() -> None:
    blueprint = CrawlBlueprint(
        spider_name="catalog_spider",
        start_urls=["https://example.com/catalog"],
        transport="cdp",
        cdp_ws_endpoint="ws://127.0.0.1:9222",
        item_selector=".product",
        fields=[CrawlFieldSpec(name="name", css=".name")],
    )

    template = generate_spider_template(blueprint=blueprint, class_name="CatalogSpider")

    assert template.template_variant == SpiderTemplateVariant.cdp_heavy
    ast.parse(template.code)
    assert "await cdp_client.close()" in template.code
    assert "try:" in template.code
    assert "SkipNonHTMLMiddleware" in template.code
    assert "async def parse(self, response: Response):" in template.code
    assert "if not isinstance(response, HTMLResponse):" in template.code
    assert "async def _build_follow_requests" in template.code


def test_run_crawl_blueprint_closes_cdp_client(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeDefaultHTTP:
        def __init__(self) -> None:
            self.closed = False

        async def close(self) -> None:
            self.closed = True

    class FakeEngine:
        instances: list[FakeEngine] = []

        def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
            self.http = FakeDefaultHTTP()
            self.initial_http = self.http
            FakeEngine.instances.append(self)

        async def run(self) -> None:
            return None

    class FakeCDPClient:
        def __init__(self) -> None:
            self.connected = False
            self.closed = False

        async def connect(self) -> None:
            self.connected = True

        async def close(self) -> None:
            self.closed = True

    fake_client = FakeCDPClient()
    monkeypatch.setattr(mcp_server, "Engine", FakeEngine)
    monkeypatch.setattr(mcp_server, "_build_cdp_client", lambda blueprint: fake_client)

    blueprint = CrawlBlueprint(
        spider_name="catalog_spider",
        start_urls=["https://example.com/catalog"],
        transport="cdp",
        cdp_ws_endpoint="ws://127.0.0.1:9222",
        item_selector=".product",
        fields=[CrawlFieldSpec(name="name", css=".name")],
    )

    result = asyncio.run(run_crawl_blueprint(blueprint))
    engine = FakeEngine.instances[-1]

    assert result.items == []
    assert engine.initial_http.closed is True
    assert fake_client.connected is True
    assert fake_client.closed is True


def test_run_crawl_blueprint_reports_inferred_variant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeDefaultHTTP:
        async def close(self) -> None:
            return None

    class FakeEngine:
        def __init__(self, spider, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
            self.spider = spider
            self.http = FakeDefaultHTTP()

        async def run(self) -> None:
            return None

    monkeypatch.setattr(mcp_server, "Engine", FakeEngine)

    blueprint = CrawlBlueprint(
        spider_name="catalog_spider",
        start_urls=["https://example.com/catalog"],
        follow_links_selector="a.detail",
        fields=[CrawlFieldSpec(name="name", css="h1")],
    )

    result = asyncio.run(run_crawl_blueprint(blueprint))

    assert result.execution_variant == SpiderTemplateVariant.list_detail


def test_run_crawl_blueprint_sitemap_variant_uses_xml_start_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeDefaultHTTP:
        async def close(self) -> None:
            return None

    class FakeEngine:
        instances: list[FakeEngine] = []

        def __init__(self, spider, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
            self.spider = spider
            self.http = FakeDefaultHTTP()
            FakeEngine.instances.append(self)

        async def run(self) -> None:
            return None

    monkeypatch.setattr(mcp_server, "Engine", FakeEngine)

    blueprint = CrawlBlueprint(
        spider_name="sitemap_spider",
        start_urls=["https://example.com/sitemap.xml"],
        fields=[CrawlFieldSpec(name="title", css="title")],
    )

    result = asyncio.run(run_crawl_blueprint(blueprint))
    spider = FakeEngine.instances[-1].spider
    first_request = asyncio.run(anext(spider.start_requests()))

    assert result.execution_variant == SpiderTemplateVariant.sitemap_xml
    assert first_request.callback == spider.parse_sitemap
    assert first_request.meta["allow_non_html"] is True
    assert first_request.dont_filter is True


def test_run_crawl_blueprint_list_detail_variant_schedules_detail_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeDefaultHTTP:
        async def close(self) -> None:
            return None

    class FakeEngine:
        instances: list[FakeEngine] = []

        def __init__(self, spider, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
            self.spider = spider
            self.http = FakeDefaultHTTP()
            FakeEngine.instances.append(self)

        async def run(self) -> None:
            return None

    monkeypatch.setattr(mcp_server, "Engine", FakeEngine)

    blueprint = CrawlBlueprint(
        spider_name="catalog_spider",
        start_urls=["https://example.com/catalog"],
        follow_links_selector="a.detail",
        pagination_selector="a.next",
        fields=[CrawlFieldSpec(name="name", css="h1")],
    )

    asyncio.run(
        run_crawl_blueprint(blueprint, variant=SpiderTemplateVariant.list_detail)
    )
    spider = FakeEngine.instances[-1].spider
    response = HTMLResponse(
        url="https://example.com/catalog",
        status=200,
        headers={"content-type": "text/html"},
        body=SAMPLE_HTML.encode(),
        request=Request(url="https://example.com/catalog", callback=spider.parse),
    )

    scheduled = asyncio.run(_collect_requests(spider.parse(response)))

    assert [request.callback for request in scheduled[:2]] == [
        spider.parse_detail,
        spider.parse_detail,
    ]
    assert scheduled[0].meta["listing_url"] == "https://example.com/catalog"
    assert scheduled[-1].callback == spider.parse


def test_silkworm_playbook_surfaces_framework_patterns() -> None:
    playbook = silkworm_playbook()

    assert "response.follow(...)" in playbook
    assert "SkipNonHTMLMiddleware" in playbook
    assert 'meta={"allow_non_html": True}' in playbook


def test_template_variants_reference_explains_auto_and_overrides() -> None:
    reference = template_variants_reference()

    assert 'variant="auto"' in reference
    assert 'variant="list_only"' in reference
    assert 'variant="list_detail"' in reference
    assert 'variant="sitemap_xml"' in reference
    assert 'variant="cdp_heavy"' in reference
    assert 'transport="cdp"' in reference


def test_generate_spider_template_infers_list_detail_variant() -> None:
    blueprint = CrawlBlueprint(
        spider_name="catalog_spider",
        start_urls=["https://example.com/catalog"],
        follow_links_selector="a.detail",
        pagination_selector="a.next",
        fields=[CrawlFieldSpec(name="name", css="h1")],
    )

    template = generate_spider_template(blueprint=blueprint, class_name="CatalogSpider")

    assert template.template_variant == SpiderTemplateVariant.list_detail
    ast.parse(template.code)
    assert "async def parse_detail(self, response: Response):" in template.code
    assert 'meta={"listing_url": response.url}' in template.code
    assert "callback=self.parse_detail" in template.code


def test_generate_spider_template_supports_explicit_sitemap_variant() -> None:
    blueprint = CrawlBlueprint(
        spider_name="sitemap_spider",
        start_urls=["https://example.com/start"],
        fields=[CrawlFieldSpec(name="title", css="title")],
    )

    template = generate_spider_template(
        blueprint=blueprint,
        class_name="SitemapSpider",
        variant=SpiderTemplateVariant.sitemap_xml,
    )

    assert template.template_variant == SpiderTemplateVariant.sitemap_xml
    ast.parse(template.code)
    assert "import xml.etree.ElementTree as ET" in template.code
    assert "callback=self.parse_sitemap" in template.code
    assert 'meta={"allow_non_html": True}' in template.code
    assert "async def parse_page(self, response: Response):" in template.code


def test_generate_spider_template_infers_list_only_variant() -> None:
    blueprint = CrawlBlueprint(
        spider_name="catalog_spider",
        start_urls=["https://example.com/catalog"],
        item_selector=".product",
        pagination_selector="a.next",
        fields=[CrawlFieldSpec(name="name", css=".name")],
    )

    template = generate_spider_template(blueprint=blueprint, class_name="CatalogSpider")

    assert template.template_variant == SpiderTemplateVariant.list_only
    ast.parse(template.code)
    assert "async def _build_pagination_requests" in template.code
    assert "async def parse_detail" not in template.code


def test_crawl_blueprint_coerces_delay_strings_to_floats() -> None:
    fixed_delay_blueprint = CrawlBlueprint.model_validate(
        {
            "spider_name": "catalog_spider",
            "start_urls": ["https://example.com/catalog"],
            "item_selector": ".product",
            "fields": [{"name": "name", "css": ".name"}],
            "delay_seconds": "1.0",
        }
    )
    randomized_delay_blueprint = CrawlBlueprint.model_validate(
        {
            "spider_name": "catalog_spider_randomized",
            "start_urls": ["https://example.com/catalog"],
            "item_selector": ".product",
            "fields": [{"name": "name", "css": ".name"}],
            "delay_min_seconds": "0.5",
            "delay_max_seconds": "1.5",
        }
    )

    assert fixed_delay_blueprint.delay_seconds == 1.0
    assert randomized_delay_blueprint.delay_min_seconds == 0.5
    assert randomized_delay_blueprint.delay_max_seconds == 1.5


def test_crawl_blueprint_schema_accepts_string_delay_values() -> None:
    schema = CrawlBlueprint.model_json_schema()
    delay_seconds_schema = schema["properties"]["delay_seconds"]
    accepted_types = set()
    for entry in delay_seconds_schema["anyOf"]:
        if "type" in entry:
            accepted_types.add(entry["type"])
        for nested_entry in entry.get("anyOf", []):
            accepted_types.add(nested_entry["type"])

    assert accepted_types == {"number", "string", "null"}
