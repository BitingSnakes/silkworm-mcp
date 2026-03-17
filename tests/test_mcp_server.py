from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import mcp_server
from mcp_server import (
    CrawlBlueprint,
    CrawlFieldSpec,
    DocumentStore,
    SelectorMode,
    clear_documents,
    generate_spider_template,
    list_documents,
    query_selector,
    run_crawl_blueprint,
    server_status,
    silkworm_fetch,
    store_html_document,
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


def test_server_status_reports_runtime_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
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


def test_silkworm_fetch_rejects_non_http_urls() -> None:
    with pytest.raises(ValueError, match="absolute URL"):
        asyncio.run(silkworm_fetch("file:///tmp/not-allowed.html"))


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

    assert "await cdp_client.close()" in template.code
    assert "try:" in template.code


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
