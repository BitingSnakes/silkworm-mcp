from __future__ import annotations

import asyncio

import scraper_rs

from mcp_server import (
    CrawlBlueprint,
    CrawlFieldSpec,
    _extract_item,
    _extract_static_item,
    generate_spider_template,
    list_documents,
    query_selector,
    store_html_document,
    validate_spider_code,
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


class _FakeScope:
    async def select(self, query: str):
        return []

    async def select_first(self, query: str):
        return None

    async def xpath(self, query: str):
        return []

    async def xpath_first(self, query: str):
        return None


def main() -> None:
    summary = store_html_document(
        html=SAMPLE_HTML,
        source_url="https://example.com/catalog",
        label="catalog-demo",
    )
    print("stored handle:", summary.handle)
    print("title:", summary.title)
    print("document count:", len(list_documents()))

    query = query_selector(
        document_handle=summary.handle,
        query=".product .name",
        mode="css",
    )
    print("selector matches:", query.total_matches)
    print("first match:", query.matches[0].text)

    blueprint = CrawlBlueprint(
        spider_name="catalog_spider",
        start_urls=["https://example.com/catalog"],
        transport="cdp",
        cdp_ws_endpoint="ws://127.0.0.1:9222",
        cdp_timeout_seconds=30.0,
        default_user_agent="catalog-bot/1.0",
        delay_min_seconds=0.25,
        delay_max_seconds=0.75,
        retry_max_times=5,
        sleep_http_codes=[403, 429],
        output_jsonl_path="data/catalog.jl",
        item_selector=".product",
        pagination_selector="a.next",
        follow_links_selector="a.detail",
        max_requests=10,
        max_items=25,
        fields=[
            CrawlFieldSpec(name="name", css=".name"),
            CrawlFieldSpec(name="price", css=".price"),
            CrawlFieldSpec(
                name="detail_url",
                css="a.detail",
                extractor="attr",
                attr_name="href",
                absolute_url=True,
            ),
        ],
    )
    template = generate_spider_template(
        blueprint=blueprint,
        class_name="CatalogSpider",
    )
    print("template class:", template.class_name)
    print("template preview:", template.code.splitlines()[0])
    assert "run_spider_uvloop" in template.code
    assert "UserAgentMiddleware" in template.code
    assert "RetryMiddleware" in template.code
    assert 'output_jsonl_path' in template.code
    assert 'BLUEPRINT.get("transport") == "cdp"' in template.code
    assert "CDPClient(" in template.code
    validation = validate_spider_code(
        template.code,
        expected_class_name="CatalogSpider",
    )
    print("template validation syntax:", validation.syntax_ok)
    print("template validation classes:", validation.spider_classes)
    assert validation.syntax_ok is True
    assert validation.uses_cdp_client is True
    assert validation.uses_run_spider_uvloop is True
    assert validation.issues == []

    document = scraper_rs.Document(SAMPLE_HTML)
    try:
        first_product = document.select_first(".product")
        assert first_product is not None
        static_item = _extract_static_item(
            first_product,
            blueprint.fields,
            base_url="https://example.com/catalog",
        )
    finally:
        document.close()
    print("static detail_url:", static_item["detail_url"])
    assert static_item["name"] == "Widget A"
    assert static_item["detail_url"] == "https://example.com/items/1"

    synthetic_source_blueprint = CrawlBlueprint(
        start_urls=["https://example.com/catalog"],
        item_selector=".product",
        include_source_url=False,
        fields=[
            CrawlFieldSpec(name="name", css=".name"),
            CrawlFieldSpec(name="source_url", default=None),
        ],
    )
    extracted = asyncio.run(
        _extract_item(
            _FakeScope(),
            synthetic_source_blueprint.fields,
            base_url="https://example.com/catalog",
        )
    )
    print("synthetic source_url:", extracted["source_url"])

    try:
        CrawlBlueprint(
            start_urls=["https://example.com/catalog"],
            fields=[CrawlFieldSpec(name="name", css=".name")],
            delay_seconds=1.0,
            delay_min_seconds=0.1,
            delay_max_seconds=0.3,
        )
    except ValueError:
        print("delay validation: ok")
    else:
        raise AssertionError("Expected mixed delay configuration to fail validation")


if __name__ == "__main__":
    main()
