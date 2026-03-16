from __future__ import annotations

from mcp_server import (
    CrawlBlueprint,
    CrawlFieldSpec,
    generate_spider_template,
    list_documents,
    query_selector,
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


if __name__ == "__main__":
    main()
