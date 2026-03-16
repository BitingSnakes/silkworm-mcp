# silkworm-mcp

`silkworm-mcp` is a full-featured MCP server for building scrapers with:

- `silkworm-rs`: async crawling, fetching, follow links, and spider execution
- `scraper-rs`: fast Rust-backed HTML parsing with CSS and XPath selectors

It is designed for LLM-assisted scraper development, so the server exposes both low-level page inspection tools and higher-level workflow helpers for validating selector plans and generating starter spider code.

## Features

- Fetch pages through silkworm's regular HTTP client or CDP renderer.
- Cache HTML in-memory and reuse it via `document_handle`.
- Inspect pages with summaries, prettified HTML, CSS/XPath queries, selector comparisons, and link extraction.
- Run ad hoc crawls from a structured `CrawlBlueprint`.
- Generate reusable silkworm spider templates from the same blueprint.
- Publish MCP resources and prompts so clients can discover workflows and schemas.

## Tools

- `store_html_document`
- `list_documents`
- `delete_document`
- `clear_documents`
- `inspect_document`
- `prettify_document`
- `query_selector`
- `compare_selectors`
- `extract_links`
- `silkworm_fetch`
- `silkworm_fetch_cdp`
- `run_crawl_blueprint`
- `generate_spider_template`

## Run

Install dependencies:

```bash
uv sync
```

Run over stdio for a desktop MCP client:

```bash
uv run python mcp_server.py --transport stdio
```

Run over HTTP:

```bash
uv run python mcp_server.py --transport http --host 127.0.0.1 --port 8000
```

The project also exposes a console entrypoint:

```bash
uv run silkworm-mcp --transport stdio
```

## Example Workflow

1. Call `silkworm_fetch` for the target page.
2. Use the returned `document_handle` with `inspect_document`.
3. Iterate on `query_selector` and `compare_selectors`.
4. Use `extract_links` to verify pagination or detail pages.
5. Feed the stable plan into `run_crawl_blueprint`.
6. Convert the same blueprint into code with `generate_spider_template`.

## Blueprint Example

```json
{
  "spider_name": "products_spider",
  "start_urls": ["https://example.com/products"],
  "item_selector": ".product-card",
  "pagination_selector": "a.next",
  "fields": [
    {
      "name": "title",
      "css": ".title",
      "extractor": "text"
    },
    {
      "name": "price",
      "css": ".price",
      "extractor": "text"
    },
    {
      "name": "detail_url",
      "css": "a",
      "extractor": "attr",
      "attr_name": "href",
      "absolute_url": true
    }
  ]
}
```

## Local Demo

`mcp_test.py` is a transport-free smoke test that exercises the exported server functions and schema models:

- document storage
- selector querying
- spider template generation

Run it with:

```bash
uv run python mcp_test.py
```
