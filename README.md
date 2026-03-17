# silkworm-mcp

`silkworm-mcp` is a full-featured MCP server for building scrapers with:

- `silkworm-rs`: async crawling, fetching, follow links, and spider execution
- `scraper-rs`: fast Rust-backed HTML parsing with CSS and XPath selectors

It is designed for LLM-assisted scraper development, so the server exposes both low-level page inspection tools and higher-level workflow helpers for validating selector plans and generating starter spider code.

## Features

- Fetch pages through silkworm's regular HTTP client or CDP renderer.
- Query selectors directly against a CDP-rendered DOM snapshot.
- Extract structured records from live rendered pages before committing to a full crawl.
- Cache HTML in-memory and reuse it via `document_handle`.
- Bound the in-memory cache with max-document, max-bytes, and idle-TTL controls.
- Inspect pages with summaries, prettified HTML, CSS/XPath queries, selector comparisons, and link extraction.
- Run ad hoc crawls from a structured `CrawlBlueprint`.
- Generate reusable silkworm spider templates from the same blueprint and statically validate them.
- Expose MCP diagnostics plus HTTP `/healthz` and `/readyz` routes for production monitoring.
- Publish MCP resources and prompts so clients can discover workflows, Silkworm idioms, and blueprint schemas.

## Tools

- `store_html_document`
- `list_documents`
- `delete_document`
- `clear_documents`
- `server_status`
- `inspect_document`
- `prettify_document`
- `query_selector`
- `compare_selectors`
- `extract_links`
- `silkworm_fetch`
- `silkworm_fetch_cdp`
- `query_selector_cdp`
- `extract_structured_data_cdp`
- `run_crawl_blueprint`
- `generate_spider_template`
- `validate_spider_code`

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

HTTP deployments also expose:

- `GET /healthz`: process liveness
- `GET /readyz`: readiness, optionally including a CDP browser probe

The project also exposes a console entrypoint:

```bash
uv run silkworm-mcp --transport stdio
```

Key runtime environment variables:

- `SILKWORM_MCP_DOCUMENT_MAX_COUNT`
- `SILKWORM_MCP_DOCUMENT_MAX_TOTAL_BYTES`
- `SILKWORM_MCP_DOCUMENT_TTL_SECONDS`
- `SILKWORM_MCP_LOG_LEVEL`
- `SILKWORM_MCP_READINESS_REQUIRE_CDP`
- `SILKWORM_MCP_READINESS_CDP_WS_ENDPOINT`

## Example Workflow

1. Call `silkworm_fetch` for the target page.
2. Use the returned `document_handle` with `inspect_document`.
3. Iterate on `query_selector` and `compare_selectors`.
4. For JS-heavy pages, use `query_selector_cdp` or `extract_structured_data_cdp` against the rendered DOM.
5. Use `extract_links` to verify pagination or detail pages.
6. Feed the stable plan into `run_crawl_blueprint`.
7. Convert the same blueprint into code with `generate_spider_template`, then check it with `validate_spider_code`.

Useful built-in MCP references:

- `silkworm://reference/overview`
- `silkworm://reference/silkworm-cheatsheet`
- `silkworm://reference/silkworm-playbook`
- `silkworm://reference/scraper-rs-cheatsheet`
- `silkworm://reference/crawl-blueprint-schema`

## Blueprint Example

```json
{
  "spider_name": "products_spider",
  "start_urls": ["https://example.com/products"],
  "transport": "cdp",
  "cdp_ws_endpoint": "ws://127.0.0.1:9222",
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

Use `transport: "cdp"` when pages require JavaScript rendering. `run_crawl_blueprint` will connect to the configured CDP endpoint, and `generate_spider_template` will emit a starter spider that runs through `CDPClient` instead of the default HTTP client.

## Local Demo

`mcp_test.py` is a transport-free smoke test that exercises the exported server functions and schema models:

- document storage
- selector querying
- spider template generation

Run it with:

```bash
uv run python mcp_test.py
```

## Testing

Run the automated test suite with:

```bash
uv run --group dev pytest
```

## Production Notes

- Use `server_status` or the `silkworm://status` resource to inspect cache usage, uptime, and runtime configuration.
- The document cache is bounded by count, total bytes, and idle TTL so long-running HTTP deployments do not grow unbounded.
- `compose.yml` publishes only the MCP HTTP port; the Lightpanda CDP port stays internal to the container.
- The generated CDP spider template and `run_crawl_blueprint` now close CDP clients explicitly on shutdown.
