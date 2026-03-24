# silkworm-mcp

This is a full-featured MCP server for building scrapers with:

- [silkworm-rs](https://github.com/BitingSnakes/silkworm): async crawling, fetching, follow links, and spider execution
- [scraper-rs](https://github.com/RustedBytes/scraper-rs): fast Rust-backed HTML parsing with CSS and XPath selectors

It is designed for LLM-assisted scraper development, so the server exposes both low-level page inspection tools and higher-level workflow helpers for validating selector plans and generating starter spider code.

An example: https://github.com/BitingSnakes/silkworm-example

## Features

- Fetch pages through silkworm's regular HTTP client or CDP renderer.
- Query selectors directly against a CDP-rendered DOM snapshot.
- Extract structured records from live rendered pages before committing to a full crawl.
- Cache HTML in a local document store and reuse it via `document_handle`.
- Bound the document cache with max-document, max-bytes, and idle-TTL controls.
- Inspect pages with summaries, parsed DOM trees, prettified HTML, CSS/XPath queries, selector comparisons, and link extraction.
- Run ad hoc crawls from a structured `CrawlBlueprint`.
- Generate reusable silkworm spider templates from the same blueprint and statically validate them, including pattern-specific variants for list-only, list+detail, sitemap/XML, and CDP-heavy crawls.
- Expose MCP diagnostics plus HTTP `/healthz` and `/readyz` routes for production monitoring.
- Publish MCP resources and prompts so clients can discover workflows, Silkworm idioms, and blueprint schemas.

## Tools

- `store_html_document`
- `list_documents`
- `delete_document`
- `clear_documents`
- `server_status`
- `inspect_document`
- `parse_html_document`
- `parse_html_fragment`
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

## Docker

Build the image:

```bash
docker build -t silkworm-mcp .
```

Run the container over HTTP on port `8000`:

```bash
docker run --rm -it -p 8000:8000 silkworm-mcp
```

The container entrypoint starts two processes by default:

- the MCP server over HTTP on `0.0.0.0:8000`
- a bundled Lightpanda browser on `127.0.0.1:9222` for CDP-backed tools such as `silkworm_fetch_cdp`, `query_selector_cdp`, and `extract_structured_data_cdp`

Useful container environment variables:

- `MCP_TRANSPORT` (default: `http`)
- `MCP_HOST` (default: `0.0.0.0`)
- `MCP_PORT` (default: `8000`)
- `MCP_PATH`
- `LIGHTPANDA_ENABLED` (default: `1`)
- `LIGHTPANDA_HOST` (default: `127.0.0.1`)
- `LIGHTPANDA_PORT` (default: `9222`)
- `LIGHTPANDA_ADVERTISE_HOST` (default: unset, falls back to `LIGHTPANDA_HOST`)
- `LIGHTPANDA_LOG_FORMAT` (default: `pretty`)
- `LIGHTPANDA_LOG_LEVEL` (default: `info`)

When Lightpanda binds to `0.0.0.0` inside a container, set `LIGHTPANDA_ADVERTISE_HOST` to a reachable hostname such as the container DNS name. Otherwise `/json/version` can advertise `ws://0.0.0.0:9222/`, which remote CDP clients cannot use.

Example with custom document-cache limits:

```bash
docker run --rm -it \
  -p 8000:8000 \
  -e SILKWORM_MCP_DOCUMENT_MAX_COUNT=256 \
  -e SILKWORM_MCP_DOCUMENT_MAX_TOTAL_BYTES=64000000 \
  -e SILKWORM_MCP_DOCUMENT_TTL_SECONDS=7200 \
  silkworm-mcp
```

For local development, `compose.yml` provides the same setup with health checks and restart policy:

```bash
docker compose up --build
```

Then verify the container is ready:

```bash
curl http://127.0.0.1:8000/readyz
```

Key runtime environment variables:

- `SILKWORM_MCP_DOCUMENT_MAX_COUNT`
- `SILKWORM_MCP_DOCUMENT_MAX_TOTAL_BYTES`
- `SILKWORM_MCP_DOCUMENT_TTL_SECONDS`
- `SILKWORM_MCP_DOCUMENT_STORE_PATH`
- `SILKWORM_MCP_LOG_LEVEL`
- `SILKWORM_MCP_READINESS_REQUIRE_CDP`
- `SILKWORM_MCP_READINESS_CDP_WS_ENDPOINT`

## Example Workflow

1. Call `silkworm_fetch` for the target page.
2. Use the returned `document_handle` with `inspect_document`.
3. Use `parse_html_document` or `parse_html_fragment` when you need exact parser structure, node types, or parser errors.
4. Iterate on `query_selector` and `compare_selectors`.
5. For JS-heavy pages, use `query_selector_cdp` or `extract_structured_data_cdp` against the rendered DOM.
6. Use `extract_links` to verify pagination or detail pages.
7. Feed the stable plan into `run_crawl_blueprint`.
8. Convert the same blueprint into code with `generate_spider_template`, then check it with `validate_spider_code`.

Useful built-in MCP references:

- `silkworm://reference/overview`
- `silkworm://reference/silkworm-cheatsheet`
- `silkworm://reference/silkworm-playbook`
- `silkworm://reference/template-variants`
- `silkworm://reference/scraper-rs-cheatsheet`
- `silkworm://reference/crawl-blueprint-schema`

Use `transport: "cdp"` when pages require JavaScript rendering. `run_crawl_blueprint` will connect to the configured CDP endpoint, and `generate_spider_template` will emit a starter spider that runs through `CDPClient` instead of the default HTTP client.

Both `run_crawl_blueprint` and `generate_spider_template` accept a `variant` override. When omitted, they infer a crawl style from the blueprint:

- `list_only`: listing pages emit items directly, with optional pagination
- `list_detail`: listing pages schedule detail requests and a separate `parse_detail`
- `sitemap_xml`: sitemap/XML entrypoints are fetched with `meta={"allow_non_html": True}` and parsed before scheduling page requests
- `cdp_heavy`: rendered-page crawls keep the CDP execution path and a general-purpose parse/follow flow

`run_crawl_blueprint` returns the resolved `execution_variant`, and `generate_spider_template` returns the resolved `template_variant`, so clients can see which crawl shape was actually used.

## Testing

Run the automated test suite with:

```bash
just test
```

## Acknowledgement

This project builds on the excellent work behind [FastMCP](https://github.com/jlowin/fastmcp), [silkworm-rs](https://github.com/BitingSnakes/silkworm), and [scraper-rs](https://github.com/RustedBytes/scraper-rs). Together they provide the MCP server framework, crawling runtime, and HTML parsing foundations that make this project possible.
