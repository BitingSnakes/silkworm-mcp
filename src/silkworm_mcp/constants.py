from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from importlib import metadata
from pathlib import Path
from textwrap import dedent
import tomllib

SERVER_NAME = "silkworm-mcp"
DEFAULT_HTML_MAX_SIZE_BYTES = 5_000_000
DEFAULT_TEXT_PREVIEW_CHARS = 1_200
DEFAULT_HTML_PREVIEW_CHARS = 1_500
DEFAULT_DOCUMENT_MAX_COUNT = 128
DEFAULT_DOCUMENT_MAX_TOTAL_BYTES = 32_000_000
DEFAULT_DOCUMENT_TTL_SECONDS = 3_600.0
DEFAULT_DOCUMENT_STORE_PATH = "/tmp/silkworm-mcp-documents.sqlite3"
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_HTTP_HEALTH_PATH = "/healthz"
DEFAULT_HTTP_READY_PATH = "/readyz"
DEFAULT_READINESS_PROBE_TIMEOUT_SECONDS = 5.0
VALID_LOG_LEVELS = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}
PROCESS_STARTED_AT = datetime.now(timezone.utc)


def _load_server_version() -> str:
    pyproject_path = Path(__file__).resolve().parents[2] / "pyproject.toml"
    if pyproject_path.exists():
        with pyproject_path.open("rb") as pyproject_file:
            pyproject_data = tomllib.load(pyproject_file)
        return str(pyproject_data["project"]["version"])

    return metadata.version(SERVER_NAME)


SERVER_VERSION = _load_server_version()

SERVER_INSTRUCTIONS = dedent(
    """
    Use this server to design, debug, and validate scrapers built with silkworm and scraper-rs.
    Treat it as an interactive scraper workbench: fetch pages, inspect structure, test selectors,
    prove the crawl shape, and only then generate or validate spider code.

    Core capabilities:
    - Fetch pages over normal HTTP with `silkworm_fetch` or a rendered browser session with `silkworm_fetch_cdp`.
    - Cache fetched HTML in the document store and reuse it through `document_handle`.
    - Inspect DOM structure, parse HTML into node trees, test CSS/XPath selectors, compare alternatives,
      prettify markup, and extract links before committing to crawl logic.
    - Run ad hoc crawls from a `CrawlBlueprint`, generate starter spider code, and validate generated code.

    Recommended workflow:
    1. Fetch one representative page with `silkworm_fetch`.
    2. Switch to `silkworm_fetch_cdp` only if important content is missing, delayed, or clearly render-dependent.
    3. Reuse the returned `document_handle` instead of resending raw HTML.
    4. Inspect the page with `inspect_document`, then use `parse_html_document` or `parse_html_fragment` when parser-level structure matters.
    5. Refine extraction with `query_selector`, `compare_selectors`, `extract_links`, and `prettify_document`.
    6. Validate pagination and detail-link behavior on a small sample before scaling the crawl.
    7. Once the extraction plan is stable, run `run_crawl_blueprint`, then use `generate_spider_template` and `validate_spider_code`.

    Operating guidance:
    - Prefer `document_handle`-based workflows to reduce token usage and avoid repeating large inputs.
    - Use CSS selectors by default; switch to XPath only when structure or text relationships make it clearly better.
    - Keep selectors relative to the item container whenever possible.
    - Prefer list-page extraction first; only follow detail links when key fields are missing from listings.
    - Treat silkworm as an async Spider/Request/Response framework where `parse()` yields items and follow-up `Request`s.
    - Guard selector-heavy parsing with `isinstance(response, HTMLResponse)` unless the crawl intentionally accepts XML, JSON, or binary responses.
    - Prefer `response.follow(...)` for pagination and detail links so callbacks, relative URLs, and headers stay consistent.
    - Choose middleware deliberately: `UserAgentMiddleware` by default, `RetryMiddleware` for transient failures, `DelayMiddleware` for rate-sensitive targets, and `SkipNonHTMLMiddleware` for normal HTML crawls.
    - Keep generated spiders production-oriented: bounded `max_requests`, bounded `max_items`, explicit timeouts, and optional JSON Lines output.
    - For repeated fields use `all_matches=true`; for links use `extractor="attr"`, `attr_name="href"`, and usually `absolute_url=true`.
    - Include `_source_url` on items unless there is a strong reason not to, because it simplifies crawl debugging and downstream validation.
    """
).strip()


class SelectorMode(str, Enum):
    css = "css"
    xpath = "xpath"


class FieldExtractor(str, Enum):
    text = "text"
    html = "html"
    attr = "attr"


class CrawlTransport(str, Enum):
    http = "http"
    cdp = "cdp"


class SpiderTemplateVariant(str, Enum):
    auto = "auto"
    list_only = "list_only"
    list_detail = "list_detail"
    sitemap_xml = "sitemap_xml"
    cdp_heavy = "cdp_heavy"
