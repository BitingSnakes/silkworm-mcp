from __future__ import annotations

import asyncio
import json
import os
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from fastmcp import Client
from fastmcp.client import PythonStdioTransport

ROOT = Path(__file__).resolve().parents[1]
EXPECTED_TOOL_NAMES = {
    "store_html_document",
    "list_documents",
    "delete_document",
    "clear_documents",
    "server_status",
    "generate_regex",
    "inspect_document",
    "prettify_document",
    "parse_html_document",
    "parse_html_fragment",
    "query_selector",
    "compare_selectors",
    "find_selectors_by_text",
    "extract_links",
    "silkworm_fetch",
    "silkworm_fetch_cdp",
    "query_selector_cdp",
    "extract_structured_data_cdp",
    "validate_spider_code",
    "run_crawl_blueprint",
    "generate_spider_template",
}
SAMPLE_HTML = """
<html>
  <body>
    <section class="products">
      <article class="product">
        <a class="detail" href="/items/1"><h2 class="name">Widget A</h2></a>
        <span class="price">$10</span>
      </article>
      <article class="product">
        <a class="detail" href="/items/2"><h2 class="name">Widget B</h2></a>
        <span class="price">$20</span>
      </article>
    </section>
    <a class="next" href="/page/2">Next</a>
  </body>
</html>
""".strip()


class _FixtureHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        pages = {
            "/catalog": """
                <html>
                  <body>
                    <section class="products">
                      <article class="product">
                        <a class="detail" href="/items/1"><h2 class="name">Widget A</h2></a>
                        <span class="price">$10</span>
                      </article>
                      <article class="product">
                        <a class="detail" href="/items/2"><h2 class="name">Widget B</h2></a>
                        <span class="price">$20</span>
                      </article>
                    </section>
                    <a class="next" href="/page/2">Next</a>
                  </body>
                </html>
            """,
            "/page/2": """
                <html>
                  <body>
                    <section class="products">
                      <article class="product">
                        <a class="detail" href="/items/3"><h2 class="name">Widget C</h2></a>
                        <span class="price">$30</span>
                      </article>
                    </section>
                  </body>
                </html>
            """,
            "/items/1": """
                <html>
                  <body>
                    <article class="detail-page">
                      <h1 class="name">Widget A</h1>
                      <span class="price">$10</span>
                      <p class="description">Alpha widget</p>
                    </article>
                  </body>
                </html>
            """,
            "/items/2": """
                <html>
                  <body>
                    <article class="detail-page">
                      <h1 class="name">Widget B</h1>
                      <span class="price">$20</span>
                      <p class="description">Beta widget</p>
                    </article>
                  </body>
                </html>
            """,
            "/items/3": """
                <html>
                  <body>
                    <article class="detail-page">
                      <h1 class="name">Widget C</h1>
                      <span class="price">$30</span>
                      <p class="description">Gamma widget</p>
                    </article>
                  </body>
                </html>
            """,
        }
        body = pages.get(self.path)
        if body is None:
            self.send_response(404)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"not found")
            return

        encoded = body.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format: str, *args: object) -> None:
        return


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _http_json(url: str) -> tuple[int, dict[str, object]]:
    try:
        with urlopen(url, timeout=1) as response:
            payload = json.loads(response.read().decode("utf-8"))
            return int(response.status), payload
    except HTTPError as exc:
        payload = json.loads(exc.read().decode("utf-8"))
        return int(exc.code), payload


def _wait_for_http_json(
    url: str,
    *,
    timeout_seconds: float = 20.0,
) -> tuple[int, dict[str, object]]:
    deadline = time.time() + timeout_seconds
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            return _http_json(url)
        except (URLError, ConnectionError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            time.sleep(0.2)
    raise AssertionError(f"Timed out waiting for {url}: {last_error}")


@contextmanager
def _run_server(*args: str, env: dict[str, str]) -> subprocess.Popen[str]:
    process = subprocess.Popen(
        [sys.executable, "mcp_server.py", *args],
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        yield process
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


@contextmanager
def _fixture_site() -> tuple[str, int]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _FixtureHandler)
    port = int(server.server_address[1])
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}", port
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _write_fake_cdp_sitecustomize(target_dir: Path) -> None:
    (target_dir / "sitecustomize.py").write_text(
        """
from urllib.request import Request as UrlRequest, urlopen

from silkworm import HTMLResponse, Request
import silkworm.cdp


class FakeCDPClient:
    def __init__(
        self,
        ws_endpoint: str,
        concurrency: int | None = None,
        timeout: float | None = None,
        html_max_size_bytes: int = 5000000,
    ) -> None:
        self.ws_endpoint = ws_endpoint
        self.concurrency = concurrency
        self.timeout = timeout
        self.html_max_size_bytes = html_max_size_bytes

    async def connect(self) -> None:
        return

    async def close(self) -> None:
        return

    async def fetch(self, request: Request) -> HTMLResponse:
        upstream = UrlRequest(
            request.url,
            headers={str(key): str(value) for key, value in request.headers.items()},
            method=request.method,
        )
        with urlopen(upstream, timeout=self.timeout or 20.0) as response:
            body = response.read()
            headers = {key: value for key, value in response.headers.items()}
            return HTMLResponse(
                url=response.geturl(),
                status=response.status,
                headers=headers,
                body=body,
                request=request,
                doc_max_size_bytes=self.html_max_size_bytes,
            )


silkworm.cdp.CDPClient = FakeCDPClient
""".strip()
        + "\n",
        encoding="utf-8",
    )


def test_stdio_transport_covers_every_tool(tmp_path: Path) -> None:
    with _fixture_site() as (base_url, _port):
        python_path_dir = tmp_path / "pythonpath"
        python_path_dir.mkdir()
        _write_fake_cdp_sitecustomize(python_path_dir)

        async def exercise() -> None:
            transport = PythonStdioTransport(
                script_path=ROOT / "mcp_server.py",
                args=["--transport", "stdio"],
                cwd=str(ROOT),
                env={
                    **os.environ,
                    "PYTHONPATH": str(python_path_dir)
                    + os.pathsep
                    + os.environ.get("PYTHONPATH", ""),
                    "SILKWORM_MCP_DOCUMENT_STORE_PATH": str(
                        tmp_path / "stdio-documents.sqlite3"
                    ),
                    "SILKWORM_MCP_READINESS_REQUIRE_CDP": "false",
                },
                keep_alive=False,
                log_file=tmp_path / "stdio-server.log",
            )

            async with Client(transport) as client:
                tools = await client.list_tools()
                tool_names = {tool.name for tool in tools}
                assert tool_names == EXPECTED_TOOL_NAMES

                stored = await client.call_tool(
                    "store_html_document",
                    {
                        "html": SAMPLE_HTML,
                        "source_url": f"{base_url}/catalog",
                        "label": "catalog",
                    },
                )
                stored_handle = stored.structured_content["handle"]
                assert stored.structured_content["heading_preview"] == [
                    "Widget A",
                    "Widget B",
                ]

                listed = await client.call_tool("list_documents", {})
                assert listed.structured_content["result"][0]["handle"] == stored_handle

                inspected = await client.call_tool(
                    "inspect_document",
                    {"document_handle": stored_handle},
                )
                assert inspected.structured_content["link_count"] == 3

                prettified = await client.call_tool(
                    "prettify_document",
                    {"document_handle": stored_handle},
                )
                assert (
                    '<section class="products">'
                    in prettified.structured_content["prettified_html"]
                )

                parsed_document = await client.call_tool(
                    "parse_html_document",
                    {"document_handle": stored_handle},
                )
                assert parsed_document.structured_content["parse_mode"] == "document"
                assert (
                    parsed_document.structured_content["root"]["node_type"]
                    == "document"
                )

                parsed_fragment = await client.call_tool(
                    "parse_html_fragment",
                    {"html": "<li><span>Fragment</span></li>"},
                )
                assert parsed_fragment.structured_content["parse_mode"] == "fragment"
                assert (
                    parsed_fragment.structured_content["root"]["children"][0]["tag"]
                    == "li"
                )

                queried = await client.call_tool(
                    "query_selector",
                    {
                        "document_handle": stored_handle,
                        "query": ".product .name",
                        "mode": "css",
                    },
                )
                assert queried.structured_content["total_matches"] == 2
                assert queried.structured_content["matches"][0]["text"] == "Widget A"

                regex = await client.call_tool(
                    "generate_regex",
                    {
                        "test_cases": ["big", "BIGGER"],
                        "case_insensitive": True,
                        "capturing_groups": True,
                    },
                )
                assert regex.structured_content["pattern"] == "(?i)^big(ger)?$"
                assert regex.structured_content["anchors_enabled"] is True

                inverse = await client.call_tool(
                    "find_selectors_by_text",
                    {
                        "document_handle": stored_handle,
                        "text_query": "Widget A",
                    },
                )
                assert inverse.structured_content["total_matches"] == 1
                assert inverse.structured_content["matches"][0]["tag"] == "h2"
                assert inverse.structured_content["matches"][0]["css"]
                assert inverse.structured_content["matches"][0]["xpath"]

                compared = await client.call_tool(
                    "compare_selectors",
                    {
                        "document_handle": stored_handle,
                        "selectors": [".product", ".product .name", "a.detail"],
                        "mode": "css",
                    },
                )
                assert (
                    compared.structured_content["comparisons"][0]["total_matches"] == 2
                )
                assert (
                    compared.structured_content["comparisons"][2]["total_matches"] == 2
                )

                links = await client.call_tool(
                    "extract_links",
                    {
                        "document_handle": stored_handle,
                        "query": "a[href]",
                        "mode": "css",
                    },
                )
                assert links.structured_content["links"][0]["absolute_url"] == (
                    f"{base_url}/items/1"
                )

                fetched = await client.call_tool(
                    "silkworm_fetch",
                    {"url": f"{base_url}/catalog", "store_document": True},
                )
                fetched_handle = fetched.structured_content["document_handle"]
                assert fetched.structured_content["via"] == "http"
                assert fetched.structured_content["summary"]["link_count"] == 3

                fetched_cdp = await client.call_tool(
                    "silkworm_fetch_cdp",
                    {
                        "url": f"{base_url}/catalog",
                        "ws_endpoint": "ws://fake-cdp.test/devtools/browser",
                        "store_document": True,
                    },
                )
                fetched_cdp_handle = fetched_cdp.structured_content["document_handle"]
                assert fetched_cdp.structured_content["via"] == "cdp"
                assert fetched_cdp.structured_content["summary"]["heading_preview"][
                    0
                ] == ("Widget A")

                queried_cdp = await client.call_tool(
                    "query_selector_cdp",
                    {
                        "url": f"{base_url}/catalog",
                        "query": ".product .price",
                        "mode": "css",
                        "ws_endpoint": "ws://fake-cdp.test/devtools/browser",
                    },
                )
                assert queried_cdp.structured_content["total_matches"] == 2
                assert queried_cdp.structured_content["matches"][1]["text"] == "$20"

                extracted_cdp = await client.call_tool(
                    "extract_structured_data_cdp",
                    {
                        "url": f"{base_url}/catalog",
                        "item_selector": ".product",
                        "ws_endpoint": "ws://fake-cdp.test/devtools/browser",
                        "fields": [
                            {"name": "name", "css": ".name"},
                            {"name": "price", "css": ".price"},
                            {
                                "name": "detail_url",
                                "css": "a.detail",
                                "extractor": "attr",
                                "attr_name": "href",
                                "absolute_url": True,
                            },
                        ],
                    },
                )
                assert extracted_cdp.structured_content["returned_items"] == 2
                assert extracted_cdp.structured_content["items"][0]["detail_url"] == (
                    f"{base_url}/items/1"
                )

                blueprint = {
                    "spider_name": "catalog_spider",
                    "start_urls": [f"{base_url}/catalog"],
                    "item_selector": ".detail-page",
                    "follow_links_selector": "a.detail",
                    "pagination_selector": "a.next",
                    "max_requests": 8,
                    "max_items": 3,
                    "fields": [
                        {"name": "name", "css": ".name"},
                        {"name": "price", "css": ".price"},
                        {"name": "description", "css": ".description"},
                    ],
                }

                generated = await client.call_tool(
                    "generate_spider_template",
                    {
                        "blueprint": blueprint,
                        "class_name": "CatalogSpider",
                    },
                )
                template_code = generated.structured_content["code"]
                assert generated.structured_content["template_variant"] == "list_detail"
                assert "class CatalogSpider(Spider):" in template_code

                validated = await client.call_tool(
                    "validate_spider_code",
                    {
                        "code": template_code,
                        "expected_class_name": "CatalogSpider",
                    },
                )
                assert validated.structured_content["syntax_ok"] is True
                assert validated.structured_content["issues"] == []

                crawled = await client.call_tool(
                    "run_crawl_blueprint",
                    {"blueprint": blueprint},
                )
                assert crawled.structured_content["execution_variant"] == "list_detail"
                assert crawled.structured_content["emitted_items"] == 3
                crawled_items = crawled.structured_content["items"]
                assert {item["name"] for item in crawled_items} == {
                    "Widget A",
                    "Widget B",
                    "Widget C",
                }
                assert all(
                    item["_listing_url"]
                    in {f"{base_url}/catalog", f"{base_url}/page/2"}
                    for item in crawled_items
                )

                deleted = await client.call_tool(
                    "delete_document",
                    {"handle": fetched_handle},
                )
                assert deleted.structured_content["deleted"] is True

                status = await client.call_tool(
                    "server_status",
                    {"include_documents": True, "probe_cdp": False},
                )
                handles = {
                    document["handle"]
                    for document in status.structured_content["documents"]
                }
                assert stored_handle in handles
                assert fetched_handle not in handles
                assert fetched_cdp_handle in handles
                assert status.structured_content["health"]["ready"] is True

                cleared = await client.call_tool("clear_documents", {})
                assert cleared.structured_content["deleted_count"] >= 2

                listed_after_clear = await client.call_tool("list_documents", {})
                assert listed_after_clear.structured_content["result"] == []

        asyncio.run(exercise())


def test_streamable_http_exposes_health_routes(tmp_path: Path) -> None:
    port = _find_free_port()
    env = {
        **os.environ,
        "SILKWORM_MCP_DOCUMENT_STORE_PATH": str(tmp_path / "http-documents.sqlite3"),
        "SILKWORM_MCP_READINESS_REQUIRE_CDP": "false",
    }

    with _run_server(
        "--transport",
        "streamable-http",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--path",
        "/mcp",
        env=env,
    ):
        health_status, health = _wait_for_http_json(f"http://127.0.0.1:{port}/healthz")
        ready_status, readiness = _wait_for_http_json(f"http://127.0.0.1:{port}/readyz")

        assert health_status == 200
        assert health["status"] == "ok"
        assert health["ready"] is True
        assert ready_status == 200
        assert readiness["ready"] is True
        assert readiness["cdp_required"] is False
