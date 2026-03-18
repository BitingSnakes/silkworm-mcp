from __future__ import annotations

import logging

from fastmcp import FastMCP
from rnet import Emulation

from .constants import SERVER_INSTRUCTIONS, SERVER_NAME, SERVER_VERSION
from .documents import DocumentStore
from .settings import load_server_settings

SERVER_SETTINGS = load_server_settings()
logger = logging.getLogger(__name__)

mcp = FastMCP(
    SERVER_NAME,
    instructions=SERVER_INSTRUCTIONS,
    version=SERVER_VERSION,
    website_url="https://github.com/BitingSnakes/silkworm-mcp",
    mask_error_details=SERVER_SETTINGS.mask_error_details,
    strict_input_validation=SERVER_SETTINGS.strict_input_validation,
)

DOCUMENT_STORE = DocumentStore(
    max_document_count=SERVER_SETTINGS.document_max_count,
    max_total_bytes=SERVER_SETTINGS.document_max_total_bytes,
    ttl_seconds=SERVER_SETTINGS.document_ttl_seconds,
    store_path=SERVER_SETTINGS.document_store_path,
)
EMULATION_NAMES = sorted(name for name in dir(Emulation) if not name.startswith("_"))
