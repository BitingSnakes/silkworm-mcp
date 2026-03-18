from __future__ import annotations

import os
from typing import Literal
from urllib.parse import urlparse

from pydantic import BaseModel, Field, field_validator, model_validator

from .constants import (
    DEFAULT_DOCUMENT_MAX_COUNT,
    DEFAULT_DOCUMENT_MAX_TOTAL_BYTES,
    DEFAULT_DOCUMENT_STORE_PATH,
    DEFAULT_DOCUMENT_TTL_SECONDS,
    DEFAULT_HTTP_HEALTH_PATH,
    DEFAULT_HTTP_READY_PATH,
    DEFAULT_LOG_LEVEL,
    DEFAULT_READINESS_PROBE_TIMEOUT_SECONDS,
    VALID_LOG_LEVELS,
)


class ServerSettings(BaseModel):
    document_max_count: int = Field(default=DEFAULT_DOCUMENT_MAX_COUNT, ge=1, le=5_000)
    document_max_total_bytes: int = Field(
        default=DEFAULT_DOCUMENT_MAX_TOTAL_BYTES,
        ge=4_096,
        le=1_000_000_000,
    )
    document_ttl_seconds: float | None = Field(
        default=DEFAULT_DOCUMENT_TTL_SECONDS,
        ge=0.1,
        le=604_800,
    )
    document_store_path: str | None = DEFAULT_DOCUMENT_STORE_PATH
    log_level: str = DEFAULT_LOG_LEVEL
    mask_error_details: bool = True
    strict_input_validation: bool = True
    default_transport: Literal["stdio", "http", "sse", "streamable-http"] = "stdio"
    default_host: str = "127.0.0.1"
    default_port: int = Field(default=8000, ge=1, le=65535)
    default_path: str | None = None
    http_health_path: str = DEFAULT_HTTP_HEALTH_PATH
    http_ready_path: str = DEFAULT_HTTP_READY_PATH
    readiness_require_cdp: bool = False
    readiness_cdp_ws_endpoint: str = "ws://127.0.0.1:9222"
    readiness_probe_timeout_seconds: float = Field(
        default=DEFAULT_READINESS_PROBE_TIMEOUT_SECONDS,
        ge=0.1,
        le=60.0,
    )

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, value: str) -> str:
        normalized = value.strip().upper()
        if normalized not in VALID_LOG_LEVELS:
            choices = ", ".join(sorted(VALID_LOG_LEVELS))
            raise ValueError(f"log_level must be one of: {choices}")
        return normalized

    @field_validator("http_health_path", "http_ready_path")
    @classmethod
    def validate_http_path(cls, value: str) -> str:
        if not value.startswith("/"):
            raise ValueError("HTTP route paths must start with '/'.")
        return value.rstrip("/") or "/"

    @field_validator("readiness_cdp_ws_endpoint")
    @classmethod
    def validate_ws_endpoint(cls, value: str) -> str:
        _validate_url(
            value,
            field_name="readiness_cdp_ws_endpoint",
            schemes={"ws", "wss"},
        )
        return value

    @model_validator(mode="after")
    def validate_routes(self) -> "ServerSettings":
        if self.http_health_path == self.http_ready_path:
            raise ValueError(
                "http_health_path and http_ready_path must be different routes."
            )
        return self


def _env_bool(name: str, default: bool) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    normalized = raw_value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"Environment variable {name} must be a boolean value.")


def _env_optional_str(name: str, default: str | None) -> str | None:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    value = raw_value.strip()
    return value or None


def _env_int(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return int(raw_value)


def _env_optional_float(name: str, default: float | None) -> float | None:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    value = raw_value.strip()
    if not value:
        return None
    return float(value)


def _validate_url(url: str, *, field_name: str, schemes: set[str]) -> None:
    parsed = urlparse(url)
    if parsed.scheme.lower() not in schemes or not parsed.netloc:
        allowed = ", ".join(sorted(schemes))
        raise ValueError(
            f"{field_name} must be an absolute URL using one of: {allowed}"
        )


def load_server_settings() -> ServerSettings:
    payload = {
        "document_max_count": _env_int(
            "SILKWORM_MCP_DOCUMENT_MAX_COUNT",
            DEFAULT_DOCUMENT_MAX_COUNT,
        ),
        "document_max_total_bytes": _env_int(
            "SILKWORM_MCP_DOCUMENT_MAX_TOTAL_BYTES",
            DEFAULT_DOCUMENT_MAX_TOTAL_BYTES,
        ),
        "document_ttl_seconds": _env_optional_float(
            "SILKWORM_MCP_DOCUMENT_TTL_SECONDS",
            DEFAULT_DOCUMENT_TTL_SECONDS,
        ),
        "document_store_path": _env_optional_str(
            "SILKWORM_MCP_DOCUMENT_STORE_PATH",
            DEFAULT_DOCUMENT_STORE_PATH,
        ),
        "log_level": os.getenv("SILKWORM_MCP_LOG_LEVEL", DEFAULT_LOG_LEVEL),
        "mask_error_details": _env_bool("SILKWORM_MCP_MASK_ERROR_DETAILS", True),
        "strict_input_validation": _env_bool(
            "SILKWORM_MCP_STRICT_INPUT_VALIDATION",
            True,
        ),
        "default_transport": os.getenv("SILKWORM_MCP_TRANSPORT", "stdio"),
        "default_host": os.getenv("SILKWORM_MCP_HOST", "127.0.0.1"),
        "default_port": _env_int("SILKWORM_MCP_PORT", 8000),
        "default_path": _env_optional_str("SILKWORM_MCP_PATH", None),
        "http_health_path": os.getenv(
            "SILKWORM_MCP_HTTP_HEALTH_PATH",
            DEFAULT_HTTP_HEALTH_PATH,
        ),
        "http_ready_path": os.getenv(
            "SILKWORM_MCP_HTTP_READY_PATH",
            DEFAULT_HTTP_READY_PATH,
        ),
        "readiness_require_cdp": _env_bool(
            "SILKWORM_MCP_READINESS_REQUIRE_CDP",
            False,
        ),
        "readiness_cdp_ws_endpoint": os.getenv(
            "SILKWORM_MCP_READINESS_CDP_WS_ENDPOINT",
            "ws://127.0.0.1:9222",
        ),
        "readiness_probe_timeout_seconds": float(
            os.getenv(
                "SILKWORM_MCP_READINESS_PROBE_TIMEOUT_SECONDS",
                str(DEFAULT_READINESS_PROBE_TIMEOUT_SECONDS),
            )
        ),
    }
    return ServerSettings.model_validate(payload)
