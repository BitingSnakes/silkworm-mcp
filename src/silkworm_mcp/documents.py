from __future__ import annotations

import json
import os
import sqlite3
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Literal

from .models import DocumentStoreStats, StoredDocumentInfo


@dataclass(slots=True)
class StoredDocument:
    handle: str
    html: str
    html_bytes: int
    label: str | None = None
    source_url: str | None = None
    stored_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_accessed_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    fetched_via: str | None = None
    status: int | None = None
    headers: dict[str, str] = field(default_factory=dict)

    def touch(self, at: datetime | None = None) -> None:
        self.last_accessed_at = at or datetime.now(timezone.utc)

    def expires_at(self, ttl_seconds: float | None) -> datetime | None:
        if ttl_seconds is None:
            return None
        return self.last_accessed_at + timedelta(seconds=ttl_seconds)

    def info(self, ttl_seconds: float | None) -> StoredDocumentInfo:
        expires_at = self.expires_at(ttl_seconds)
        return StoredDocumentInfo(
            handle=self.handle,
            label=self.label,
            source_url=self.source_url,
            stored_at=self.stored_at.isoformat(),
            last_accessed_at=self.last_accessed_at.isoformat(),
            expires_at=expires_at.isoformat() if expires_at is not None else None,
            fetched_via=self.fetched_via,
            status=self.status,
            html_chars=len(self.html),
            html_bytes=self.html_bytes,
        )


class DocumentStore:
    def __init__(
        self,
        *,
        max_document_count: int,
        max_total_bytes: int,
        ttl_seconds: float | None,
        store_path: str | None = None,
    ) -> None:
        self._lock = threading.Lock()
        self._documents: dict[str, StoredDocument] = {}
        self._max_document_count = max_document_count
        self._max_total_bytes = max_total_bytes
        self._ttl_seconds = ttl_seconds
        self._store_path = store_path
        self._total_bytes = 0
        self._created_documents = 0
        self._evicted_documents = 0
        self._expired_documents = 0
        self._rejected_documents = 0
        if self._store_path:
            directory = os.path.dirname(self._store_path)
            if directory:
                os.makedirs(directory, exist_ok=True)
            self._initialize_persistent_store()

    def _initialize_persistent_store(self) -> None:
        assert self._store_path is not None
        with sqlite3.connect(self._store_path) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS documents (
                    handle TEXT PRIMARY KEY,
                    html TEXT NOT NULL,
                    html_bytes INTEGER NOT NULL,
                    label TEXT,
                    source_url TEXT,
                    stored_at TEXT NOT NULL,
                    last_accessed_at TEXT NOT NULL,
                    fetched_via TEXT,
                    status INTEGER,
                    headers_json TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_documents_last_accessed
                ON documents(last_accessed_at, stored_at, handle)
                """
            )
            connection.commit()

    def _connect(self) -> sqlite3.Connection:
        assert self._store_path is not None
        connection = sqlite3.connect(self._store_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _document_from_row(self, row: sqlite3.Row) -> StoredDocument:
        return StoredDocument(
            handle=str(row["handle"]),
            html=str(row["html"]),
            html_bytes=int(row["html_bytes"]),
            label=row["label"],
            source_url=row["source_url"],
            stored_at=datetime.fromisoformat(str(row["stored_at"])),
            last_accessed_at=datetime.fromisoformat(str(row["last_accessed_at"])),
            fetched_via=row["fetched_via"],
            status=row["status"],
            headers=json.loads(str(row["headers_json"])) if row["headers_json"] else {},
        )

    def _is_expired(self, document: StoredDocument, now: datetime) -> bool:
        expires_at = document.expires_at(self._ttl_seconds)
        return expires_at is not None and expires_at <= now

    def _remove_document_locked(
        self,
        handle: str,
        *,
        reason: Literal["expired", "evicted"] | None = None,
    ) -> StoredDocument | None:
        document = self._documents.pop(handle, None)
        if document is None:
            return None
        self._total_bytes = max(0, self._total_bytes - document.html_bytes)
        if reason == "expired":
            self._expired_documents += 1
        elif reason == "evicted":
            self._evicted_documents += 1
        return document

    def _prune_expired_locked(self, now: datetime) -> None:
        if self._store_path:
            if self._ttl_seconds is None:
                return
            cutoff = (now - timedelta(seconds=self._ttl_seconds)).isoformat()
            with self._connect() as connection:
                cursor = connection.execute(
                    "DELETE FROM documents WHERE last_accessed_at <= ?",
                    (cutoff,),
                )
                deleted = int(cursor.rowcount or 0)
                if deleted:
                    self._expired_documents += deleted
                connection.commit()
            return
        expired_handles = [
            handle
            for handle, document in self._documents.items()
            if self._is_expired(document, now)
        ]
        for handle in expired_handles:
            self._remove_document_locked(handle, reason="expired")

    def _evict_one_locked(self) -> bool:
        if self._store_path:
            with self._connect() as connection:
                row = connection.execute(
                    """
                    SELECT handle
                    FROM documents
                    ORDER BY last_accessed_at ASC, stored_at ASC, handle ASC
                    LIMIT 1
                    """
                ).fetchone()
                if row is None:
                    return False
                connection.execute(
                    "DELETE FROM documents WHERE handle = ?",
                    (str(row["handle"]),),
                )
                connection.commit()
            self._evicted_documents += 1
            return True
        if not self._documents:
            return False
        handle, _document = min(
            self._documents.items(),
            key=lambda item: (
                item[1].last_accessed_at,
                item[1].stored_at,
                item[0],
            ),
        )
        self._remove_document_locked(handle, reason="evicted")
        return True

    def add(
        self,
        html: str,
        *,
        label: str | None = None,
        source_url: str | None = None,
        fetched_via: str | None = None,
        status: int | None = None,
        headers: dict[str, str] | None = None,
    ) -> StoredDocument:
        now = datetime.now(timezone.utc)
        html_bytes = len(html.encode("utf-8"))
        if html_bytes > self._max_total_bytes:
            with self._lock:
                self._rejected_documents += 1
            raise ValueError(
                "Document is too large for the configured in-memory cache budget."
            )

        handle = uuid.uuid4().hex[:12]
        document = StoredDocument(
            handle=handle,
            html=html,
            html_bytes=html_bytes,
            label=label,
            source_url=source_url,
            stored_at=now,
            last_accessed_at=now,
            fetched_via=fetched_via,
            status=status,
            headers=dict(headers or {}),
        )
        if self._store_path:
            with self._lock:
                self._prune_expired_locked(now)
                with self._connect() as connection:
                    while True:
                        row = connection.execute(
                            """
                            SELECT COUNT(*) AS document_count,
                                   COALESCE(SUM(html_bytes), 0) AS total_bytes
                            FROM documents
                            """
                        ).fetchone()
                        assert row is not None
                        document_count = int(row["document_count"])
                        total_bytes = int(row["total_bytes"])
                        if (
                            document_count < self._max_document_count
                            and total_bytes + document.html_bytes
                            <= self._max_total_bytes
                        ):
                            break
                        if not self._evict_one_locked():
                            break
                    connection.execute(
                        """
                        INSERT INTO documents (
                            handle,
                            html,
                            html_bytes,
                            label,
                            source_url,
                            stored_at,
                            last_accessed_at,
                            fetched_via,
                            status,
                            headers_json
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            document.handle,
                            document.html,
                            document.html_bytes,
                            document.label,
                            document.source_url,
                            document.stored_at.isoformat(),
                            document.last_accessed_at.isoformat(),
                            document.fetched_via,
                            document.status,
                            json.dumps(document.headers),
                        ),
                    )
                    connection.commit()
                self._created_documents += 1
            return document
        with self._lock:
            self._prune_expired_locked(now)
            while (
                len(self._documents) >= self._max_document_count
                or self._total_bytes + document.html_bytes > self._max_total_bytes
            ):
                if not self._evict_one_locked():
                    break
            self._documents[handle] = document
            self._total_bytes += document.html_bytes
            self._created_documents += 1
        return document

    def get(self, handle: str) -> StoredDocument:
        if self._store_path:
            with self._lock:
                now = datetime.now(timezone.utc)
                self._prune_expired_locked(now)
                with self._connect() as connection:
                    row = connection.execute(
                        "SELECT * FROM documents WHERE handle = ?",
                        (handle,),
                    ).fetchone()
                    if row is None:
                        raise ValueError(f"Unknown document handle: {handle}")
                    connection.execute(
                        "UPDATE documents SET last_accessed_at = ? WHERE handle = ?",
                        (now.isoformat(), handle),
                    )
                    connection.commit()
                document = self._document_from_row(row)
                document.touch(now)
            return document
        with self._lock:
            now = datetime.now(timezone.utc)
            self._prune_expired_locked(now)
            document = self._documents.get(handle)
            if document is not None:
                document.touch(now)
        if document is None:
            raise ValueError(f"Unknown document handle: {handle}")
        return document

    def list(self) -> list[StoredDocument]:
        if self._store_path:
            with self._lock:
                self._prune_expired_locked(datetime.now(timezone.utc))
                with self._connect() as connection:
                    rows = connection.execute(
                        """
                        SELECT *
                        FROM documents
                        ORDER BY last_accessed_at DESC, stored_at DESC, handle DESC
                        """
                    ).fetchall()
            return [self._document_from_row(row) for row in rows]
        with self._lock:
            self._prune_expired_locked(datetime.now(timezone.utc))
            documents = list(self._documents.values())
        documents.sort(
            key=lambda doc: (
                doc.last_accessed_at,
                doc.stored_at,
                doc.handle,
            ),
            reverse=True,
        )
        return documents

    def delete(self, handle: str) -> bool:
        if self._store_path:
            with self._lock:
                with self._connect() as connection:
                    cursor = connection.execute(
                        "DELETE FROM documents WHERE handle = ?",
                        (handle,),
                    )
                    connection.commit()
                return bool(cursor.rowcount)
        with self._lock:
            return self._remove_document_locked(handle) is not None

    def clear(self) -> int:
        if self._store_path:
            with self._lock:
                with self._connect() as connection:
                    row = connection.execute(
                        "SELECT COUNT(*) AS document_count FROM documents"
                    ).fetchone()
                    deleted = int(row["document_count"]) if row is not None else 0
                    connection.execute("DELETE FROM documents")
                    connection.commit()
                return deleted
        with self._lock:
            deleted = len(self._documents)
            self._documents.clear()
            self._total_bytes = 0
        return deleted

    def stats(self) -> DocumentStoreStats:
        if self._store_path:
            with self._lock:
                self._prune_expired_locked(datetime.now(timezone.utc))
                with self._connect() as connection:
                    row = connection.execute(
                        """
                        SELECT COUNT(*) AS document_count,
                               COALESCE(SUM(html_bytes), 0) AS total_bytes
                        FROM documents
                        """
                    ).fetchone()
            return DocumentStoreStats(
                document_count=int(row["document_count"]) if row is not None else 0,
                total_bytes=int(row["total_bytes"]) if row is not None else 0,
                max_document_count=self._max_document_count,
                max_total_bytes=self._max_total_bytes,
                ttl_seconds=self._ttl_seconds,
                created_documents=self._created_documents,
                evicted_documents=self._evicted_documents,
                expired_documents=self._expired_documents,
                rejected_documents=self._rejected_documents,
            )
        with self._lock:
            self._prune_expired_locked(datetime.now(timezone.utc))
            return DocumentStoreStats(
                document_count=len(self._documents),
                total_bytes=self._total_bytes,
                max_document_count=self._max_document_count,
                max_total_bytes=self._max_total_bytes,
                ttl_seconds=self._ttl_seconds,
                created_documents=self._created_documents,
                evicted_documents=self._evicted_documents,
                expired_documents=self._expired_documents,
                rejected_documents=self._rejected_documents,
            )
