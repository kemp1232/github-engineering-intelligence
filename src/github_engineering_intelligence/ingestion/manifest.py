
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

MANIFEST_PATH = Path("data/metadata/ingestion.db")


def get_connection() -> sqlite3.Connection:
    MANIFEST_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    return sqlite3.connect(MANIFEST_PATH)


def initialize_manifest() -> None:
    with closing(get_connection()) as conn:
        with conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ingestion_manifest (
                    archive_hour TEXT PRIMARY KEY,
                    status TEXT NOT NULL CHECK (
                        status IN (
                            'processing',
                            'success',
                            'missing',
                            'failed'
                        )
                    ),
                    file_path TEXT,
                    file_size_bytes INTEGER,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    error_message TEXT,
                    updated_at TEXT NOT NULL
                )
            """)


def mark_processing(archive_hour: str) -> None:
    updated_at = datetime.now(timezone.utc).isoformat()

    with closing(get_connection()) as conn:
        with conn:
            conn.execute("""
                INSERT INTO ingestion_manifest (
                    archive_hour,
                    status,
                    attempt_count,
                    updated_at
                )
                VALUES (?, 'processing', 1, ?)

                ON CONFLICT(archive_hour) DO UPDATE SET
                    status = 'processing',
                    attempt_count = attempt_count + 1,
                    file_path = NULL,
                    file_size_bytes = NULL,
                    error_message = NULL,
                    updated_at = excluded.updated_at
            """, (archive_hour, updated_at))


def mark_result(
    archive_hour: str,
    status: str,
    file_path: Path | None = None,
    error_message: str | None = None,
) -> None:
    updated_at = datetime.now(timezone.utc).isoformat()

    file_size = (
        file_path.stat().st_size
        if file_path is not None
        else None
    )

    with closing(get_connection()) as conn:
        with conn:
            conn.execute("""
                UPDATE ingestion_manifest
                SET
                    status = ?,
                    file_path = ?,
                    file_size_bytes = ?,
                    error_message = ?,
                    updated_at = ?
                WHERE archive_hour = ?
            """, (
                status,
                str(file_path) if file_path else None,
                file_size,
                error_message,
                updated_at,
                archive_hour,
            ))


if __name__ == "__main__":
    initialize_manifest()
    print(f"Manifest initialized: {MANIFEST_PATH}")
