
from contextlib import closing

import pytest

from github_engineering_intelligence.ingestion import manifest


@pytest.fixture
def test_manifest(tmp_path, monkeypatch):
    database_path = tmp_path / "ingestion.db"

    monkeypatch.setattr(
        manifest,
        "MANIFEST_PATH",
        database_path,
    )

    manifest.initialize_manifest()

    return manifest


def test_mark_processing(test_manifest):
    archive_hour = "2026-11-01T00:00:00Z"

    test_manifest.mark_processing(archive_hour)

    with closing(test_manifest.get_connection()) as conn:
        result = conn.execute("""
            SELECT status, attempt_count
            FROM ingestion_manifest
            WHERE archive_hour = ?
        """, (archive_hour,)).fetchone()

    assert result == ("processing", 1)


def test_failure_recovery(test_manifest, tmp_path):
    archive_hour = "2026-11-01T00:00:00Z"

    test_manifest.mark_processing(archive_hour)

    test_manifest.mark_result(
        archive_hour,
        "failed",
        error_message="Connection refused",
    )

    test_manifest.mark_processing(archive_hour)

    archive = tmp_path / "archive.json.gz"
    archive.write_bytes(b"sample archive data")

    test_manifest.mark_result(
        archive_hour,
        "success",
        file_path=archive,
    )

    with closing(test_manifest.get_connection()) as conn:
        result = conn.execute("""
            SELECT status, attempt_count, error_message
            FROM ingestion_manifest
            WHERE archive_hour = ?
        """, (archive_hour,)).fetchone()

        count = conn.execute("""
            SELECT COUNT(*)
            FROM ingestion_manifest
        """).fetchone()[0]

    assert result == ("success", 2, None)
    assert count == 1
