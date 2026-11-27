
import pytest

from github_engineering_intelligence.ingestion import (
    downloader,
    manifest,
)


@pytest.fixture
def isolated_ingestion(tmp_path, monkeypatch):
    bronze_dir = tmp_path / "bronze"
    database_path = tmp_path / "metadata" / "ingestion.db"

    monkeypatch.setattr(
        downloader,
        "BRONZE_DIR",
        bronze_dir,
    )

    monkeypatch.setattr(
        manifest,
        "MANIFEST_PATH",
        database_path,
    )

    monkeypatch.setattr(
        downloader.time,
        "sleep",
        lambda seconds: None,
    )

    return bronze_dir
