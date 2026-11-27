
import gzip

from github_engineering_intelligence.ingestion.downloader import (
    validate_archive,
)

from unittest.mock import MagicMock

import pytest
import requests

from github_engineering_intelligence.ingestion import downloader

def test_valid_archive(tmp_path):
    archive = tmp_path / "valid.json.gz"

    with gzip.open(archive, "wb") as file:
        file.write(b'{"id": "123"}\n')

    assert validate_archive(archive) is True


def test_corrupted_archive(tmp_path):
    archive = tmp_path / "corrupted.json.gz"

    archive.write_bytes(b"invalid gzip data")

    assert validate_archive(archive) is False


def test_empty_archive(tmp_path):
    archive = tmp_path / "empty.json.gz"

    with gzip.open(archive, "wb"):
        pass

    assert validate_archive(archive) is False


def test_missing_archive(tmp_path):
    archive = tmp_path / "missing.json.gz"

    assert validate_archive(archive) is False

def test_successful_download(
    isolated_ingestion,
    monkeypatch,
):
    compressed_data = gzip.compress(
        b'{"id": "123"}\n'
    )

    response = MagicMock()
    response.__enter__.return_value = response
    response.iter_content.return_value = [compressed_data]

    mock_get = MagicMock(return_value=response)

    monkeypatch.setattr(
        downloader.requests,
        "get",
        mock_get,
    )

    path = downloader.download_archive(
        2026, 11, 1, 0
    )

    assert path.exists()
    assert downloader.validate_archive(path)
    assert path.read_bytes() == compressed_data
    assert mock_get.call_count == 1
    assert not list(
        isolated_ingestion.glob("*.part")
    )


def test_idempotent_download(
    isolated_ingestion,
    monkeypatch,
):
    isolated_ingestion.mkdir(parents=True)

    archive = isolated_ingestion / (
        "2026-11-01-0.json.gz"
    )

    archive.write_bytes(
        gzip.compress(b'{"id": "123"}\n')
    )

    mock_get = MagicMock()

    monkeypatch.setattr(
        downloader.requests,
        "get",
        mock_get,
    )

    result = downloader.download_archive(
        2026, 11, 1, 0
    )

    assert result == archive
    mock_get.assert_not_called()


def test_retry_exhaustion(
    isolated_ingestion,
    monkeypatch,
):
    mock_get = MagicMock(
        side_effect=requests.exceptions.ConnectionError(
            "Connection refused"
        )
    )

    mock_sleep = MagicMock()

    monkeypatch.setattr(
        downloader.requests,
        "get",
        mock_get,
    )

    monkeypatch.setattr(
        downloader.time,
        "sleep",
        mock_sleep,
    )

    with pytest.raises(
        requests.exceptions.ConnectionError
    ):
        downloader.download_archive(2026, 11, 1, 0)

    assert mock_get.call_count == 4

    delays = [
        call.args[0]
        for call in mock_sleep.call_args_list
    ]

    assert delays == [2, 4, 8]
    assert not list(
        isolated_ingestion.glob("*.part")
    )


def test_unavailable_archive(
    isolated_ingestion,
    monkeypatch,
):
    response = MagicMock()
    response.status_code = 404
    response.__enter__.return_value = response

    response.raise_for_status.side_effect = (
        requests.exceptions.HTTPError(
            "Not Found",
            response=response,
        )
    )

    mock_get = MagicMock(return_value=response)

    monkeypatch.setattr(
        downloader.requests,
        "get",
        mock_get,
    )

    with pytest.raises(
        downloader.ArchiveUnavailableError
    ):
        downloader.download_archive(2099, 1, 1, 0)

    assert mock_get.call_count == 1
