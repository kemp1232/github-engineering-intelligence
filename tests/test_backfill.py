
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from github_engineering_intelligence.ingestion import (
    backfill,
)

from github_engineering_intelligence.ingestion.downloader import (
    ArchiveUnavailableError,
)


def test_successful_backfill(monkeypatch):
    mock_download = MagicMock()

    monkeypatch.setattr(
        backfill,
        "download_archive",
        mock_download,
    )

    result = backfill.backfill_archives(
        datetime(2026, 11, 1, 0),
        datetime(2026, 11, 1, 3),
    )

    assert result is True
    assert mock_download.call_count == 3

    requested_hours = [
        call.args
        for call in mock_download.call_args_list
    ]

    assert requested_hours == [
        (2026, 11, 1, 0),
        (2026, 11, 1, 1),
        (2026, 11, 1, 2),
    ]


def test_backfill_with_missing_archive(monkeypatch):
    mock_download = MagicMock(
        side_effect=[
            None,
            ArchiveUnavailableError(
                "Archive unavailable"
            ),
            None,
        ]
    )

    monkeypatch.setattr(
        backfill,
        "download_archive",
        mock_download,
    )

    result = backfill.backfill_archives(
        datetime(2026, 11, 1, 0),
        datetime(2026, 11, 1, 3),
    )

    assert result is False
    assert mock_download.call_count == 3


def test_invalid_backfill_range():
    with pytest.raises(ValueError):
        backfill.backfill_archives(
            datetime(2026, 11, 2),
            datetime(2026, 11, 1),
        )


def test_unaligned_backfill_range():
    with pytest.raises(ValueError):
        backfill.backfill_archives(
            datetime(2026, 11, 1, 0, 30),
            datetime(2026, 11, 1, 3),
        )
