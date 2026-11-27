
from datetime import datetime, timedelta

import requests

from github_engineering_intelligence.ingestion.downloader import (
    ArchiveUnavailableError,
    CorruptedArchiveError,
    download_archive,
)


def backfill_archives(start: datetime, end: datetime) -> bool:
    if start >= end:
        raise ValueError("Start must be earlier than end")

    for value in (start, end):
        if value.minute or value.second or value.microsecond:
            raise ValueError("Timestamps must align to full hours")

    current = start

    total = 0
    successful = 0
    missing = 0
    failed = 0

    while current < end:
        total += 1

        print(f"\nProcessing: {current.isoformat()} UTC")

        try:
            download_archive(
                current.year,
                current.month,
                current.day,
                current.hour,
            )

            successful += 1

        except ArchiveUnavailableError:
            missing += 1
            print(f"Missing archive: {current}")

        except (
            requests.exceptions.RequestException,
            CorruptedArchiveError,
        ) as error:
            failed += 1
            print(f"Failed archive: {current}: {error}")

        current += timedelta(hours=1)

    print("\nBackfill summary")
    print("----------------")
    print(f"Total:      {total}")
    print(f"Successful: {successful}")
    print(f"Missing:    {missing}")
    print(f"Failed:     {failed}")

    return missing == 0 and failed == 0
