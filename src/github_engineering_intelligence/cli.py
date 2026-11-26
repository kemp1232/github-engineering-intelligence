
import argparse
import sys
from datetime import date

import requests

from github_engineering_intelligence.ingestion.downloader import (
    ArchiveUnavailableError,
    CorruptedArchiveError,
    download_archive,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="GitHub Engineering Intelligence Platform"
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
    )

    ingest_parser = subparsers.add_parser(
        "ingest",
        help="Download a single hourly GitHub archive",
    )

    ingest_parser.add_argument(
        "--date",
        type=date.fromisoformat,
        required=True,
        help="Archive date in YYYY-MM-DD format (UTC)",
    )

    ingest_parser.add_argument(
        "--hour",
        type=int,
        choices=range(24),
        metavar="0-23",
        required=True,
        help="Archive hour in UTC",
    )

    args = parser.parse_args()

    try:
        if args.command == "ingest":
            download_archive(
                args.date.year,
                args.date.month,
                args.date.day,
                args.hour,
            )

        return 0

    except ArchiveUnavailableError as error:
        print(f"Archive unavailable: {error}", file=sys.stderr)
        return 1

    except (
        requests.exceptions.RequestException,
        CorruptedArchiveError,
        OSError,
    ) as error:
        print(f"Ingestion failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
