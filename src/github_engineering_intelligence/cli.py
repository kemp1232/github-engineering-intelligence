
import argparse
import sys
from datetime import date, datetime, timezone

import requests

from github_engineering_intelligence.ingestion.downloader import (
    ArchiveUnavailableError,
    CorruptedArchiveError,
    download_archive,
)

from github_engineering_intelligence.ingestion.backfill import (
    backfill_archives,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="GitHub Engineering Intelligence Platform"
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
    )

    # Single archive ingestion
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

    # Historical backfill
    backfill_parser = subparsers.add_parser(
        "backfill",
        help="Download historical GitHub archives",
    )

    backfill_parser.add_argument(
        "--start",
        type=datetime.fromisoformat,
        required=True,
        help="Start timestamp in UTC (inclusive)",
    )

    backfill_parser.add_argument(
        "--end",
        type=datetime.fromisoformat,
        required=True,
        help="End timestamp in UTC (exclusive)",
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

        elif args.command == "backfill":
            start = args.start
            end = args.end

            if start.tzinfo is not None:
                start = start.astimezone(
                    timezone.utc
                ).replace(tzinfo=None)

            if end.tzinfo is not None:
                end = end.astimezone(
                    timezone.utc
                ).replace(tzinfo=None)

            success = backfill_archives(start, end)

            return 0 if success else 1

        return 0

    except ValueError as error:
        print(f"Invalid input: {error}", file=sys.stderr)
        return 1

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
