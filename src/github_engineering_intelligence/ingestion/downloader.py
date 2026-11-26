
import gzip
import time
import zlib
from datetime import datetime
from pathlib import Path

import requests

BASE_URL = "https://data.gharchive.org"
BRONZE_DIR = Path("data/bronze")

MAX_ATTEMPTS = 4
RETRY_DELAY = 2


class ArchiveUnavailableError(Exception):
    pass


class CorruptedArchiveError(Exception):
    pass


def validate_archive(path: Path) -> bool:
    try:
        with gzip.open(path, "rb") as file:
            if not file.read(1):
                return False

            while file.read(1024 * 1024):
                pass

        return True

    except (OSError, EOFError, zlib.error):
        return False


# Responsible for a single download attempt.
def _download_once(
    year: int,
    month: int,
    day: int,
    hour: int,
) -> Path:
    filename = f"{year:04d}-{month:02d}-{day:02d}-{hour}.json.gz"
    url = f"{BASE_URL}/{filename}"

    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    destination = BRONZE_DIR / filename
    temporary = BRONZE_DIR / f"{filename}.part"

    if destination.exists():
        if validate_archive(destination):
            print(f"Skipping existing archive: {destination}")
            return destination

        print(f"Existing archive is corrupted: {destination}")

    try:
        print(f"Downloading: {url}")

        with requests.get(url, stream=True, timeout=60) as response:
            response.raise_for_status()

            with temporary.open("wb") as file:
                for chunk in response.iter_content(
                    chunk_size=1024 * 1024
                ):
                    file.write(chunk)

        print("Validating archive...")

        if not validate_archive(temporary):
            raise CorruptedArchiveError(
                "Downloaded archive is corrupted"
            )

        temporary.replace(destination)

        print(f"Saved to: {destination}")

        return destination

    finally:
        temporary.unlink(missing_ok=True)


# Responsible for retries and error handling.
def download_archive(
    year: int,
    month: int,
    day: int,
    hour: int,
) -> Path:
    datetime(year, month, day, hour)

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            return _download_once(year, month, day, hour)

        except requests.exceptions.HTTPError as error:
            status = error.response.status_code

            if status == 404:
                raise ArchiveUnavailableError(
                    "Requested archive is unavailable"
                ) from error

            if status != 429 and not 500 <= status <= 599:
                raise

            if attempt == MAX_ATTEMPTS:
                raise

            print(f"HTTP error: {status}")

        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
            CorruptedArchiveError,
        ) as error:
            if attempt == MAX_ATTEMPTS:
                raise

            print(f"Download failed: {error}")

        delay = RETRY_DELAY * (2 ** (attempt - 1))

        print(
            f"Attempt {attempt}/{MAX_ATTEMPTS} failed. "
            f"Retrying in {delay} seconds..."
        )

        time.sleep(delay)

    raise RuntimeError("Retry loop exited unexpectedly")