from collections.abc import Iterator
import argparse
import tarfile
import os.path
import os
import re
import math
from typing import TypeVar
import requests
from requests.adapters import HTTPAdapter, Retry
import mimetypes
import resource
from piexif import load, GPSIFD, ExifIFD

from datetime import datetime, timedelta
import pytz
from tzwhere import tzwhere

from rich.progress import (
    BarColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    DownloadColumn,
    TransferSpeedColumn,
)

from immich_takeout.processed_file_tracker import ProcessedFileTracker
from immich_takeout.metadata_matching import extract_metadata
from immich_takeout.local_file import LocalFile

DATETIME_STR_FORMAT = "%Y:%m:%d %H:%M:%S"
TZ_GUESSER = tzwhere.tzwhere()
MAX_NAME_LENGTH = 90


def cli():
    parser = argparse.ArgumentParser(
        description="CLI command to upload google takeout tars directly to immich without extracting"
    )
    parser.add_argument(
        "files",
        help="list of google taketout tar.gz files",
        type=argparse.FileType("rb"),
        nargs="+",
    )
    parser.add_argument(
        "--dry-run", help="Do not upload the images", action="store_true"
    )
    parser.add_argument("--api-key", help="API Key for Immich")
    parser.add_argument("--api-url", help="URL for Immich")
    args = parser.parse_args()

    tars = [tarfile.open(fileobj=f) for f in args.files]

    progress = Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        expand=True,
    )

    with progress:
        skip = ProcessedFileTracker("uploaded_main2.json")
        progress.log(f"Loaded {len(skip)} skipped files")
        try:
            upload_files(
                process_files(
                    extract_metadata(
                        tars=tars,
                        skip=skip,
                        progress=progress,
                    ),
                    progress=progress,
                ),
                api_key=args.api_key,
                api_url=args.api_url,
                dry_run=args.dry_run,
                progress=progress,
                skip=skip,
            )
        finally:
            skip.write_file()


def process_files(
    items: Iterator[LocalFile], progress: Progress
) -> Iterator[LocalFile]:
    for item in progress.track(items, description="Processing files"):
        # Filter to only your images
        if item.is_from_partner_sharing:
            continue

        metadata_time = item.metadata_original_timestamp

        if os.path.splitext(item.name)[1].lower() not in (".jpeg", ".jpg"):
            if metadata_time != item.last_modified:
                item.timestamp_differs = True
            progress.log(
                f"{metadata_time.isoformat()} {'[bright_black]ORIG[/]'} - {os.path.basename(item.name)}"
            )
            yield item
            continue

        # Read 128kb for EXIF??
        orig_binary = item.file_obj.read(1204 * 128)
        try:
            exif_data = load(orig_binary)
        except (ValueError, TypeError) as e:
            progress.log(f"Error reading EXIF, uploading directly {item.name} - {e}")
            yield item
            continue

        item.gps = item.metadata_gps or extract_exif_gps(exif_data)
        item.exif_original_time = extract_exif_date(exif_data)
        timestamp_differs, new_timestamp = check_timestamp_exif(
            exif_time=item.exif_original_time,
            exif_gps=item.gps,
            metadata_time=metadata_time,
        )
        item.original_time = new_timestamp
        item.timestamp_differs = timestamp_differs

        progress.log(
            f"{new_timestamp.isoformat()} {'[bright_magenta]UPDT[/]' if timestamp_differs else '[bright_black]ORIG[/]'} - {os.path.basename(item.name)}"
        )

        yield item


T = TypeVar("T")


def chunk_iterator(iterator: Iterator[T], size) -> Iterator[list[T]]:
    items = []
    for x in iterator:
        items.append(x)
        if len(items) >= size:
            yield items
            items = []
    if len(items):
        yield items


def parse_timezone(offset: str) -> pytz.FixedOffset:
    sign, hours, minutes = re.match(r"([+\-]?)(\d{2}):(\d{2})", offset).groups()
    sign = -1 if sign == "-" else 1
    hours, minutes = int(hours), int(minutes)
    return pytz.FixedOffset(sign * (hours * 60) + minutes)


def calculate_timezone(exif_time: datetime, metadata_time: datetime) -> datetime:
    """Calculate the timezone by differing the local time to UTC"""
    guess_tz = pytz.FixedOffset(
        math.floor(exif_time.utcoffset().total_seconds() / 60)
        - math.floor((metadata_time - exif_time).total_seconds() / 60)
    )
    return guess_tz.localize(exif_time.replace(tzinfo=None))


def check_timestamp_exif(
    exif_time: datetime | None,
    exif_gps: tuple[float, float] | None,
    metadata_time: datetime,
) -> tuple[bool, datetime]:
    if exif_time != metadata_time:
        if not exif_time:
            # No timestamp in EXIF, add it
            if exif_gps:
                new_tz = find_tz(exif_gps)
                if new_tz:
                    return True, metadata_time.astimezone(new_tz)
            # (using UTC, cant calculate timezone)
            return True, metadata_time
        has_tz = bool(exif_time.tzinfo)
        if not has_tz and abs(
            pytz.utc.localize(exif_time) - metadata_time
        ) <= timedelta(hours=12):
            # Timezone difference, calculate!
            return True, calculate_timezone(pytz.utc.localize(exif_time), metadata_time)
        elif has_tz:
            # Photo moved, but has TZ, so move it but keep TZ the same
            return True, metadata_time.astimezone(exif_time.tzinfo)
        else:
            # Bigger gap, just grab UTC, cant work out the right local time...
            if exif_gps:
                new_tz = find_tz(exif_gps)
                if new_tz:
                    return True, metadata_time.astimezone(new_tz)
            return True, metadata_time
    else:
        # All good!
        return False, exif_time


def extract_exif_date(exif_data) -> datetime | None:
    if ExifIFD.DateTimeOriginal not in exif_data["Exif"]:
        return None
    exif_time = datetime.strptime(
        exif_data["Exif"][ExifIFD.DateTimeOriginal].decode("ascii"), DATETIME_STR_FORMAT
    )
    if ExifIFD.OffsetTimeOriginal in exif_data["Exif"]:
        tz = parse_timezone(
            exif_data["Exif"][ExifIFD.OffsetTimeOriginal].decode("ascii")
        )
        exif_time = tz.localize(exif_time)
    return exif_time


def extract_exif_gps(exif_data) -> tuple[float, float] | None:
    if "GPS" not in exif_data:
        return None
    if GPSIFD.GPSLatitude not in exif_data["GPS"]:
        return None
    return dms_to_dd(exif_data["GPS"])


def dms_to_dd(gps_exif) -> tuple[float, float]:
    # convert the rational tuples by dividing each (numerator, denominator) pair
    lat = [n / d for n, d in gps_exif[GPSIFD.GPSLatitude]]
    lon = [n / d for n, d in gps_exif[GPSIFD.GPSLongitude]]

    # now you have lat and lon, which are lists of [degrees, minutes, seconds]
    # from the formula above
    dd_lat = lat[0] + lat[1] / 60 + lat[2] / 3600
    dd_lon = lon[0] + lon[1] / 60 + lon[2] / 3600

    # if latitude ref is 'S', make latitude negative
    if gps_exif[GPSIFD.GPSLatitudeRef].decode("ascii") == "S":
        dd_lat = -dd_lat

    # if longitude ref is 'W', make longitude negative
    if gps_exif[GPSIFD.GPSLongitudeRef].decode("ascii") == "W":
        dd_lon = -dd_lon

    return dd_lat, dd_lon


def find_tz(exif_gps):
    new_tz_name = TZ_GUESSER.tzNameAt(exif_gps[0], exif_gps[1])
    if new_tz_name:
        return pytz.timezone(new_tz_name)


def upload_files(
    files: Iterator[LocalFile],
    api_key: str,
    api_url: str,
    dry_run: bool,
    skip: ProcessedFileTracker,
    progress: Progress,
):
    session = requests.session()
    session.mount(
        "https://",
        HTTPAdapter(
            max_retries=Retry(
                total=20,
                backoff_factor=10,
                allowed_methods=("GET", "HEAD", "POST"),
                status_forcelist=[500, 502, 503, 504, 408, 429],
            ),
        ),
    )

    for item in files:
        if not dry_run:
            item.file_obj.seek(0)
            response = session.request(
                "POST",
                url=os.path.join(api_url, "api/asset/upload"),
                headers={
                    "Accept": "application/json",
                    "x-api-key": api_key,
                },
                files={"assetData": FixName(item.file_obj, item.name)},
                data={
                    "deviceAssetId": item.device_asset_id,
                    "deviceId": "gphotos-takeout-import",
                    "assetType": mimetypes.guess_type(item.name, strict=False)[0]
                    .split("/")[0]
                    .upper(),
                    "fileCreatedAt": item.original_time.isoformat(),
                    "fileModifiedAt": item.last_modified.isoformat(),
                    "isFavorite": "false",
                    "fileExtension": os.path.splitext(item.name)[1].lstrip("."),
                },
                timeout=60,
            )
            if not response.ok:
                progress.log(f"[red]HTTP {response.status_code} {response.reason}")
                progress.log(response.text)
                response.raise_for_status()
            else:
                data = response.json()
                skip.add(item.filename_from_archive)
                if data["duplicate"]:
                    progress.log(f"[yellow]Duplicate[/yellow] {item.name}")
                else:
                    progress.log(f"✔ Uploaded {item.name}")
                    if item.timestamp_differs:
                        update_asset_metadata(
                            session=session,
                            api_key=api_key,
                            api_url=api_url,
                            asset_id=data["id"],
                            item=item,
                        )


def update_asset_metadata(
    session: requests.Session,
    api_key: str,
    api_url: str,
    asset_id: str,
    item: LocalFile,
):
    json_data = {
        "dateTimeOriginal": item.original_time.isoformat(),
    }
    if item.gps:
        json_data["latitude"], json_data["longitude"] = item.gps
    resp = session.put(
        url=os.path.join(api_url, "api/asset", asset_id),
        json=json_data,
        headers={
            "x-api-key": api_key,
        },
    )
    resp.raise_for_status()


class FixName(object):
    def __init__(self, file_, name):
        self._file = file_
        self.name = name

    def __getattr__(self, attr):
        return getattr(self._file, attr)


def limit_memory(maxsize):
    soft, hard = resource.getrlimit(resource.RLIMIT_AS)
    resource.setrlimit(resource.RLIMIT_AS, (maxsize, hard))


if __name__ == "__main__":
    limit_memory(13 * 1024 * 1024 * 1024)
    cli()
