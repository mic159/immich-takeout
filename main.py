import itertools
from collections.abc import Iterator
import argparse
import tarfile
import os.path
import os
import re
import math
import json
from typing import IO, TypeVar
import requests
from requests.adapters import HTTPAdapter, Retry
import mimetypes
import tempfile
import hashlib
from piexif import load, ExifIFD, insert, dump

from datetime import datetime, timezone, timedelta
import pytz

from rich.progress import (
    BarColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    DownloadColumn,
    TransferSpeedColumn,
)

DATETIME_STR_FORMAT = "%Y:%m:%d %H:%M:%S"


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
        upload_files(
            process_files(
                extract_metadata(tars=tars, progress=progress), progress=progress
            ),
            api_key=args.api_key,
            api_url=args.api_url,
            dry_run=args.dry_run,
            progress=progress,
        )


def extract_metadata(
    tars: list[tarfile.TarFile], progress: Progress
) -> Iterator[tuple[str, IO[bytes], dict, int]]:
    metadata: dict[str, dict] = {}
    tar_infos: dict[str, tuple[tarfile.TarFile, tarfile.TarInfo]] = {}
    seen = set()
    metadata_progress = progress.add_task(description="metadata", total=None)
    info_progress = progress.add_task(description="files", total=None)
    for tar in progress.track(tars):
        tar.fileobj = progress.wrap_file(
            tar.fileobj,
            total=os.path.getsize(tar.name),
            description=os.path.basename(tar.name),
        )
        tar_name = os.path.basename(tar.name)
        progress.log(f"Processing [bold blue]{tar_name}")
        for tarinfo in iterate_tarfile(tar):
            filename, ext = os.path.splitext(tarinfo.name)
            if ext == ".json":
                data = json.load(tar.extractfile(tarinfo))
                if filename in metadata or filename in seen:
                    progress.log(f"[red bold]❌ ERROR: Duplicate metadata found!!!")
                    progress.log(f"[red]  - filename: {filename}")
                    progress.log(f"[red]  - Tar: {tar.name}")
                    progress.log(
                        f"[red]  - In Metadata: {filename in metadata}, In seen {filename in seen}"
                    )
                metadata[filename] = data
            else:
                filename = tarinfo.name
                tar_infos[filename] = (tar, tarinfo)
            progress.update(metadata_progress, completed=len(metadata), total=None)
            progress.update(info_progress, completed=len(tar_infos), total=None)
            if filename in tar_infos and filename in metadata:
                tarfle, data_file_tarinfo = tar_infos[filename]
                yield (
                    filename,
                    tarfle.extractfile(data_file_tarinfo),
                    metadata[filename],
                    data_file_tarinfo.size
                )
                del tar_infos[filename]
                del metadata[filename]
                seen.add(filename)
        progress.log(
            f"Dangling metadata: {len(metadata)}, Dangling files: {len(tar_infos)}"
        )

    progress.log("Finished extracting files")
    progress.log(f"Num files: {len(seen)}")
    if len(metadata):
        progress.log(f"[yellow]⚠ Metadata dangling: {len(metadata)}")
    if len(tar_infos):
        progress.log(f"[yellow]⚠ Files dangling: {len(tar_infos)}")


def process_files(
    iter: Iterator[tuple[str, IO[bytes], dict, int]], progress: Progress
) -> Iterator[tuple[str, IO[bytes], int]]:
    skipped = 0
    for filename, fle, data, filesize in progress.track(
        iter, description="Processing files"
    ):
        # Filter to only your images
        if "fromPartnerSharing" in data.get("googlePhotosOrigin", {}):
            skipped += 1
            fle.close()
            continue
        if os.path.splitext(filename)[1].lower() not in (".jpeg", ".jpg"):
            yield filename, fle, filesize
            continue

        orig_binary = fle.read()
        exif_data = load(orig_binary)

        needs_rewrite, new_timestamp = check_timestamp_exif(
            exif_time=extract_exif_date(exif_data),
            metadata_time=datetime.fromtimestamp(
                int(data["photoTakenTime"]["timestamp"]), timezone.utc
            ),
        )
        if needs_rewrite:
            update_exif_data(exif_data, new_timestamp)
            # Hack for thumbnail issues
            if 'thumbnail' in exif_data and exif_data['thumbnail'] and len(exif_data['thumbnail']) > 64000:
                progress.log(f"WARN: Large thumbnail, erasing {filename} {len(exif_data['thumbnail'])}")
                del exif_data['thumbnail']
            # Write out altered images
            # Piexif must write to files by path, so it needs to be a named file
            tmp_fle = tempfile.NamedTemporaryFile("wb+", suffix=".jpg", delete=True)
            insert(dump(exif_data), orig_binary, tmp_fle.name)
            # Update filesize
            filesize = os.path.getsize(tmp_fle.name)
            fle.close()
            yield filename, tmp_fle, filesize
        else:
            # Good, emit directly
            yield filename, fle, filesize
    progress.log(f"Skipped {skipped}")


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


def get_file_info(name: str, fle: IO[bytes], filesize: int) -> tuple[str, str]:
    fle.seek(0)
    digest = hashlib.file_digest(fle, "sha1").hexdigest()
    device_asset_id = f"{os.path.basename(name).replace(' ', '')}-{filesize}"
    return device_asset_id, digest


def format_timezone(dt: datetime) -> str:
    tzstring = dt.strftime("%z")
    return f"{tzstring[:-2]}:{tzstring[-2:]}"


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
    exif_time: datetime | None, metadata_time: datetime
) -> tuple[bool, datetime]:
    if exif_time != metadata_time:
        if not exif_time:
            # No timestamp in EXIF, add it (using UTC, cant calculate timezone)
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
            return True, metadata_time
    else:
        # All good!
        return False, exif_time


def update_exif_data(exif_data, new_timestamp):
    exif_data["Exif"][ExifIFD.DateTimeOriginal] = new_timestamp.strftime(
        DATETIME_STR_FORMAT
    ).encode("ascii")
    exif_data["Exif"][ExifIFD.OffsetTimeOriginal] = format_timezone(
        new_timestamp
    ).encode("ascii")


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


def deduplicate(
    files: Iterator[tuple[str, IO[bytes], int]],
    session: requests.Session,
    api_key: str,
    api_url: str,
    progress: Progress,
) -> Iterator[tuple[str, str, IO[bytes]]]:
    for chunk in chunk_iterator(files, size=30):
        start = datetime.now()
        info = [get_file_info(n, fle, size) for n, fle, size in chunk]
        end = datetime.now()
        response = session.request(
            "POST",
            url=f"{api_url}/api/asset/bulk-upload-check",
            headers={
                "Accept": "application/json",
                "x-api-key": api_key,
            },
            json={
                "assets": [
                    {"id": device_asset_id, "checksum": digest}
                    for device_asset_id, digest in info
                ]
            },
            timeout=60,
        )
        if not response.ok:
            progress.log(f"[red]HTTP {response.status_code} {response.reason}")
            progress.log(response.text)
            response.raise_for_status()
        else:
            data = response.json()
            num_duplicate = sum(
                1
                for x in data["results"]
                if x["action"] == "reject" and x["reason"] == "duplicate"
            )
            progress.log(
                f"Deduplicated. Hashing: {end - start} API: {response.elapsed}s  num: {num_duplicate}/{len(chunk)}"
            )
            for (
                (name, fle, filesize),
                (device_asset_id, _),
                result,
            ) in itertools.zip_longest(chunk, info, data["results"]):
                if result["action"] == "reject" and result["reason"] == "duplicate":
                    fle.close()
                    continue
                yield name, device_asset_id, fle


def upload_files(
    files: Iterator[tuple[str, IO[bytes], int]],
    api_key: str,
    api_url: str,
    dry_run: bool,
    progress: Progress,
):
    session = requests.session()
    session.mount(
        "https://",
        HTTPAdapter(
            max_retries=Retry(
                total=5, backoff_factor=5, status_forcelist=[500, 502, 503, 504, 408, 429]
            ),
        ),
    )
    for name, device_asset_id, fle in deduplicate(
        files, session=session, progress=progress, api_key=api_key, api_url=api_url
    ):
        fle.seek(0)
        if not dry_run:
            response = session.request(
                "POST",
                url=f"{api_url}/api/asset/upload",
                headers={
                    "Accept": "application/json",
                    "x-api-key": api_key,
                },
                files={"assetData": FixName(fle, name)},
                data={
                    "deviceAssetId": device_asset_id,
                    "deviceId": "gphotos-takeout-import",
                    "assetType": mimetypes.guess_type(name, strict=False)[0]
                    .split("/")[0]
                    .upper(),
                    "fileCreatedAt": datetime(year=2020, month=1, day=1).isoformat(),
                    "fileModifiedAt": datetime(year=2020, month=1, day=1).isoformat(),
                    "isFavorite": "false",
                    "fileExtension": os.path.splitext(name)[1].lstrip("."),
                },
                timeout=60,
            )
            if not response.ok:
                progress.log("Uploading...", name)
                progress.log(f"[red]HTTP {response.status_code} {response.reason}")
                progress.log(response.text)
                response.raise_for_status()
            else:
                data = response.json()
                if data["duplicate"]:
                    progress.log(f"Duplicate uploaded {name}")
                else:
                    progress.log(
                        f"Uploaded in {response.elapsed} {name} id: {data['id']}"
                    )
        fle.close()


class FixName(object):
    def __init__(self, file_, name):
        self._file = file_
        self.name = name

    def __getattr__(self, attr):
        return getattr(self._file, attr)


def iterate_tarfile(tarfle: tarfile.TarFile):
    item = tarfle.next()
    while item:
        yield item
        item = tarfle.next()


if __name__ == "__main__":
    cli()
