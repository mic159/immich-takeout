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
import resource
from piexif import load, GPSIFD, ExifIFD, insert, dump

from datetime import datetime, timezone, timedelta
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

DATETIME_STR_FORMAT = "%Y:%m:%d %H:%M:%S"
TZ_GUESSER = tzwhere.tzwhere()
MAX_NAME_LENGTH = 90  # Google Photos truncates filenames in the tars at this size.


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
        skip = TrackProcessedFiles("uploaded.json", progress)
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


class TrackProcessedFiles(object):
    def __init__(self, filename, progress):
        self.filename = filename
        self.progress = progress
        self.items = set()
        self.read_file()

    def read_file(self):
        if os.path.exists(self.filename):
            with open(self.filename, "r") as fle:
                self.items = set(json.load(fle))

    def write_file(self):
        with open("uploaded.json", "w") as fle:
            self.progress.log(
                f"Flushing uploaded.json {len(self.items)} {len(list(self.items))}"
            )
            json.dump(list(self.items), fle)

    def add(self, name):
        self.items.add(name)
        self.progress.log(f"Recording {name} {len(self.items)}")
        if len(self.items) % 100 == 0:
            self.write_file()

    def __contains__(self, name):
        return name in self.items

    def __len__(self):
        return len(self.items)


def normalise_filename(filename: str) -> tuple[str, bool]:
    if filename.endswith(".json"):
        filename, _ = os.path.splitext(filename)
        was_metadata = True
    else:
        was_metadata = False
    if not filename.endswith(")"):
        return filename, was_metadata
    base, remainder = filename.rsplit("(", 1)
    filename, ext = os.path.splitext(base)
    return filename + "(" + remainder + ext, was_metadata


def fix_truncated_name(filename, metadata):
    """
    Google Photos truncates filenames in the tar after 90 characters.
    That means if the file extension is shorter than ".json", we need to recover data from
    within the metadata file and get the cut off characters.
    """
    original_filename = metadata["title"]
    if os.path.basename(filename) != original_filename and len(
        filename
    ) >= MAX_NAME_LENGTH - len(".json"):
        fname, ext = os.path.splitext(original_filename)
        dir = os.path.dirname(filename)
        new_filename = fname[: MAX_NAME_LENGTH - len(dir) - len(ext) - 1] + ext
        return os.path.join(dir, new_filename)
    return filename


def extract_metadata(
    tars: list[tarfile.TarFile], skip: TrackProcessedFiles, progress: Progress
) -> Iterator[tuple[str, IO[bytes], dict, int]]:
    metadata: dict[str, dict] = {}
    tar_infos: dict[str, tuple[tarfile.TarFile, tarfile.TarInfo]] = {}
    unmatched_max = 1
    unmatched_taskid = progress.add_task(description="Unmatched Files", total=None)
    for tar in progress.track(tars):
        tar.fileobj = progress.wrap_file(
            tar.fileobj,
            total=os.path.getsize(tar.name),
            description=os.path.basename(tar.name),
        )
        tar_name = os.path.basename(tar.name)
        progress.log(f"Processing [bold blue]{tar_name}")
        for tarinfo in iterate_tarfile(tar):
            filename, was_metadata = normalise_filename(tarinfo.name)
            if filename in skip:
                continue
            if was_metadata:
                data = json.load(tar.extractfile(tarinfo))
                filename = fix_truncated_name(filename, data)
                metadata[filename] = data
            else:
                tar_infos[filename] = (tar, tarinfo)
                unmatched_max = max(len(tar_infos), unmatched_max)
                progress.update(
                    unmatched_taskid, completed=len(tar_infos), total=unmatched_max
                )
            if filename in tar_infos and filename in metadata:
                if filename in skip:
                    continue
                tarfle, data_file_tarinfo = tar_infos[filename]
                yield (
                    filename,
                    tarfle.extractfile(data_file_tarinfo),
                    metadata[filename],
                    data_file_tarinfo.size,
                )
                del tar_infos[filename]
                del metadata[filename]
        progress.log(
            f"Dangling metadata: {len(metadata)}, Dangling files: {len(tar_infos)}"
        )

    progress.log("Finished extracting files")
    progress.log(f"Num files: {len(skip)}")
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
        try:
            exif_data = load(orig_binary)
        except (ValueError, TypeError) as e:
            progress.log(f"Error, uploading directly {filename} - {e}")
            yield filename, fle, filesize
            continue

        needs_rewrite, new_timestamp = check_timestamp_exif(
            exif_time=extract_exif_date(exif_data),
            exif_gps=extract_exif_gps(exif_data),
            metadata_time=datetime.fromtimestamp(
                int(data["photoTakenTime"]["timestamp"]), timezone.utc
            ),
        )
        progress.log(
            f"{new_timestamp.isoformat()} {'[bright_magenta]UPDT[/]' if needs_rewrite else '[bright_black]ORIG[/]'} - {os.path.basename(filename)}"
        )
        if needs_rewrite:
            update_exif_data(exif_data, new_timestamp)
            # Hack for thumbnail issues
            if (
                "thumbnail" in exif_data
                and exif_data["thumbnail"]
                and len(exif_data["thumbnail"]) > 64000
            ):
                progress.log(
                    f"WARN: Large thumbnail, erasing {filename} {len(exif_data['thumbnail'])}"
                )
                del exif_data["thumbnail"]
            # Write out altered images
            # Piexif must write to files by path, so it needs to be a named file
            tmp_fle = tempfile.NamedTemporaryFile("wb+", suffix=".jpg", delete=True)
            try:
                insert(dump(exif_data), orig_binary, tmp_fle.name)
            except (TypeError, ValueError) as e:
                progress.log(f"Error, uploading directly {filename} - {e}")
                tmp_fle.close()
                yield filename, fle, filesize
                continue
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


def deduplicate(
    files: Iterator[tuple[str, IO[bytes], int]],
    session: requests.Session,
    api_key: str,
    api_url: str,
    uploaded: TrackProcessedFiles,
    progress: Progress,
) -> Iterator[tuple[str, str, IO[bytes]]]:
    for chunk in chunk_iterator(files, size=30):
        start = datetime.now()
        info = [get_file_info(n, fle, size) for n, fle, size in chunk]
        end = datetime.now()
        response = session.request(
            "POST",
            url=os.path.join(api_url, "api/asset/bulk-upload-check"),
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
                    uploaded.add(name)
                    continue
                yield name, device_asset_id, fle


def upload_files(
    files: Iterator[tuple[str, IO[bytes], int]],
    api_key: str,
    api_url: str,
    dry_run: bool,
    skip: TrackProcessedFiles,
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

    for name, device_asset_id, fle in deduplicate(
        files,
        session=session,
        api_key=api_key,
        api_url=api_url,
        uploaded=skip,
        progress=progress,
    ):
        fle.seek(0)
        if not dry_run:
            response = session.request(
                "POST",
                url=os.path.join(api_url, "api/asset/upload"),
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
                skip.add(name)
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


def limit_memory(maxsize):
    soft, hard = resource.getrlimit(resource.RLIMIT_AS)
    resource.setrlimit(resource.RLIMIT_AS, (maxsize, hard))


if __name__ == "__main__":
    limit_memory(13 * 1024 * 1024 * 1024)
    cli()
