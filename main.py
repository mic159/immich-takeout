from collections.abc import Iterator
import argparse
import tarfile
import os.path
import os
import requests
from requests.adapters import HTTPAdapter, Retry
import resource
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
from rich.filesize import decimal

from immich_takeout.processed_file_tracker import ProcessedFileTracker
from immich_takeout.metadata_matching import extract_metadata
from immich_takeout.local_file import LocalFile
from immich_takeout.report import Report
from immich_takeout.utils import FixName
from immich_takeout.process_files import process_files

DATETIME_STR_FORMAT = "%Y:%m:%d %H:%M:%S"
TZ_GUESSER = tzwhere.tzwhere()
MAX_NAME_LENGTH = 90

# TODO:
# Sidecar / XMP / XML building, to make it a single request
# Move Exif & image time guessing
# Concurrent uploading, queueing


def cli():
    parser = argparse.ArgumentParser(
        description="CLI command to upload google takeout tars directly to immich without extracting"
    )
    parser.add_argument(
        "files",
        help="list of google takeout tar.gz files",
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
        skip = ProcessedFileTracker("uploaded.json")
        skip.read_file()
        progress.log(f"Loaded {len(skip)} skipped files")
        report = Report("report.csv")
        try:
            upload_files(
                process_files(
                    extract_metadata(
                        tars=tars,
                        skip=skip,
                        progress=progress,
                        report=report,
                    ),
                    progress=progress,
                    report=report,
                ),
                api_key=args.api_key,
                api_url=args.api_url,
                dry_run=args.dry_run,
                progress=progress,
                skip=skip,
            )
        finally:
            skip.write_file()
            report.close()


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
                total=2,
                backoff_factor=1,
                allowed_methods=("GET", "HEAD", "POST"),
                status_forcelist=[500, 502, 503, 504, 408, 429],
            ),
        ),
    )

    for item in files:
        if not dry_run:
            item.file_obj.seek(0)
            # with progress.console.status()
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
                    "fileCreatedAt": item.original_time.isoformat(),
                    "fileModifiedAt": item.last_modified.isoformat(),
                    "isFavorite": "false",
                    "isArchived": str(item.is_archived).lower(),
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
                    progress.log(f"[yellow]Duplicate rejected[/yellow] {item.name}")
                else:
                    progress.log(
                        f"[green]✔ Uploaded[/green] {decimal(item.file_size)} in {response.elapsed}"
                    )
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


def limit_memory(maxsize):
    soft, hard = resource.getrlimit(resource.RLIMIT_AS)
    resource.setrlimit(resource.RLIMIT_AS, (maxsize, hard))


if __name__ == "__main__":
    limit_memory(13 * 1024 * 1024 * 1024)
    cli()
