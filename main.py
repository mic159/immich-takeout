from collections.abc import Iterator
import argparse
import tarfile
import os.path
import json
from typing import IO, BinaryIO
import requests
import mimetypes
import tempfile

from exif import Image, DATETIME_STR_FORMAT
from datetime import datetime

from plum.exceptions import UnpackError
from rich.progress import (
    BarColumn,
    Progress,
    MofNCompleteColumn,
    TextColumn,
    TimeElapsedColumn,
)


def cli():
    parser = argparse.ArgumentParser(
        description='CLI command to upload google takeout tars directly to immich without extracting'
    )
    parser.add_argument(
        'files',
        help='list of google taketout tar.gz files',
        type=argparse.FileType('rb'),
        nargs='+'
    )
    args = parser.parse_args()

    tars = [
        tarfile.open(fileobj=f)
        for f in args.files
    ]

    progress = Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
    )
    with progress:
        upload_files(
            process_files(
                extract_metadata(
                    tars=tars,
                    progress=progress
                ),
                progress=progress
            ),
            progress=progress
        )


def extract_metadata(tars: list[tarfile.TarFile], progress: Progress) -> Iterator[tuple[str, BinaryIO, dict]]:
    metadata: dict[str, dict] = {}
    tar_infos: dict[str, tuple[tarfile.TarFile, tarfile.TarInfo]] = {}
    seen = set()
    for tar in progress.track(tars, description="Tar files"):
        tar_meta_files = 0
        tar_data_files = 0
        progress.log(f"Processing [bold blue]{os.path.basename(tar.name)}")
        for tarinfo in progress.track(iterate_tarfile(tar), description="Extracting files"):
            filename, ext = os.path.splitext(tarinfo.name)
            if ext == '.json':
                data = json.load(tar.extractfile(tarinfo))
                if filename in metadata or filename in seen:
                    progress.log(f"[red bold]❌ ERROR: Duplicate metadata found!!!")
                    progress.log(f"[red]  - filename: {filename}")
                    progress.log(f"[red]  - Tar: {tar.name}")
                    progress.log(f"[red]  - In Metadata: {filename in metadata}, In seen {filename in seen}")
                metadata[filename] = data
                tar_meta_files += 1
            else:
                filename = tarinfo.name
                tar_infos[filename] = (tar, tarinfo)
                tar_data_files += 1
            if filename in tar_infos and filename in metadata:
                archive, data_file_tarinfo = tar_infos[filename]
                yield filename, archive.extractfile(data_file_tarinfo), metadata[filename]
                del tar_infos[filename]
                del metadata[filename]
                seen.add(filename)
        progress.log(f"Dangling metadata: {len(metadata)}, Dangling files: {len(tar_infos)}")

    progress.log("Finished extracting files")
    progress.log(f"Num files: {len(seen)}")
    if len(metadata):
        progress.log(f"[yellow]⚠ Metadata dangling: {len(metadata)}")
    if len(tar_infos):
        progress.log(f"[yellow]⚠ Files dangling: {len(tar_infos)}")


def process_files(iter: Iterator[tuple[str, BinaryIO, dict]], progress: Progress) -> Iterator[str, BinaryIO]:
    skipped = set()
    for filename, fle, data in progress.track(iter, description="Processing files"):
        # Filter to only your images
        if "fromPartnerSharing" in data.get("googlePhotosOrigin", {}):
            skipped.add(filename)
        if os.path.splitext(filename)[1].lower() not in ('.jpeg', '.jpg'):
            yield filename, fle
            continue
        try:
            image = Image(fle)
        except UnpackError:
            progress.log(f"[yellow]Error reading EXIF[/yellow], uploading anyway '{filename}'")
            yield filename, fle
            continue

        try:
            exif_time = datetime.strptime(image.datetime_original, DATETIME_STR_FORMAT)
        except AttributeError:
            try:
                exif_time = datetime.strptime(image.datetime, DATETIME_STR_FORMAT)
            except AttributeError:
                exif_time = datetime.now()
        metadata_time = datetime.fromtimestamp(int(data['photoTakenTime']['timestamp']))

        if exif_time != metadata_time:
            # skipped.add(fileinfo.name)
            # progress.log(f"[yellow]Date incorrect[/yellow] {fileinfo.name}")
            # progress.log(f"   - Diff: {exif_time - metadata_time}")
            # progress.log(f"   - EXIF: {exif_time}")
            # progress.log(f"   - Metadata: {metadata_time}")
            image.datetime = metadata_time.strftime(DATETIME_STR_FORMAT)
            image.datetime_original = metadata_time.strftime(DATETIME_STR_FORMAT)
            with tempfile.TemporaryFile() as tmp:
                tmp.write(image.get_file())
                yield filename, tmp
        else:
            yield filename, fle
    progress.log(f"Skipped {len(skipped)}")

    with open('skipped.json', 'w') as datfle:
        json.dump(list(skipped), datfle)


def upload_files(files: Iterator[str, BinaryIO], progress: Progress):
    for name, fle in files:
        progress.log('Uploading...', name)
        fle.seek(0)
        response = requests.request(
            'POST',
            url='https://photos.apps.bitwarfare.net/api/asset/upload',
            headers={
                'Accept': 'application/json',
                'x-api-key': '',
            },
            files={
                'assetData': FixName(fle, name)
            },
            data={
                'deviceAssetId': os.path.basename(name).replace(' ', ''),  # `${path.basename(filePath)}-${fileStat.size}`.replace(/\s+/g, '')
                'deviceId': 'gphotos-takeout-import',
                'assetType': mimetypes.guess_type(name, strict=False)[0].split('/')[0].upper(),
                'fileCreatedAt': datetime(year=2020, month=1, day=1).isoformat(),
                'fileModifiedAt': datetime(year=2020, month=1, day=1).isoformat(),
                'isFavorite': 'false',
                'fileExtension': os.path.splitext(name)[1].lstrip('.'),
            }
        )
        if not response.ok:
            progress.log(f"[red]HTTP {response.status_code} {response.reason}")
            progress.log(response.text)
            response.raise_for_status()
        else:
            progress.log(f" - {response.text}")


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


if __name__ == '__main__':
    cli()



# [00:41:32] Processing takeout-20230105T132237Z-001.tgz                                                                                                                              main.py:57
# [01:36:03]  - Found 3728 metadata files and 3846 files                                                                                                                              main.py:69
#            Processing takeout-20230105T132237Z-002.tgz                                                                                                                              main.py:57
# [01:40:34]  - Found 251 metadata files and 280 files                                                                                                                                main.py:69
#            Processing takeout-20230105T132237Z-003.tgz                                                                                                                              main.py:57
# [02:00:08]  - Found 1185 metadata files and 1193 files                                                                                                                              main.py:69
#            Processing takeout-20230105T132237Z-004.tgz                                                                                                                              main.py:57
# [03:47:23]  - Found 12943 metadata files and 10230 files                                                                                                                            main.py:69
#            Processing takeout-20230105T132237Z-005.tgz                                                                                                                              main.py:57
# [05:36:56]  - Found 2867 metadata files and 5189 files                                                                                                                              main.py:69
#            Processing takeout-20230105T132237Z-006.tgz                                                                                                                              main.py:57
# Tar files         ━━━━━━━━━━━━━━━╺━━━━━━━━━━━━━━━━━━━━━━━━  5/13   6:29:06
# Decoding metadata ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 7574/?  6:29:06
# Decoding metadata ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 531/?   5:34:35
# Decoding metadata ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 2378/?  5:30:05
# Decoding metadata ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 23173/? 5:10:30
# Decoding metadata ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 8056/?  3:23:15
# Decoding metadata ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 9902/?  1:33:42
