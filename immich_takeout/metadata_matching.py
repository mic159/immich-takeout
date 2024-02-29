import os
import json
import tarfile
from typing import Iterator
from rich.progress import Progress

from .local_file import LocalFile
from .processed_file_tracker import ProcessedFileTracker

MAX_NAME_LENGTH = 90


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


def extract_number_from_filename(filename: str) -> str:
    if not filename.endswith(")"):
        return ""
    _, remainder = filename.rsplit("(", 1)
    return "(" + remainder


def fix_truncated_name(filename, metadata):
    """
    For use on the ".json" metadata files.
    Google Photos truncates filenames in the tar after 90 characters.
    That means if the file extension is shorter than ".json", we need to recover data from
    within the metadata file and get the cut off characters.
    """
    original_filename = metadata["title"]
    if os.path.basename(filename) != original_filename and len(
        filename
    ) >= MAX_NAME_LENGTH - len(".json"):
        fname, ext = os.path.splitext(original_filename)
        dirname = os.path.dirname(filename)
        # The number on a duplicated filename is ontop of the truncated name, and needs adding back in.
        numbered_mark = extract_number_from_filename(filename)
        new_filename = (
            fname[: MAX_NAME_LENGTH - len(dirname) - len(ext) - 1] + numbered_mark + ext
        )
        return os.path.join(dirname, new_filename)
    return filename


def iterate_tarfile(tarfle: tarfile.TarFile):
    item = tarfle.next()
    while item:
        yield item
        item = tarfle.next()


def extract_metadata(
    tars: list[tarfile.TarFile], skip: ProcessedFileTracker, progress: Progress
) -> Iterator[LocalFile]:
    metadata: dict[str, dict] = dict()
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
            if filename.endswith(".MP") or filename.endswith("archive_browser.html"):
                # MD files are duplicated data from inside the jpeg, no need to process them
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
                tarfle, data_file_tarinfo = tar_infos[filename]
                yield LocalFile(
                    filename,
                    metadata[filename],
                    data_file_tarinfo,
                    tarfle.extractfile(data_file_tarinfo),
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
    with open("missing.json", "w") as fle:
        json.dump(
            {
                "metadata": list(metadata.keys()),
                "files": list(tar_infos.keys()),
            },
            fle,
        )
