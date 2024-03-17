import os
import json
import tarfile
from typing import Iterator
from rich.progress import Progress

from .local_file import LocalFile
from .processed_file_tracker import ProcessedFileTracker
from .report import Report

MAX_NAME_LENGTH = 90


def normalise_filename(filename: str) -> tuple[str, bool]:
    """
    Removes the .json extension so the metadata can be matched with the original file

    NOTE: Will not handle the truncating due to the 90 character max length.
    See: fix_truncated_name()
    """
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
    if not filename.endswith(")") and ")." not in filename:
        return ""
    _, remainder = filename.rsplit("(", 1)
    if os.path.extsep in remainder:
        remainder, _ = os.path.splitext(remainder)
    return "(" + remainder


def rebuild_numbered_filename(filename: str, number: str, new_ext: str) -> str:
    """
    For use to go from MP file to jpg file with numbers
    Eg, test(2).MP -> test.MP(2).jpg
    """
    base, remainder = filename.rsplit("(", 1)
    _, original_extension = remainder.split(")", 1)
    return base + original_extension + number + new_ext


def fix_truncated_name(filename, metadata) -> str:
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


def cleanup_motion_videos(tar_infos: dict[str, any], seen: set[str]):
    names = set(tar_infos.keys())
    for full_fname in names:
        fname, ext = os.path.splitext(full_fname)
        if ext.lower() == ".mp4":
            # Samsung motion photos
            if fname.endswith(")") and "(" in fname:
                # Numbered file, move the number
                jpeg_name = rebuild_numbered_filename(
                    fname,
                    number=extract_number_from_filename(fname),
                    new_ext=".jpg",
                )
            else:
                jpeg_name = fname + ".jpg"
            if jpeg_name in seen:
                del tar_infos[full_fname]
        if ext in (".MP", ".MP ") or ext.startswith(".MP~"):
            # Google pixel motion photos
            if fname.endswith(")") and "(" in fname:
                # Numbered file, move the number
                jpeg_name = rebuild_numbered_filename(
                    full_fname,
                    number=extract_number_from_filename(fname),
                    new_ext=".jpg",
                )
            else:
                jpeg_name = full_fname + ".jpg"
            if jpeg_name in seen:
                del tar_infos[full_fname]
        if fname.endswith("-edited"):
            # Google photos edited, only keep the original
            del tar_infos[full_fname]


def match_files(
    was_metadata: bool,
    filename: str,
    tar_infos: dict[str, tuple[tarfile.TarFile, tarfile.TarInfo]],
    metadata: dict[str, dict],
) -> tuple[tuple[tarfile.TarFile, tarfile.TarInfo] | None, dict | None]:
    if filename in tar_infos and filename in metadata:
        return tar_infos[filename], metadata[filename]
    if was_metadata and not os.path.splitext(filename)[1]:
        # Had no file extension, lets see if the file had jpg
        filename_jpg = os.path.extsep.join([filename, "jpg"])
        if filename in metadata and filename_jpg in tar_infos:
            return tar_infos[filename_jpg], metadata[filename]
    if not was_metadata and os.path.splitext(filename)[1]:
        # Let's see if we can match by dropping the file extension
        filename_noext, _ = os.path.splitext(filename)
        if filename_noext in metadata and filename in tar_infos:
            return tar_infos[filename], metadata[filename_noext]
    return None, None


def extract_metadata(
    tars: list[tarfile.TarFile],
    skip: ProcessedFileTracker,
    progress: Progress,
    report: Report,
) -> Iterator[LocalFile]:
    metadata: dict[str, dict] = dict()
    seen: set[str] = set()
    tar_infos: dict[str, tuple[tarfile.TarFile, tarfile.TarInfo]] = {}
    unmatched_max = 1
    unmatched_taskid = progress.add_task(description="Unmatched Files", total=None)
    for tar in progress.track(tars):
        if not progress.disable:
            # Skip this for unit tests
            tar.fileobj = progress.wrap_file(
                tar.fileobj,
                total=os.path.getsize(tar.name),
                description=os.path.basename(tar.name),
            )
        tar_name = os.path.basename(tar.name)
        progress.log(f"Processing [bold blue]{tar_name}")
        for tarinfo in iterate_tarfile(tar):
            filename, was_metadata = normalise_filename(tarinfo.name)
            if filename in skip or filename.endswith("archive_browser.html"):
                continue
            if was_metadata:
                data = json.load(tar.extractfile(tarinfo))
                filename = fix_truncated_name(filename, data)
                metadata[filename] = data
                metadata[filename]["tar_name"] = tar_name
                metadata[filename]["metadata_filename"] = filename
            else:
                tar_infos[filename] = (tar, tarinfo)
                unmatched_max = max(len(tar_infos), unmatched_max)
                progress.update(
                    unmatched_taskid, completed=len(tar_infos), total=unmatched_max
                )
            matched_data_file, matched_metadata = match_files(
                was_metadata=was_metadata,
                filename=filename,
                tar_infos=tar_infos,
                metadata=metadata,
            )
            if matched_data_file is not None and matched_metadata is not None:
                tarfle, data_file_tarinfo = matched_data_file
                local_file = LocalFile(
                    takeout_metadata=matched_metadata,
                    tarinfo=data_file_tarinfo,
                    fileobj=tarfle.extractfile(data_file_tarinfo),
                    tarfile_name=os.path.basename(tarfle.name),
                )
                yield local_file
                del tar_infos[data_file_tarinfo.name]
                del metadata[matched_metadata["metadata_filename"]]
                seen.add(filename)
                report.report_matched(local_file)
        progress.log(
            f"Dangling metadata: {len(metadata)}, Dangling files: {len(tar_infos)}"
        )

    progress.log("Finished extracting files")
    progress.log("Cleaning up unmatched files")
    cleanup_motion_videos(tar_infos, seen)
    if len(metadata):
        progress.log(f"[yellow]⚠ Metadata dangling: {len(metadata)}")
        report.report_hanging_metadata(metadata=metadata)
    if len(tar_infos):
        progress.log(f"[yellow]⚠ Files dangling: {len(tar_infos)}")
        report.report_hanging_files(tar_infos=tar_infos)
    progress.log(f"[green]Matched {len(seen)} files")
