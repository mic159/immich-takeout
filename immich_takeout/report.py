from csv import DictWriter
import tarfile
import os.path

from .local_file import LocalFile


class Report:
    def __init__(self, filename, disable=False):
        if not disable:
            self.fle = open(filename, "w")
            self.writer = DictWriter(
                self.fle,
                fieldnames=(
                    "file",
                    "archive_metadata",
                    "archive_file",
                    "state",
                    "photo_taken_time",
                ),
            )
            self.writer.writeheader()
        else:
            self.fle = None
            self.writer = None

    def report_matched(self, fle: LocalFile):
        if not self.writer:
            return
        self.writer.writerow(
            {
                "file": fle.filename_from_archive,
                "archive_metadata": fle.takeout_metadata.get("tar_name"),
                "archive_file": fle.archive_filename,
                "state": "matched",
                "photo_taken_time": fle.original_time.isoformat(),
            }
        )

    def report_hanging_metadata(self, metadata: dict[str, dict]):
        if not self.writer:
            return
        self.writer.writerows(
            {
                "file": name,
                "archive_metadata": data["tar_name"],
                "state": "Dangling metadata",
            }
            for name, data in metadata.items()
        )

    def report_hanging_files(
        self, tar_infos: dict[str, tuple[tarfile.TarFile, tarfile.TarInfo]]
    ):
        if not self.writer:
            return
        self.writer.writerows(
            {
                "file": name,
                "archive_file": os.path.basename(tar.name),
                "state": "Dangling file",
            }
            for name, (tar, _) in tar_infos.items()
        )

    def report_skipped(self, filename: str, archive_filename: str, reason: str):
        if not self.writer:
            return
        self.writer.writerow(
            {
                "file": filename,
                "archive_file": archive_filename,
                "state": f"Skipped, {reason}",
            }
        )

    def report_unsupported_extension(self, fle: LocalFile):
        if not self.writer:
            return
        self.writer.writerow(
            {
                "file": fle.filename_from_archive,
                "archive_metadata": fle.takeout_metadata.get("tar_name"),
                "archive_file": fle.archive_filename,
                "state": "Skipped, Unsupported",
                "photo_taken_time": fle.original_time.isoformat(),
            }
        )

    def report_partner_sharing(self, fle: LocalFile):
        if not self.writer:
            return
        self.writer.writerow(
            {
                "file": fle.filename_from_archive,
                "archive_metadata": fle.takeout_metadata.get("tar_name"),
                "archive_file": fle.archive_filename,
                "state": "Skipped, Partner Sharing",
                "photo_taken_time": fle.original_time.isoformat(),
            }
        )

    def close(self):
        if self.fle:
            self.fle.close()
