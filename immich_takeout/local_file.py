import os.path
import tarfile
import hashlib
import mimetypes
from datetime import datetime, timezone


class LocalFile(object):
    def __init__(
        self,
        takeout_metadata: dict,
        tarinfo: tarfile.TarInfo,
        fileobj,
        tarfile_name: str,
    ):
        self.archive_filename = tarfile_name
        self.filename_from_archive = tarinfo.name
        self.takeout_metadata = takeout_metadata
        self.file_size = tarinfo.size
        self.file_obj = fileobj
        # NOTE: Assuming Google Takeout archive mtimes are UTC?
        self.last_modified = datetime.fromtimestamp(tarinfo.mtime, tz=timezone.utc)
        self.name = takeout_metadata.get("title") or self.filename_from_archive
        if os.path.extsep not in self.name:
            # Some files seem to have no file extension in the "title", but do in the original filename.
            # Add it back in.
            _, ext = os.path.splitext(self.filename_from_archive)
            if ext:
                self.name += ext

        # Metadata
        self.original_time = self.metadata_original_timestamp or self.last_modified
        self.gps = None
        self.exif_original_time = None
        self.timestamp_differs = False

    @property
    def device_asset_id(self):
        return f"{self.name.replace(' ', '')}-{self.file_size}"

    @property
    def file_sha1(self):
        self.file_obj.seek(0)
        return hashlib.file_digest(self.file_obj, "sha1").hexdigest()

    @property
    def is_from_partner_sharing(self):
        return "fromPartnerSharing" in self.takeout_metadata.get(
            "googlePhotosOrigin", {}
        )

    @property
    def metadata_original_timestamp(self):
        if "photoTakenTime" not in self.takeout_metadata:
            return None
        return datetime.fromtimestamp(
            int(self.takeout_metadata["photoTakenTime"]["timestamp"]), timezone.utc
        )

    @property
    def metadata_gps(self) -> tuple[float, float] | None:
        if "geoData" in self.takeout_metadata:
            if self.takeout_metadata["geoData"]["latitude"]:
                lat = self.takeout_metadata["geoData"]["latitude"]
                longitude = self.takeout_metadata["geoData"]["longitude"]
                return lat, longitude
        if "geoDataExif" in self.takeout_metadata:
            if self.takeout_metadata["geoDataExif"]["latitude"]:
                lat = self.takeout_metadata["geoDataExif"]["latitude"]
                longitude = self.takeout_metadata["geoDataExif"]["longitude"]
                return lat, longitude
        return None

    @property
    def file_extension(self):
        return os.path.splitext(self.name)[1]

    @property
    def is_archived(self) -> bool:
        return self.filename_from_archive.startswith("Takeout/Google Photos/Archive/")
