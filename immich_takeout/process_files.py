from collections.abc import Iterator
import re
import math
from piexif import load, GPSIFD, ExifIFD

from datetime import datetime, timedelta
import pytz
from tzwhere import tzwhere

from rich.progress import Progress
from rich.filesize import decimal

from immich_takeout.local_file import LocalFile
from immich_takeout.report import Report

DATETIME_STR_FORMAT = "%Y:%m:%d %H:%M:%S"
TZ_GUESSER = tzwhere.tzwhere()
MAX_NAME_LENGTH = 90


def process_files(
    items: Iterator[LocalFile], progress: Progress, report: Report
) -> Iterator[LocalFile]:
    for item in progress.track(items, description="Processing files"):
        # Filter to only your images
        if item.is_from_partner_sharing:
            report.report_partner_sharing(item)
            continue

        metadata_time = item.metadata_original_timestamp

        if item.file_extension.lower() not in (".jpeg", ".jpg"):
            if metadata_time != item.last_modified:
                item.timestamp_differs = True
            if item.file_extension.lower() in (".vob", ".thm"):
                progress.log(f"[red]Skipping unsupported file extension {item.name}")
                report.report_unsupported_extension(item)
                continue
            progress.log(
                f"{metadata_time.isoformat()} {'[bright_black]ORIG[/]'} - {item.name} - {decimal(item.file_size)}"
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
            f"{new_timestamp.isoformat()} {'[bright_magenta]UPDT[/]' if timestamp_differs else '[bright_black]ORIG[/]'} - {item.name} - {decimal(item.file_size)}"
        )

        yield item


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
    try:
        exif_time = datetime.strptime(
            exif_data["Exif"][ExifIFD.DateTimeOriginal].decode("ascii"),
            DATETIME_STR_FORMAT,
        )
    except ValueError:
        return None
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
    try:
        return dms_to_dd(exif_data["GPS"])
    except ZeroDivisionError:
        return None


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
