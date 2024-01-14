import unittest
from datetime import datetime
from piexif import ExifIFD

from main import check_timestamp_exif, extract_exif_date, normalise_filename


DATETIME_STR_FORMAT = "%Y:%m:%d %H:%M:%S"
ISOFORMAT = "%Y-%m-%d %H:%M:%S%z"


class TestTimeLogic(unittest.TestCase):
    def mock_data(
        self, timestamp: str | None, metadata_datetime: str, timezone: str = None
    ) -> tuple[dict, datetime]:
        return (
            {
                "Exif": {
                    **(
                        {ExifIFD.DateTimeOriginal: timestamp.encode("ascii")}
                        if timestamp
                        else {}
                    ),
                    **(
                        {ExifIFD.OffsetTimeOriginal: timezone.encode("ascii")}
                        if timezone
                        else {}
                    ),
                }
            },
            datetime.strptime(metadata_datetime, ISOFORMAT),
        )

    def test_same_time(self):
        exif_datetime, metadata_datetime = self.mock_data(
            timestamp="2022:12:19 15:05:31",
            timezone="+11:00",
            metadata_datetime="2022-12-19 04:05:31+00:00",
        )

        change, new_time = check_timestamp_exif(
            exif_time=extract_exif_date(exif_datetime),
            metadata_time=metadata_datetime,
        )
        self.assertFalse(change)
        self.assertEqual(metadata_datetime, new_time)

    def test_no_time(self):
        exif_datetime, metadata_datetime = self.mock_data(
            timestamp=None,
            metadata_datetime="2022-12-19 04:05:31+00:00",
        )

        change, new_time = check_timestamp_exif(
            exif_time=extract_exif_date(exif_datetime),
            metadata_time=metadata_datetime,
        )
        self.assertTrue(change)
        self.assertEqual(metadata_datetime, new_time)

    def test_timezone_fix(self):
        exif_datetime, metadata_datetime = self.mock_data(
            timestamp="2018:09:23 17:42:21",
            metadata_datetime="2018-09-23 21:42:21+00:00",
        )

        change, new_time = check_timestamp_exif(
            exif_time=extract_exif_date(exif_datetime),
            metadata_time=metadata_datetime,
        )
        self.assertTrue(change)
        self.assertEqual(new_time.isoformat(" "), "2018-09-23 17:42:21-04:00")

    def test_date_wrong_no_tz(self):
        exif_datetime, metadata_datetime = self.mock_data(
            timestamp="2018:09:21 19:53:05",
            metadata_datetime="2018-09-23 15:19:41+00:00",
        )

        change, new_time = check_timestamp_exif(
            exif_time=extract_exif_date(exif_datetime),
            metadata_time=metadata_datetime,
        )
        self.assertTrue(change)
        self.assertEqual(new_time.isoformat(" "), "2018-09-23 15:19:41+00:00")

    def test_moved_date_with_timezone(self):
        exif_datetime, metadata_datetime = self.mock_data(
            timestamp="2018:09:21 19:53:05",
            timezone="+11:00",
            metadata_datetime="2018-09-23 15:19:41+00:00",
        )

        change, new_time = check_timestamp_exif(
            exif_time=extract_exif_date(exif_datetime),
            metadata_time=metadata_datetime,
        )
        self.assertTrue(change)
        self.assertEqual(new_time.isoformat(" "), "2018-09-24 02:19:41+11:00")


class TestMetadataMatching(unittest.TestCase):
    def test_normalised_numbered(self):
        self.assertEqual(
            normalise_filename(
                "Takeout/Google Photos/Photos from 2022/PXL_20221220_060913910.jpg(1).json"
            ),
            (
                "Takeout/Google Photos/Photos from 2022/PXL_20221220_060913910(1).jpg",
                True,
            ),
        )
        self.assertEqual(
            normalise_filename(
                "Takeout/Google Photos/Photos from 2022/PXL_20221220_060913910(1).jpg"
            ),
            (
                "Takeout/Google Photos/Photos from 2022/PXL_20221220_060913910(1).jpg",
                False,
            ),
        )

    def test_normalised_no_change(self):
        self.assertEqual(
            normalise_filename(
                "Takeout/Google Photos/Photos from 2022/PXL_20221220_060913910.jpg.json"
            ),
            ("Takeout/Google Photos/Photos from 2022/PXL_20221220_060913910.jpg", True),
        )
        self.assertEqual(
            normalise_filename(
                "Takeout/Google Photos/Photos from 2022/PXL_20221220_060913910.jpg"
            ),
            (
                "Takeout/Google Photos/Photos from 2022/PXL_20221220_060913910.jpg",
                False,
            ),
        )


if __name__ == "__main__":
    unittest.main()
