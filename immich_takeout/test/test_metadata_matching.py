import unittest
from rich.progress import Progress

from immich_takeout.metadata_matching import (
    normalise_filename,
    fix_truncated_name,
    extract_metadata,
    cleanup_motion_videos,
)
from immich_takeout.processed_file_tracker import ProcessedFileTracker
from immich_takeout.report import Report
from .utils import create_mock_archive, create_mock_metadata, create_mock_image


DATETIME_STR_FORMAT = "%Y:%m:%d %H:%M:%S"
ISOFORMAT = "%Y-%m-%d %H:%M:%S%z"


MOCK_PROGRESS = Progress(auto_refresh=False, disable=True)


class TestMetadataMatching(unittest.TestCase):
    def test_normalised_numbered(self):
        meta_filename = (
            "Takeout/Google Photos/Photos from 2022/PXL_20221220_060913910.jpg(1).json"
        )
        image_filename = (
            "Takeout/Google Photos/Photos from 2022/PXL_20221220_060913910(1).jpg"
        )
        self.assertEqual(
            normalise_filename(meta_filename),
            (image_filename, True),
        )
        self.assertEqual(
            normalise_filename(image_filename),
            (image_filename, False),
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

    def test_normalised_max_size(self):
        meta_filename = "Takeout/Google Photos/Photos from 2023/story_image_v2_336d088f-fbe5-43a1-b765-58c29b9.json"
        image_filename = "Takeout/Google Photos/Photos from 2023/story_image_v2_336d088f-fbe5-43a1-b765-58c29b9a.jpg"
        meta_data = {
            "title": "story_image_v2_336d088f-fbe5-43a1-b765-58c29b9a5b2f_640_wide.jpg",
        }
        self.assertEqual(
            fix_truncated_name(normalise_filename(meta_filename)[0], meta_data),
            image_filename,
        )
        self.assertEqual(
            normalise_filename(image_filename),
            (image_filename, False),
        )

    def test_cleanup_motion_pictures_google(self):
        seen = {"Takeout/Google Photos/Photos from 2020/PXL_20201115_044452482.MP.jpg"}
        tar_infos = {
            "Takeout/Google Photos/Photos from 2020/PXL_20201115_044452482.MP": None
        }
        cleanup_motion_videos(tar_infos=tar_infos, seen=seen)
        self.assertNotIn(
            "Takeout/Google Photos/Photos from 2020/PXL_20201115_044452482.MP",
            tar_infos,
        )

    def test_cleanup_motion_pictures_google_numbered(self):
        seen = {
            "Takeout/Google Photos/Photos from 2020/PXL_20201115_044452482.MP(1).jpg"
        }
        tar_infos = {
            "Takeout/Google Photos/Photos from 2020/PXL_20201115_044452482(1).MP": None
        }
        cleanup_motion_videos(tar_infos=tar_infos, seen=seen)
        self.assertNotIn(
            "Takeout/Google Photos/Photos from 2020/PXL_20201115_044452482(1).MP",
            tar_infos,
        )

    def test_normalised_max_size_with_number(self):
        meta_filename = "Takeout/Google Photos/Photos from 2023/story_video_10719a13-534f-4c77-9fe7-0a92a3186d(1).json"
        image_filename = "Takeout/Google Photos/Photos from 2023/story_video_10719a13-534f-4c77-9fe7-0a92a3186da(1).mp4"
        meta_data = {
            "title": "story_video_10719a13-534f-4c77-9fe7-0a92a3186da5_720_high.mp4",
        }
        self.assertEqual(
            fix_truncated_name(normalise_filename(meta_filename)[0], meta_data),
            image_filename,
        )
        self.assertEqual(
            normalise_filename(image_filename),
            (image_filename, False),
        )


class TestMetadataMatchingArchives(unittest.TestCase):
    def test_missing_file_extension_image_first(self):
        archive = create_mock_archive(
            [
                create_mock_image(
                    filename="Takeout/Google Photos/Photos from 2014/2014-04-30.jpg"
                ),
                create_mock_metadata(
                    filename="Takeout/Google Photos/Photos from 2014/2014-04-30.json",
                    meta_title="2014-04-30",
                ),
            ]
        )
        actual = extract_metadata(
            tars=[archive],
            progress=MOCK_PROGRESS,
            skip=ProcessedFileTracker("test.json"),
            report=Report("test.csv", disable=True),
        )
        actual = list(actual)
        self.assertEqual(len(actual), 1)
        self.assertEqual(
            "Takeout/Google Photos/Photos from 2014/2014-04-30.jpg",
            actual[0].filename_from_archive,
        )

    def test_missing_file_extension_metadata_first(self):
        archive = create_mock_archive(
            [
                create_mock_metadata(
                    filename="Takeout/Google Photos/Photos from 2014/2014-04-30.json",
                    meta_title="2014-04-30",
                ),
                create_mock_image(
                    filename="Takeout/Google Photos/Photos from 2014/2014-04-30.jpg"
                ),
            ]
        )
        actual = extract_metadata(
            tars=[archive],
            progress=MOCK_PROGRESS,
            skip=ProcessedFileTracker("test.json"),
            report=Report("test.csv", disable=True),
        )
        actual = list(actual)
        self.assertEqual(len(actual), 1)
        self.assertEqual(
            "Takeout/Google Photos/Photos from 2014/2014-04-30.jpg",
            actual[0].filename_from_archive,
        )

    def test_matching_file_numbered(self):
        archive = create_mock_archive(
            [
                create_mock_metadata(
                    filename="Takeout/Google Photos/Photos from 2020/PXL_20200930_080707712.PORTRAIT-01.COVER.jpg.json",
                    meta_title="PXL_20200930_080707712.PORTRAIT-01.COVER.jpg",
                ),
                create_mock_image(
                    filename="Takeout/Google Photos/Photos from 2020/PXL_20200930_080707712.PORTRAIT-01.COVER.jpg"
                ),
                create_mock_metadata(
                    filename="Takeout/Google Photos/Photos from 2020/PXL_20200930_080707712.PORTRAIT-01.COVER.jpg(1).json",
                    meta_title="PXL_20200930_080707712.PORTRAIT-01.COVER.jpg",
                ),
                create_mock_image(
                    filename="Takeout/Google Photos/Photos from 2020/PXL_20200930_080707712.PORTRAIT-01.COVER(1).jpg"
                ),
            ]
        )
        actual = extract_metadata(
            tars=[archive],
            progress=MOCK_PROGRESS,
            skip=ProcessedFileTracker("test.json"),
            report=Report("test.csv", disable=True),
        )
        actual = list(actual)
        print([f.name for f in actual])
        self.assertEqual(len(actual), 2)
