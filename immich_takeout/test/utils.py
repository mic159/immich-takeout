import io
import tarfile
import json
import os.path


BLANK_IMAGE_FILENAME = os.path.join(os.path.dirname(__file__), "Blank.jpg")


def create_mock_metadata(
    filename: str, meta_title: str
) -> tuple[tarfile.TarInfo, io.BytesIO]:
    bytes_fle = io.BytesIO()
    json_string = json.dumps(
        obj={
            "title": meta_title,
        }
    )
    bytes_fle.write(json_string.encode())
    tarinfo = tarfile.TarInfo(name=filename)
    tarinfo.size = bytes_fle.tell()
    bytes_fle.seek(0)
    return tarinfo, bytes_fle


def create_mock_image(filename: str) -> tuple[tarfile.TarInfo, io.BytesIO]:
    tarinfo = tarfile.TarInfo(name=filename)
    fd = io.BytesIO()
    with open(BLANK_IMAGE_FILENAME, "rb") as image_fle:
        fd.write(image_fle.read())
    tarinfo.size = fd.tell()
    fd.seek(0)
    image_fle.close()
    return tarinfo, fd


def create_mock_archive(
    mock_files: list[tuple[tarfile.TarInfo, io.BytesIO]]
) -> tarfile.TarFile:
    fd = io.BytesIO()
    tar = tarfile.open(fileobj=fd, mode="w")
    for tarinfo, fle in mock_files:
        tar.addfile(tarinfo=tarinfo, fileobj=fle)
    fd.seek(0)
    return tarfile.open(fileobj=fd, name="test.tar")
