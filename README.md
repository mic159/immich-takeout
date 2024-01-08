# Import Google Takeout to Immich
Import a Google Takeout tar file into Immich

This tool updates the EXIF data from your images and uploads the
resulting images to Immich directly without having to extract and
use external tools to modify them.


## Pre-work: Google takeout

Please export your Google Photos images using Google Takeout.
Select tar files (not zip).

Download all the files

## Pre-work: Un-gzip the tars

After downloading the files, they will be `.tgz` files (ie. compressed with gzip).
To make this tool faster, please un-gzip the tars first. Don't worry, the overall
size will be basically the same, as we are talking about images here, so they didn't
really get compressed anyway.

```shell
gunzip -k *.tgz
```

Explanation: The metadata `.json` files in the archives are not always in the same place as
the photo/video. To avoid having to 

## Running
Run this tool with the API information, and pass it all the tar files as arguments.

```shell
python main.py --api-key XXX --api-url https://xxx/ *.tar
```

