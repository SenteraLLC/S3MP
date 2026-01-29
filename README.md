# S3 Mirror Path Utilities
Utilities to aid in interaction with S3 objects by using a local mirror of the S3 bucket.
## Primary Classes
### MirrorPath
A path connecting an S3 Key to a local file. Provides utilities to produce the corresponding path/key, check existance, transfer, load/save data, and interact with the key's locality.
### KeySegment
S3 buckets are technically flat storage, but use `/` to delimit "folders". This class provides a representation of each segment of a key, and optionally it's depth.
### S3MPConfig
Global settings while using the package. Most S3 interactions take place within the same bucket, use the same callbacks, etc, so these are stored here.
## Primary Functions
### get_matching_s3_keys
Provide a List of `KeySegment`s to match against, and this function will find all matching keys. The List does not need to be complete, e.g. you can match several key sections, have a gap, and then match several more key sections at a greater depth. This gives flexibility when processing on a lot of data stored at a set structure but spread out across many paths.
### replace_key_segments
Provide a list of `KeySegment`s, and replace those within an existing key. Most processing jobs take an input path, and the output is offset a change to only a few key segments, which this function handles easily.
`MirrorPath` also owns a call to this function which returns a new `MirrorPath` with the new key.
### replace_key_segments_at_relative_depth
Provide a list of `KeySegment`s, and replace those within an existing key, but at depths relative to the length of the key. This is useful when you want to replace the last few segments of a key, but don't know how many segments are in the key.
`MirrorPath` also owns a call to this function which returns a new `MirrorPath` with the new key.
### `MirrorPath` locality functions: `get_sibling`, `get_parent`, `get_child`
Provide a file name to any of these and get a `MirrorPath` relative to the current file. This calls `replace_key_segments_at_relative_depth` under the hood, but is far more user-friendly than that function for small replacements.

### Image Metadata Parsing
For image files stored in S3, you can parse EXIF metadata including camera parameters, GPS coordinates, and sensor orientation. This is useful for aerial imagery processing where you need to calculate ground sample distance (GSD) or other photogrammetric properties.

**Note:** Image metadata parsing requires optional dependencies. Install with:
```bash
uv sync --extra image
```

```python
from S3MP.mirror_path import MirrorPath

# Create a MirrorPath to an image
image_mp = MirrorPath.from_s3_key("...IMG_1234.jpg")

# Parse metadata directly from MirrorPath
metadata = image_mp.parse_image_metadata()

# Access parsed metadata
print(f"Image size: {metadata.width}x{metadata.height}")
print(f"Camera: {metadata.sensor_make} {metadata.sensor_model}")
print(f"Location: ({metadata.latitude}, {metadata.longitude})")
print(f"Altitude: {metadata.altitude}m")

# Compute GSD at image center
if metadata.altitude and metadata.focal_length:
    center_coords = (metadata.width / 2, metadata.height / 2)
    gsd = metadata.compute_gsd(center_coords)
    print(f"GSD at center: {gsd} mm/pixel")

    # Or use the mirror_path function, which calls the same metadata method
    gsd = image_mp.compute_gsd(center_coords)

# Compute nadir angle (angle from directly below camera)
nadir_angle = metadata.compute_nadir_angle(center_coords)
print(f"Nadir angle: {nadir_angle} degrees")
```

The image will be automatically downloaded to the local mirror if not already present before parsing.


## Example Use Case
Consider a processing project with the following folder structure:

For input images:
```
[Year]/[Month]/[Day]/[Images with HHMM Stamp]
```
With some example files:
```
2016/02/01/IMG_0800.png
2020/01/01/IMG_1245.png
2020/01/02/IMG_0800.png
```
One task could involve these images getting processed into an output structure:
```
[Year]/[Month]/Processed/[Images with DD_HHMM_processed Stamp]
```
With some example files:
```
2016/02/processed/01_0800_processed.png
2020/01/processed/01_1245_processed.png
2020/01/processed/02_0800_processed.png
```


The project could specify it's structure with the following `KeySegment` constants:
```
class RootSegments:
    YEAR = KeySegment(0)
    MONTH = KeySegment(1)

class RawImageSemgnets:
    DAY = KeySegment(2)
    IMAGE_FILES = KeySegment(3, is_file=True)

class ProcessedImageSegments:
    PROCESSED_DIR = KeySegment(2, name="processed")
    PROCESSED_IMAGE_FILES = KeySegment(3, is_file=True)
```

Given some function `process`, the script to run on images from 2020 would be:
```
segments = [
    ImageDirSegments.YEAR("2020"),
    ImageDirSegments.IMAGE_FILES(".png")
]
for s3_key in get_matching_s3_keys(segments):
    # Setup Paths
    image_mp = MirrorPath.from_s3_key(key)
    input_segments = get_segments_from_key(s3_key)
    _, _, day_seg, img_fn_seg = input_segments
    output_mp = image_mp.replace_key_segments(
        [
            ProcessedImageSegments.PROCESSED_DIR,
            ProcessedImageSegments.PROCESSED_IMAGE_FILES(
                f"{day_seg.name}_{img_fn_seg.name}_processed.png"
            )
        ]
    )

    # Download
    image_mp.download_to_mirror()

    # Process
    process(image_mp.local_path, output_mp.local_path)

    # Upload
    output_mp.upload_from_mirror()
```
Although the "Setup Paths" section looks a little dense, overall this example is doing a lot of work. This grabs every key from the base `2020` directory down to every `.png` file at the specified depth, produces the new path (and key) based on the scheme for each one, and then downloads, processes, and uploads each file. As a bonus, all of the processed files remain in the local mirror, and all download functions have `overwrite` parameters, so this can act as a cache if necessary.


## Environmental Setup

To specify the local directory to store the mirror at, use the `set_env_mirror_root` function in `global_config.py`. This will create a `.env` file in the root of the package, and will be loaded on import.
If no mirror root is specified and no `.env` file is found, a temporary directory will be used.

## Installation
[uv](https://docs.astral.sh/uv/) is a fast, cross-platform Python package installer and resolver.

1) [Set up SSH](https://github.com/SenteraLLC/install-instructions/blob/master/ssh_setup.md)
2) Install [uv](https://docs.astral.sh/uv/getting-started/installation/)
3) Install package

        git clone git@github.com:SenteraLLC/S3MP.git
        cd S3MP
        uv sync

    To install with dev dependencies (for testing and development):

        uv sync --dev

    To install with image metadata parsing dependencies (requires Sentera internal packages):

        uv sync --extra image

4) Set up ``pre-commit`` to ensure all commits to adhere to style conventions.

        uv run pre-commit install
