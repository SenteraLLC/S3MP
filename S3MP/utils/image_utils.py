"""Image metadata parsing and GSD computation utilities."""

from __future__ import annotations

import numpy as np
from imgparse import MetadataParser
from imgparse.types import Dimensions, Euler, WorldCoords
from numpy.typing import NDArray

from S3MP.mirror_path import MirrorPath


class ImageMetadata:
    """Container for image metadata parsed from image files."""

    def __init__(
        self,
        mirror_path: MirrorPath,
        dimensions: Dimensions,
        sensor_make: str,
        sensor_model: str,
        coordinates: WorldCoords | None = None,
        rotation: Euler | None = None,
        focal_length: float | None = None,
        altitude: float | None = None,
        distortion_params: list[float] | None = None,
    ):
        """Initialize ImageMetadata object.

        Args:
            mirror_path: MirrorPath object to the image
            dimensions: Image dimensions (height, width)
            sensor_make: Camera sensor manufacturer
            sensor_model: Camera sensor model
            coordinates: Geographic coordinates (lat, lon) if available
            rotation: Euler angles (roll, pitch, yaw) if available
            focal_length: Focal length in pixels if available
            altitude: Altitude in meters if available
            distortion_params: Distortion parameters if available
        """
        self.mirror_path = mirror_path
        self.name = self.mirror_path.local_path.stem

        self.height = dimensions.height
        self.width = dimensions.width
        self.sensor_make = sensor_make
        self.sensor_model = sensor_model

        self.latitude: float | None = None
        self.longitude: float | None = None
        if coordinates is not None:
            self.latitude = coordinates.lat
            self.longitude = coordinates.lon

        self.roll: float | None = None
        self.pitch: float | None = None
        self.yaw: float | None = None
        if rotation is not None:
            self.roll = rotation.roll
            self.pitch = rotation.pitch
            self.yaw = rotation.yaw

        self.focal_length = focal_length
        self.altitude = altitude
        self.distortion_params = distortion_params

    @classmethod
    def parse_metadata(cls, mirror_path: MirrorPath) -> ImageMetadata:
        """Initialize ImageMetadata object by parsing metadata from image file.

        Args:
            mirror_path: MirrorPath object to the image file

        Returns:
            ImageMetadata object with parsed metadata
        """
        # Download image to mirror if not present for metadata parsing
        mirror_path.download_to_mirror_if_not_present()

        parser = MetadataParser(mirror_path.local_path)
        return cls(
            mirror_path,
            parser.dimensions(),
            parser.make(),
            parser.model(),
            parser.location(),
            parser.rotation(),
            parser.focal_length_pixels(),
            parser.relative_altitude(),
            parser.distortion_parameters,
        )

    @property
    def rotation_mat(self) -> NDArray[np.float64]:
        """Return the rotation matrix of the sensor.

        Returns:
            3x3 rotation matrix

        Raises:
            ValueError: If rotation values are not set
        """
        if self.roll is None or self.pitch is None or self.yaw is None:
            raise ValueError("Rotation values must be set")

        sensor_roll = np.radians(self.roll)
        sensor_pitch = np.radians(self.pitch)
        sensor_yaw = np.radians(self.yaw)

        roll_mat = np.array(
            [
                [1, 0, 0],
                [0, np.cos(sensor_roll), -np.sin(sensor_roll)],
                [0, np.sin(sensor_roll), np.cos(sensor_roll)],
            ],
            dtype=np.float64,
        )
        pitch_mat = np.array(
            [
                [np.cos(sensor_pitch), 0, np.sin(sensor_pitch)],
                [0, 1, 0],
                [-np.sin(sensor_pitch), 0, np.cos(sensor_pitch)],
            ],
            dtype=np.float64,
        )
        yaw_mat = np.array(
            [
                [np.cos(sensor_yaw), -np.sin(sensor_yaw), 0],
                [np.sin(sensor_yaw), np.cos(sensor_yaw), 0],
                [0, 0, 1],
            ],
            dtype=np.float64,
        )
        return yaw_mat @ pitch_mat @ roll_mat

    @property
    def intrinsics_mat(self) -> NDArray[np.float64]:
        """Return the intrinsics matrix of the sensor.

        Returns:
            3x3 intrinsics matrix

        Raises:
            ValueError: If focal length is not set
        """
        if self.focal_length is None:
            raise ValueError("Focal length must be set")

        return np.array(
            [
                [self.focal_length, 0, self.width / 2],
                [0, self.focal_length, self.height / 2],
                [0, 0, 1],
            ],
            dtype=np.float64,
        )

    def pixel_ray(self, x: float, y: float) -> NDArray[np.float64]:
        """Compute the pixel ray of a point in the image from the center of the camera.

        Args:
            x: Pixel x coordinate
            y: Pixel y coordinate

        Returns:
            3D ray vector from camera center through pixel
        """
        rbd2frd = np.array(
            [
                [0, -1, 0],
                [1, 0, 0],
                [0, 0, 1],
            ],
            dtype=np.float64,
        )

        homogeneous_coords = np.array([x, y, 1], dtype=np.float64)
        return self.rotation_mat @ rbd2frd @ np.linalg.inv(self.intrinsics_mat) @ homogeneous_coords

    def compute_gsd(self, coords: tuple[float, float]) -> float:
        """Compute the GSD (ground sample distance) in mm for a point in the image.

        GSD represents the real-world distance represented by one pixel in the image.

        Args:
            coords: (x, y) pixel coordinates in the image

        Returns:
            GSD value in millimeters

        Raises:
            ValueError: If altitude or focal length is not set
        """
        if self.altitude is None or self.focal_length is None:
            raise ValueError("Altitude and focal length must be set")

        pixel_ray = self.pixel_ray(coords[0], coords[1])

        # The z component of the pixel ray is the scaling factor for the pixel's effective focal length
        return float(round(self.altitude / (pixel_ray[2] * self.focal_length) * 1000, 2))

    def compute_nadir_angle(self, coords: tuple[float, float]) -> float:
        """Compute the angle of a point in the image relative to nadir.

        Nadir is the point on the ground directly below the camera.

        Args:
            coords: (x, y) pixel coordinates in the image

        Returns:
            Angle in degrees from nadir (0 = directly below camera)
        """
        pixel_ray = self.pixel_ray(coords[0], coords[1])
        return float(np.degrees(np.arccos(pixel_ray[2] / np.linalg.norm(pixel_ray))))


def compute_gsd(mirror_path: MirrorPath, coords: tuple[float, float]) -> float:
    """Compute the GSD (ground sample distance) in mm for a point in an image.

    GSD represents the real-world distance represented by one pixel in the image.
    This function parses the image metadata and calculates GSD based on camera
    parameters and altitude.

    Args:
        mirror_path: MirrorPath object to the image file
        coords: (x, y) pixel coordinates in the image

    Returns:
        GSD value in millimeters

    Raises:
        ValueError: If altitude or focal length is not available in image metadata
    """
    metadata = ImageMetadata.parse_metadata(mirror_path)
    return metadata.compute_gsd(coords)
