"""Utilities for loading local files."""
from pathlib import Path
from typing import Dict
from numpy.typing import ArrayLike
import json

def get_local_file_size_bytes(path: Path) -> int:
    """Get the size of a local file in bytes."""
    return path.stat().st_size


def delete_local_path(path: Path):
    """Delete a local path."""
    if path.exists():
        if path.is_dir():
            for child in path.iterdir():
                child.unlink()
            path.rmdir()
        else:
            path.unlink()

# Load functions


def load_json(path: str) -> Dict:
    """Load a json file."""
    with open(path, "r") as f:
        return json.load(f)


# def load_image(path: str) -> ArrayLike:
#     """Load an image file."""
#     return cv2.imread(path)


# def load_numpy(path: str) -> ArrayLike:
#     """Load a numpy file."""
#     return np.load(path)


DEFAULT_LOAD_LEDGER = {
    "json": load_json,
    "npy": load_numpy,
    "png": load_image,
    "jpg": load_image,
    "jpeg": load_image,
}

# Save functions


def save_json(path: str, data: Dict, indent: int = 4):
    """Save a json file."""
    with open(path, "w") as f:
        json.dump(data, f, indent=indent)


def save_numpy(path: str, data: ArrayLike):
    """Save a numpy file."""
    np.save(path, data)


def save_image(path: str, data: ArrayLike):
    """Save an image file."""
    cv2.imwrite(path, data)


DEFAULT_SAVE_LEDGER = {
    "json": save_json,
    "npy": save_numpy,
    "png": save_image,
    "jpg": save_image,
    "jpeg": save_image,
}
