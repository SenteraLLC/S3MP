"""Utilities for loading local files."""

import json
from pathlib import Path


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


def load_json(path: str) -> dict:
    """Load a json file."""
    with open(path) as f:
        result: dict = json.load(f)
        return result


DEFAULT_LOAD_LEDGER = {
    "json": load_json,
}


# Save functions
def save_json(path: str, data: dict, indent: int = 4):
    """Save a json file."""
    with open(path, "w") as f:
        json.dump(data, f, indent=indent)


DEFAULT_SAVE_LEDGER = {
    "json": save_json,
}
