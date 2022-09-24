"""S3 key modification utilities."""
import itertools
from dataclasses import dataclass
from typing import List, Tuple
from S3MP.prefix_queries import get_folders_within_folder, get_files_within_folder


@dataclass
class KeySegment:
    """S3 key segment."""

    depth: int
    name: str = None
    is_file: bool = False  # Most things are folders.

    def __call__(self, *args, **kwargs):
        """Set data via calling."""
        if len(args) == 1 and type(args[0]) == str:
            self.name = args[0]
        if "name" in kwargs:
            self.name = kwargs["name"] 

        return self  # For chaining 

def get_arbitrary_keys_from_names(names: List[str]) -> List[KeySegment]:
    """Get arbitrary keys from a list of names."""
    return [KeySegment(depth=idx, name=name) for idx, name in enumerate(names)]

def get_segments_from_key(key: str) -> List[KeySegment]:
    """Get segments from a key."""
    return [KeySegment(depth=idx, name=name) for idx, name in enumerate(key.split("/"))]

def build_s3_key(segments: List[KeySegment]) -> Tuple[str, int]:
    """Build an S3 key from a list of segments."""
    segments = sorted(segments, key=lambda x: x.depth)
    empty_depths = [
        depth
        for depth in range(segments[-1].depth + 1)
        if depth not in [seg.depth for seg in segments]
    ]
    depth = empty_depths[0] if empty_depths else segments[-1].depth + 1
    path = "/".join([seg.name for seg in segments[: depth]])
    return path, depth


def replace_key_segments(key: str, segments: List[KeySegment]) -> str:
    """Replace segments of a key with new segments."""
    if type(segments) == KeySegment:
        segments = [segments]
    segments = sorted(segments, key=lambda x: x.depth)
    key_segments = key.split("/")
    for segment in segments:
        key_segments[segment.depth] = segment.name
    return "/".join(key_segments)


def replace_key_segments_at_relative_depth(key: str, segments: List[KeySegment]) -> str:
    """
    Replace segments of a key with new segments at a relative depth.
    0 would be the deepest segment, -1 would be the second deepest, etc.
    """
    if type(segments) == KeySegment:
        segments = [segments]
    segments = sorted(segments, key=lambda x: x.depth)
    key_segments = [seg for seg in key.split("/") if seg]
    og_key_len = len(key_segments)
    for segment in segments:
        new_depth = segment.depth + og_key_len - 1
        if new_depth >= len(key_segments):
            key_segments.append("")
        key_segments[new_depth] = segment.name
    return "/".join(key_segments)



def unpack_s3_obj_generator(path: str, filter_name: str, is_file: bool):
    """Produce generator for S3 objects, and then unpack it. Used for multiprocessing."""
    if is_file:
        objs_at_depth = get_files_within_folder(path, filter_name)
    else:
        objs_at_depth = get_folders_within_folder(path, filter_name)
    return [f"{path}{obj}" for obj in objs_at_depth]


def get_filter_name(segments: List[KeySegment], current_depth: int) -> str:
    """Get the filter name for the current depth."""
    return (
        segs_at_current_depth[0].name
        if (
            segs_at_current_depth := [
                seg for seg in segments if seg.depth == current_depth
            ]
        )
        else None
    )


async def dfs_matching_key_gen(
    segments: List[KeySegment], path: str = None, current_depth: int = None
):
    """Generate all matching keys from a path, depth first."""
    if current_depth is None:
        segments = sorted(segments, key=lambda x: x.depth)
        path, current_depth = build_s3_key(segments)

    filter_name = get_filter_name(segments, current_depth)
    file_search_flag = (current_depth == segments[-1].depth) and (segments[-1].is_file)
    paths_at_depth = unpack_s3_obj_generator(path, filter_name, file_search_flag)
    if current_depth == segments[-1].depth:
        for path in paths_at_depth:
            yield path
    n_paths = len(paths_at_depth)
    if n_paths == 0:
        return 
    
    for path in paths_at_depth:
        async for matching_key in dfs_matching_key_gen(segments, path, current_depth + 1):
            yield matching_key


def sync_dfs_matching_key_gen(
    segments: List[KeySegment], path: str = None, current_depth: int = None
):
    """Synchronous generation of all matching keys from a path, depth first."""
    if current_depth is None:
        segments = sorted(segments, key=lambda x: x.depth)
        path, current_depth = build_s3_key(segments)

    filter_name = get_filter_name(segments, current_depth)
    file_search_flag = (current_depth == segments[-1].depth) and (segments[-1].is_file)
    paths_at_depth = unpack_s3_obj_generator(path, filter_name, file_search_flag)
    if current_depth == segments[-1].depth:
        yield from paths_at_depth
    n_paths = len(paths_at_depth)
    if n_paths == 0:
        return 
    
    for path in paths_at_depth:
        yield from sync_dfs_matching_key_gen(segments, path, current_depth + 1)


def get_matching_s3_keys(segments: List[KeySegment]) -> List[str]:
    """
    Get all S3 keys matching the given segments.

    Find the maximum uninterupted prefix, and search individual folders, pruning when possible.
    """
    segments = sorted(segments, key=lambda x: x.depth)
    max_depth = segments[-1].depth

    # We can only filter on segments with names
    segment_depths = [segment.depth for segment in segments if segment.name]
    empty_depths = [
        depth for depth in range(max_depth + 1) if depth not in segment_depths
    ]

    prefix_len = empty_depths[0] if empty_depths else len(segments)
    initial_prefix = "/".join([seg.name for seg in segments[: prefix_len]])
    current_paths = [initial_prefix]
    for current_depth in range(prefix_len, max_depth + 1):
        # Determine if there is a filter for the current depth.
        filter_name = get_filter_name(segments, current_depth)
        # Search for files at max depth
        file_search_flag = (current_depth == max_depth) and (segments[-1].is_file)
        new_paths = [
            unpack_s3_obj_generator(path, filter_name, file_search_flag)
            for path in current_paths
        ]
        current_paths = itertools.chain(*new_paths)

    return list(current_paths)
