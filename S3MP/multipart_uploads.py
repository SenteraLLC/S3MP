"""S3MP multipart uploads."""
# import asyncio
# from multiprocessing import Process, Queue
import concurrent.futures
import math
from S3MP.async_utils import sync_gather_threads
from S3MP.global_config import S3MPConfig
from S3MP.transfer_configs import MB

from S3MP.mirror_path import MirrorPath


# TODO prefix optimization
def get_mpu(mirror_path: MirrorPath):
    """Check if a multipart upload has started."""
    bucket = mirror_path._get_bucket()
    mpus = bucket.multipart_uploads.all()
    for mpu in mpus:
        if mpu.key == mirror_path.s3_key:
            if list(mpu.parts.all()):
                return mpu
            mpu.abort()  # Abort empty uploads


def resume_multipart_upload(
    mirror_path: MirrorPath,
    max_threads: int = 30,
):
    """Start or resume a multipart upload from a mirror path."""
    mpu = get_mpu(mirror_path)
    if not mpu:
        print("Multipart upload not found, starting new one.")
        return mirror_path.upload_from_mirror_if_not_present()

    mpu_parts = list(mpu.parts.all())
    mpu_parts.sort(key=lambda part: part.part_number)

    total_size_bytes = mirror_path.get_size_bytes(on_s3=False)
    part_size = mpu_parts[0].size
    n_total_parts = math.ceil(total_size_bytes / part_size)
    n_uploaded_parts = len(mpu_parts)

    mpu_dict = {
        "Parts": [
            {"ETag": part.e_tag, "PartNumber": part.part_number} for part in mpu_parts
        ]
    }

    with open(mirror_path.local_path, "rb") as f:
        f.seek(part_size * n_uploaded_parts)
        thread_futures = []
        uploaded_parts = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
            for part_number in range(n_uploaded_parts + 1, n_total_parts + 1):
                current_data = f.read(part_size)
                # print(
                #     f"Currrent data size: {len(current_data)}, part size: {part_size}"
                # )
                part = mpu.Part(part_number)
                uploaded_parts.append(part)
                thread_futures.append(executor.submit(part.upload, Body=current_data))
            for u_part, thread_future in zip(uploaded_parts, thread_futures):
                mpu_dict["Parts"].append(
                    {
                        "ETag": thread_future.result()["ETag"],
                        "PartNumber": u_part.part_number,
                    }
                )

    mpu.complete(
        MultipartUpload={
            "Parts": [
                {"ETag": part.e_tag, "PartNumber": part.part_number}
                for part in mpu_parts
            ]
        }
    )
