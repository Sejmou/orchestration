"""
This hacky script copies fetched data from a failed Prefect flow run out of the container related to the flow run
and uploads it to S3 under the given prefix.
"""

import os
import subprocess
from datetime import datetime, timedelta, timezone
from argparse import ArgumentParser

from prefect_aws import S3Bucket

from utils.zstd import compress_file
from utils.public_ip import get_public_ip
from utils.scraping import DATA_DIR


def docker_copy(container_id: str, source_path: str, target_path: str) -> None:
    """
    Copies files/directories from a Docker container to the host.

    Args:
        container_id: The ID or name of the Docker container.
        source_path: Path inside the container to copy from.
        target_path: Host path to copy to.

    Example:
        docker_copy("5ad852a7dc03", "/app/tmp", "./5ad852a7dc03/")
    """
    print(
        f"Copying from container {container_id} at {source_path} to host at {target_path}"
    )
    cmd = ["docker", "cp", f"{container_id}:{source_path}", target_path]
    print(f"Running command: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def get_subdirs(path: str) -> list[str]:
    return [
        name for name in os.listdir(path) if os.path.isdir(os.path.join(path, name))
    ]


def extract_timestamp_from_filename(filename: str) -> datetime:
    timestamp_str = "_".join(
        filename.split("_")[:2]
    )  # first two parts of the filename are expected to be date and time
    try:
        return datetime.strptime(timestamp_str, "%Y-%m-%d_%H-%M-%S").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        try:
            # parse just the date part if attempt with date and time fails
            return datetime.strptime(timestamp_str.split("_")[0], "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            raise ValueError(f"Could not extract timestamp from filename: {filename}")


def size_bytes_human_readable(size: int) -> str:
    """Convert bytes to a human-readable format."""
    out = float(size)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if out < 1024:
            return f"{out:.2f} {unit}"
        out /= 1024
    return f"{out:.2f} PB"


def upload_incompletely_fetched_data(container_id: str, s3_prefix: str):
    public_ip = get_public_ip()
    container_dir_path = f"/app/{DATA_DIR}/."  # /. is required to copy the _contents_ of the directory rather than the directory itself
    host_dir_path = (
        f"{os.path.dirname(__file__)}/tmp/docker_container_task_data/{container_id}/"
    )
    os.makedirs(host_dir_path, exist_ok=True)

    docker_copy(container_id, container_dir_path, host_dir_path)

    flow_run_dirs = get_subdirs(host_dir_path)
    if len(flow_run_dirs) != 1:
        raise ValueError(
            f"Expected exactly one flow run subdirectory, found {len(flow_run_dirs)}: {flow_run_dirs}"
        )
    flow_run_id = flow_run_dirs[0]
    flow_data_path = os.path.join(host_dir_path, flow_run_id, "fetched.jsonl")
    output_last_modified = datetime.fromtimestamp(
        os.path.getmtime(flow_data_path), tz=timezone.utc
    )

    # Compress the copied file
    compressed_path = compress_file(flow_data_path, remove_input_file=True)
    print(
        f"Compressed data to {compressed_path} ({size_bytes_human_readable(os.path.getsize(compressed_path))})"
    )

    # parse timestamp from existing files under prefix
    bucket: S3Bucket = S3Bucket.load("s3-bucket")  # type: ignore
    existing_files: list[dict] = bucket.list_objects(s3_prefix)  # type: ignore
    keys = [f["Key"] for f in existing_files]
    filenames = [os.path.basename(key) for key in keys]

    # check if flow_run_id already exists in any of the filenames; return the confliciting filename if it does
    conflicting_files = [f for f in existing_files if flow_run_id in f["Key"]]
    if conflicting_files:
        # add human-readable size to the conflicting files
        for f in conflicting_files:
            f["Size_human_readable"] = size_bytes_human_readable(f["Size"])
        raise ValueError(
            f"Flow run ID {flow_run_id} already exists in S3 under prefix {s3_prefix}. "
            f"Conflicting file(s): {conflicting_files}"
        )

    print(f"Found existing files: {filenames}")
    if not keys:
        timestamp_str = output_last_modified.strftime("%Y-%m-%d_%H-%M-%S")
        print(
            f"No existing files found, using last modified timestamp string in S3 key: {timestamp_str}"
        )
    else:
        print(f"{len(filenames)} existing files found in S3 under prefix {s3_prefix}.")
        lexicographically_largest_file = max(filenames)
        print(f"Lexicographically largest file: {lexicographically_largest_file}")
        existing_timestamp = extract_timestamp_from_filename(
            lexicographically_largest_file
        )
        print(f"Extracted timestamp: {existing_timestamp}")

        if output_last_modified <= existing_timestamp:
            # use largest existing timestamp + 1 second as timestamp_str to make it lexicographically larger
            # if it isn't larger, import will not be triggered for file after upload
            timestamp_str = (existing_timestamp + timedelta(seconds=1)).strftime(
                "%Y-%m-%d_%H-%M-%S"
            )
            print(
                f"Using adjusted timestamp string {timestamp_str} (largest existing + 1 second) for S3 upload to make it lexicographically larger than existing files (otherwise import won't trigger)."
            )
        else:
            timestamp_str = output_last_modified.strftime("%Y-%m-%d_%H-%M-%S")
            print(f"Using last modified timestamp string for S3 key: {timestamp_str}")

    # Upload the compressed file to S3
    s3_key = (
        f"{s3_prefix}/{timestamp_str}_{public_ip}_{flow_run_id}_incomplete.jsonl.zst"
    )
    bucket.upload_from_path(compressed_path, to_path=s3_key)
    os.remove(compressed_path)


if __name__ == "__main__":
    parser = ArgumentParser(
        description="Upload incompletely fetched data from a Prefect flow run to S3."
    )
    parser.add_argument("container_id", type=str, help="Docker container ID or name")
    parser.add_argument("s3_prefix", type=str, help="S3 prefix to upload the data to")

    args = parser.parse_args()
    upload_incompletely_fetched_data(
        container_id=args.container_id,
        s3_prefix=args.s3_prefix,
    )
