"""
This hacky script copies fetched data from a failed Prefect flow run out of the container related to the flow run
and uploads it to S3 under the given prefix.
"""

import os
import subprocess
from datetime import datetime, timezone
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
    cmd = ["docker", "cp", f"{container_id}:{source_path}", target_path]
    subprocess.run(cmd, check=True)


def upload_incompletely_fetched_data(
    container_id: str, flow_run_id: str, s3_prefix: str
):
    public_ip = get_public_ip()
    container_file_path = f"{DATA_DIR}/{flow_run_id}/fetched.jsonl"
    host_file_path = f"./{flow_run_id}_fetched.jsonl"
    output_last_modified = datetime.fromtimestamp(
        os.path.getmtime(host_file_path), tz=timezone.utc
    )
    timestamp_str = output_last_modified.strftime("%Y-%m-%d_%H-%M-%S")

    # Copy the fetched data from the container to the host
    docker_copy(container_id, container_file_path, host_file_path)

    # Compress the copied file
    compressed_path = compress_file(host_file_path, remove_input_file=True)

    # Upload the compressed file to S3
    bucket: S3Bucket = S3Bucket.load("s3-bucket")  # type: ignore
    s3_key = f"{s3_prefix}/{flow_run_id}_{timestamp_str}_{public_ip}.jsonl.zst"
    bucket.upload_from_path(compressed_path, to_path=s3_key)
    print(
        f"Uploaded output data for incomplete flow run {flow_run_id} in container {container_id} to S3 at {s3_key}."
    )
    os.remove(compressed_path)


if __name__ == "__main__":
    parser = ArgumentParser(
        description="Upload incompletely fetched data from a Prefect flow run to S3."
    )
    parser.add_argument("container_id", type=str, help="Docker container ID or name")
    parser.add_argument("flow_run_id", type=str, help="Prefect flow run ID")
    parser.add_argument("s3_prefix", type=str, help="S3 prefix to upload the data to")

    args = parser.parse_args()
    upload_incompletely_fetched_data(
        container_id=args.container_id,
        flow_run_id=args.flow_run_id,
        s3_prefix=args.s3_prefix,
    )
