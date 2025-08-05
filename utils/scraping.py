import json
from datetime import datetime, timezone
from typing import Any, Callable
from prefect import task
from prefect_aws import S3Bucket
import os
import shutil
import time

from utils.zstd import compress_file
from utils.public_ip import get_public_ip

DATA_DIR = "./tmp/prefect_task_data"


def _preprocess_for_write(data) -> dict | list:
    now_str = datetime.now(timezone.utc).isoformat()
    if isinstance(data, list):
        return [_preprocess_for_write(item) for item in data]
    elif isinstance(data, dict) and "observed_at" not in data:
        data["observed_at"] = now_str
    else:
        data = {
            "data": data,
            "observed_at": now_str,
        }
    return data


def compute_eta_seconds(
    start_time: float, processed_count: int, total: int
) -> float | None:
    """
    Compute estimated remaining time (ETA) in seconds.

    Args:
        start_time: The timestamp (in seconds) when processing started (e.g., from time.time()).
        processed_count: The number of items processed so far.
        total: The total number of items to process.

    Returns:
        Estimated remaining time in seconds, or None if progress cannot be determined yet (e.g., if no items have been processed).
    """
    if processed_count <= 0 or total <= 0:
        return
    if processed_count > total:
        # this should not be possible, but don't raise Exception if it happens; instead, exit early
        print("Processed count exceeds total count, cannot compute ETA.")
        return

    elapsed = time.time() - start_time
    progress = processed_count / total

    if progress == 0:
        return

    eta_seconds = (elapsed / progress) - elapsed
    return max(0.0, eta_seconds)  # avoid negative ETA


def eta_str(seconds: float | None) -> str:
    if seconds is None:
        return "N/A"
    seconds = int(seconds)
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0 or hours > 0:  # show minutes if hours or minutes exist
        parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")

    return " ".join(parts)


@task(log_prints=True)
def _fetch_and_write_data[T](
    inputs: list[T], fetch_fn: Callable[[T], Any], flow_run_data_dir: str
):
    processed_inputs_fp = os.path.join(flow_run_data_dir, "processed_inputs.txt")
    inputs_len_initial = len(inputs)
    print(f"Got {inputs_len_initial} inputs")
    if os.path.exists(processed_inputs_fp):
        with open(processed_inputs_fp, "r") as f_out:
            processed_inputs = set([line for line in f_out.readlines()])
            inputs = [el for el in inputs if str(el) not in processed_inputs]
            inputs_len_after_filtering = len(inputs)
            already_processed = inputs_len_initial - inputs_len_after_filtering
            if already_processed:
                print(f"Data already fetched for {already_processed} inputs, skipping")

    fetched_data_fp = os.path.join(flow_run_data_dir, "fetched.jsonl")

    start = time.time()
    last_log = start
    total = len(inputs)
    with open(processed_inputs_fp, "a") as f_in:
        with open(fetched_data_fp, "a") as f_out:
            for i, input_el in enumerate(inputs):
                item_no = i + 1
                fetched_data = fetch_fn(input_el)
                if fetched_data is None:
                    print(f"No data for input {input_el}")
                    f_in.write(str(input_el) + "\n")
                    f_in.flush()
                    continue
                data_to_write = _preprocess_for_write(fetched_data)
                if isinstance(data_to_write, list):
                    for item in data_to_write:
                        f_out.write(json.dumps(item) + "\n")
                else:
                    f_out.write(json.dumps(data_to_write) + "\n")
                f_out.flush()
                f_in.write(str(input_el) + "\n")
                f_in.flush()

                now = time.time()
                if now - last_log > 20 or i == total:
                    percent = 100 * i / total
                    eta = eta_str(
                        compute_eta_seconds(start, processed_count=item_no, total=total)
                    )
                    print(f"Progress: {i}/{total} ({percent:.1f}%), ETA: {eta}")
                    last_log = now


@task(log_prints=True)
def _compress_and_upload_file(file_path: str, bucket: S3Bucket, s3_key: str):
    compressed_path = compress_file(file_path, remove_input_file=True)
    bucket.upload_from_path(
        compressed_path,
        to_path=s3_key,
    )
    print(
        f"File {compressed_path} compressed and uploaded to {s3_key} in S3 bucket {bucket}."
    )
    return compressed_path


@task(log_prints=True)
def fetch_and_upload_data[T](
    flow_run_id: str, inputs: list[T], fetch_fn: Callable[[T], Any], s3_prefix: str
):
    public_ip = get_public_ip()
    bucket: S3Bucket = S3Bucket.load("s3-bucket")  # type: ignore

    flow_run_data_dir = os.path.join(DATA_DIR, flow_run_id)
    if not os.path.exists(flow_run_data_dir):
        os.makedirs(flow_run_data_dir)

    _fetch_and_write_data(inputs, fetch_fn, flow_run_data_dir)

    output_file = os.path.join(flow_run_data_dir, "fetched.jsonl")

    output_last_modified = datetime.fromtimestamp(
        os.path.getmtime(output_file), tz=timezone.utc
    )
    s3_key = f"{s3_prefix}/{output_last_modified.strftime('%Y-%m-%d_%H-%M-%S')}_{public_ip}_{flow_run_id}.jsonl.zst"
    _compress_and_upload_file(output_file, bucket, s3_key=s3_key)

    # if this is reached, we know that everything has gone well and we can delete any remaining files
    shutil.rmtree(flow_run_data_dir)
