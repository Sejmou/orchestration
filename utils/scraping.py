import json
from datetime import datetime, timezone
from typing import Any, Callable
from prefect import task
from prefect_aws import S3Bucket
from utils.zstd import compress_file
import os

DATA_DIR = "/tmp/prefect_task_data"


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


@task(name="Fetch data and write to file")
def _fetch_and_write_data[
    T
](inputs: list[T], fetch_fn: Callable[[T], Any], flow_run_data_dir: str):
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

    with open(processed_inputs_fp) as f_in:
        with open(fetched_data_fp, "a") as f_out:
            for input_el in inputs:
                fetched_data = fetch_fn(input_el)
                data_to_write = _preprocess_for_write(fetched_data)
                if isinstance(data_to_write, list):
                    for item in data_to_write:
                        f_out.write(json.dumps(item) + "\n")
                else:
                    f_out.write(json.dumps(data_to_write) + "\n")
                f_out.flush()
                f_in.write(str(input_el) + "\n")
                f_in.flush()


@task(name="Compress with zstd and upload to S3")
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


def fetch_and_upload_data[
    T
](flow_run_id: str, inputs: list[T], fetch_fn: Callable[[T], Any], s3_prefix: str):
    bucket = S3Bucket.load("s3-bucket")

    flow_run_data_dir = os.path.join(DATA_DIR, flow_run_id)
    if not os.path.exists(flow_run_data_dir):
        os.makedirs(flow_run_data_dir)

    _fetch_and_write_data(inputs, fetch_fn, flow_run_data_dir)

    output_file = os.path.join(flow_run_data_dir, "fetched.jsonl")

    output_last_modified = datetime.fromtimestamp(
        os.path.getmtime(output_file), tz=timezone.utc
    )
    s3_key = (
        f"{s3_prefix}/{output_last_modified.strftime('%Y-%m-%d_%H-%M-%S')}.jsonl.zst"
    )
    _compress_and_upload_file(output_file, bucket, s3_key=s3_key)
