import json
from datetime import datetime, timezone
from typing import Any, Callable
from prefect import task
from prefect.runtime.flow_run import get_id
from prefect_aws import S3Bucket
import os
import shutil
from pydantic import BaseModel
from contextlib import ExitStack

from utils.zstd import compress_file
from utils.public_ip import get_public_ip

DATA_DIR = "./tmp/prefect_task_data"


class RunMetaConfig(BaseModel):
    metadata: dict
    """
    Additional metadata about a data processingrun to be uploaded to S3. Arbitrary, JSON-serializable data.
    """

    s3_prefix: str
    """
    The prefix under which the metadata about the data processing run should be uploaded to S3.

    NOTE: This is usually different from the prefix under which the processed data should be uploaded
    (as only files with the same data schema should be stored under the same prefix; usually run metadata differs in shape from the actual processing results)

    The uploaded file will have the format <timestamp-of-run-start>_<public_ip>_<flow_run_id>.jsonl.zst.
    """


def _preprocess_for_write(data, timestamp_key: str = "observed_at") -> dict | list:
    now_str = datetime.now(timezone.utc).isoformat()
    if isinstance(data, list):
        return [_preprocess_for_write(item) for item in data]
    elif isinstance(data, dict) and timestamp_key not in data:
        data[timestamp_key] = now_str
    else:
        data = {
            "data": data,
            timestamp_key: now_str,
        }
    return data


@task(name="Process data and write results to file")
def _process_inputs_and_write_outputs[T](
    inputs: list[T],
    processing_fn: Callable[[T], Any],
    flow_run_data_dir: str,
    timestamp_key: str = "observed_at",
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
                print(
                    f"Data already processed for {already_processed} inputs, skipping"
                )

    processed_data_fp = os.path.join(flow_run_data_dir, "processed_outputs.jsonl")
    failed_inputs_fp = os.path.join(flow_run_data_dir, "failed_inputs.jsonl")

    with ExitStack() as stack:
        f_in = stack.enter_context(open(processed_inputs_fp, "a"))
        f_out = stack.enter_context(open(processed_data_fp, "a"))
        f_in_failed = stack.enter_context(open(failed_inputs_fp, "a"))
        for input_el in inputs:
            try:
                processed_data = processing_fn(input_el)
                if processed_data is None:
                    print(f"No data for input {input_el}")
                    f_in.write(str(input_el) + "\n")
                    f_in.flush()
                    continue
                data_to_write = _preprocess_for_write(processed_data, timestamp_key)
                if isinstance(data_to_write, list):
                    for item in data_to_write:
                        f_out.write(json.dumps(item) + "\n")
                else:
                    f_out.write(json.dumps(data_to_write) + "\n")
                f_out.flush()
                f_in.write(str(input_el) + "\n")
                f_in.flush()
            except Exception as e:
                print(f"Error processing input {input_el}: {e}")
                if isinstance(input_el, dict) or isinstance(input_el, list):
                    try:
                        input_el = json.dumps(input_el)
                    except Exception as e:
                        print(f"Error dumping input {input_el}: {e}")
                        input_el = str(input_el)
                else:
                    input_el = {"input": str(input_el)}

                f_in_failed.write(str(input_el) + "\n")
                f_in_failed.flush()

    return processed_data_fp, failed_inputs_fp


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


def process_and_upload_data[T](
    inputs: list[T],
    processing_fn: Callable[[T], Any],
    outputs_s3_prefix: str,
    failures_s3_prefix: str | None = None,
    timestamp_key: str = "observed_at",
    run_meta_config: RunMetaConfig | None = None,
):
    public_ip = get_public_ip()
    bucket = S3Bucket.load("s3-bucket")

    flow_run_id = get_id()
    flow_run_data_dir = os.path.join(DATA_DIR, flow_run_id)
    if not os.path.exists(flow_run_data_dir):
        os.makedirs(flow_run_data_dir)

    if run_meta_config:
        raw_meta_path = os.path.join(flow_run_data_dir, "run_meta_config.json")
        with open(raw_meta_path, "w") as f:
            json.dump(run_meta_config.model_dump(), f)
        run_meta_s3_key = f"{run_meta_config.s3_prefix}/{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}_{public_ip}_{flow_run_id}.jsonl.zst"
        _compress_and_upload_file(raw_meta_path, bucket, s3_key=run_meta_s3_key)

    output_file, failed_inputs_file = _process_inputs_and_write_outputs(
        inputs, processing_fn, flow_run_data_dir, timestamp_key=timestamp_key
    )

    if os.path.getsize(output_file) == 0:
        print("No data processed successfully :(")
    else:
        output_last_modified = datetime.fromtimestamp(
            os.path.getmtime(output_file), tz=timezone.utc
        )
        output_s3_key = f"{outputs_s3_prefix}/{output_last_modified.strftime('%Y-%m-%d_%H-%M-%S')}_{public_ip}_{flow_run_id}.jsonl.zst"
        _compress_and_upload_file(output_file, bucket, s3_key=output_s3_key)

    if failures_s3_prefix:
        if os.path.getsize(failed_inputs_file) == 0:
            print("No failures occurred, so nothing to upload :)")
        else:
            failed_inputs_s3_key = f"{failures_s3_prefix}/{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}_{public_ip}_{flow_run_id}.jsonl.zst"
            _compress_and_upload_file(
                failed_inputs_file, bucket, s3_key=failed_inputs_s3_key
            )

    # if this is reached, we know that everything has gone well and we can delete any remaining files
    shutil.rmtree(flow_run_data_dir)
