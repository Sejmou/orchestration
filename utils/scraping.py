import json
from datetime import datetime, timezone
from prefect import task
from prefect_aws import S3Bucket
from utils.zstd import compress_file


def _get_data_to_write(raw_data) -> dict | list:
    now_str = datetime.now(timezone.utc).isoformat()
    if isinstance(raw_data, list):
        return [_get_data_to_write(item) for item in raw_data]
    elif isinstance(raw_data, dict) and "observed_at" not in raw_data:
        raw_data["observed_at"] = now_str
    else:
        raw_data = {
            "data": raw_data,
            "observed_at": now_str,
        }
    return raw_data


def write_scraped_data_to_file(scraped_data, file_path: str):
    with open(file_path, "a") as f:
        data_to_write = _get_data_to_write(scraped_data)
        if isinstance(data_to_write, list):
            for item in data_to_write:
                f.write(json.dumps(item) + "\n")
        else:
            f.write(json.dumps(data_to_write) + "\n")
        f.flush()


@task(name="Compress with zstd and upload to S3")
def compress_and_upload_file(file_path: str, bucket: S3Bucket, s3_key: str):
    compressed_path = compress_file(file_path, remove_input_file=True)
    bucket.upload_from_path(
        compressed_path,
        to_path=s3_key,
    )
    print(
        f"File {compressed_path} compressed and uploaded to {s3_key} in S3 bucket {bucket}."
    )
    return compressed_path
