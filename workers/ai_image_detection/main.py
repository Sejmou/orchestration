from io import BytesIO
from typing import Literal, cast
import PIL
from PIL import Image
from PIL.Image import Image as PILImage
import hashlib
import imagehash
import torch
import transformers
from datetime import datetime
import sys
import platform
import os
import requests
from transformers import Pipeline, pipeline
from flow_utils.flow_deployment import create_image_config
from flow_utils.batch_processing import RunMetaConfig, process_and_upload_data
from prefect import flow
from prefect_aws import AwsCredentials, S3Bucket
from pydantic import BaseModel
from pydantic import HttpUrl
import boto3
from types_boto3_s3 import Client as S3Client
from botocore.exceptions import ClientError


class EntityIdAndImageUrl(BaseModel):
    """
    Inputs for a run of the AI image detection flow, i.e. Spotify artist or album ID (depending on entity_type) and its image URL.
    """

    id: str
    image_url: HttpUrl


def extract_pipeline_metadata(pipe: Pipeline, seed_used: int):
    """Extract comprehensive reproducibility metadata from a HuggingFace pipeline"""

    model = pipe.model
    config = model.config

    # Basic model information
    metadata = {
        "model_identifier": getattr(config, "name_or_path", None),
        "architectural_class": model.__class__.__name__,
        "model_config": config.to_dict(),
    }

    # Try to get model commit hash/revision
    try:
        # Check if model has _commit_hash attribute (newer transformers versions)
        if hasattr(model, "_commit_hash"):
            metadata["model_commit_hash"] = model._commit_hash
        elif hasattr(config, "_commit_hash"):
            metadata["model_commit_hash"] = config._commit_hash
        else:
            metadata["model_commit_hash"] = None
    except:
        metadata["model_commit_hash"] = None

    # Framework and library versions
    metadata["transformers_version"] = transformers.__version__
    metadata["pil_version"] = PIL.__version__
    metadata["imagehash_version"] = imagehash.__version__

    if torch.cuda.is_available():
        metadata["pytorch_version"] = torch.__version__
        metadata["cuda_version"] = torch.version.cuda
        metadata["cudnn_version"] = (
            torch.backends.cudnn.version()
            if torch.backends.cudnn.is_available()
            else None
        )
    else:
        metadata["pytorch_version"] = torch.__version__
        metadata["cuda_version"] = None
        metadata["cudnn_version"] = None

    # Hardware specifications
    metadata["hardware"] = get_hardware_info()

    metadata["random_seed_used"] = seed_used

    # Pipeline-specific components
    if hasattr(pipe, "tokenizer") and pipe.tokenizer:
        metadata["tokenizer_name_or_path"] = getattr(
            pipe.tokenizer, "name_or_path", None
        )
        metadata["tokenizer_class"] = pipe.tokenizer.__class__.__name__

    if hasattr(pipe, "feature_extractor") and pipe.feature_extractor:
        metadata["feature_extractor_name_or_path"] = getattr(
            pipe.feature_extractor, "name_or_path", None
        )
        metadata["feature_extractor_class"] = pipe.feature_extractor.__class__.__name__

    # Custom hyperparameters passed to pipeline
    # Note: This captures current pipeline settings, not necessarily what was passed during creation
    metadata["pipeline_task"] = pipe.task

    # System and timestamp info
    metadata["extraction_timestamp"] = datetime.now().isoformat()
    metadata["python_version"] = sys.version
    metadata["platform"] = platform.platform()
    metadata["architecture"] = platform.architecture()

    # Clean up None values
    return {k: v for k, v in metadata.items() if v is not None}


def get_hardware_info():
    """Get detailed hardware information"""
    hardware_info = {}

    # GPU Information
    if torch.cuda.is_available():
        hardware_info["gpu_available"] = True
        hardware_info["gpu_count"] = torch.cuda.device_count()

        # Get info for each GPU
        gpu_details = []
        for i in range(torch.cuda.device_count()):
            gpu_props = torch.cuda.get_device_properties(i)
            gpu_info = {
                "device_id": i,
                "name": gpu_props.name,
                "total_memory_gb": round(gpu_props.total_memory / 1024**3, 2),
                "compute_capability": f"{gpu_props.major}.{gpu_props.minor}",
                "multiprocessor_count": gpu_props.multiprocessor_count,
            }
            gpu_details.append(gpu_info)

        hardware_info["gpu_details"] = gpu_details
    else:
        hardware_info["gpu_available"] = False

    # CPU Information
    try:
        import psutil

        hardware_info["cpu_count"] = psutil.cpu_count(logical=True)
        hardware_info["cpu_count_physical"] = psutil.cpu_count(logical=False)
        hardware_info["memory_total_gb"] = round(
            psutil.virtual_memory().total / 1024**3, 2
        )
    except ImportError:
        hardware_info["cpu_count"] = os.cpu_count()

    return hardware_info


def set_reproducible_seeds(seed=42):
    """Set seeds for reproducibility - call this before creating your pipeline"""
    import random
    import numpy as np

    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)

    if torch.cuda.is_available():
        torch.cuda.manual_seed(seed)
        torch.cuda.manual_seed_all(seed)
        # For deterministic behavior (may impact performance)
        torch.backends.cudnn.deterministic = True
        torch.backends.cudnn.benchmark = False

    return seed


def get_file_extension_from_format(image: PILImage) -> str:
    format_to_extension = {
        "JPEG": ".jpg",
        "PNG": ".png",
        "GIF": ".gif",
        "BMP": ".bmp",
        "TIFF": ".tiff",
        "WEBP": ".webp",
        "ICO": ".ico",
    }
    assert image.format is not None, "Image format is None"
    try:
        return format_to_extension[image.format]
    except KeyError:
        raise ValueError(f"Got unexpected Pillow image format: {image.format}")


def create_s3_client() -> S3Client:
    """Create a boto3 S3 client using the existing s3-creds Prefect configuration"""
    # Load the existing AWS credentials from Prefect
    aws_creds = cast(AwsCredentials, AwsCredentials.load("s3-creds"))
    assert aws_creds.aws_secret_access_key is not None, "AWS secret access key is None"
    return cast(
        S3Client,
        boto3.client(
            "s3",
            aws_access_key_id=aws_creds.aws_access_key_id,
            aws_secret_access_key=aws_creds.aws_secret_access_key.get_secret_value(),
            endpoint_url=aws_creds.aws_client_parameters.endpoint_url,
        ),
    )


def upload_to_s3(
    s3_client: S3Client, bucket_name: str, file_obj: BytesIO, s3_key: str
) -> None:
    """Upload a file object to S3"""
    try:
        file_obj.seek(0)  # Reset file pointer to beginning
        s3_client.upload_fileobj(file_obj, Bucket=bucket_name, Key=s3_key)
        # print(f"Successfully uploaded {s3_key} to S3 bucket {bucket_name}")
    except ClientError as e:
        print(f"Error uploading {s3_key} to S3: {e}")
        raise


@flow(name="sp-ai-image-detection", log_prints=True)
def run_ai_image_detection_and_upload_results(
    inputs: list[EntityIdAndImageUrl],
    entity_type: Literal["artists", "albums"],
    store_images_in_s3: bool = True,
):
    # Load the S3 bucket object (includes bucket name, which is actually the only thing we need from it)
    # NOTE: while we _could_ use the s3_bucket_obj directly for file uploads, it adds a log for every single image upload, which is really unncessary - hence, we replace it with a plain boto3 client
    bucket_name = cast(S3Bucket, S3Bucket.load("s3-bucket")).bucket_name
    # Create a boto3 client
    s3_client = create_s3_client()
    model_name = "Organika/sdxl-detector"

    # Set seeds first for reproducibility
    seed_used = set_reproducible_seeds(42)

    # Create pipeline
    pipe = pipeline("image-classification", model=model_name)

    # Extract metadata
    metadata = extract_pipeline_metadata(pipe, seed_used)

    def run_inference(item: EntityIdAndImageUrl):
        entity_id, image_url = item.id, str(item.image_url)
        img_bytes = requests.get(image_url).content
        img = Image.open(BytesIO(img_bytes))
        sha256_hash = hashlib.sha256(img_bytes).hexdigest()
        avg_hash = str(imagehash.average_hash(img))
        phash = str(imagehash.phash(img))
        if store_images_in_s3:
            image_s3_key = f"spotify/ai-image-detection/images/{sha256_hash}{get_file_extension_from_format(img)}"
            upload_to_s3(s3_client, bucket_name, BytesIO(img_bytes), image_s3_key)

        inference_result = pipe(img)
        pred_dict = {
            "id": entity_id,
            "image_url": image_url,
            "sha256_hash": sha256_hash,
            "avg_hash": avg_hash,
            "phash": phash,
            "model": model_name,
            "inference_result": inference_result,
        }

        return pred_dict

    process_and_upload_data(
        inputs=inputs,
        processing_fn=run_inference,
        outputs_s3_prefix=f"spotify/ai-image-detection/results/{entity_type}",
        failures_s3_prefix=f"spotify/ai-image-detection/failures/{entity_type}",
        timestamp_key="inference_timestamp",
        run_meta_config=RunMetaConfig(
            metadata=metadata,
            s3_prefix=f"spotify/ai-image-detection/run-metadata/{entity_type}",
        ),
    )


if __name__ == "__main__":
    # test locally
    # run_ai_image_detection_and_upload_results(
    #     inputs=[
    #         EntityIdAndImageUrl(
    #             id="24NB7jXxw1l6NKzfOnYB5b",
    #             image_url="https://i.scdn.co/image/ab6761610000e5ebce057f6ff99b56dd8b341b24",
    #         )
    #     ],
    #     entity_type="artists",
    #     store_images_in_s3=True,
    # )

    # deploy
    # NOTE: run this from the project root!
    # always update to specific version tag
    run_ai_image_detection_and_upload_results.deploy(
        "api",
        work_pool_name="Docker",
        image=create_image_config(
            flow_identifier="sp-ai-image-detection",
            version="v1.5",
            dockerfile_path="Dockerfile_ai_image_detection",
            private_repo=False,
        ),
        build=False,
        push=False,
    )
