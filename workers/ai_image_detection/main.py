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
from prefect_aws import S3Bucket
from prefect import flow


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


def get_file_extension_from_format(image: PILImage):
    format_to_extension = {
        "JPEG": ".jpg",
        "PNG": ".png",
        "GIF": ".gif",
        "BMP": ".bmp",
        "TIFF": ".tiff",
        "WEBP": ".webp",
        "ICO": ".ico",
    }
    return format_to_extension.get(image.format, ".jpg")


@flow(name="sp-ai-image-detection", log_prints=True)
def run_ai_image_detection_and_upload_results(
    ids_and_image_urls: list[tuple[str, str]],
    entity_type: Literal["artists", "albums"],
    store_images_in_s3: bool = True,
):
    s3_bucket = cast(S3Bucket, S3Bucket.load("s3-bucket"))
    model_name = "Organika/sdxl-detector"

    # Set seeds first for reproducibility
    seed_used = set_reproducible_seeds(42)

    # Create pipeline
    pipe = pipeline("image-classification", model=model_name)

    # Extract metadata
    metadata = extract_pipeline_metadata(pipe, seed_used)

    def run_inference(image_url: str):
        entity_id, image_url = image_url
        img_bytes = requests.get(image_url).content
        img = Image.open(BytesIO(img_bytes))
        sha256_hash = hashlib.sha256(img_bytes).hexdigest()
        avg_hash = str(imagehash.average_hash(img))
        phash = str(imagehash.phash(img))
        if store_images_in_s3:
            image_s3_key = f"spotify/ai-image-detection/images/{sha256_hash}{get_file_extension_from_format(img)}"
            s3_bucket.upload_from_file_object(BytesIO(img_bytes), to_path=image_s3_key)

        inference_result = pipe(image_url)
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
        inputs=ids_and_image_urls,
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
    # NOTE: run this from the project root!
    run_ai_image_detection_and_upload_results.deploy(
        "api",
        work_pool_name="Docker",
        image=create_image_config(
            flow_identifier="sp-ai-image-detection",
            version="v1.1",
            dockerfile_path="Dockerfile_ai_image_detection",
        ),
        build=False,
        push=False,
    )
