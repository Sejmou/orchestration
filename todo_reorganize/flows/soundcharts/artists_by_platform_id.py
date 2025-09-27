from prefect import flow, task
from prefect.runtime import flow_run

from utils.scraping import process_and_upload_data
from utils.apis.soundcharts import SoundChartsCredentials, create_client
from utils.flow_deployment import create_image_config

sc = create_client(SoundChartsCredentials.load("soundcharts-creds"))  # type: ignore


@task(name="sc-artist-by-platform-id")
def fetch_soundchart_artist_by_platform_id(platform: str, identifier: str | int):
    """
    Fetches metadata for a single artist using an ID from another platform.
    """
    metadata = sc.artist.get_artist_by_platform_id(platform, identifier)
    if metadata == {}:
        return None
    metadata["input"] = {
        "platform": platform,
        "identifier": identifier,
    }
    return metadata


@flow(name="sc-artists-by-platform-id", log_prints=True)
def fetch_artists_by_platform_ids(platform: str, identifiers: list[str | int]):
    flow_run_id = flow_run.get_id()
    if not flow_run_id:
        raise ValueError(
            "Could not get flow run ID (required for storing data locally before uploading to S3)"
        )

    process_and_upload_data(
        inputs=identifiers,
        processing_fn=lambda identifier: fetch_soundchart_artist_by_platform_id(
            platform=platform, identifier=identifier
        ),
        flow_run_id=flow_run_id,
        outputs_s3_prefix=f"soundcharts/raw-api-data-by-endpoint-and-version/artist/by-platform/{platform}/v2.9",
    )


if __name__ == "__main__":
    # deploy flow so that it becomes available via API and runs can be submitted
    fetch_artists_by_platform_ids.deploy(
        "api",
        work_pool_name="Docker",
        image=create_image_config("sc-artists-by-platform-id", "v1.0"),
    )
