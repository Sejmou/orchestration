from prefect import flow, task
from prefect.runtime import flow_run

from utils.scraping import fetch_and_upload_data
from utils.apis.soundcharts import SoundChartsCredentials, create_client
from utils.flow_deployment import create_image_config

sc = create_client(SoundChartsCredentials.load("soundcharts-creds"))  # type: ignore


@task(name="Fetch SoundChart artist metadata by platform ID")
def fetch_soundchart_artist_by_platform_id(platform: str, identifier: str | int):
    """
    Fetches metadata for a single artist using their UUID.
    """
    metadata = sc.artist.get_artist_by_platform_id(platform, identifier)
    if metadata == {}:
        return None
    metadata["input"] = {
        "platform": platform,
        "identifier": identifier,
    }
    return metadata


@flow(name="Fetch metadata for artists by platform ID", log_prints=True)
def fetch_artists_by_platform_ids(platform: str, identifiers: list[str | int]):
    flow_run_id = flow_run.get_id()
    if not flow_run_id:
        raise ValueError(
            "Could not get flow run ID (required for storing data locally before uploading to S3)"
        )

    fetch_and_upload_data(
        inputs=identifiers,
        fetch_fn=lambda identifier: fetch_soundchart_artist_by_platform_id(
            platform=platform, identifier=identifier
        ),
        flow_run_id=flow_run_id,
        s3_prefix="soundcharts/artists/by_platform_id",
    )


if __name__ == "__main__":
    fetch_artists_by_platform_ids.deploy(
        "Fetch SoundCharts artist metadata by platform ID",
        work_pool_name="Docker",
        image=create_image_config("soundcharts-artists-by-platform-id", "v1.0"),
    )
