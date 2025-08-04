from prefect import flow, task
from utils.scraping import fetch_and_upload_data
from prefect.runtime import flow_run

from utils.apis.soundcharts import SoundChartsCredentials, create_client
from utils.flow_deployment import create_image_config

sc = create_client(SoundChartsCredentials.load("soundcharts-creds"))  # type: ignore


@task(name="sc-artist")
def fetch_artist_metadata(artist_uuid: str):
    """
    Fetches metadata for a single artist using their UUID.
    """
    metadata = sc.artist.get_artist_metadata(artist_uuid)
    return metadata


@flow(name="sc-artists", log_prints=True)
def fetch_metadata_for_artists(artist_uuids: list[str]):
    flow_run_id = flow_run.get_id()
    if not flow_run_id:
        raise ValueError(
            "Could not get flow run ID (required for storing data locally before uploading to S3)"
        )
    fetch_and_upload_data(
        inputs=artist_uuids,
        fetch_fn=fetch_artist_metadata,
        flow_run_id=flow_run_id,
        s3_prefix="soundcharts/raw-api-data-by-endpoint-and-version/artist/v2.9",
    )


if __name__ == "__main__":
    # deploy flow so that it becomes available via API and runs can be submitted
    fetch_metadata_for_artists.deploy(
        "api",
        work_pool_name="Docker",
        image=create_image_config("sc-artists", "v1.1"),
    )
