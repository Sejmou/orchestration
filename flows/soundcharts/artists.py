from prefect import flow, task
from soundcharts.client import SoundchartsClient
from utils.scraping import fetch_and_upload_data
from prefect.runtime import flow_run

sc = SoundchartsClient(app_id="your_app_id", api_key="your_api_key")


@task(name="Fetch metadata for SoundCharts artist UUID")
def fetch_artist_metadata(artist_uuid: str):
    """
    Fetches metadata for a single artist using their UUID.
    """
    metadata = sc.artist.get_artist_metadata(artist_uuid)
    return metadata


@flow()
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
        s3_prefix="soundcharts/artists",
    )


if __name__ == "__main__":
    fetch_metadata_for_artists.serve()
