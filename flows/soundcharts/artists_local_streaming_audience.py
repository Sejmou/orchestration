from typing import Literal
from prefect import flow, task
from utils.scraping import fetch_and_upload_data
from prefect.runtime import flow_run
from datetime import date

from utils.apis.soundcharts import SoundChartsCredentials, create_client
from utils.flow_deployment import create_image_config

sc = create_client(SoundChartsCredentials.load("soundcharts-creds"))  # type: ignore

# TODO: update as we figure out the shape of returned data
type SupportedPlatform = Literal["spotify", "youtube"]


def fetch_artist_local_streaming_audience(
    artist_uuid: str, platform: SupportedPlatform, start_date: date, end_date: date
):
    """
    Fetches a SoundCharts artists' local streaming audience over time for a specific platform (e.g. top Spotify listeners by region and country if `platform='spotify'`).

    API docs: https://doc.api.soundcharts.com/documentation/reference/artist/get-local-streaming-audience
    """
    try:
        metadata = sc.artist.get_local_streaming_audience(
            artist_uuid,
            platform=platform,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
        )
        return metadata
    except Exception as e:
        print(
            f"Failed to fetch {platform} data for UUID {artist_uuid} and range {start_date.isoformat()} - {end_date.isoformat()} due to exception:\n{e}"
        )


@flow(name="sc-artists-local-streaming-audience", log_prints=True)
def fetch_local_streaming_audience_for_artists(
    artist_uuids: list[str],
    start_date: date,
    end_date: date,
    platform: SupportedPlatform = "spotify",
):
    flow_run_id = flow_run.get_id()
    if not flow_run_id:
        raise ValueError(
            "Could not get flow run ID (required for storing data locally before uploading to S3)"
        )
    fetch_and_upload_data(
        inputs=artist_uuids,
        fetch_fn=lambda uuid: fetch_artist_local_streaming_audience(
            uuid, platform=platform, start_date=start_date, end_date=end_date
        ),
        flow_run_id=flow_run_id,
        s3_prefix=f"soundcharts/raw-api-data-by-endpoint-and-version/artist/streaming/{platform}/v2",
    )


if __name__ == "__main__":
    # deploy flow so that it becomes available via API and runs can be submitted
    fetch_local_streaming_audience_for_artists.deploy(
        "api",
        work_pool_name="Docker",
        image=create_image_config("sc-artists-local-streaming-audience", "v1.2"),
    )
