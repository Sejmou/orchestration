from prefect import flow, task
from soundcharts.client import SoundchartsClient

sc = SoundchartsClient(app_id="your_app_id", api_key="your_api_key")


@task()
def fetch_artist_metadata(artist_uuid: str):
    """
    Fetches metadata for a single artist using their UUID.
    """
    metadata = sc.artist.get_artist_metadata(artist_uuid)
    return metadata


@flow()
def fetch_metadata_for_artists(artist_uuids: list[str]):
    billie_metadata = sc.artist.get_artist_metadata(
        "11e81bcc-9c1c-ce38-b96b-a0369fe50396"
    )
    print(billie_metadata)
