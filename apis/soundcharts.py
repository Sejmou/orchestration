from dotenv import load_dotenv
import os
from prefect.blocks.core import Block
from pydantic import SecretStr
from soundcharts.client import SoundchartsClient


class SoundChartsCredentials(Block):
    app_id: str
    api_key: SecretStr


def create_client(creds: SoundChartsCredentials):
    return SoundchartsClient(
        app_id=creds.app_id, api_key=creds.api_key.get_secret_value()
    )


def store_soundcharts_credentials():
    """
    Store SoundCharts API credentials as Prefect secret.
    """
    load_dotenv()

    creds = SoundChartsCredentials(
        app_id=os.environ["SOUNDCHARTS_APP_ID"],
        api_key=SecretStr(os.environ["SOUNDCHARTS_API_KEY"]),
    )

    sc = create_client(creds)

    billie_eilish = sc.artist.get_artist_metadata(
        "11e81bcc-9c1c-ce38-b96b-a0369fe50396"
    )
    print(
        "Successfully fetched SoundCharts artist metadata for Billie Eilish:",
        billie_eilish,
    )

    creds.save("soundcharts-creds", overwrite=True)
    print("Sucessfully stored Soundcharts credentials :)")


if __name__ == "__main__":
    store_soundcharts_credentials()
