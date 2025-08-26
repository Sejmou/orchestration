from prefect import flow
from prefect.schedules import Schedule

from utils.databases.clickhouse import create_client, ClickHouseCredentials, query_pl_df
from flows.spotify.artists import fetch_spotify_artists
from utils.flow_deployment import create_image_config


@flow(log_prints=True)
async def fetch_missing_artists():
    ch_config = await ClickHouseCredentials.load("clickhouse-etl-config")  # type: ignore
    ch_client = create_client(ch_config)
    print(f"Getting missing artist IDs")
    ids = query_pl_df(
        f"""
        SELECT * FROM spotify.data_artist_streams
        WHERE artist_id NOT IN (SELECT id FROM spotify.artists)
        ORDER BY artist_total_streams DESC
        """,
        ch_client,
    )["artist_id"].to_list()

    print(f"Found {len(ids)} missing artist IDs, fetching data...")

    await fetch_spotify_artists(ids)


if __name__ == "__main__":
    fetch_missing_artists.deploy(
        "Fetch missing Spotify artists",
        work_pool_name="Docker",
        image=create_image_config("spotify-fetch-missing-artists", "v1.0"),
        schedule=Schedule(
            cron="50 7 * * *",
            timezone="Europe/Berlin",
        ),
    )
