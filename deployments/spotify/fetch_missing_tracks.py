from prefect import flow
from prefect.schedules import Schedule

from utils.databases.clickhouse import create_client, ClickHouseCredentials, query_pl_df
from flows.spotify.tracks import fetch_spotify_tracks
from utils.flow_deployment import create_image_config


@flow(log_prints=True)
async def fetch_missing_tracks(region="de"):
    ch_config = await ClickHouseCredentials.load("clickhouse-etl-config")  # type: ignore
    ch_client = create_client(ch_config)
    print(f"Getting missing track IDs")
    ids = query_pl_df(
        f"""
        SELECT
            track_id,
            sum(streams) AS total_streams
        FROM spotify.track_id_streams
        WHERE track_id NOT IN (SELECT id FROM spotify.data_track_id_meta_de)
        GROUP BY track_id ORDER BY total_streams DESC
        """,
        ch_client,
    )["track_id"].to_list()

    print(f"Found {len(ids)} missing track IDs, fetching data...")

    await fetch_spotify_tracks(ids, region=region)


if __name__ == "__main__":
    fetch_missing_tracks.deploy(
        "Fetch missing Spotify tracks (DE region)",
        work_pool_name="Docker",
        image=create_image_config("spotify-fetch-missing-tracks-de", "v1.0"),
        schedule=Schedule(
            cron="20 7 * * *",
            timezone="Europe/Berlin",
        ),
    )
