from prefect import flow, task
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from store_ch_secrets import ClickHouseCredentials, create_client, ClickHouseClient


@flow()
def update_spotify_views():
    """
    Flow to update Spotify views in ClickHouse.
    """
    views = [
        "spotify.top_track_ids_by_streams_de_mv",
        "spotify.top_track_ids_by_streams_at_mv",
        "spotify.top_track_ids_by_streams_ch_mv",
        "spotify.top_artist_ids_by_streams_de_mv",
        "spotify.top_artist_ids_by_streams_at_mv",
        "spotify.top_artist_ids_by_streams_ch_mv",
        "spotify.top_unknown_track_ids_de_at_ch_by_streams_mv",
    ]
    for view in views:
        update_refreshable_materialized_view(view)


@task(log_prints=True)
def update_refreshable_materialized_view(view_name: str):
    etl_creds = ClickHouseCredentials.load("clickhouse-etl-config")
    client = create_client(etl_creds)
    print(f"Refreshing materialized view: {view_name}")
    client.command(f"SYSTEM REFRESH VIEW {view_name}")
    print(f"Waiting for view {view_name} to be refreshed...")
    client.command(f"SYSTEM WAIT VIEW {view_name}")
    print(f"View {view_name} refreshed successfully.")


if __name__ == "__main__":
    update_spotify_views.serve()
    # update_spotify_views.deploy(
    #     name="Update Spotify Views",
    #     work_pool_name="Docker",
    #     image="my_registry/my_image:my_image_tag",
    #     push=False,
    # )
