from prefect import flow
from prefect.schedules import Schedule

from flows.clickhouse.copy_data import copy_data_flow
from utils.flow_deployment import create_image_config


@flow
def sc_copy_artist_top_sp_regions_tbls():
    """
    Updates the ClickHouse tables
        - `soundcharts.artist_top_sp_cities_over_time`
        - `soundcharts.artist_top_sp_countries_over_time`
        - `soundcharts.artist_local_streaming_audience_spotify_last_crawl_dates`

    on Kubernetes with new data from the ETL server.
    """
    try:
        copy_data_flow(
            etl_tbl_or_view="soundcharts.data_artist_top_sp_cities_over_time",
            k8s_tbl="soundcharts.artist_top_sp_cities_over_time",
            k8s_view_name="soundcharts.data_artist_top_sp_cities_over_time",
            use_observed_at=True,
        )
    except Exception as e:
        print(f"Failed to copy artist top Spotify cities data: {e}")
    try:
        copy_data_flow(
            etl_tbl_or_view="soundcharts.data_artist_top_sp_countries_over_time",
            k8s_tbl="soundcharts.artist_top_sp_countries_over_time",
            k8s_view_name="soundcharts.data_artist_top_sp_countries_over_time",
            use_observed_at=True,
        )
    except Exception as e:
        print(f"Failed to copy artist top Spotify countries data: {e}")
    try:
        copy_data_flow(
            etl_tbl_or_view="soundcharts.data_artist_local_streaming_audience_spotify_last_crawl_dates",
            k8s_tbl="soundcharts.artist_local_streaming_audience_spotify_last_crawl_dates",
            k8s_view_name="soundcharts.data_artist_local_streaming_audience_spotify_last_crawl_dates",
            use_observed_at=True,
        )
    except Exception as e:
        print(
            f"Failed to copy artist local streaming audience Spotify last crawl dates data: {e}"
        )


if __name__ == "__main__":
    sc_copy_artist_top_sp_regions_tbls.deploy(
        "daily-update",
        work_pool_name="Docker",
        tags=["SoundCharts", "Spotify"],
        image=create_image_config("sc-copy-artist-top-sp-region-tbls", "v1.0"),
        schedule=Schedule(
            cron="0 7 * * *",
            timezone="Europe/Berlin",
        ),
    )
