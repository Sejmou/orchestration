from prefect.schedules import Schedule

from flows.clickhouse.copy_data import copy_data_flow, CopyDataParams
from utils.flow_deployment import create_image_config

copy_data_flow.deploy(
    "sc-artist-meta",
    work_pool_name="Docker",
    tags=["SoundCharts", "Spotify"],
    image=create_image_config("clickhouse-copy-data", "v1.0"),
    schedule=Schedule(
        cron="5 7 * * *",
        timezone="Europe/Berlin",
        parameters=CopyDataParams(
            use_observed_at=True,
            etl_tbl_or_view="soundcharts.data_artists",
            k8s_tbl="soundcharts.artists",
            k8s_view_name="soundcharts.data_artists",
        ).model_dump(),
    ),
)
