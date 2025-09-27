from prefect.schedules import Schedule

from flows.clickhouse.copy_data import copy_data_flow, CopyDataParams
from utils.flow_deployment import create_image_config

copy_data_flow.deploy(
    "Copy (Update) Spotify artists",
    work_pool_name="Docker",
    image=create_image_config("sp-artists-ch-etl-to-k8s", "v1.0"),
    schedule=Schedule(
        cron="15 7 * * *",
        timezone="Europe/Berlin",
        parameters=CopyDataParams(
            use_observed_at=True,
            etl_tbl_or_view="spotify.data_artists",
            k8s_tbl="spotify.artists",
            k8s_view_name="spotify.data_artists",
        ).model_dump(),
    ),
)
