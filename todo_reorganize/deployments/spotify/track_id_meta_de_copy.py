from prefect.schedules import Schedule

from flows.clickhouse.copy_data import copy_data_flow, CopyDataParams
from utils.flow_deployment import create_image_config

copy_data_flow.deploy(
    "Copy (Update) Spotify track ID meta DE",
    work_pool_name="Docker",
    image=create_image_config("clickhouse-copy-data", "v1.0"),
    schedule=Schedule(
        cron="15 7 * * *",
        timezone="Europe/Berlin",
        parameters=CopyDataParams(
            use_observed_at=True,
            etl_tbl_or_view="spotify.data_track_id_meta_de",
            k8s_tbl="spotify.track_id_meta_de",
            k8s_view_name="spotify.data_track_id_meta_de",
        ).model_dump(),
    ),
)
