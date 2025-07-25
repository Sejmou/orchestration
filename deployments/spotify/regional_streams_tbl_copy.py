from prefect.schedules import Schedule

from flows.clickhouse.copy_table import CopyTableParams, copy_table_flow
from utils.flow_deployment import create_image_config

REGIONS = [
    "DE",
    "AT",
    "CH",
]


def create_schedules() -> list[Schedule]:
    tbl_name = "track_streams_isrcs_unique_users_no_artists"
    return [
        Schedule(
            cron="0 7 * * *",
            timezone="Europe/Berlin",
            parameters=CopyTableParams(
                database="spotify",
                table_name=f"{tbl_name}_{region.lower()}",
                view_name=f"data_{tbl_name}_{region.lower()}",
                has_observed_at=True,
            ).model_dump(),
            slug=f"sp-streams-{region.lower()}",
        )
        for region in REGIONS
    ]


if __name__ == "__main__":
    copy_table_flow.deploy(
        "Copy Spotify regional streams tables (DE, AT, CH)",
        tags=["Spotify", "ClickHouse"],
        schedules=create_schedules(),
        work_pool_name="Docker",
        image=create_image_config("clickhouse-copy-table", "v1.1"),
    )
