from prefect import flow, task

from utils.databases.clickhouse import (
    ClickHouseCredentials,
    create_client,
)
from utils.flow_deployment import create_image_config


@flow()
def refresh_views(views: list[str]):
    """
    Refreshes refreshable materialized views in ClickHouse in the order as provided in the list.

    Waits for each refresh to finish before continuing.
    """
    for view in views:
        update_refreshable_materialized_view(view)


@task(log_prints=True)
def update_refreshable_materialized_view(view_name: str):
    etl_creds: ClickHouseCredentials = ClickHouseCredentials.load("clickhouse-etl-config")  # type: ignore
    client = create_client(etl_creds)
    print(f"Refreshing materialized view: {view_name}")
    client.command(f"SYSTEM REFRESH VIEW {view_name}")
    print(f"Waiting for view {view_name} to be refreshed...")
    client.command(f"SYSTEM WAIT VIEW {view_name}")
    print(f"View {view_name} refreshed successfully.")


if __name__ == "__main__":
    refresh_views.deploy(
        "Refresh ClickHouse materialized views",
        work_pool_name="Docker",
        image=create_image_config("clickhouse-refresh-views", "v1.0"),
    )
