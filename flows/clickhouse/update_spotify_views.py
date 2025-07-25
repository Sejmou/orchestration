from prefect import flow, task
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.databases.clickhouse import (
    ClickHouseCredentials,
    create_client,
)


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
    refresh_views.serve()
    # refresh_views.deploy(
    #     name="Refresh ClickHouse Views",
    #     work_pool_name="Docker",
    #     image="my_registry/my_image:my_image_tag",
    #     push=False,
    # )
