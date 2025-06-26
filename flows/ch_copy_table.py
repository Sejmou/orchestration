from typing import Literal
from prefect import flow, task
from prefect.blocks.system import Secret
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from store_ch_secrets import ClickHouseCredentials, create_client, ClickHouseClient


def copy_table(
    source_client: ClickHouseClient,
    source_creds: ClickHouseCredentials,
    target_client: ClickHouseClient,
    db: str,
    table: str,
    source_native_port: int = 9000,
    # if provided, the data_view_name will be used to compare row counts
    data_view_name: str | None = None,
    has_observed_at: bool = False,
):
    def execute_query(query: str, location: Literal["source", "target"]) -> None:
        client = source_client if location == "source" else target_client
        print(f"Executing query on {location} server: {query}")
        client.query(query)

    def execute_query_df(query: str, location: Literal["source", "target"]):
        client = source_client if location == "source" else target_client
        return client.query_df(query)

    def get_row_count(
        location: Literal["source", "target"], db: str, table: str
    ) -> int:
        query = f"SELECT count() from {db}.{table if not data_view_name else data_view_name}"
        return execute_query_df(query, location).iloc[0, 0]

    def get_create_stmt(client: ClickHouseClient, db: str, table_or_view: str) -> str:
        return client.query_df(
            f"SELECT create_table_query from system.tables WHERE database = '{db}' and table = '{table_or_view}'"
        ).iloc[0, 0]

    def table_exists(client: ClickHouseClient, db: str, table: str) -> bool:
        return (
            client.query_df(
                f"SELECT count() from system.tables WHERE database = '{db}' and table = '{table}'"
            ).iloc[0, 0]
            == 1
        )

    execute_query(f"CREATE DATABASE IF NOT EXISTS {db}", "target")

    if not table_exists(target_client, db, table):
        print(f"Table {db}.{table} does not exist on target server, creating...")
        execute_query(get_create_stmt(source_client, db, table), "target")

    if data_view_name and not table_exists(target_client, db, data_view_name):
        print(
            f"Data view {db}.{data_view_name} does not exist on target server, creating..."
        )
        execute_query(get_create_stmt(source_client, db, data_view_name), "target")

    row_count_source = get_row_count("source", db, table)
    if row_count_source == 0:
        raise Exception(f"No data in {db}.{table} on source server")

    row_count_target_before_copy = get_row_count("target", db, table)
    if row_count_target_before_copy > row_count_source:
        raise Exception(
            f"Row count at target is larger than at source (source: {row_count_source}, target: {row_count_target_before_copy})"
        )

    if row_count_target_before_copy == row_count_source:
        print(
            f"{db}.{table} already synced ({row_count_source} rows on source and target)"
        )
        return

    def get_insert_stmt() -> str:
        where_clause = (
            f" WHERE observed_at >= '{execute_query_df(f'SELECT max(observed_at) from {db}.{table}', 'target').iloc[0, 0]}'"
            if has_observed_at
            else ""
        )
        return (
            f"INSERT INTO {db}.{table} SELECT * FROM remote('{source_creds.host}:{source_native_port}', {db}, {table}, '{source_creds.user}', '{source_creds.password.get_secret_value()}')"
            + where_clause
        )

    insert_stmt = get_insert_stmt()
    print(f"Copying data for {db}.{table} from source to target server")
    execute_query(insert_stmt, "target")
    print("Done copying")

    row_count_target = get_row_count("target", db, table)
    if row_count_target != row_count_source:
        raise Exception(
            f"Row count mismatch after copy (source: {row_count_source}, target: {row_count_target})"
        )


@flow(log_prints=True)
def copy_table_flow(
    database: str,
    table_name: str,
    view_name: str | None = None,
    has_observed_at: bool = False,
):
    etl_creds: ClickHouseCredentials = ClickHouseCredentials.load("clickhouse-etl-config")  # type: ignore
    # NOTE: need to use public IP of the ETL ClickHouse server for this task
    # (copy sql query is executed from ClickHouse on Kubernetes which doesn't have access to the private IP of the ETL server which is stored in the secret)
    ch_etl_public_ip = Secret.load("clickhouse-etl-public-ip")
    etl_creds.host = ch_etl_public_ip.get()
    etl_client = create_client(etl_creds)

    k8s_creds: ClickHouseCredentials = ClickHouseCredentials.load("clickhouse-k8s-config")  # type: ignore
    k8s_client = create_client(k8s_creds)

    print(f"Copying table: {table_name}")
    copy_table(
        source_client=etl_client,
        source_creds=etl_creds,
        target_client=k8s_client,
        db=database,
        table=table_name,
        source_native_port=9000,
        data_view_name=view_name,
        has_observed_at=has_observed_at,
    )


if __name__ == "__main__":
    copy_table_flow.serve()
    # update_spotify_views.deploy(
    #     name="Update Spotify Views",
    #     work_pool_name="Docker",
    #     image="my_registry/my_image:my_image_tag",
    #     push=False,
    # )
