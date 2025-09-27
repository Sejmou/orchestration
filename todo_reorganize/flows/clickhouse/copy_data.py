from prefect import flow
from prefect.blocks.system import Secret
from pydantic import BaseModel

from utils.databases.clickhouse import (
    ClickHouseCredentials,
    create_client,
)


class CopyDataParams(BaseModel):
    etl_tbl_or_view: str
    k8s_tbl: str
    k8s_view_name: str | None = None
    use_observed_at: bool = False


@flow(log_prints=True)
def copy_data_flow(
    etl_tbl_or_view: str,
    k8s_tbl: str,
    k8s_view_name: str | None = None,
    use_observed_at: bool = False,
):
    # this may look a bit convoluted, but it allows one to be sure the function's parameters stay consistent
    # with the model for the params (which is used in the deployments)
    params = CopyDataParams(
        etl_tbl_or_view=etl_tbl_or_view,
        k8s_tbl=k8s_tbl,
        k8s_view_name=k8s_view_name,
        use_observed_at=use_observed_at,
    )

    etl_creds: ClickHouseCredentials = ClickHouseCredentials.load("clickhouse-etl-config")  # type: ignore
    etl_client = create_client(etl_creds)
    etl_native_port = 9000  # Default native port for ClickHouse

    # NOTE: need to use public IP of the ETL ClickHouse server for SELECT ... FROM remote(...) sql query
    # as it is executed from ClickHouse on Kubernetes which doesn't have access to the private IP of the ETL server which is stored in the secret)
    ch_etl_public_ip: str = Secret.load("clickhouse-etl-public-ip").get()  # type: ignore

    k8s_creds: ClickHouseCredentials = ClickHouseCredentials.load("clickhouse-k8s-config")  # type: ignore
    k8s_client = create_client(k8s_creds)

    etl_tbl_or_view = params.etl_tbl_or_view
    etl_tbl_or_view_parts = etl_tbl_or_view.split(".")
    if len(etl_tbl_or_view_parts) != 2:
        raise ValueError(
            "etl table or view must be in the format 'database.table_name'"
        )
    etl_db, etl_tbl_or_view_name = etl_tbl_or_view_parts
    k8s_tbl = params.k8s_tbl
    use_observed_at = params.use_observed_at
    k8s_view_name = params.k8s_view_name

    observed_at_explainer = (
        " with observed_at > max(observed_at) at k8s" if use_observed_at else ""
    )

    print(
        f"Copying data from {etl_tbl_or_view} on ETL server to {k8s_tbl} on Kubernetes server{observed_at_explainer}"
    )

    row_count_etl_before_copy = etl_client.query_df(
        f"SELECT count(*) FROM {etl_tbl_or_view}"
    ).iloc[0, 0]
    print(f"Row count at etl before copy: {row_count_etl_before_copy}")

    row_count_k8s_before_copy = k8s_client.query_df(
        f"SELECT count(*) FROM {k8s_view_name if k8s_view_name else k8s_tbl}"
    ).iloc[0, 0]

    print(
        f"Row count at {k8s_view_name if k8s_view_name else k8s_tbl} before copy: {row_count_k8s_before_copy}"
    )
    if row_count_k8s_before_copy > row_count_etl_before_copy:
        raise Exception(
            f"Row count at k8s is larger than at etl (etl: {row_count_etl_before_copy}, k8s: {row_count_k8s_before_copy})"
        )
    if row_count_k8s_before_copy == row_count_etl_before_copy:
        print(
            f"{k8s_view_name if k8s_view_name else k8s_tbl} already synced ({row_count_etl_before_copy} rows on etl and k8s)"
        )
        return

    insert_stmt = (
        f"INSERT INTO {k8s_tbl} "
        f"SELECT * FROM remote('{ch_etl_public_ip}:{etl_native_port}', {etl_db}, {etl_tbl_or_view_name}, '{etl_creds.user}', '{etl_creds.password.get_secret_value()}')"
    )
    if use_observed_at:
        insert_stmt += f" WHERE observed_at >= '{k8s_client.query_df(f'SELECT max(observed_at) from {k8s_view_name if k8s_view_name else k8s_tbl}').iloc[0, 0]}'"
    print(f"Executing insert statement: {insert_stmt}")
    k8s_client.query(insert_stmt)
    print("Done copying data")

    row_count_k8s_after_copy = k8s_client.query_df(
        f"SELECT count(*) FROM {k8s_view_name if k8s_view_name else k8s_tbl}"
    ).iloc[0, 0]
    print(f"Row count at k8s after copy: {row_count_k8s_after_copy}")
    if row_count_k8s_after_copy != row_count_etl_before_copy:
        raise Exception(
            f"Row count at k8s after copy does not match etl (etl: {row_count_etl_before_copy}, k8s: {row_count_k8s_after_copy})"
        )
    print(
        f"Data copy completed successfully from {etl_tbl_or_view} to {k8s_view_name if k8s_view_name else k8s_tbl} (row count: {row_count_k8s_after_copy})"
    )


if __name__ == "__main__":
    copy_data_flow.serve()
    # copy_data_flow.deploy(
    #     "Copy ClickHouse data",
    #     work_pool_name="Docker",
    #     image=create_image_config("clickhouse-copy-data", "v1.0"),
    # )
