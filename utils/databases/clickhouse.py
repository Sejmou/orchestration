from dotenv import load_dotenv
import os
from prefect.blocks.core import Block
from prefect.blocks.system import Secret
from pydantic import SecretStr
import clickhouse_connect
from clickhouse_connect.driver.client import Client as ClickHouseClient


class ClickHouseCredentials(Block):
    host: str
    port: int
    user: str
    password: SecretStr


def create_client(creds: ClickHouseCredentials) -> ClickHouseClient:
    """
    Create a ClickHouse client using the provided credentials.
    """
    return clickhouse_connect.get_client(
        host=creds.host,
        port=creds.port,
        username=creds.user,
        password=creds.password.get_secret_value(),
    )


def query_pl_df(query: str, ch_client: ClickHouseClient):
    """
    A workaround for getting data from ClickHouse into a Polars dataframe (not natively supported by ClickHouse Python client from clickhouse_connect)
    if one or more result column data types aren't compatible with ch_client.query_arrow()
    """
    import polars as pl

    res = ch_client.query(query)
    # in case of non-empty result, convert binary strings to regular strings
    if len(res.result_columns) > 0:
        for i, col in enumerate(res.result_columns):
            if isinstance(col[0], bytes):
                res.result_columns[i] = [item.decode("utf-8") for item in col]  # type: ignore

    df = pl.DataFrame(res.result_columns)
    df.columns = res.column_names
    return df


def store_clickhouse_secrets():
    """
    Store ClickHouse configuration variables as Prefect secrets.
    """

    # Load environment variables from .env file
    load_dotenv()

    # ETL ClickHouse configuration
    clickhouse_etl_config = ClickHouseCredentials(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ["CLICKHOUSE_PORT"]),
        user=os.environ["CLICKHOUSE_USER"],
        password=SecretStr(os.environ["CLICKHOUSE_PASSWORD"]),
    )
    ch_etl_client = create_client(clickhouse_etl_config)
    if not ch_etl_client.ping():
        raise ConnectionError(
            "Failed to connect to ClickHouse ETL with provided credentials."
        )
    clickhouse_etl_config.save("clickhouse-etl-config", overwrite=True)
    print("ClickHouse ETL configuration stored successfully.")

    # Kubernetes ClickHouse configuration
    clickhouse_k8s_config = ClickHouseCredentials(
        host=os.environ["CLICKHOUSE_KUBERNETES_HOST"],
        port=int(os.environ["CLICKHOUSE_KUBERNETES_PORT"]),
        user=os.environ["CLICKHOUSE_KUBERNETES_USER"],
        password=SecretStr(os.environ["CLICKHOUSE_KUBERNETES_PASSWORD"]),
    )
    ch_k8s_client = create_client(clickhouse_k8s_config)
    if not ch_k8s_client.ping():
        raise ConnectionError(
            "Failed to connect to ClickHouse Kubernetes with provided credentials."
        )
    clickhouse_k8s_config.save("clickhouse-k8s-config", overwrite=True)
    print("ClickHouse Kubernetes configuration stored successfully.")

    # ETL -> K8S copy tasks require public IP of the ETL ClickHouse server
    ch_etl_public_ip = os.environ["CLICKHOUSE_ETL_PUBLIC_IP"]
    Secret(value=SecretStr(ch_etl_public_ip)).save(
        "clickhouse-etl-public-ip", overwrite=True
    )
    print("ClickHouse ETL public IP stored successfully as a Prefect secret.")


if __name__ == "__main__":
    store_clickhouse_secrets()
