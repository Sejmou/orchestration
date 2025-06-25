from dotenv import load_dotenv
import os
from prefect.blocks.core import Block
from pydantic import SecretStr
import clickhouse_connect


class ClickHouseCredentials(Block):
    host: str
    port: int
    user: str
    password: SecretStr


def store_clickhouse_secrets():
    """Store ClickHouse configuration variables as Prefect secrets"""
    # Let Prefect know about the custom block type we defined
    ClickHouseCredentials.register_type_and_schema()

    # Load environment variables from .env file
    load_dotenv()

    # ETL ClickHouse configuration
    clickhouse_etl_config = ClickHouseCredentials(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ["CLICKHOUSE_PORT"]),
        user=os.environ["CLICKHOUSE_USER"],
        password=SecretStr(os.environ["CLICKHOUSE_PASSWORD"]),
    )
    ch_etl_client = clickhouse_connect.get_client(
        host=clickhouse_etl_config.host,
        port=clickhouse_etl_config.port,
        username=clickhouse_etl_config.user,
        password=clickhouse_etl_config.password.get_secret_value(),
    )
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
    ch_k8s_client = clickhouse_connect.get_client(
        host=clickhouse_k8s_config.host,
        port=clickhouse_k8s_config.port,
        username=clickhouse_k8s_config.user,
        password=clickhouse_k8s_config.password.get_secret_value(),
    )
    if not ch_k8s_client.ping():
        raise ConnectionError(
            "Failed to connect to ClickHouse Kubernetes with provided credentials."
        )
    clickhouse_k8s_config.save("clickhouse-k8s-config", overwrite=True)
    print("ClickHouse Kubernetes configuration stored successfully.")


if __name__ == "__main__":
    store_clickhouse_secrets()
