from typing import Literal
from prefect import flow, task
from time import time
from jinja2 import Environment, StrictUndefined
from pydantic import BaseModel
from utils.databases.clickhouse import (
    ClickHouseCredentials,
    create_client,
)


class QueryMeta(BaseModel):
    query_or_template: str
    params: dict | None = None


@task(log_prints=True)
def execute_query(meta: QueryMeta, server: Literal["etl", "k8s"]):
    server_str = "ETL" if server == "etl" else "Kubernetes" + " ClickHouse server"
    print(
        f"Got query {f"with params {meta.params}" if meta.params else "without parameters"} (to be executed on {server_str}):\n{meta.query_or_template}"
    )
    creds: ClickHouseCredentials = ClickHouseCredentials.load("clickhouse-etl-config" if server == "etl" else "clickhouse-k8s-config")  # type: ignore
    client = create_client(creds)
    now = time()
    env = Environment(undefined=StrictUndefined)
    template = env.from_string(meta.query_or_template)
    query = template.render(**(meta.params or {}))
    print(f"Rendered template successfully to query:\n{query}")

    print("Executing query...")
    client.query(query)
    print(f"Done. Execution took {round(time() - now, 2)} seconds.")
    client.close()


@flow(log_prints=True)
def run_queries(query_templates: list[QueryMeta], server: Literal["etl", "k8s"]):

    print(
        f"Will execute {len(query_templates)} SQL query templates on {'ETL' if server == "etl" else "Kubernetes"} ClickHouse server"
    )
    for query_meta in query_templates:
        execute_query(query_meta, server)


if __name__ == "__main__":
    run_queries.serve()
    # for deployment, run this on the x86 machines where the worker should be built and running - cannot run this on my ARM MacBook as the image would be built for ARM
    # run_queries.deploy(
    #     "Run ClickHouse queries with Jinja template syntax support",
    #     work_pool_name="my-docker-pool",
    #     image="sejmou/sejmou-private:clickhouse-queries-v1.0",
    # )
