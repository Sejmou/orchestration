from flows.clickhouse.run_queries import run_queries
from prefect.docker import DockerImage


def create_image(tag: str) -> DockerImage:
    return DockerImage(
        f"sejmou/sejmou-private:{tag}",
        dockerfile="Dockerfile",
    )


run_queries.deploy(
    "Run ClickHouse queries with Jinja template syntax support",
    work_pool_name="Docker",
    image=create_image("prefect-clickhouse-run-queries-v1.0"),
)
