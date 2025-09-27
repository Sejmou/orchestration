from prefect.docker import DockerImage


def create_image_config(
    flow_identifier: str, version: str, dockerfile_path: str = "Dockerfile"
) -> DockerImage:
    """
    Create a Docker image configuration for flow deployment (producing images that extend from the project-wide base Dockerfile).

    Args:
        flow_identifier (str): Identifier for the flow, used in the image name (e.g., "clickhouse-run-queries").
        version (str): Version of the flow, used in the image name (e.g., "v1.0").
    """
    return DockerImage(
        f"sejmou/sejmou-private:prefect-{flow_identifier}-{version}",
        dockerfile=dockerfile_path,
    )
