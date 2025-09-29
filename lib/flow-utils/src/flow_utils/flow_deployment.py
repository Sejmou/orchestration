from prefect.docker import DockerImage


def create_image_config(
    flow_identifier: str,
    version="latest",
    private_repo=True,
    dockerfile_path: str = "Dockerfile",
) -> DockerImage:
    """
    Create a Docker image configuration for flow deployment (producing images that extend from the project-wide base Dockerfile).

    Args:
        flow_identifier (str): Identifier for the flow, used in the image name (e.g., "clickhouse-run-queries").
        version (str): Version tag (defaults to "latest").
        private_repo (bool): Whether to use the private repository (defaults to True). If True, image will always be fetched from the single free-tier Docker Hub repo (version tag is used to differentiate between worker images).
        dockerfile_path (str): Path to the Dockerfile (defaults to "Dockerfile").
    """
    if private_repo:
        return DockerImage(
            f"sejmou/sejmou-private:prefect-{flow_identifier}-{version}",
            dockerfile=dockerfile_path,
        )
    else:
        return DockerImage(
            f"sejmou/worker-{flow_identifier}:{version}",
            dockerfile=dockerfile_path,
        )
