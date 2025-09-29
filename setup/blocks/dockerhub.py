from prefect_docker import DockerRegistryCredentials
from pydantic import SecretStr
from dotenv import load_dotenv
import os


def store_dockerhub_credentials():
    """Store DockerHub credentials as Prefect secret."""
    load_dotenv()

    dockerhub_username = os.environ["DOCKERHUB_USERNAME"]
    dockerhub_token = SecretStr(os.environ["DOCKERHUB_TOKEN"])

    DockerRegistryCredentials(
        username=dockerhub_username,
        password=dockerhub_token,
        registry_url="registry-1.docker.io",
    ).save("dockerhub-creds", overwrite=True)
    print("DockerHub credentials stored successfully.")


if __name__ == "__main__":
    store_dockerhub_credentials()
