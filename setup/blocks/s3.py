from dotenv import load_dotenv
import os
from prefect_aws import AwsCredentials, AwsClientParameters, S3Bucket
from prefect.blocks.system import Secret
from pydantic import SecretStr


def store_s3_secrets():
    """Store S3 configuration variables as Prefect secrets for Wasabi S3"""

    # Load environment variables from .env file
    load_dotenv()

    creds = AwsCredentials(
        aws_access_key_id=os.environ["S3_KEY_ID"],
        aws_secret_access_key=SecretStr(os.environ["S3_SECRET"]),
        aws_client_parameters=AwsClientParameters(
            endpoint_url=os.environ["S3_ENDPOINT_URL"],
        ),
    )

    bucket_name = Secret(value=SecretStr(os.environ["S3_BUCKET"]))

    bucket = S3Bucket(
        bucket_name=bucket_name.get(),
        credentials=creds,
    )

    creds.save("s3-creds", overwrite=True)
    bucket.save("s3-bucket", overwrite=True)


if __name__ == "__main__":
    store_s3_secrets()
    print("S3 credentials and bucket stored successfully.")
