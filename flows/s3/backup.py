from prefect import flow
from prefect_shell import ShellOperation
from prefect_aws import S3Bucket
from typing import cast


class S3BackupMetadata:
    source_prefix: str
    """
    The prefix to backup from the source bucket. (Example: "spotify/")
    """

    target_prefix: str
    """
    The target prefix under which data should be backed up. If not provided, the source prefix will be used.
    """

    excludes: list[str]
    """
    Paths under the source_prefix to exclude. Each one will be passed to rclone as --exclude.
    """


@flow
def rclone_remote_backup(
    source_prefix: str, target_prefix: str | None = None, excludes: list[str] = []
):
    source_bucket = cast(S3Bucket, S3Bucket.load("s3-bucket"))
    target_bucket = cast(S3Bucket, S3Bucket.load("s3-backup-bucket"))
    excludes_str = " ".join([f"--exclude {exclude}" for exclude in excludes])
    rclone_command = f"rclone copy rds-eu-central-1:{source_bucket.bucket_name}/{source_prefix} b2-backup:{target_bucket.bucket_name}/{source_prefix if target_prefix is None else target_prefix} {excludes_str} --progress"
    ShellOperation(commands=[rclone_command]).run()


if __name__ == "__main__":
    rclone_remote_backup.serve()
