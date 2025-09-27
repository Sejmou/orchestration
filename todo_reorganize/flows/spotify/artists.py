from prefect import flow
from utils.apis.distributed_tasks import (
    NewTaskPayload,
    create_task_execute_and_wait_for_completion,
)


@flow(log_prints=True)
async def fetch_spotify_artists(ids: list[str]):
    task_payload = NewTaskPayload(
        data_source="spotify-api",
        task_type="artists",
        inputs=ids,
    )
    return await create_task_execute_and_wait_for_completion(task_payload)


if __name__ == "__main__":
    fetch_spotify_artists.serve()
