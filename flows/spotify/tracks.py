from prefect import flow
from utils.apis.distributed_tasks import (
    NewTaskPayload,
    create_task_execute_and_wait_for_completion,
)


@flow(log_prints=True)
async def fetch_spotify_tracks(ids: list[str], region: str = "de"):
    task_payload = NewTaskPayload(
        data_source="spotify-api",
        task_type="tracks",
        inputs=ids,
        params={"region": region},
    )
    return await create_task_execute_and_wait_for_completion(task_payload)


if __name__ == "__main__":
    fetch_spotify_tracks.serve()
