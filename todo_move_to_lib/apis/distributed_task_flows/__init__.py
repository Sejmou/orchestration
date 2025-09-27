from datetime import datetime
import os
import asyncio
import aiohttp
from collections import Counter, defaultdict
from dotenv import load_dotenv
from pydantic import BaseModel, SecretStr
from prefect.blocks.system import Secret
from prefect import flow


def store_distributed_tasks_api_url():
    load_dotenv()
    api_url = os.environ["DISTRIBUTED_TASKS_API_URL"]
    url_secret = Secret(value=SecretStr(api_url))
    url_secret.save("distributed-tasks-api-url", overwrite=True)
    print("Distributed tasks API URL stored successfully.")


class NewTaskPayload(BaseModel):
    """
    Input for the creation of a new distributed task.
    """

    data_source: str
    task_type: str
    inputs: list
    params: dict | None = None


class CreatedTaskMeta(BaseModel):
    """
    Returned after the creation of a new distributed task.
    """

    id: int
    dataSource: str
    taskType: str
    params: dict | None = None
    createdAt: datetime


class Scraper(BaseModel):
    id: int
    protocol: str
    host: str
    port: int
    addedAt: datetime


class SubTaskDetails(BaseModel):
    id: int
    taskId: int
    scraperTaskId: int
    scraperId: int
    scraper: Scraper


class TaskDetails(BaseModel):
    """
    Details of a distributed task. Includes metadata about the task and its subtasks.
    """

    id: int
    dataSource: str
    taskType: str
    params: dict | None = None
    createdAt: datetime
    subtasks: list[SubTaskDetails]


class TaskProgress(BaseModel):
    success_count: int
    failure_count: int
    inputs_without_output_count: int
    remaining_count: int


class DistributedTasksAPIClient:
    def __init__(self, base_url: str):
        self.base_url = base_url

    async def create_task(
        self,
        *,
        data_source: str,
        task_type: str,
        inputs: list,
        params: dict | None = None,
    ):
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.base_url}/tasks/create",
                json={
                    "task": {
                        "dataSource": data_source,
                        "taskType": task_type,
                        "inputs": inputs,
                        "params": params or {},
                    }
                },
            ) as response:
                if not response.ok:
                    text_resp = await response.text()
                    raise Exception(
                        f"Failed to create task (error code {response.status}): {text_resp}"
                    )
                data = await response.json()
                return CreatedTaskMeta.model_validate(data)

    async def execute_newly_created_task_once_ready(
        self, task_details: TaskDetails, inputs: list[str]
    ):
        progress = await self.get_task_progress(task_details)
        for count_name in [
            "success_count",
            "failure_count",
            "inputs_without_output_count",
        ]:
            count = getattr(progress, count_name)
            if count > 0:
                raise ValueError(
                    f"{count_name} == {count} -> {count_name} > 0 -> task is not newly created (or has been started too early)"
                )
        # after creating a task, it takes a while until all items are added to the remaining inputs for each subtasks
        # remaining_count of get_task_progress(...) is the sum of all remaining_counts
        # due to a bug, only once this count matches the initial input length, the task (or rather it's subtasks) will actually get all the inputs after execution starts (i.e. adding new items while a task is already running is apparently broken)
        remaining_count = progress.remaining_count
        while not remaining_count == len(inputs):
            print(
                f"{len(inputs) - remaining_count} added items need to processed before task is ready for execution"
            )
            await asyncio.sleep(1)
            progress = await self.get_task_progress(task_details)
            remaining_count = progress.remaining_count
        await self.execute_task(task_details.id)
        print("Task is executing :)")

    async def execute_task(self, task_id: int):
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.base_url}/tasks/{task_id}/execute"
            ) as response:
                if not response.ok:
                    raise Exception(
                        f"Failed to execute task (error code {response.status})"
                    )
                return await response.json()

    async def pause_task(self, task_id: int):
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.base_url}/tasks/{task_id}/pause"
            ) as response:
                if not response.ok:
                    raise Exception(
                        f"Failed to pause task (error code {response.status})"
                    )
                return await response.json()

    async def get_task_details(self, task_id: int):
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.base_url}/tasks/{task_id}") as response:
                if not response.ok:
                    raise Exception(
                        f"Failed to get task details (error code {response.status})"
                    )
                res = await response.json()
                return TaskDetails.model_validate(res)

    async def get_task_progress(self, task_details: TaskDetails):
        progresses: list[TaskProgress] = await asyncio.gather(
            *[
                self.get_scraper_subtask_progress(
                    scraper_id=subtask.scraperId,
                    subtask_id=subtask.scraperTaskId,
                )
                for subtask in task_details.subtasks
            ]
        )
        progress_dicts = [p.model_dump() for p in progresses]
        total_progress = Counter()
        for p in progress_dicts:
            total_progress.update(p)
        res = dict(total_progress)
        return TaskProgress.model_validate(res)

    async def get_scraper_subtask_progress(self, scraper_id: int, subtask_id: int):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{self.base_url}/scrapers/{scraper_id}/tasks/{subtask_id}/progress"
            ) as response:
                if not response.ok:
                    raise Exception(
                        f"Failed to get task progress (error code {response.status})"
                    )
                res = await response.json()
                return TaskProgress.model_validate(res)

    async def get_subtask_states(self, task_details: TaskDetails):
        states: list[dict] = await asyncio.gather(
            *[
                self.get_scraper_subtask_state(
                    scraper_id=subtask.scraperId,
                    subtask_id=subtask.scraperTaskId,
                )
                for subtask in task_details.subtasks
            ]
        )
        result = defaultdict(list)
        for item in states:
            result[item["status"]].append(item["id"])

        return dict(result)

    async def get_scraper_subtask_state(self, scraper_id: int, subtask_id: int):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{self.base_url}/scrapers/{scraper_id}/tasks/{subtask_id}"
            ) as response:
                if not response.ok:
                    raise Exception(
                        f"Failed to get scraper subtask state (error code {response.status})"
                    )
                return await response.json()


@flow(log_prints=True)
async def create_task_execute_and_wait_for_completion(new_task_payload: NewTaskPayload):
    base_url = (await Secret.load("distributed-tasks-api-url")).get()  # type: ignore
    client = DistributedTasksAPIClient(base_url)

    task = await client.create_task(
        data_source=new_task_payload.data_source,
        task_type=new_task_payload.task_type,
        inputs=new_task_payload.inputs,
        params=new_task_payload.params,
    )

    task_details = await client.get_task_details(task.id)

    await client.execute_newly_created_task_once_ready(
        task_details, new_task_payload.inputs
    )

    progress = await client.get_task_progress(task_details)
    while progress.remaining_count > 0:
        print(f"Task not yet completed, {progress.remaining_count} items remaining...")
        await asyncio.sleep(10)
        progress = await client.get_task_progress(task_details)


if __name__ == "__main__":
    store_distributed_tasks_api_url()
