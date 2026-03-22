from dataclasses import dataclass
from functools import cmp_to_key
from datetime import datetime
from enum import IntEnum
from typing import NamedTuple

# LEGACY CODE ASSET
# RESOLVED on deploy
from solutions.IWC.queue_cleaner import PendingTaskCleaner
from solutions.IWC.task_types import TaskSubmission, TaskDispatch
from solutions.IWC.timestamp_utils import normalize_timestamp, queue_age_seconds

class Priority(IntEnum):
    """Represents the queue ordering tiers observed in the legacy system."""
    HIGH = 1
    NORMAL = 2

@dataclass
class Provider:
    name: str
    base_url: str
    depends_on: list[str]

class DispatchSortKey(NamedTuple):
    """Explicit ordering fields for choosing the next task to dispatch."""

    priority: Priority
    group_phase: int
    group_timestamp: datetime | None
    bank_statements_rank: int
    task_timestamp: datetime

COMPANIES_HOUSE_PROVIDER = Provider(
    name="companies_house", base_url="https://fake.companieshouse.co.uk", depends_on=[]
)


CREDIT_CHECK_PROVIDER = Provider(
    name="credit_check",
    base_url="https://fake.creditcheck.co.uk",
    depends_on=["companies_house"],
)


BANK_STATEMENTS_PROVIDER = Provider(
    name="bank_statements", base_url="https://fake.bankstatements.co.uk", depends_on=[]
)

ID_VERIFICATION_PROVIDER = Provider(
    name="id_verification", base_url="https://fake.idv.co.uk", depends_on=[]
)


REGISTERED_PROVIDERS: list[Provider] = [
    BANK_STATEMENTS_PROVIDER,
    COMPANIES_HOUSE_PROVIDER,
    CREDIT_CHECK_PROVIDER,
    ID_VERIFICATION_PROVIDER,
]

TIME_SENSITIVE_BANK_STATEMENTS_THRESHOLD_SECONDS = 300

class Queue:
    def __init__(self):
        """Initialize the in-memory queue and its pending-task cleaner."""
        self._queue = []
        self._pending_task_cleaner = PendingTaskCleaner()

    def _collect_dependencies(self, task: TaskSubmission) -> list[TaskSubmission]:
        """Expand the dependencies required before the provided task can run."""
        provider = next((p for p in REGISTERED_PROVIDERS if p.name == task.provider), None)
        if provider is None:
            return []

        tasks: list[TaskSubmission] = []
        for dependency in provider.depends_on:
            dependency_task = TaskSubmission(
                provider=dependency,
                user_id=task.user_id,
                timestamp=task.timestamp,
            )
            tasks.extend(self._collect_dependencies(dependency_task))
            tasks.append(dependency_task)
        return tasks

    @staticmethod
    def _priority_for_task(task):
        """Return the current priority tier recorded for a task."""
        metadata = task.metadata
        raw_priority = metadata.get("priority", Priority.NORMAL)
        try:
            return Priority(raw_priority)
        except (TypeError, ValueError):
            return Priority.NORMAL

    @staticmethod
    def _earliest_group_timestamp_for_task(task):
        """Return the earliest timestamp used for Rule of 3 group ordering."""
        metadata = task.metadata
        return metadata.get("group_earliest_timestamp")

    @staticmethod
    def _is_bank_statements_task(task) -> bool:
        """Return whether the task targets the bank statements provider."""
        return task.provider == BANK_STATEMENTS_PROVIDER.name

    def _effective_group_timestamp_for_task(self, task):
        """Return the grouping timestamp used for dequeue ordering."""
        if self._priority_for_task(task) == Priority.HIGH:
            return self._earliest_group_timestamp_for_task(task)
        return None

    def _group_phase_for_task(self, task) -> int:
        """Separate promoted tasks from normal tasks without sentinel timestamps."""
        if self._priority_for_task(task) == Priority.HIGH:
            return 0
        return 1

    def _bank_statements_rank_for_task(self, task) -> int:
        """Rank bank statements behind non-bank tasks within the same tier or group."""
        if self._is_bank_statements_task(task):
            return 1
        return 0

    @staticmethod
    def _timestamp_for_task(task):
        """Convert a task timestamp into a comparable naive datetime."""
        return normalize_timestamp(task.timestamp)

    def _newest_pending_timestamp(self):
        """Return the newest timestamp among currently pending tasks."""
        if not self._queue:
            return None
        return max(self._timestamp_for_task(task) for task in self._queue)

    def _is_time_sensitive_bank_statements_task(self, task, newest_timestamp) -> bool:
        """Return whether a bank statements task has waited at least five minutes."""
        if newest_timestamp is None or not self._is_bank_statements_task(task):
            return False

        return (
            newest_timestamp - self._timestamp_for_task(task)
        ).total_seconds() >= TIME_SENSITIVE_BANK_STATEMENTS_THRESHOLD_SECONDS

    def _dequeue_sort_key_for_task(self, task):
        """Build the ordering key for dispatching the next pending task."""
        return DispatchSortKey(
            priority=self._priority_for_task(task),
            group_phase=self._group_phase_for_task(task),
            group_timestamp=self._effective_group_timestamp_for_task(task),
            bank_statements_rank=self._bank_statements_rank_for_task(task),
            task_timestamp=self._timestamp_for_task(task),
        )

    def _compare_time_sensitive_bank_tasks(self, left, right) -> int:
        """Compare two aged bank statements by timestamp and then FIFO."""
        left_timestamp = self._timestamp_for_task(left)
        right_timestamp = self._timestamp_for_task(right)
        if left_timestamp < right_timestamp:
            return -1
        if left_timestamp > right_timestamp:
            return 1
        return 0

    def _compare_time_sensitive_bank_task_against_normal(
        self,
        time_sensitive_task,
        normal_task,
    ) -> int:
        """Allow aged bank work ahead of equal or newer tasks, but never older ones."""
        time_sensitive_timestamp = self._timestamp_for_task(time_sensitive_task)
        normal_timestamp = self._timestamp_for_task(normal_task)

        if normal_timestamp < time_sensitive_timestamp:
            return 1
        return -1

    def _compare_tasks_for_dispatch(self, left, right, newest_timestamp) -> int:
        """Apply the R5 aged-bank exception, else fall back to legacy ordering."""
        left_is_time_sensitive = self._is_time_sensitive_bank_statements_task(
            left, newest_timestamp
        )
        right_is_time_sensitive = self._is_time_sensitive_bank_statements_task(
            right, newest_timestamp
        )

        if left_is_time_sensitive and right_is_time_sensitive:
            return self._compare_time_sensitive_bank_tasks(left, right)

        if left_is_time_sensitive != right_is_time_sensitive:
            if left_is_time_sensitive:
                return self._compare_time_sensitive_bank_task_against_normal(left, right)
            return -self._compare_time_sensitive_bank_task_against_normal(right, left)

        left_key = self._dequeue_sort_key_for_task(left)
        right_key = self._dequeue_sort_key_for_task(right)
        if left_key < right_key:
            return -1
        if left_key > right_key:
            return 1
        return 0

    def enqueue(self, item: TaskSubmission) -> int:
        """Add a task and its dependencies, then normalize pending duplicates."""
        tasks = [*self._collect_dependencies(item), item]

        for task in tasks:
            metadata = task.metadata
            metadata.setdefault("priority", Priority.NORMAL)
            metadata.setdefault("group_earliest_timestamp", None)
        self._queue = self._pending_task_cleaner.normalize([*self._queue, *tasks])
        return self.size

    def dequeue(self):
        """Return the next task to process according to the legacy ordering rules."""
        if self.size == 0:
            return None

        user_ids = {task.user_id for task in self._queue}
        task_count = {}
        priority_timestamps = {}
        for user_id in user_ids:
            user_tasks = [t for t in self._queue if t.user_id == user_id]
            earliest_timestamp = sorted(user_tasks, key=lambda t: t.timestamp)[0].timestamp
            priority_timestamps[user_id] = earliest_timestamp
            task_count[user_id] = len(user_tasks)

        for task in self._queue:
            metadata = task.metadata
            current_earliest = metadata.get("group_earliest_timestamp")
            raw_priority = metadata.get("priority")
            try:
                priority_level = Priority(raw_priority)
            except (TypeError, ValueError):
                priority_level = None

            if priority_level is None or priority_level == Priority.NORMAL:
                metadata["group_earliest_timestamp"] = None
                if task_count[task.user_id] >= 3:
                    metadata["group_earliest_timestamp"] = priority_timestamps[task.user_id]
                    metadata["priority"] = Priority.HIGH
                else:
                    metadata["priority"] = Priority.NORMAL
            else:
                metadata["group_earliest_timestamp"] = current_earliest
                metadata["priority"] = priority_level

        newest_timestamp = self._newest_pending_timestamp()
        self._queue.sort(
            key=cmp_to_key(
                lambda left, right: self._compare_tasks_for_dispatch(
                    left, right, newest_timestamp
                )
            )
        )

        task = self._queue.pop(0)
        return TaskDispatch(
            provider=task.provider,
            user_id=task.user_id,
        )

    @property
    def size(self):
        """Return the number of pending tasks currently in the queue."""
        return len(self._queue)

    @property
    def age(self):
        """Return the internal queue age in seconds."""
        return queue_age_seconds(task.timestamp for task in self._queue)

    def purge(self):
        """Clear all pending tasks from the queue."""
        self._queue.clear()
        return True

"""
===================================================================================================

The following code is only to visualise the final usecase.
No changes are needed past this point.

To test the correct behaviour of the queue system, import the `Queue` class directly in your tests.

===================================================================================================

```python
import asyncio
import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(queue_worker())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        logger.info("Queue worker cancelled on shutdown.")


app = FastAPI(lifespan=lifespan)
queue = Queue()


@app.get("/")
def read_root():
    return {
        "registered_providers": [
            {"name": p.name, "base_url": p.base_url} for p in registered_providers
        ]
    }


class DataRequest(BaseModel):
    user_id: int
    providers: list[str]


@app.post("/fetch_customer_data")
def fetch_customer_data(data: DataRequest):
    provider_names = [p.name for p in registered_providers]

    for provider in data.providers:
        if provider not in provider_names:
            logger.warning(f"Provider {provider} doesn't exists. Skipping")
            continue

        queue.enqueue(
            TaskSubmission(
                provider=provider,
                user_id=data.user_id,
                timestamp=datetime.now(),
            )
        )

    return {"status": f"{len(data.providers)} Task(s) added to queue"}


async def queue_worker():
    while True:
        if queue.size == 0:
            await asyncio.sleep(1)
            continue

        task = queue.dequeue()
        if not task:
            continue

        logger.info(f"Processing task: {task}")
        await asyncio.sleep(2)
        logger.info(f"Finished task: {task}")
```
"""
