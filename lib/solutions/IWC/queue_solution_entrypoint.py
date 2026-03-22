"""Public entrypoint exposing the typed queue implementation to the runner."""

from __future__ import annotations

from solutions.IWC.queue_solution_legacy import Queue
from solutions.IWC.task_types import TaskDispatch, TaskSubmission

class QueueSolutionEntrypoint:

    def __init__(self) -> None:
        """Create the public wrapper around the queue implementation."""
        self._queue: Queue = Queue()

    def enqueue(self, task: TaskSubmission) -> int:
        """Forward an enqueue request to the underlying queue."""
        return self._queue.enqueue(task)

    def dequeue(self) -> TaskDispatch | None:
        """Forward a dequeue request to the underlying queue."""
        return self._queue.dequeue()

    def size(self) -> int:
        """Return the current number of pending tasks."""
        return self._queue.size

    def age(self) -> int:
        """Return the queue age reported by the underlying implementation."""
        return self._queue.age

    def purge(self) -> bool:
        """Clear all pending tasks through the underlying queue."""
        return self._queue.purge()
