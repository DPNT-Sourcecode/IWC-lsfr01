from __future__ import annotations

from datetime import datetime

from solutions.IWC.task_types import TaskSubmission
from solutions.IWC.timestamp_utils import normalize_timestamp


class PendingTaskCleaner:
    """Normalizes pending queue tasks without changing queue orchestration."""

    def normalize(self, tasks: list[TaskSubmission]) -> list[TaskSubmission]:
        """Return a canonical pending queue with at most one task per user/provider."""
        deduped_by_key: dict[tuple[int, str], TaskSubmission] = {}
        ordered_keys: list[tuple[int, str]] = []

        for task in tasks:
            key = self._task_key(task)
            existing = deduped_by_key.get(key)
            if existing is None:
                deduped_by_key[key] = task
                ordered_keys.append(key)
                continue

            self._resolve_duplicate(existing, task)

        return [deduped_by_key[key] for key in ordered_keys]

    @staticmethod
    def _task_key(task: TaskSubmission) -> tuple[int, str]:
        """Build the deduplication key for a pending task."""
        return (task.user_id, task.provider)

    def _resolve_duplicate(
        self,
        existing: TaskSubmission,
        candidate: TaskSubmission,
    ) -> None:
        """Merge a duplicate task into the existing pending task."""
        chosen_timestamp = self._choose_timestamp(existing.timestamp, candidate.timestamp)
        existing.timestamp = chosen_timestamp

    def _choose_timestamp(
        self,
        left: datetime | str,
        right: datetime | str,
    ) -> datetime | str:
        """Choose the surviving timestamp using the current dedupe policy."""
        if normalize_timestamp(right) < normalize_timestamp(left):
            return right
        return left
