from __future__ import annotations

from solutions.IWC.queue_solution_entrypoint import QueueSolutionEntrypoint
from solutions.IWC.task_types import TaskDispatch, TaskSubmission


def task(provider: str, user_id: int, timestamp: str) -> TaskSubmission:
    """Build a task submission for test scenarios."""
    return TaskSubmission(provider=provider, user_id=user_id, timestamp=timestamp)


def dispatched(provider: str, user_id: int) -> tuple[str, int]:
    """Build a compact dequeue expectation tuple."""
    return (provider, user_id)


def enqueue_all(
    queue: QueueSolutionEntrypoint,
    submissions: list[TaskSubmission],
) -> list[int]:
    """Enqueue a batch of submissions and capture each returned size."""
    return [queue.enqueue(item) for item in submissions]


def drain(queue: QueueSolutionEntrypoint) -> list[tuple[str, int]]:
    """Drain the queue into compact `(provider, user_id)` tuples."""
    items: list[tuple[str, int]] = []
    while True:
        item = queue.dequeue()
        if item is None:
            return items
        items.append((item.provider, item.user_id))


def test_basic_enqueue_size_and_empty_dequeue_flow() -> None:
    """Document the basic enqueue, size, and empty dequeue flow."""
    queue = QueueSolutionEntrypoint()

    size = queue.enqueue(task("companies_house", 1, "2025-10-20 12:00:00"))

    assert size == 1
    assert queue.size() == 1
    assert queue.dequeue() == TaskDispatch(provider="companies_house", user_id=1)
    assert queue.dequeue() is None


def test_rule_of_three_promotes_all_tasks_for_user() -> None:
    """Document the prompt's Rule of 3 example."""
    queue = QueueSolutionEntrypoint()

    sizes = enqueue_all(
        queue,
        [
            task("companies_house", 1, "2025-10-20 12:00:00"),
            task("bank_statements", 2, "2025-10-20 12:00:00"),
            task("id_verification", 1, "2025-10-20 12:00:00"),
            task("bank_statements", 1, "2025-10-20 12:00:00"),
        ],
    )

    assert sizes == [1, 2, 3, 4]
    assert drain(queue) == [
        dispatched("companies_house", 1),
        dispatched("id_verification", 1),
        dispatched("bank_statements", 1),
        dispatched("bank_statements", 2),
    ]


def test_older_timestamp_wins_when_priority_is_equal() -> None:
    """Document timestamp ordering when tasks share the same priority."""
    queue = QueueSolutionEntrypoint()

    enqueue_all(
        queue,
        [
            task("bank_statements", 1, "2025-10-20 12:05:00"),
            task("bank_statements", 2, "2025-10-20 12:00:00"),
        ],
    )

    assert drain(queue) == [
        dispatched("bank_statements", 2),
        dispatched("bank_statements", 1),
    ]


def test_credit_check_enqueues_dependency_before_task() -> None:
    """Document dependency expansion for credit check tasks."""
    queue = QueueSolutionEntrypoint()

    size = queue.enqueue(task("credit_check", 1, "2025-10-20 12:00:00"))

    assert size == 2
    assert drain(queue) == [
        dispatched("companies_house", 1),
        dispatched("credit_check", 1),
    ]


def test_dependency_generated_task_counts_toward_rule_of_three_threshold() -> None:
    """Show that dependency-generated tasks still count toward Rule of 3."""
    queue = QueueSolutionEntrypoint()

    enqueue_all(
        queue,
        [
            task("credit_check", 1, "2025-10-20 12:00:00"),
            task("id_verification", 1, "2025-10-20 12:01:00"),
            task("bank_statements", 2, "2025-10-20 11:57:00"),
        ],
    )

    assert drain(queue) == [
        dispatched("companies_house", 1),
        dispatched("credit_check", 1),
        dispatched("id_verification", 1),
        dispatched("bank_statements", 2),
    ]


def test_two_users_with_rule_of_three_are_ordered_by_oldest_group_timestamp() -> None:
    """Show how two promoted users are ordered against each other."""
    queue = QueueSolutionEntrypoint()

    enqueue_all(
        queue,
        [
            task("companies_house", 1, "2025-10-20 12:10:00"),
            task("id_verification", 1, "2025-10-20 12:11:00"),
            task("bank_statements", 1, "2025-10-20 12:12:00"),
            task("companies_house", 2, "2025-10-20 12:00:00"),
            task("id_verification", 2, "2025-10-20 12:01:00"),
            task("bank_statements", 2, "2025-10-20 12:02:00"),
        ],
    )

    assert drain(queue) == [
        dispatched("companies_house", 2),
        dispatched("id_verification", 2),
        dispatched("bank_statements", 2),
        dispatched("companies_house", 1),
        dispatched("id_verification", 1),
        dispatched("bank_statements", 1),
    ]


def test_identical_priority_and_timestamp_preserve_enqueue_order() -> None:
    """Document the stable-sort behavior for exact ordering ties on non-bank tasks."""
    queue = QueueSolutionEntrypoint()

    enqueue_all(
        queue,
        [
            task("companies_house", 1, "2025-10-20 12:00:00"),
            task("id_verification", 2, "2025-10-20 12:00:00"),
        ],
    )

    assert drain(queue) == [
        dispatched("companies_house", 1),
        dispatched("id_verification", 2),
    ]


def test_high_priority_promotion_remains_after_first_dequeue() -> None:
    """Capture the legacy sticky-priority behavior after the first dequeue."""
    queue = QueueSolutionEntrypoint()

    enqueue_all(
        queue,
        [
            task("companies_house", 1, "2025-10-20 12:00:00"),
            task("id_verification", 1, "2025-10-20 12:01:00"),
            task("bank_statements", 1, "2025-10-20 12:02:00"),
            task("companies_house", 2, "2025-10-20 12:03:00"),
            task("bank_statements", 2, "2025-10-20 12:04:00"),
        ],
    )

    first = queue.dequeue()

    assert first == TaskDispatch(provider="companies_house", user_id=1)
    assert drain(queue) == [
        dispatched("id_verification", 1),
        dispatched("bank_statements", 1),
        dispatched("companies_house", 2),
        dispatched("bank_statements", 2),
    ]


def test_age_returns_zero_for_empty_queue() -> None:
    """Queue age should be zero when there are no pending tasks."""
    queue = QueueSolutionEntrypoint()

    assert queue.age() == 0


def test_age_returns_gap_between_oldest_and_newest_pending_tasks() -> None:
    """Queue age should reflect the oldest/newest pending timestamp gap."""
    queue = QueueSolutionEntrypoint()
    enqueue_all(
        queue,
        [
            task("companies_house", 1, "2025-10-20 12:00:00"),
            task("id_verification", 2, "2025-10-20 12:05:00"),
        ],
    )

    assert queue.age() == 300


def test_age_uses_earliest_timestamp_after_deduplication() -> None:
    """Deduped tasks should keep the earliest timestamp for age calculation."""
    queue = QueueSolutionEntrypoint()
    enqueue_all(
        queue,
        [
            task("bank_statements", 1, "2025-10-20 12:05:00"),
            task("id_verification", 2, "2025-10-20 12:04:00"),
            task("bank_statements", 1, "2025-10-20 12:00:00"),
        ],
    )

    assert queue.age() == 240


def test_age_updates_after_dequeue_changes_pending_range() -> None:
    """Queue age should be recomputed from the remaining pending tasks."""
    queue = QueueSolutionEntrypoint()
    enqueue_all(
        queue,
        [
            task("companies_house", 1, "2025-10-20 12:00:00"),
            task("id_verification", 2, "2025-10-20 12:05:00"),
            task("bank_statements", 3, "2025-10-20 12:10:00"),
        ],
    )

    assert queue.age() == 600

    queue.dequeue()

    assert queue.age() == 300


def test_purge_clears_queue_and_resets_age() -> None:
    """Purging the queue should remove pending work and reset age to zero."""
    queue = QueueSolutionEntrypoint()
    enqueue_all(
        queue,
        [
            task("companies_house", 1, "2025-10-20 12:00:00"),
            task("id_verification", 1, "2025-10-20 12:01:00"),
        ],
    )

    assert queue.age() == 60
    assert queue.purge() is True
    assert queue.age() == 0
    assert queue.size() == 0
    assert queue.dequeue() is None


def test_unknown_provider_enqueues_without_dependencies() -> None:
    """Show that unknown providers enqueue as standalone tasks."""
    queue = QueueSolutionEntrypoint()

    size = queue.enqueue(task("made_up_provider", 7, "2025-10-20 12:00:00"))

    assert size == 1
    assert drain(queue) == [dispatched("made_up_provider", 7)]


def test_duplicate_direct_enqueue_keeps_queue_size_unchanged() -> None:
    """Show that a direct duplicate does not increase queue size."""
    queue = QueueSolutionEntrypoint()

    sizes = enqueue_all(
        queue,
        [
            task("bank_statements", 1, "2025-10-20 12:00:00"),
            task("bank_statements", 1, "2025-10-20 12:05:00"),
        ],
    )

    assert sizes == [1, 1]
    assert drain(queue) == [dispatched("bank_statements", 1)]


def test_duplicate_bank_statements_enqueue_stays_behind_same_users_non_bank_task() -> None:
    """Show that deduped bank statements still run after same-user non-bank work."""
    queue = QueueSolutionEntrypoint()

    enqueue_all(
        queue,
        [
            task("bank_statements", 1, "2025-10-20 12:01:00"),
            task("bank_statements", 1, "2025-10-20 12:04:00"),
            task("id_verification", 1, "2025-10-20 12:05:00"),
        ],
    )

    assert drain(queue) == [
        dispatched("id_verification", 1),
        dispatched("bank_statements", 1),
    ]


def test_older_bank_statements_duplicate_does_not_jump_ahead_of_same_users_non_bank_task() -> None:
    """Show that an older bank duplicate stays behind same-user non-bank work."""
    queue = QueueSolutionEntrypoint()

    enqueue_all(
        queue,
        [
            task("bank_statements", 1, "2025-10-20 12:05:00"),
            task("id_verification", 1, "2025-10-20 12:01:00"),
            task("bank_statements", 1, "2025-10-20 12:00:00"),
        ],
    )

    assert drain(queue) == [
        dispatched("id_verification", 1),
        dispatched("bank_statements", 1),
    ]


def test_duplicates_do_not_count_twice_toward_rule_of_three() -> None:
    """Show that duplicate tasks do not inflate Rule of 3 counts."""
    queue = QueueSolutionEntrypoint()

    enqueue_all(
        queue,
        [
            task("companies_house", 1, "2025-10-20 12:00:00"),
            task("companies_house", 1, "2025-10-20 12:05:00"),
            task("id_verification", 1, "2025-10-20 12:06:00"),
            task("bank_statements", 2, "2025-10-20 12:02:00"),
        ],
    )

    assert drain(queue) == [
        dispatched("companies_house", 1),
        dispatched("id_verification", 1),
        dispatched("bank_statements", 2),
    ]


def test_credit_check_then_direct_dependency_task_is_deduped() -> None:
    """Show dedupe when a dependency arrives before a direct enqueue."""
    queue = QueueSolutionEntrypoint()

    sizes = enqueue_all(
        queue,
        [
            task("credit_check", 1, "2025-10-20 12:05:00"),
            task("companies_house", 1, "2025-10-20 12:00:00"),
        ],
    )

    assert sizes == [2, 2]
    assert drain(queue) == [
        dispatched("companies_house", 1),
        dispatched("credit_check", 1),
    ]


def test_direct_dependency_task_then_credit_check_is_deduped() -> None:
    """Show dedupe when a direct dependency task exists before credit check."""
    queue = QueueSolutionEntrypoint()

    sizes = enqueue_all(
        queue,
        [
            task("companies_house", 1, "2025-10-20 12:00:00"),
            task("credit_check", 1, "2025-10-20 12:05:00"),
        ],
    )

    assert sizes == [1, 2]
    assert drain(queue) == [
        dispatched("companies_house", 1),
        dispatched("credit_check", 1),
    ]


def test_dedup_keeps_dependencies_and_distinct_tasks_together() -> None:
    """Show that dedupe removes duplicates without dropping distinct pending work."""
    queue = QueueSolutionEntrypoint()

    enqueue_all(
        queue,
        [
            task("credit_check", 1, "2025-10-20 12:00:00"),
            task("credit_check", 1, "2025-10-20 12:05:00"),
            task("id_verification", 1, "2025-10-20 12:01:00"),
        ],
    )

    assert drain(queue) == [
        dispatched("companies_house", 1),
        dispatched("credit_check", 1),
        dispatched("id_verification", 1),
    ]


def test_duplicates_are_scoped_per_user_and_do_not_collide_across_users() -> None:
    """Show that dedupe is scoped to the user/provider pair."""
    queue = QueueSolutionEntrypoint()

    sizes = enqueue_all(
        queue,
        [
            task("bank_statements", 1, "2025-10-20 12:00:00"),
            task("bank_statements", 2, "2025-10-20 12:01:00"),
            task("bank_statements", 1, "2025-10-20 12:05:00"),
        ],
    )

    assert sizes == [1, 2, 2]
    assert drain(queue) == [
        dispatched("bank_statements", 1),
        dispatched("bank_statements", 2),
    ]


def test_deduped_task_still_uses_timestamp_ordering_against_other_users() -> None:
    """Show that non-bank work still beats deduped bank statements globally."""
    queue = QueueSolutionEntrypoint()

    enqueue_all(
        queue,
        [
            task("bank_statements", 1, "2025-10-20 12:05:00"),
            task("id_verification", 2, "2025-10-20 12:01:00"),
            task("bank_statements", 1, "2025-10-20 12:00:00"),
        ],
    )

    assert drain(queue) == [
        dispatched("id_verification", 2),
        dispatched("bank_statements", 1),
    ]


def test_rule_of_three_still_applies_after_duplicate_noise_is_removed() -> None:
    """Show that Rule of 3 still applies after duplicate noise is cleaned out."""
    queue = QueueSolutionEntrypoint()

    enqueue_all(
        queue,
        [
            task("companies_house", 1, "2025-10-20 12:00:00"),
            task("id_verification", 1, "2025-10-20 12:01:00"),
            task("bank_statements", 1, "2025-10-20 12:02:00"),
            task("bank_statements", 1, "2025-10-20 12:05:00"),
            task("bank_statements", 2, "2025-10-20 12:02:00"),
        ],
    )

    assert drain(queue) == [
        dispatched("companies_house", 1),
        dispatched("id_verification", 1),
        dispatched("bank_statements", 1),
        dispatched("bank_statements", 2),
    ]


def test_non_prioritized_bank_statements_goes_to_end_of_global_queue() -> None:
    """Show the prompt example where normal bank statements are globally delayed."""
    queue = QueueSolutionEntrypoint()

    enqueue_all(
        queue,
        [
            task("bank_statements", 1, "2025-10-20 12:00:00"),
            task("id_verification", 1, "2025-10-20 12:01:00"),
            task("companies_house", 2, "2025-10-20 12:02:00"),
        ],
    )

    assert drain(queue) == [
        dispatched("id_verification", 1),
        dispatched("companies_house", 2),
        dispatched("bank_statements", 1),
    ]


def test_prioritized_bank_statements_runs_after_same_users_non_bank_tasks() -> None:
    """Show that promoted bank statements stay behind own non-bank tasks only."""
    queue = QueueSolutionEntrypoint()

    enqueue_all(
        queue,
        [
            task("companies_house", 1, "2025-10-20 12:00:00"),
            task("id_verification", 1, "2025-10-20 12:01:00"),
            task("bank_statements", 1, "2025-10-20 12:02:00"),
            task("companies_house", 2, "2025-10-20 11:00:00"),
        ],
    )

    assert drain(queue) == [
        dispatched("companies_house", 1),
        dispatched("id_verification", 1),
        dispatched("bank_statements", 1),
        dispatched("companies_house", 2),
    ]


def test_normal_priority_bank_statements_keep_timestamp_ordering_at_queue_tail() -> None:
    """Show timestamp ordering still applies within the delayed bank-statements bucket."""
    queue = QueueSolutionEntrypoint()

    enqueue_all(
        queue,
        [
            task("bank_statements", 1, "2025-10-20 12:05:00"),
            task("bank_statements", 2, "2025-10-20 12:00:00"),
            task("id_verification", 3, "2025-10-20 11:00:00"),
        ],
    )

    assert drain(queue) == [
        dispatched("id_verification", 3),
        dispatched("bank_statements", 2),
        dispatched("bank_statements", 1),
    ]


def test_older_bank_duplicate_updates_order_within_bank_statements_bucket() -> None:
    """Show that an older duplicate can move within the delayed bank bucket."""
    queue = QueueSolutionEntrypoint()

    enqueue_all(
        queue,
        [
            task("bank_statements", 1, "2025-10-20 12:05:00"),
            task("bank_statements", 2, "2025-10-20 12:02:00"),
            task("id_verification", 3, "2025-10-20 12:01:00"),
            task("bank_statements", 1, "2025-10-20 12:00:00"),
        ],
    )

    assert drain(queue) == [
        dispatched("id_verification", 3),
        dispatched("bank_statements", 1),
        dispatched("bank_statements", 2),
    ]


def test_credit_check_and_dependencies_still_run_before_normal_bank_statements() -> None:
    """Show that dependency resolution still beats delayed normal bank statements."""
    queue = QueueSolutionEntrypoint()

    enqueue_all(
        queue,
        [
            task("credit_check", 1, "2025-10-20 12:02:00"),
            task("bank_statements", 2, "2025-10-20 12:00:00"),
        ],
    )

    assert drain(queue) == [
        dispatched("companies_house", 1),
        dispatched("credit_check", 1),
        dispatched("bank_statements", 2),
    ]


def test_aged_bank_statements_can_jump_ahead_of_newer_non_bank_work() -> None:
    """A stale bank statement may overtake newer non-bank tasks."""
    queue = QueueSolutionEntrypoint()

    enqueue_all(
        queue,
        [
            task("id_verification", 1, "2025-10-20 12:00:00"),
            task("bank_statements", 2, "2025-10-20 12:01:00"),
            task("companies_house", 3, "2025-10-20 12:07:00"),
        ],
    )

    assert drain(queue) == [
        dispatched("id_verification", 1),
        dispatched("bank_statements", 2),
        dispatched("companies_house", 3),
    ]


def test_aged_bank_statements_cannot_jump_ahead_of_older_tasks() -> None:
    """A stale bank statement still stays behind older pending work."""
    queue = QueueSolutionEntrypoint()

    enqueue_all(
        queue,
        [
            task("bank_statements", 1, "2025-10-20 12:05:00"),
            task("companies_house", 2, "2025-10-20 12:00:00"),
            task("id_verification", 3, "2025-10-20 12:11:00"),
        ],
    )

    assert drain(queue) == [
        dispatched("companies_house", 2),
        dispatched("bank_statements", 1),
        dispatched("id_verification", 3),
    ]


def test_non_aged_bank_statements_still_stay_in_delayed_bucket() -> None:
    """A not-yet-stale bank statement should still behave like R3."""
    queue = QueueSolutionEntrypoint()

    enqueue_all(
        queue,
        [
            task("bank_statements", 1, "2025-10-20 12:03:00"),
            task("companies_house", 2, "2025-10-20 12:07:00"),
            task("id_verification", 3, "2025-10-20 12:06:00"),
        ],
    )

    assert drain(queue) == [
        dispatched("id_verification", 3),
        dispatched("companies_house", 2),
        dispatched("bank_statements", 1),
    ]


def test_aged_bank_statements_with_identical_timestamps_use_fifo() -> None:
    """Aged bank statement ties should preserve enqueue order."""
    queue = QueueSolutionEntrypoint()

    enqueue_all(
        queue,
        [
            task("bank_statements", 1, "2025-10-20 12:02:00"),
            task("bank_statements", 2, "2025-10-20 12:02:00"),
            task("companies_house", 3, "2025-10-20 12:08:00"),
        ],
    )

    assert drain(queue) == [
        dispatched("bank_statements", 1),
        dispatched("bank_statements", 2),
        dispatched("companies_house", 3),
    ]


def test_aged_bank_statements_beats_newer_rule_of_three_bank_task_on_tie_break() -> None:
    """Older stale bank work should beat a newer Rule-of-3 user's bank task."""
    queue = QueueSolutionEntrypoint()

    enqueue_all(
        queue,
        [
            task("id_verification", 1, "2025-10-20 12:00:00"),
            task("bank_statements", 2, "2025-10-20 12:02:00"),
            task("bank_statements", 1, "2025-10-20 12:02:00"),
            task("companies_house", 1, "2025-10-20 12:03:00"),
            task("companies_house", 3, "2025-10-20 12:10:00"),
        ],
    )

    assert drain(queue) == [
        dispatched("id_verification", 1),
        dispatched("bank_statements", 2),
        dispatched("bank_statements", 1),
        dispatched("companies_house", 1),
        dispatched("companies_house", 3),
    ]


def test_aged_bank_statements_beats_equal_timestamp_non_bank_work() -> None:
    """Aged bank work should beat equal-timestamp non-bank work."""
    queue = QueueSolutionEntrypoint()

    enqueue_all(
        queue,
        [
            task("id_verification", 1, "2025-10-20 12:00:00"),
            task("companies_house", 1, "2025-10-20 12:02:00"),
            task("bank_statements", 1, "2025-10-20 12:02:00"),
            task("companies_house", 2, "2025-10-20 12:10:00"),
        ],
    )

    assert drain(queue) == [
        dispatched("id_verification", 1),
        dispatched("bank_statements", 1),
        dispatched("companies_house", 1),
        dispatched("companies_house", 2),
    ]
