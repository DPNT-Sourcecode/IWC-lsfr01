"""Shared timestamp helpers for queue ordering and metrics."""

from __future__ import annotations

from datetime import datetime
from typing import Iterable


def normalize_timestamp(timestamp: datetime | str) -> datetime:
    """Convert a supported timestamp into a comparable naive datetime."""
    if isinstance(timestamp, datetime):
        return timestamp.replace(tzinfo=None)
    return datetime.fromisoformat(timestamp).replace(tzinfo=None)


def queue_age_seconds(timestamps: Iterable[datetime | str]) -> int:
    """Return the oldest/newest gap in seconds for the provided timestamps."""
    normalized = [normalize_timestamp(timestamp) for timestamp in timestamps]
    if not normalized:
        return 0
    return int((max(normalized) - min(normalized)).total_seconds())
