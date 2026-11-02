"""Retry scheduler supporting exponential backoff and randomized full jitter."""

import asyncio
import logging
from datetime import UTC, datetime, timedelta

from aegisflow.core.models import RetryPolicy

logger = logging.getLogger(__name__)


class RetryScheduler:
    """Manages retry evaluations and non-blocking backoff scheduling for failed tasks."""

    @staticmethod
    def should_retry(attempt: int, retry_policy: RetryPolicy) -> bool:
        """Return True if the current attempt number is eligible for retry.

        Args:
            attempt: Current 1-indexed attempt number.
            retry_policy: Configured retry policy for the task.
        """
        return attempt <= retry_policy.max_retries

    @staticmethod
    def calculate_retry(
        attempt: int,
        retry_policy: RetryPolicy,
    ) -> tuple[float, datetime]:
        """Calculate the backoff delay and scheduled execution timestamp.

        Args:
            attempt: 1-indexed attempt number.
            retry_policy: Configured retry policy with backoff and jitter options.

        Returns:
            Tuple of (delay_seconds, next_retry_at_timestamp).
        """
        delay = retry_policy.calculate_delay(attempt)
        next_retry_at = datetime.now(UTC) + timedelta(seconds=delay)
        return delay, next_retry_at

    @staticmethod
    async def wait_for_retry(delay_seconds: float) -> None:
        """Wait for the scheduled delay non-blockingly on the event loop."""
        if delay_seconds > 0:
            await asyncio.sleep(delay_seconds)
