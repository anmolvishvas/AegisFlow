"""Unit tests for RetryPolicy calculations and RetryScheduler."""

from datetime import UTC, datetime

from aegisflow.core.models import RetryPolicy
from aegisflow.engine.retry import RetryScheduler


def test_retry_policy_delay_calculation() -> None:
    policy = RetryPolicy(
        max_retries=3,
        initial_interval_seconds=1.0,
        backoff_coefficient=2.0,
        max_interval_seconds=10.0,
        jitter=False,
    )

    # attempt 1: 1.0 * (2^0) = 1.0
    assert policy.calculate_delay(1) == 1.0
    # attempt 2: 1.0 * (2^1) = 2.0
    assert policy.calculate_delay(2) == 2.0
    # attempt 3: 1.0 * (2^2) = 4.0
    assert policy.calculate_delay(3) == 4.0
    # attempt 4: 1.0 * (2^3) = 8.0
    assert policy.calculate_delay(4) == 8.0
    # attempt 5: 1.0 * (2^4) = 16.0 -> capped at max_interval 10.0
    assert policy.calculate_delay(5) == 10.0


def test_retry_policy_jitter_distribution() -> None:
    policy = RetryPolicy(
        max_retries=3,
        initial_interval_seconds=2.0,
        backoff_coefficient=2.0,
        jitter=True,
    )

    delays = [policy.calculate_delay(2) for _ in range(50)]
    # Without jitter, attempt 2 would be exactly 4.0. With jitter, all values are in [0, 4.0]
    assert all(0.0 <= d <= 4.0 for d in delays)
    # Ensure there is variation
    assert len(set(delays)) > 1


def test_retry_scheduler_should_retry() -> None:
    policy = RetryPolicy(max_retries=2)

    assert RetryScheduler.should_retry(1, policy) is True
    assert RetryScheduler.should_retry(2, policy) is True
    assert RetryScheduler.should_retry(3, policy) is False


def test_retry_scheduler_calculate_retry() -> None:
    policy = RetryPolicy(
        max_retries=3,
        initial_interval_seconds=1.5,
        jitter=False,
    )

    now = datetime.now(UTC)
    delay, next_retry_at = RetryScheduler.calculate_retry(1, policy)

    assert delay == 1.5
    assert next_retry_at > now
