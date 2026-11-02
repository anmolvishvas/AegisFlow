"""Unit tests for AsyncRunner and ProcessRunner task executors."""

import asyncio

import pytest

from aegisflow.core.models import TaskDefinition
from aegisflow.workers.async_runner import AsyncRunner
from aegisflow.workers.process_runner import ProcessRunner


@pytest.mark.asyncio
async def test_async_runner_success() -> None:
    async def sample_coro(a: int, b: int) -> int:
        await asyncio.sleep(0.01)
        return a * b

    task_def = TaskDefinition(
        task_id="t_mult",
        name="mult",
        fn=sample_coro,
        is_async=True,
    )

    runner = AsyncRunner()
    result = await runner.run(task_def, args=(3, 7), kwargs={})
    assert result.success is True
    assert result.result == 21
    assert result.duration_seconds > 0.0
    await runner.close()


@pytest.mark.asyncio
async def test_async_runner_exception() -> None:
    async def failing_coro() -> None:
        raise ValueError("Network timeout simulated")

    task_def = TaskDefinition(
        task_id="t_fail",
        name="fail",
        fn=failing_coro,
        is_async=True,
    )

    runner = AsyncRunner()
    result = await runner.run(task_def, args=(), kwargs={})
    assert result.success is False
    assert "Network timeout simulated" in (result.error or "")
    assert result.traceback is not None
    await runner.close()


@pytest.mark.asyncio
async def test_async_runner_timeout() -> None:
    async def slow_coro() -> None:
        await asyncio.sleep(0.2)

    task_def = TaskDefinition(
        task_id="t_slow",
        name="slow",
        fn=slow_coro,
        is_async=True,
        timeout_seconds=0.05,
    )

    runner = AsyncRunner()
    result = await runner.run(task_def, args=(), kwargs={})
    assert result.success is False
    assert "timed out after 0.05 seconds" in (result.error or "")
    await runner.close()


@pytest.mark.asyncio
async def test_process_runner_success() -> None:
    def sync_compute(n: int) -> int:
        return sum(i * 2 for i in range(n))

    task_def = TaskDefinition(
        task_id="t_sync",
        name="sync",
        fn=sync_compute,
        is_async=False,
    )

    runner = ProcessRunner(max_workers=2)
    result = await runner.run(task_def, args=(100,), kwargs={})
    assert result.success is True
    assert result.result == sum(i * 2 for i in range(100))
    await runner.close()


@pytest.mark.asyncio
async def test_process_runner_exception() -> None:
    def sync_failing() -> None:
        raise KeyError("Missing database key")

    task_def = TaskDefinition(
        task_id="t_sync_fail",
        name="sync_fail",
        fn=sync_failing,
        is_async=False,
    )

    runner = ProcessRunner(max_workers=2)
    result = await runner.run(task_def, args=(), kwargs={})
    assert result.success is False
    assert "Missing database key" in (result.error or "")
    await runner.close()
