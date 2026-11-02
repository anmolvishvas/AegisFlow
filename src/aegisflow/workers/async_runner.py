"""Asynchronous coroutine task execution runner."""

import asyncio
import time
import traceback
from typing import Any

from aegisflow.core.models import TaskDefinition
from aegisflow.workers.base_runner import BaseRunner, TaskExecutionResult


class AsyncRunner(BaseRunner):
    """Executes asynchronous coroutine tasks directly on the asyncio event loop."""

    async def run(
        self,
        task_def: TaskDefinition,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> TaskExecutionResult:
        start_time = time.perf_counter()
        task_id = task_def.task_id

        try:
            coro = task_def.fn(*args, **kwargs)
            if task_def.timeout_seconds is not None:
                result = await asyncio.wait_for(coro, timeout=task_def.timeout_seconds)
            else:
                result = await coro

            duration = time.perf_counter() - start_time
            return TaskExecutionResult(
                task_id=task_id,
                success=True,
                result=result,
                duration_seconds=duration,
            )

        except TimeoutError:
            duration = time.perf_counter() - start_time
            return TaskExecutionResult(
                task_id=task_id,
                success=False,
                error=f"Task timed out after {task_def.timeout_seconds} seconds",
                traceback=traceback.format_exc(),
                duration_seconds=duration,
            )

        except Exception as e:
            duration = time.perf_counter() - start_time
            return TaskExecutionResult(
                task_id=task_id,
                success=False,
                error=str(e),
                traceback=traceback.format_exc(),
                duration_seconds=duration,
            )

    async def close(self) -> None:
        """No background threads or child processes to shut down for async runner."""
