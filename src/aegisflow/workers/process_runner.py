"""Execution runner for synchronous, blocking, or CPU-bound tasks."""

import asyncio
import time
import traceback
from concurrent.futures import Executor, ThreadPoolExecutor
from functools import partial
from typing import Any

from aegisflow.core.models import TaskDefinition
from aegisflow.workers.base_runner import BaseRunner, TaskExecutionResult


class ProcessRunner(BaseRunner):
    """Executes synchronous tasks in an executor pool without blocking the async event loop."""

    def __init__(self, executor: Executor | None = None, max_workers: int = 4) -> None:
        self._custom_executor = executor is not None
        self._executor = executor or ThreadPoolExecutor(max_workers=max_workers)

    async def run(
        self,
        task_def: TaskDefinition,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> TaskExecutionResult:
        start_time = time.perf_counter()
        task_id = task_def.task_id
        loop = asyncio.get_running_loop()

        # Wrap with partial to bind kwargs and args
        bound_fn = partial(task_def.fn, *args, **kwargs)

        try:
            future = loop.run_in_executor(self._executor, bound_fn)
            if task_def.timeout_seconds is not None:
                result = await asyncio.wait_for(future, timeout=task_def.timeout_seconds)
            else:
                result = await future

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
        """Shutdown the underlying worker executor."""
        if not self._custom_executor:
            self._executor.shutdown(wait=False)
