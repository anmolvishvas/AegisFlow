"""Worker supervisor daemon managing concurrency, task execution, and graceful shutdowns."""

import asyncio
import contextlib
import logging
import signal
import sys
from typing import Any
from uuid import uuid4

from aegisflow.core.events import (
    TaskCompleted,
    TaskFailed,
    TaskStarted,
)
from aegisflow.core.models import TaskDefinition
from aegisflow.engine.lease import HeartbeatMonitor
from aegisflow.storage.base import EventStoreInterface, LeaseManagerInterface
from aegisflow.workers.async_runner import AsyncRunner
from aegisflow.workers.base_runner import TaskExecutionResult
from aegisflow.workers.process_runner import ProcessRunner

logger = logging.getLogger(__name__)


class WorkerSupervisorDaemon:
    """Manages worker concurrency slots, leasing, task execution, and shutdown signals."""

    def __init__(
        self,
        event_store: EventStoreInterface,
        lease_manager: LeaseManagerInterface,
        worker_id: str | None = None,
        max_concurrency: int = 8,
    ) -> None:
        self.worker_id = worker_id or f"worker-{uuid4().hex[:8]}"
        self.event_store = event_store
        self.lease_manager = lease_manager
        self.max_concurrency = max_concurrency
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self.async_runner = AsyncRunner()
        self.process_runner = ProcessRunner()
        self._active_tasks: dict[str, asyncio.Task[Any]] = {}
        self._stopping = False

    @property
    def active_task_count(self) -> int:
        """Return the number of tasks currently in-flight."""
        return len(self._active_tasks)

    @property
    def is_stopping(self) -> bool:
        """Return True if the worker is in the process of shutting down."""
        return self._stopping

    def install_signal_handlers(self) -> None:
        """Register OS signal handlers for graceful shutdown on SIGINT and SIGTERM."""
        if sys.platform != "win32":
            loop = asyncio.get_running_loop()

            def _handle_signal() -> None:
                asyncio.create_task(self.shutdown())

            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(sig, _handle_signal)
                except (NotImplementedError, RuntimeError):
                    pass

    async def execute_task(
        self,
        task_def: TaskDefinition,
        workflow_id: str,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        lease_ttl_seconds: float = 15.0,
    ) -> TaskExecutionResult:
        """Acquire lease, monitor heartbeat, and execute a task to completion."""
        if self._stopping:
            return TaskExecutionResult(
                task_id=task_def.task_id,
                success=False,
                error="Worker is shutting down and cannot accept new tasks",
            )

        task_id = task_def.task_id
        kw = kwargs or {}

        async with self._semaphore:
            # 1. Acquire exclusive lease
            lease_info = await self.lease_manager.acquire_lease(
                task_id=task_id,
                workflow_id=workflow_id,
                worker_id=self.worker_id,
                ttl_seconds=lease_ttl_seconds,
            )

            # 2. Append TaskStarted event
            await self.event_store.append_event(
                TaskStarted(
                    workflow_id=workflow_id,
                    task_id=task_id,
                    worker_id=self.worker_id,
                )
            )

            current_task = asyncio.current_task()

            def on_lease_lost() -> None:
                if current_task and not current_task.done():
                    logger.warning("Cancelling task execution '%s' because lease was lost", task_id)
                    current_task.cancel()

            # 3. Execute with active heartbeat renewal
            heartbeat = HeartbeatMonitor(
                lease_manager=self.lease_manager,
                lease_info=lease_info,
                ttl_seconds=lease_ttl_seconds,
                on_lease_lost=on_lease_lost,
            )

            async with heartbeat:
                if task_def.is_async:
                    result = await self.async_runner.run(task_def, args, kw)
                else:
                    result = await self.process_runner.run(task_def, args, kw)

            # 4. Record outcome event
            if result.success:
                await self.event_store.append_event(
                    TaskCompleted(
                        workflow_id=workflow_id,
                        task_id=task_id,
                        result=result.result,
                    )
                )
            else:
                await self.event_store.append_event(
                    TaskFailed(
                        workflow_id=workflow_id,
                        task_id=task_id,
                        error=result.error or "Unknown task failure",
                        traceback=result.traceback,
                        is_retryable=True,
                    )
                )

            # 5. Cleanly release lease
            await self.lease_manager.release_lease(
                task_id=task_id,
                worker_id=self.worker_id,
                fencing_token=lease_info.fencing_token,
            )

            return result

    async def shutdown(self, grace_period_seconds: float = 10.0) -> None:
        """Initiate graceful shutdown, wait for running tasks, and clean up runners."""
        if self._stopping:
            return
        self._stopping = True
        logger.info(
            "Worker '%s' initiating graceful shutdown (grace period: %ss)",
            self.worker_id,
            grace_period_seconds,
        )

        # Wait for active tasks up to grace period
        if self._active_tasks:
            tasks = list(self._active_tasks.values())
            try:
                await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True),
                    timeout=grace_period_seconds,
                )
            except TimeoutError:
                logger.warning(
                    "Grace period elapsed. Cancelling %d remaining tasks",
                    len(self._active_tasks),
                )
                for t in tasks:
                    if not t.done():
                        t.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await asyncio.gather(*tasks, return_exceptions=True)

        await self.async_runner.close()
        await self.process_runner.close()
        logger.info("Worker '%s' shutdown complete", self.worker_id)
