"""Distributed lease heartbeat monitoring and lease reaper services."""

import asyncio
import contextlib
import logging
from collections.abc import Callable
from types import TracebackType
from typing import Any

from aegisflow.core.exceptions import ConcurrencyConflictError, TaskLeaseExpiredError
from aegisflow.storage.base import LeaseInfo, LeaseManagerInterface

logger = logging.getLogger(__name__)


class HeartbeatMonitor:
    """Maintains an active lease heartbeat in the background for a running task."""

    def __init__(
        self,
        lease_manager: LeaseManagerInterface,
        lease_info: LeaseInfo,
        ttl_seconds: float = 15.0,
        on_lease_lost: Callable[[], Any] | None = None,
    ) -> None:
        self.lease_manager = lease_manager
        self.task_id = lease_info.task_id
        self.worker_id = lease_info.worker_id
        self.fencing_token = lease_info.fencing_token
        self.ttl_seconds = ttl_seconds
        self.on_lease_lost = on_lease_lost
        self._interval = max(ttl_seconds / 3.0, 0.1)
        self._task: asyncio.Task[None] | None = None
        self._running = False

    async def start(self) -> None:
        """Start the background heartbeat renewal loop."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._heartbeat_loop())

    async def stop(self) -> None:
        """Stop the heartbeat loop and await termination."""
        if not self._running:
            return
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        self._task = None

    async def _heartbeat_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._interval)
            if not self._running:
                break
            try:
                await self.lease_manager.renew_lease(
                    task_id=self.task_id,
                    worker_id=self.worker_id,
                    fencing_token=self.fencing_token,
                    ttl_seconds=self.ttl_seconds,
                )
            except (ConcurrencyConflictError, TaskLeaseExpiredError) as e:
                logger.warning(
                    "Heartbeat failed for task '%s': %s. Lease lost!",
                    self.task_id,
                    e,
                )
                self._running = False
                if self.on_lease_lost:
                    if asyncio.iscoroutinefunction(self.on_lease_lost):
                        await self.on_lease_lost()
                    else:
                        self.on_lease_lost()
                break
            except Exception as e:
                logger.error("Unexpected error renewing heartbeat for '%s': %s", self.task_id, e)

    async def __aenter__(self) -> "HeartbeatMonitor":
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.stop()


class LeaseReaper:
    """Background service that periodically reaps expired task leases."""

    def __init__(
        self,
        lease_manager: LeaseManagerInterface,
        scan_interval_seconds: float = 5.0,
        on_leases_reaped: Callable[[list[LeaseInfo]], Any] | None = None,
    ) -> None:
        self.lease_manager = lease_manager
        self.scan_interval = scan_interval_seconds
        self.on_leases_reaped = on_leases_reaped
        self._running = False
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Start the periodic lease reaper loop."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._reap_loop())

    async def stop(self) -> None:
        """Stop the lease reaper loop."""
        if not self._running:
            return
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        self._task = None

    async def _reap_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self.scan_interval)
            if not self._running:
                break
            try:
                expired = await self.lease_manager.reap_expired_leases()
                if expired and self.on_leases_reaped:
                    if asyncio.iscoroutinefunction(self.on_leases_reaped):
                        await self.on_leases_reaped(expired)
                    else:
                        self.on_leases_reaped(expired)
            except Exception as e:
                logger.error("Error running lease reaper: %s", e)
