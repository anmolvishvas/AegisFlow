"""Integration tests for WorkerSupervisorDaemon, HeartbeatMonitor, and LeaseReaper."""

import asyncio
from pathlib import Path

import pytest

from aegisflow.core.models import TaskDefinition
from aegisflow.engine.lease import HeartbeatMonitor, LeaseReaper
from aegisflow.storage.base import LeaseInfo
from aegisflow.storage.sqlite import SQLiteStorage
from aegisflow.workers.daemon import WorkerSupervisorDaemon


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "worker_test.db"


@pytest.mark.asyncio
async def test_heartbeat_monitor_extends_lease(db_path: Path) -> None:
    storage = SQLiteStorage(database_path=db_path)
    await storage.initialize()

    lease = await storage.acquire_lease("t_heartbeat", "wf_1", "worker_1", ttl_seconds=0.3)

    monitor = HeartbeatMonitor(storage, lease, ttl_seconds=0.3)
    await monitor.start()

    # Sleep past the 0.1s heartbeat interval (0.3 / 3 = 0.1s)
    await asyncio.sleep(0.15)
    await monitor.stop()

    conn = await storage._get_conn()
    async with conn.execute(
        "SELECT expires_at FROM leases WHERE task_id = ?;", ("t_heartbeat",)
    ) as cursor:
        row = await cursor.fetchone()
        assert row is not None
        assert row["expires_at"] > lease.expires_at.isoformat()

    await storage.close()


@pytest.mark.asyncio
async def test_lease_reaper_cleans_expired(db_path: Path) -> None:
    storage = SQLiteStorage(database_path=db_path)
    await storage.initialize()

    await storage.acquire_lease("t_reap_1", "wf_1", "worker_1", ttl_seconds=0.03)

    reaped_items: list[str] = []

    def on_reaped(leases: list[LeaseInfo]) -> None:
        for item in leases:
            reaped_items.append(item.task_id)

    reaper = LeaseReaper(storage, scan_interval_seconds=0.03, on_leases_reaped=on_reaped)
    await reaper.start()

    await asyncio.sleep(0.12)
    await reaper.stop()

    assert "t_reap_1" in reaped_items
    await storage.close()


@pytest.mark.asyncio
async def test_worker_daemon_execute_async_task(db_path: Path) -> None:
    storage = SQLiteStorage(database_path=db_path)
    await storage.initialize()
    await storage.create_workflow("wf_daemon", "DaemonFlow")

    daemon = WorkerSupervisorDaemon(
        event_store=storage,
        lease_manager=storage,
        worker_id="test_worker_alpha",
        max_concurrency=4,
    )

    async def async_handler(msg: str) -> str:
        await asyncio.sleep(0.02)
        return f"Echo: {msg}"

    task_def = TaskDefinition(
        task_id="t_echo",
        name="echo",
        fn=async_handler,
        is_async=True,
    )

    result = await daemon.execute_task(
        task_def=task_def,
        workflow_id="wf_daemon",
        args=("Hello World",),
    )

    assert result.success is True
    assert result.result == "Echo: Hello World"

    # Verify events recorded in SQLite WAL
    events = await storage.get_events("wf_daemon")
    event_types = [e.event_type for e in events]
    assert "TaskStarted" in event_types
    assert "TaskCompleted" in event_types

    await daemon.shutdown(grace_period_seconds=1.0)
    await storage.close()


@pytest.mark.asyncio
async def test_worker_daemon_graceful_shutdown(db_path: Path) -> None:
    storage = SQLiteStorage(database_path=db_path)
    await storage.initialize()
    await storage.create_workflow("wf_shutdown", "ShutdownFlow")

    daemon = WorkerSupervisorDaemon(
        event_store=storage,
        lease_manager=storage,
        worker_id="worker_shutting_down",
    )

    await daemon.shutdown(grace_period_seconds=0.5)
    assert daemon.is_stopping is True

    # Attempting to execute task on a stopped worker returns rejection
    task_def = TaskDefinition(task_id="t_noop", name="noop", fn=lambda: None, is_async=False)
    rejected = await daemon.execute_task(task_def, "wf_shutdown")
    assert rejected.success is False
    assert "shutting down" in (rejected.error or "")

    await storage.close()
