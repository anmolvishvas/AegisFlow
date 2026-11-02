"""Integration tests for SQLiteStorage event sourcing and distributed task leasing."""

import asyncio
from pathlib import Path
import pytest

from aegisflow.core.events import (
    TaskCompleted,
    TaskScheduled,
    WorkflowCompleted,
    WorkflowInitiated,
)
from aegisflow.core.exceptions import (
    ConcurrencyConflictError,
    TaskLeaseExpiredError,
    WorkflowAlreadyExistsError,
)
from aegisflow.core.states import WorkflowState
from aegisflow.storage.sqlite import SQLiteStorage


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "test_aegisflow.db"


@pytest.mark.asyncio
async def test_sqlite_initialization_and_pragmas(db_path: Path) -> None:
    storage = SQLiteStorage(database_path=db_path)
    await storage.initialize()

    conn = await storage._get_conn()
    async with conn.execute("PRAGMA journal_mode;") as cursor:
        row = await cursor.fetchone()
        assert row is not None
        assert row[0].lower() == "wal"

    await storage.close()


@pytest.mark.asyncio
async def test_workflow_lifecycle(db_path: Path) -> None:
    storage = SQLiteStorage(database_path=db_path)
    await storage.initialize()

    await storage.create_workflow(
        workflow_id="wf-001",
        name="TestFlow",
        status=WorkflowState.PENDING,
        input_data={"order_id": 42},
    )

    record = await storage.get_workflow_record("wf-001")
    assert record is not None
    assert record["workflow_id"] == "wf-001"
    assert record["status"] == WorkflowState.PENDING
    assert record["input_data"] == {"order_id": 42}

    # Duplicate creation raises WorkflowAlreadyExistsError
    with pytest.raises(WorkflowAlreadyExistsError):
        await storage.create_workflow("wf-001", "Duplicate")

    # Update workflow status to COMPLETED with output
    await storage.update_workflow_status(
        workflow_id="wf-001",
        status=WorkflowState.COMPLETED,
        output_data={"result": "success"},
    )
    updated = await storage.get_workflow_record("wf-001")
    assert updated is not None
    assert updated["status"] == WorkflowState.COMPLETED
    assert updated["output_data"] == {"result": "success"}

    await storage.close()


@pytest.mark.asyncio
async def test_event_sourcing_append_and_retrieve(db_path: Path) -> None:
    storage = SQLiteStorage(database_path=db_path)
    await storage.initialize()

    wf_id = "wf-events"
    await storage.create_workflow(wf_id, "OrderPipeline")

    e1 = WorkflowInitiated(workflow_id=wf_id, workflow_name="OrderPipeline")
    e2 = TaskScheduled(workflow_id=wf_id, task_id="task-1", task_name="validate")
    e3 = TaskCompleted(workflow_id=wf_id, task_id="task-1", result={"valid": True})
    e4 = WorkflowCompleted(workflow_id=wf_id, output_data={"done": True})

    await storage.append_event(e1)
    await storage.append_event(e2)
    await storage.append_event(e3)
    await storage.append_event(e4)

    events = await storage.get_events(wf_id)
    assert len(events) == 4
    assert isinstance(events[0], WorkflowInitiated)
    assert isinstance(events[1], TaskScheduled)
    assert isinstance(events[2], TaskCompleted)
    assert isinstance(events[3], WorkflowCompleted)
    assert events[2].result == {"valid": True}

    await storage.close()


@pytest.mark.asyncio
async def test_active_workflows_filtering(db_path: Path) -> None:
    storage = SQLiteStorage(database_path=db_path)
    await storage.initialize()

    await storage.create_workflow("wf-active", "Flow1", status=WorkflowState.RUNNING)
    await storage.create_workflow("wf-done", "Flow2", status=WorkflowState.COMPLETED)
    await storage.create_workflow("wf-failed", "Flow3", status=WorkflowState.FAILED)
    await storage.create_workflow("wf-pending", "Flow4", status=WorkflowState.PENDING)

    active = await storage.get_active_workflows()
    assert set(active) == {"wf-active", "wf-pending"}

    await storage.close()


@pytest.mark.asyncio
async def test_lease_acquisition_and_fencing_token(db_path: Path) -> None:
    storage = SQLiteStorage(database_path=db_path)
    await storage.initialize()

    # First worker acquires lease
    lease1 = await storage.acquire_lease(
        task_id="task-charge",
        workflow_id="wf-1",
        worker_id="worker-a",
        ttl_seconds=5.0,
    )
    assert lease1.fencing_token == 1
    assert lease1.worker_id == "worker-a"

    # Second worker tries to acquire active lease -> raises ConcurrencyConflictError
    with pytest.raises(ConcurrencyConflictError) as exc_info:
        await storage.acquire_lease(
            task_id="task-charge",
            workflow_id="wf-1",
            worker_id="worker-b",
            ttl_seconds=5.0,
        )
    assert "actively leased by worker 'worker-a'" in str(exc_info.value)

    # Renew lease with correct fencing token
    renewed = await storage.renew_lease(
        task_id="task-charge",
        worker_id="worker-a",
        fencing_token=1,
        ttl_seconds=10.0,
    )
    assert renewed.fencing_token == 1
    assert renewed.expires_at > lease1.expires_at

    # Renew with obsolete fencing token -> raises ConcurrencyConflictError
    with pytest.raises(ConcurrencyConflictError):
        await storage.renew_lease(
            task_id="task-charge",
            worker_id="worker-a",
            fencing_token=0,
            ttl_seconds=5.0,
        )

    # Release lease
    await storage.release_lease("task-charge", "worker-a", 1)

    # Now worker-b can acquire lease -> gets fencing_token = 2
    lease2 = await storage.acquire_lease(
        task_id="task-charge",
        workflow_id="wf-1",
        worker_id="worker-b",
        ttl_seconds=5.0,
    )
    assert lease2.fencing_token == 2
    assert lease2.worker_id == "worker-b"

    await storage.close()


@pytest.mark.asyncio
async def test_reap_expired_leases(db_path: Path) -> None:
    storage = SQLiteStorage(database_path=db_path)
    await storage.initialize()

    # Acquire lease with 0.05 second TTL
    await storage.acquire_lease(
        task_id="task-fast-expire",
        workflow_id="wf-1",
        worker_id="worker-a",
        ttl_seconds=0.05,
    )

    await asyncio.sleep(0.06)

    reaped = await storage.reap_expired_leases()
    assert len(reaped) == 1
    assert reaped[0].task_id == "task-fast-expire"

    # Renewing a reaped lease raises TaskLeaseExpiredError
    with pytest.raises(TaskLeaseExpiredError):
        await storage.renew_lease(
            task_id="task-fast-expire",
            worker_id="worker-a",
            fencing_token=1,
            ttl_seconds=5.0,
        )

    await storage.close()


@pytest.mark.asyncio
async def test_concurrent_writers_in_wal_mode(db_path: Path) -> None:
    storage = SQLiteStorage(database_path=db_path)
    await storage.initialize()

    wf_id = "wf-concurrent"
    await storage.create_workflow(wf_id, "ConcurrentPipeline")

    async def append_worker(task_index: int) -> None:
        event = TaskCompleted(
            workflow_id=wf_id,
            task_id=f"task-{task_index}",
            result={"index": task_index},
        )
        await storage.append_event(event)

    # Run 20 concurrent appends
    await asyncio.gather(*(append_worker(i) for i in range(20)))

    events = await storage.get_events(wf_id)
    assert len(events) == 20

    await storage.close()
