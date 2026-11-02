"""Chaos and crash recovery tests simulating worker death, lease expiration, and replay idempotency."""

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from aegisflow.core.events import (
    TaskCompleted,
    TaskLeased,
    TaskScheduled,
    TaskStarted,
    WorkflowInitiated,
    WorkflowRunning,
)
from aegisflow.core.exceptions import ConcurrencyConflictError
from aegisflow.core.states import WorkflowState
from aegisflow.dsl.task import task
from aegisflow.dsl.workflow import workflow
from aegisflow.engine.orchestrator import WorkflowOrchestrator
from aegisflow.engine.replay import get_runnable_tasks_after_recovery, replay_workflow
from aegisflow.storage.sqlite import SQLiteStorage
from aegisflow.workers.daemon import WorkerSupervisorDaemon


@pytest.mark.asyncio
async def test_worker_crash_lease_expiration_and_takeover(tmp_path: Path) -> None:
    """Chaos Scenario: Worker crashes mid-task, lease expires, reaper purges lease, new worker takes over."""
    db_file = tmp_path / "chaos_lease.db"
    storage = SQLiteStorage(database_path=db_file)
    await storage.initialize()

    w_id = "wf_chaos_lease_1"
    task_id = "heavy_computation"

    # Step 1: Simulate Worker 1 leasing the task with short TTL
    lease_1 = await storage.acquire_lease(
        task_id=task_id,
        workflow_id=w_id,
        worker_id="worker_dead_1",
        ttl_seconds=0.04,
    )
    assert lease_1 is not None
    assert lease_1.fencing_token == 1
    assert lease_1.worker_id == "worker_dead_1"

    # Step 2: Worker 1 'crashes' (no heartbeats sent). Wait for lease to expire.
    await asyncio.sleep(0.12)

    # Step 3: Worker 2 takes over the task and receives a higher monotonic fencing token
    lease_2 = await storage.acquire_lease(
        task_id=task_id,
        workflow_id=w_id,
        worker_id="worker_alive_2",
        ttl_seconds=0.04,
    )
    assert lease_2 is not None
    assert lease_2.worker_id == "worker_alive_2"
    assert lease_2.fencing_token == 2  # Monotonic fence increment prevents split-brain

    # Step 4: Worker 2 crashes; after expiration, reaper purges the lease
    await asyncio.sleep(0.12)
    reaped = await storage.reap_expired_leases()
    assert any(lease.task_id == task_id for lease in reaped)

    await storage.close()


@pytest.mark.asyncio
async def test_replay_idempotency_after_simulated_crash(tmp_path: Path) -> None:
    """Chaos Scenario: Multi-task workflow crashes mid-stream; recovery skips completed steps."""
    db_file = tmp_path / "chaos_recovery.db"
    storage = SQLiteStorage(database_path=db_file)
    await storage.initialize()

    executions: dict[str, int] = {"task_a": 0, "task_b": 0, "task_c": 0}

    @task
    def task_a() -> str:
        executions["task_a"] += 1
        return "result_a"

    @task
    def task_b(task_a: str) -> str:
        executions["task_b"] += 1
        return f"{task_a}_b"

    @task
    def task_c(task_b: str) -> str:
        executions["task_c"] += 1
        return f"{task_b}_c"

    task_b.after(task_a)
    task_c.after(task_b)

    wf = workflow("IdempotentChaosPipeline", tasks=[task_a, task_b, task_c])
    wf_def = wf.to_definition()
    wf_id = "wf_chaos_idempotent_1"

    # Simulate that task_a already succeeded and was committed to WAL before crash
    await storage.append_event(WorkflowInitiated(workflow_id=wf_id, workflow_name=wf_def.name))
    await storage.append_event(WorkflowRunning(workflow_id=wf_id))
    await storage.append_event(
        TaskScheduled(workflow_id=wf_id, task_id="task_a", task_name="task_a")
    )
    await storage.append_event(
        TaskLeased(
            workflow_id=wf_id,
            task_id="task_a",
            worker_id="w_crashed",
            fencing_token=1,
            lease_expires_at=datetime.now(UTC) + timedelta(minutes=10),
        )
    )
    await storage.append_event(
        TaskStarted(workflow_id=wf_id, task_id="task_a", worker_id="w_crashed")
    )
    await storage.append_event(
        TaskCompleted(workflow_id=wf_id, task_id="task_a", result="result_a")
    )

    # Replay historical state to verify crash recovery analysis
    events = await storage.get_events(wf_id)
    recovered_state = replay_workflow(wf_def, events)
    runnable = get_runnable_tasks_after_recovery(recovered_state, wf_def)

    # Invariant: task_a must NOT be runnable, task_b should be runnable
    assert "task_a" not in runnable
    assert "task_b" in runnable
    assert recovered_state.task_results["task_a"] == "result_a"

    # Now run orchestrator resume from recovered state
    daemon = WorkerSupervisorDaemon(event_store=storage, lease_manager=storage)
    orchestrator = WorkflowOrchestrator(
        event_store=storage,
        lease_manager=storage,
        worker_daemon=daemon,
    )

    final_state = await orchestrator.resume(workflow_id=wf_id, workflow_def=wf_def)
    assert final_state.status == WorkflowState.COMPLETED

    assert final_state.workflow_output == "result_a_b_c"

    # Verify task_a was never re-executed in Python code
    assert executions["task_a"] == 0
    assert executions["task_b"] == 1
    assert executions["task_c"] == 1

    await daemon.shutdown()
    await storage.close()


@pytest.mark.asyncio
async def test_concurrent_recovery_fencing_protection(tmp_path: Path) -> None:
    """Chaos Scenario: Dual recovery workers race to acquire lease; fencing tokens prevent dual execution."""
    db_file = tmp_path / "chaos_fence.db"
    storage = SQLiteStorage(database_path=db_file)
    await storage.initialize()

    w_id = "wf_fence_race"
    t_id = "race_task"

    # Worker A acquires lease
    lease_a = await storage.acquire_lease(
        task_id=t_id, workflow_id=w_id, worker_id="worker_A", ttl_seconds=0.03
    )
    assert lease_a is not None
    assert lease_a.fencing_token == 1

    # Worker B cannot acquire while lease A is active (raises ConcurrencyConflictError)
    with pytest.raises(ConcurrencyConflictError):
        await storage.acquire_lease(
            task_id=t_id, workflow_id=w_id, worker_id="worker_B", ttl_seconds=1.0
        )

    # Wait for lease A to expire
    await asyncio.sleep(0.08)

    # Worker B now acquires with higher fencing token
    lease_b = await storage.acquire_lease(
        task_id=t_id, workflow_id=w_id, worker_id="worker_B", ttl_seconds=1.0
    )
    assert lease_b is not None
    assert lease_b.fencing_token == 2

    # Worker A (zombie) tries to renew with stale token 1 -> Raises ConcurrencyConflictError
    with pytest.raises(ConcurrencyConflictError):
        await storage.renew_lease(
            task_id=t_id, worker_id="worker_A", fencing_token=1, ttl_seconds=1.0
        )

    await storage.close()
