"""Integration tests for crash recovery, WAL event replay, and idempotent resumption."""

from pathlib import Path

import pytest

from aegisflow.core.events import (
    TaskCompleted,
    TaskScheduled,
    TaskStarted,
    WorkflowInitiated,
    WorkflowRunning,
)
from aegisflow.core.exceptions import WorkflowNotFoundError
from aegisflow.core.states import WorkflowState
from aegisflow.dsl.task import task
from aegisflow.dsl.workflow import workflow
from aegisflow.engine.orchestrator import WorkflowOrchestrator
from aegisflow.storage.sqlite import SQLiteStorage


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "recovery_test.db"


@pytest.mark.asyncio
async def test_workflow_crash_recovery_resumes_without_reexecuting(db_path: Path) -> None:
    storage = SQLiteStorage(database_path=db_path)
    await storage.initialize()

    call_counts = {"step_1": 0, "step_2": 0, "step_3": 0}

    @task
    def step_1(x: int) -> int:
        call_counts["step_1"] += 1
        return x * 10

    @task
    def step_2(step_1: int) -> int:
        call_counts["step_2"] += 1
        return step_1 + 5

    @task
    def step_3(step_2: int) -> int:
        call_counts["step_3"] += 1
        return step_2 * 2

    step_2.after(step_1)
    step_3.after(step_2)

    wf = workflow("resumable_flow", tasks=[step_1, step_2, step_3])
    wf_def = wf.to_definition()

    workflow_id = "wf_crash_test_1"

    # Simulate mid-workflow crash state directly in the WAL:
    # step_1 has already completed and produced 100.
    # step_2 was scheduled before crash occurred.
    await storage.append_event(
        WorkflowInitiated(
            workflow_id=workflow_id,
            workflow_name=wf_def.name,
            input_data={"x": 10},
        )
    )
    await storage.append_event(WorkflowRunning(workflow_id=workflow_id))
    await storage.append_event(
        TaskScheduled(
            workflow_id=workflow_id,
            task_id="step_1",
            task_name="step_1",
            input_args=(10,),
        )
    )
    await storage.append_event(
        TaskStarted(
            workflow_id=workflow_id,
            task_id="step_1",
            worker_id="old_crashed_worker",
        )
    )
    await storage.append_event(
        TaskCompleted(
            workflow_id=workflow_id,
            task_id="step_1",
            result=100,
        )
    )

    # Now create a brand new orchestrator (simulating worker reboot / new leader)
    orchestrator = WorkflowOrchestrator(
        event_store=storage,
        lease_manager=storage,
    )

    # Resume the workflow
    state = await orchestrator.resume(workflow_id, wf_def)

    assert state.status == WorkflowState.COMPLETED
    assert state.is_successful
    # step_1 output (100) -> step_2 (100 + 5 = 105) -> step_3 (105 * 2 = 210)
    assert state.workflow_output == 210
    assert state.task_results["step_1"] == 100
    assert state.task_results["step_2"] == 105
    assert state.task_results["step_3"] == 210

    # CRITICAL: step_1 was already in the WAL, so it MUST NOT have been invoked again!
    assert call_counts["step_1"] == 0
    assert call_counts["step_2"] == 1
    assert call_counts["step_3"] == 1

    await storage.close()


@pytest.mark.asyncio
async def test_resume_already_completed_workflow(db_path: Path) -> None:
    storage = SQLiteStorage(database_path=db_path)
    await storage.initialize()

    @task
    def quick_task() -> str:
        return "done"

    wf = workflow("quick_flow", tasks=[quick_task])
    wf_def = wf.to_definition()

    orchestrator = WorkflowOrchestrator(
        event_store=storage,
        lease_manager=storage,
    )

    state_initial = await orchestrator.run(wf_def)
    assert state_initial.status == WorkflowState.COMPLETED

    # Resuming immediately returns completed state without re-running
    state_resumed = await orchestrator.resume(state_initial.workflow_id, wf_def)
    assert state_resumed.status == WorkflowState.COMPLETED
    assert state_resumed.workflow_output == "done"

    await storage.close()


@pytest.mark.asyncio
async def test_resume_nonexistent_workflow_raises_error(db_path: Path) -> None:
    storage = SQLiteStorage(database_path=db_path)
    await storage.initialize()

    @task
    def noop_task() -> None:
        pass

    wf = workflow("noop_flow", tasks=[noop_task])
    orchestrator = WorkflowOrchestrator(event_store=storage, lease_manager=storage)

    with pytest.raises(WorkflowNotFoundError):
        await orchestrator.resume("wf_does_not_exist", wf.to_definition())

    await storage.close()
