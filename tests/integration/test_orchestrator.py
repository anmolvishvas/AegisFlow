"""Integration tests for WorkflowOrchestrator, topological scheduling, and error propagation."""

from pathlib import Path

import pytest

from aegisflow.core.states import TaskState, WorkflowState
from aegisflow.dsl.task import task
from aegisflow.dsl.workflow import workflow
from aegisflow.engine.orchestrator import WorkflowOrchestrator
from aegisflow.storage.sqlite import SQLiteStorage


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "orchestrator_test.db"


@pytest.mark.asyncio
async def test_linear_workflow_execution(db_path: Path) -> None:
    storage = SQLiteStorage(database_path=db_path)
    await storage.initialize()

    @task
    def add_ten(x: int) -> int:
        return x + 10

    @task
    def multiply_two(add_ten: int) -> int:
        return add_ten * 2

    @task
    def subtract_three(multiply_two: int) -> int:
        return multiply_two - 3

    multiply_two.after(add_ten)
    subtract_three.after(multiply_two)

    wf = workflow("linear_pipeline", tasks=[add_ten, multiply_two, subtract_three])
    wf_def = wf.to_definition()

    orchestrator = WorkflowOrchestrator(
        event_store=storage,
        lease_manager=storage,
    )

    state = await orchestrator.run(wf_def, x=5)

    assert state.status == WorkflowState.COMPLETED
    assert state.is_successful
    assert state.workflow_output == 27
    assert state.task_results["add_ten"] == 15
    assert state.task_results["multiply_two"] == 30
    assert state.task_results["subtract_three"] == 27

    # Verify WAL sequence
    events = await storage.get_events(state.workflow_id)
    event_types = [e.event_type for e in events]
    assert event_types[0] == "WorkflowInitiated"
    assert event_types[1] == "WorkflowRunning"
    assert event_types[-1] == "WorkflowCompleted"

    await storage.close()


@pytest.mark.asyncio
async def test_fan_out_fan_in_diamond_workflow(db_path: Path) -> None:
    storage = SQLiteStorage(database_path=db_path)
    await storage.initialize()

    @task
    async def produce_items() -> list[int]:
        return [1, 2, 3]

    @task
    async def calc_squares(produce_items: list[int]) -> list[int]:
        return [x * x for x in produce_items]

    @task
    async def calc_cubes(produce_items: list[int]) -> list[int]:
        return [x * x * x for x in produce_items]

    @task
    async def merge_sums(calc_squares: list[int], calc_cubes: list[int]) -> int:
        return sum(calc_squares) + sum(calc_cubes)

    calc_squares.after(produce_items)
    calc_cubes.after(produce_items)
    merge_sums.after(calc_squares, calc_cubes)

    wf = workflow("diamond_pipeline", tasks=[produce_items, calc_squares, calc_cubes, merge_sums])
    wf_def = wf.to_definition()

    orchestrator = WorkflowOrchestrator(
        event_store=storage,
        lease_manager=storage,
    )

    state = await orchestrator.run(wf_def)

    assert state.status == WorkflowState.COMPLETED
    assert state.is_successful
    # squares: 1+4+9=14, cubes: 1+8+27=36 -> 14+36 = 50
    assert state.workflow_output == 50
    assert state.completed_tasks == {"produce_items", "calc_squares", "calc_cubes", "merge_sums"}

    await storage.close()


@pytest.mark.asyncio
async def test_workflow_failure_propagation(db_path: Path) -> None:
    storage = SQLiteStorage(database_path=db_path)
    await storage.initialize()

    @task
    def step_one() -> str:
        return "step_1_ok"

    @task
    def failing_step(step_one: str) -> None:
        raise ValueError("Critical step failure!")

    @task
    def step_three(failing_step: str) -> str:
        return "should_not_run"

    failing_step.after(step_one)
    step_three.after(failing_step)

    wf = workflow("failing_pipeline", tasks=[step_one, failing_step, step_three])
    wf_def = wf.to_definition()

    orchestrator = WorkflowOrchestrator(
        event_store=storage,
        lease_manager=storage,
    )

    state = await orchestrator.run(wf_def)

    assert state.status == WorkflowState.FAILED
    assert not state.is_successful
    assert state.task_states["step_one"] == TaskState.COMPLETED
    assert state.task_states["failing_step"] == TaskState.FAILED
    assert "step_three" not in state.task_states

    # Verify WorkflowFailed in WAL
    events = await storage.get_events(state.workflow_id)
    assert events[-1].event_type == "WorkflowFailed"

    await storage.close()
