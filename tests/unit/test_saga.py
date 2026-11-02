"""Unit tests for SagaCoordinator reverse topological rollbacks."""

from pathlib import Path
from typing import Any

import pytest

from aegisflow.core.models import TaskDefinition, WorkflowDefinition
from aegisflow.engine.saga import SagaCoordinator
from aegisflow.storage.sqlite import SQLiteStorage


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "saga_unit_test.db"


def test_saga_reverse_topological_order() -> None:
    def noop() -> None:
        pass

    def comp() -> None:
        pass

    task_a = TaskDefinition(task_id="A", name="A", fn=noop, is_async=False, compensate_fn=comp)
    task_b = TaskDefinition(
        task_id="B", name="B", fn=noop, is_async=False, compensate_fn=None, dependencies={"A"}
    )
    task_c = TaskDefinition(
        task_id="C", name="C", fn=noop, is_async=False, compensate_fn=comp, dependencies={"B"}
    )
    task_d = TaskDefinition(
        task_id="D", name="D", fn=noop, is_async=False, compensate_fn=comp, dependencies={"C"}
    )

    wf_def = WorkflowDefinition(
        name="saga_test_wf",
        tasks={"A": task_a, "B": task_b, "C": task_c, "D": task_d},
    )

    coordinator = SagaCoordinator()

    # If A, B, C completed before D failed:
    # A has compensation, B does not, C has compensation.
    # Reverse topological order: C must be compensated before A!
    completed = {"A", "B", "C"}
    order = coordinator.get_compensation_order(wf_def, completed)
    assert order == ["C", "A"]


@pytest.mark.asyncio
async def test_saga_execution_order_and_events(db_path: Path) -> None:
    storage = SQLiteStorage(database_path=db_path)
    await storage.initialize()

    executed_compensations: list[str] = []

    def comp_a(res: Any) -> str:
        executed_compensations.append("comp_a")
        return f"refunded_{res}"

    def comp_b(res: Any) -> str:
        executed_compensations.append("comp_b")
        return f"released_{res}"

    def dummy() -> None:
        pass

    task_a = TaskDefinition(
        task_id="t_a", name="t_a", fn=dummy, is_async=False, compensate_fn=comp_a
    )
    task_b = TaskDefinition(
        task_id="t_b",
        name="t_b",
        fn=dummy,
        is_async=False,
        compensate_fn=comp_b,
        dependencies={"t_a"},
    )

    wf_def = WorkflowDefinition(name="wf_saga", tasks={"t_a": task_a, "t_b": task_b})

    coordinator = SagaCoordinator()
    completed = {"t_a", "t_b"}
    results = {"t_a": 100, "t_b": "seat_5"}

    compensated = await coordinator.execute_compensations(
        workflow_def=wf_def,
        workflow_id="wf_saga_1",
        completed_tasks=completed,
        task_results=results,
        event_store=storage,
        reason="Task 3 failed",
    )

    # t_b depends on t_a, so t_b must be compensated first, then t_a!
    assert executed_compensations == ["comp_b", "comp_a"]
    assert compensated == ["t_b", "t_a"]

    events = await storage.get_events("wf_saga_1")
    event_types = [e.event_type for e in events]
    assert event_types == [
        "WorkflowCompensating",
        "TaskCompensated",
        "TaskCompensated",
        "WorkflowCompensated",
    ]

    await storage.close()
