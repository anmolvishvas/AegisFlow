"""Integration tests for retries with exponential backoff, Dead-Letter Queue, and Saga rollbacks."""

from pathlib import Path
from typing import Any

import pytest

from aegisflow.core.states import TaskState, WorkflowState
from aegisflow.dsl.task import task
from aegisflow.dsl.workflow import workflow
from aegisflow.engine.dlq import DeadLetterManager
from aegisflow.engine.orchestrator import WorkflowOrchestrator
from aegisflow.storage.sqlite import SQLiteStorage


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "resilience_test.db"


@pytest.mark.asyncio
async def test_retry_transient_failure_then_success(db_path: Path) -> None:
    storage = SQLiteStorage(database_path=db_path)
    await storage.initialize()

    attempt_count = 0

    @task(retries=3, initial_interval=0.01, backoff=1.5, jitter=False)
    def flaky_task(val: int) -> int:
        nonlocal attempt_count
        attempt_count += 1
        if attempt_count == 1:
            raise RuntimeError("Transient connection glitch")
        return val * 10

    wf = workflow("flaky_flow", tasks=[flaky_task])
    wf_def = wf.to_definition()

    orchestrator = WorkflowOrchestrator(
        event_store=storage,
        lease_manager=storage,
    )

    state = await orchestrator.run(wf_def, val=5)

    assert state.status == WorkflowState.COMPLETED
    assert state.is_successful
    assert state.workflow_output == 50
    assert attempt_count == 2

    # Verify TaskRetried and TaskCompleted in WAL
    events = await storage.get_events(state.workflow_id)
    event_types = [e.event_type for e in events]
    assert "TaskRetried" in event_types
    assert "TaskCompleted" in event_types
    assert event_types[-1] == "WorkflowCompleted"

    await storage.close()


@pytest.mark.asyncio
async def test_retry_exhaustion_moves_to_dlq_with_alert(db_path: Path) -> None:
    storage = SQLiteStorage(database_path=db_path)
    await storage.initialize()

    attempts = 0

    @task(retries=2, initial_interval=0.01, backoff=1.0, jitter=False)
    def always_failing_task() -> None:
        nonlocal attempts
        attempts += 1
        raise ValueError("Fatal database disk corruption")

    wf = workflow("fatal_flow", tasks=[always_failing_task])
    wf_def = wf.to_definition()

    dlq_alerts: list[tuple[str, str, str | None]] = []

    def on_alert(workflow_id: str, reason: str, error: str | None) -> None:
        dlq_alerts.append((workflow_id, reason, error))

    dlq_manager = DeadLetterManager(event_store=storage)
    dlq_manager.register_alert_hook(on_alert)

    orchestrator = WorkflowOrchestrator(
        event_store=storage,
        lease_manager=storage,
        dlq_manager=dlq_manager,
        enable_dlq_on_failure=True,
    )

    state = await orchestrator.run(wf_def)

    assert state.status == WorkflowState.DEAD_LETTER
    assert not state.is_successful
    assert state.task_states["always_failing_task"] == TaskState.FAILED
    assert attempts == 3  # Initial attempt (1) + 2 retries (attempts 2, 3)

    # Verify DLQ alert hook was notified
    assert len(dlq_alerts) == 1
    assert dlq_alerts[0][0] == state.workflow_id
    assert "exhausted 3 attempts" in dlq_alerts[0][1]

    # Verify workflow is listed in DLQ storage
    dlq_workflows = await dlq_manager.list_dead_letter_workflows()
    assert len(dlq_workflows) == 1
    assert dlq_workflows[0]["workflow_id"] == state.workflow_id
    assert dlq_workflows[0]["status"] == "DEAD_LETTER"

    await storage.close()


@pytest.mark.asyncio
async def test_end_to_end_saga_payment_rollback(db_path: Path) -> None:
    storage = SQLiteStorage(database_path=db_path)
    await storage.initialize()

    audit_trail: list[str] = []

    # 1. Step 1: Charge Card
    def refund_card(tx_id: Any) -> str:
        audit_trail.append(f"refunded:{tx_id}")
        return f"refund_{tx_id}_ok"

    @task(compensate=refund_card)
    def charge_card(amount: int) -> str:
        audit_trail.append(f"charged:{amount}")
        return f"tx_ch_{amount}"

    # 2. Step 2: Reserve Inventory
    def release_inventory(reservation_id: Any) -> str:
        audit_trail.append(f"released:{reservation_id}")
        return f"release_{reservation_id}_ok"

    @task(compensate=release_inventory)
    def reserve_inventory(charge_card: str) -> str:
        audit_trail.append(f"reserved_for:{charge_card}")
        return "res_item_99"

    # 3. Step 3: Issue Ticket (Unrecoverable failure)
    @task(retries=0)
    def issue_ticket(reserve_inventory: str) -> None:
        audit_trail.append("ticket_failure")
        raise RuntimeError("Ticket printing hardware fault")

    reserve_inventory.after(charge_card)
    issue_ticket.after(reserve_inventory)

    wf = workflow("payment_saga", tasks=[charge_card, reserve_inventory, issue_ticket])
    wf_def = wf.to_definition()

    orchestrator = WorkflowOrchestrator(
        event_store=storage,
        lease_manager=storage,
    )

    state = await orchestrator.run(wf_def, amount=100)

    # Workflow must end in COMPENSATED state!
    assert state.status == WorkflowState.COMPENSATED
    assert not state.is_successful
    assert state.is_terminal

    # Verify execution order:
    # 1. charge_card -> 2. reserve_inventory -> 3. issue_ticket (fails)
    # Then compensations in strict REVERSE topological order:
    # 4. release_inventory FIRST -> 5. refund_card SECOND!
    assert audit_trail == [
        "charged:100",
        "reserved_for:tx_ch_100",
        "ticket_failure",
        "released:res_item_99",
        "refunded:tx_ch_100",
    ]

    # Verify event stream in SQLite WAL
    events = await storage.get_events(state.workflow_id)
    event_types = [e.event_type for e in events]
    assert "WorkflowCompensating" in event_types
    assert "TaskCompensated" in event_types
    assert event_types[-1] == "WorkflowCompensated"

    await storage.close()
