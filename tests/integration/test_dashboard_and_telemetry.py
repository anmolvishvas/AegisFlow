"""Integration tests for FastAPI dashboard and OpenTelemetry distributed tracing."""

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from aegisflow.core.events import (
    TaskCompleted,
    TaskScheduled,
    WorkflowCompleted,
    WorkflowInitiated,
)
from aegisflow.core.states import WorkflowState
from aegisflow.dashboard.app import create_dashboard_app
from aegisflow.dsl.task import task
from aegisflow.dsl.workflow import workflow
from aegisflow.engine.orchestrator import WorkflowOrchestrator
from aegisflow.observability.telemetry import AegisTelemetry, telemetry
from aegisflow.storage.sqlite import SQLiteStorage
from aegisflow.workers.daemon import WorkerSupervisorDaemon


@pytest.mark.asyncio
async def test_dashboard_api_and_html_views(tmp_path: Path) -> None:
    """Verify all dashboard endpoints (REST and HTMX views)."""
    db_file = tmp_path / "dashboard_test.db"
    wf_id = "wf_dash_1"

    # Pre-populate SQLite with a workflow and DLQ workflow
    storage = SQLiteStorage(database_path=db_file)
    await storage.initialize()

    await storage.append_event(
        WorkflowInitiated(
            workflow_id=wf_id,
            workflow_name="InvoiceProcessing",
            input_data={"amount": 99.9},
        )
    )
    await storage.append_event(
        TaskScheduled(workflow_id=wf_id, task_id="validate", task_name="validate")
    )
    await storage.append_event(
        TaskCompleted(workflow_id=wf_id, task_id="validate", result={"valid": True})
    )
    await storage.append_event(
        WorkflowCompleted(workflow_id=wf_id, output_data={"status": "approved"})
    )

    # Insert a DLQ workflow
    conn = await storage._get_conn()
    await conn.execute(
        """
        INSERT INTO workflows (workflow_id, name, status, input_data, created_at, updated_at)
        VALUES (?, ?, ?, ?, datetime('now'), datetime('now'));
        """,
        (
            "wf_dlq_failed",
            "DeadWorkflow",
            WorkflowState.DEAD_LETTER.value,
            json.dumps({"reason": "exhausted"}),
        ),
    )
    await conn.commit()
    await storage.close()

    # Create FastAPI TestClient
    app = create_dashboard_app(database_path=db_file)
    with TestClient(app) as client:
        # 1. GET /api/workflows
        res = client.get("/api/workflows")
        assert res.status_code == 200
        workflows = res.json()
        assert len(workflows) == 2
        wf_names = {w["name"] for w in workflows}
        assert "InvoiceProcessing" in wf_names
        assert "DeadWorkflow" in wf_names

        # 2. GET /api/workflows/{workflow_id}
        res = client.get(f"/api/workflows/{wf_id}")
        assert res.status_code == 200
        detail = res.json()
        assert detail["workflow"]["workflow_id"] == wf_id
        assert len(detail["events"]) == 4

        # 3. GET /api/workflows/nonexistent -> 404
        res = client.get("/api/workflows/nonexistent-id")
        assert res.status_code == 404

        # 4. GET /api/dlq
        res = client.get("/api/dlq")
        assert res.status_code == 200
        dlq_list = res.json()
        assert len(dlq_list) == 1
        assert dlq_list[0]["workflow_id"] == "wf_dlq_failed"

        # 5. GET / (Index HTML)
        res = client.get("/")
        assert res.status_code == 200
        assert "AegisFlow Dashboard" in res.text
        assert "InvoiceProcessing" in res.text

        # 6. GET /workflows/{workflow_id} (Detail HTML)
        res = client.get(f"/workflows/{wf_id}")
        assert res.status_code == 200
        assert "InvoiceProcessing" in res.text
        assert "validate" in res.text


def test_telemetry_unit() -> None:
    """Verify AegisTelemetry context managers and span propagation."""
    custom_telemetry = AegisTelemetry(service_name="test_service")
    assert custom_telemetry.service_name == "test_service"

    # Test workflow span context manager
    with custom_telemetry.trace_workflow(
        workflow_id="wf_span_1",
        workflow_name="TelemetryTestWorkflow",
        attributes={"tenant": "acme"},
    ) as span:
        if custom_telemetry.is_enabled:
            assert span is not None

    # Test task span context manager
    with custom_telemetry.trace_task(
        task_id="t1",
        workflow_id="wf_span_1",
        attributes={"attempt": 1},
    ) as span:
        if custom_telemetry.is_enabled:
            assert span is not None

    # Test exception recording in span
    with pytest.raises(ValueError, match="Span failure test"):
        with custom_telemetry.trace_task(task_id="t_err", workflow_id="wf_span_1"):
            raise ValueError("Span failure test")


@pytest.mark.asyncio
async def test_telemetry_integrated_in_orchestrator(tmp_path: Path) -> None:
    """Verify that WorkflowOrchestrator runs workflows with active telemetry spans."""
    db_file = tmp_path / "telemetry_orchestration.db"
    storage = SQLiteStorage(database_path=db_file)
    await storage.initialize()

    daemon = WorkerSupervisorDaemon(event_store=storage, lease_manager=storage)
    orchestrator = WorkflowOrchestrator(
        event_store=storage,
        lease_manager=storage,
        worker_daemon=daemon,
    )

    @task
    def step_a(x: int) -> int:
        return x * 3

    @task
    def step_b(step_a: int) -> int:
        return step_a + 10

    step_b.after(step_a)
    wf = workflow("TracedWorkflow", tasks=[step_a, step_b])
    wf_def = wf.to_definition()
    wf_id = "wf_traced_run_1"

    # Ensure global telemetry instance is active during execution
    assert telemetry.is_enabled

    state = await orchestrator.run(wf_def, workflow_id=wf_id, x=5)
    assert state.status == WorkflowState.COMPLETED
    assert state.task_results["step_a"] == 15
    assert state.task_results["step_b"] == 25

    await daemon.shutdown()
    await storage.close()
