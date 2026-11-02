"""Unit tests for the AegisFlow Typer/Rich CLI interface."""

import json
from pathlib import Path

from typer.testing import CliRunner

from aegisflow._version import __version__
from aegisflow.cli.main import app
from aegisflow.core.events import TaskCompleted, TaskScheduled, WorkflowCompleted, WorkflowInitiated
from aegisflow.core.states import WorkflowState
from aegisflow.storage.sqlite import SQLiteStorage

runner = CliRunner()


def test_cli_version() -> None:
    """Verify that aegisflow version prints current package version."""
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert "AegisFlow" in result.stdout
    assert __version__ in result.stdout


def test_cli_status_workflow_not_found(tmp_path: Path) -> None:
    """Verify status command exits with code 1 when workflow does not exist."""
    db_file = tmp_path / "test.db"
    result = runner.invoke(app, ["status", "missing-wf-123", "--db", str(db_file)])
    assert result.exit_code == 1
    assert "missing-wf-123" in result.stdout
    assert "not found" in result.stdout


def test_cli_status_workflow_success(tmp_path: Path) -> None:
    """Verify status command renders panel and WAL events table for an existing workflow."""
    import asyncio

    db_file = tmp_path / "test.db"
    wf_id = "wf_cli_status_test"

    async def _setup() -> None:
        storage = SQLiteStorage(database_path=db_file)
        await storage.initialize()
        await storage.append_event(
            WorkflowInitiated(
                workflow_id=wf_id, workflow_name="OrderPipeline", input_data={"val": 42}
            )
        )
        await storage.append_event(
            TaskScheduled(workflow_id=wf_id, task_id="step1", task_name="step1")
        )
        await storage.append_event(
            TaskCompleted(workflow_id=wf_id, task_id="step1", result="step1_done")
        )
        await storage.append_event(
            WorkflowCompleted(workflow_id=wf_id, output_data={"status": "ok"})
        )
        await storage.close()

    asyncio.run(_setup())

    result = runner.invoke(app, ["status", wf_id, "--db", str(db_file)])
    assert result.exit_code == 0
    assert "OrderPipeline" in result.stdout
    assert wf_id in result.stdout
    assert "COMPLETED" in result.stdout
    assert "Write-Ahead Log Events" in result.stdout
    assert "step1" in result.stdout


def test_cli_dlq_empty(tmp_path: Path) -> None:
    """Verify dlq list shows message when no dead-letter workflows exist."""
    import asyncio

    db_file = tmp_path / "test.db"

    async def _setup() -> None:
        storage = SQLiteStorage(database_path=db_file)
        await storage.initialize()
        await storage.close()

    asyncio.run(_setup())

    result = runner.invoke(app, ["dlq", "list", "--db", str(db_file)])
    assert result.exit_code == 0
    assert "No dead-lettered workflows found" in result.stdout


def test_cli_dlq_with_workflows(tmp_path: Path) -> None:
    """Verify dlq list displays table containing dead-lettered workflows."""
    import asyncio

    db_file = tmp_path / "test.db"
    wf_id = "wf_dead_123"

    async def _setup() -> None:
        storage = SQLiteStorage(database_path=db_file)
        await storage.initialize()
        conn = await storage._get_conn()
        await conn.execute(
            """
            INSERT INTO workflows (workflow_id, name, status, input_data, created_at, updated_at)
            VALUES (?, ?, ?, ?, datetime('now'), datetime('now'));
            """,
            (wf_id, "FailingWorkflow", WorkflowState.DEAD_LETTER.value, json.dumps({})),
        )
        await conn.commit()
        await storage.close()

    asyncio.run(_setup())

    result = runner.invoke(app, ["dlq", "list", "--db", str(db_file)])
    assert result.exit_code == 0
    assert "Dead-Letter Queue (DLQ)" in result.stdout
    assert wf_id in result.stdout
    assert "FailingWorkflow" in result.stdout


def test_cli_watch_completed_workflow(tmp_path: Path) -> None:
    """Verify watch command finishes when workflow is in a terminal state."""
    import asyncio

    db_file = tmp_path / "test.db"
    wf_id = "wf_cli_watch_test"

    async def _setup() -> None:
        storage = SQLiteStorage(database_path=db_file)
        await storage.initialize()
        await storage.append_event(
            WorkflowInitiated(workflow_id=wf_id, workflow_name="WatchPipeline")
        )
        await storage.append_event(WorkflowCompleted(workflow_id=wf_id, output_data={"done": True}))
        await storage.close()

    asyncio.run(_setup())

    result = runner.invoke(app, ["watch", wf_id, "--db", str(db_file), "--interval", "0.01"])
    assert result.exit_code == 0
    assert "WatchPipeline" in result.stdout
