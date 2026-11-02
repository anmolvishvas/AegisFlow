"""Unit tests for immutable event models and serialization."""

import pytest
from pydantic import ValidationError

from aegisflow.core.events import (
    TaskCompleted,
    TaskFailed,
    WorkflowInitiated,
)


def test_workflow_initiated_serialization() -> None:
    event = WorkflowInitiated(
        workflow_id="wf-123",
        workflow_name="OrderFlow",
        input_data={"amount": 99.9, "currency": "USD"},
    )
    assert event.workflow_id == "wf-123"
    assert event.event_type == "WorkflowInitiated"
    assert event.input_data["amount"] == 99.9

    # Test JSON roundtrip
    json_data = event.model_dump_json()
    reconstructed = WorkflowInitiated.model_validate_json(json_data)
    assert reconstructed.event_id == event.event_id
    assert reconstructed.workflow_name == "OrderFlow"


def test_event_immutability() -> None:
    event = TaskCompleted(
        workflow_id="wf-123",
        task_id="task-1",
        result={"status": "ok"},
    )
    with pytest.raises(ValidationError):
        # Events are frozen dataclasses/models
        event.task_id = "task-2"  # type: ignore[misc]


def test_task_failed_event() -> None:
    event = TaskFailed(
        workflow_id="wf-123",
        task_id="task-charge",
        error="Card declined",
        traceback="Traceback...",
        is_retryable=True,
    )
    assert event.is_retryable is True
    assert event.error == "Card declined"
