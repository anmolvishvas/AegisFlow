"""Dead-Letter Queue (DLQ) manager and alerting infrastructure for unrecoverable failures."""

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from aegisflow.core.states import WorkflowState
from aegisflow.storage.base import EventStoreInterface

logger = logging.getLogger(__name__)

AlertHook = Callable[[str, str, str | None], Any]


class DeadLetterManager:
    """Manages workflows that exhausted all retry policies and unrecoverable failures."""

    def __init__(self, event_store: EventStoreInterface) -> None:
        self.event_store = event_store
        self._alert_hooks: list[AlertHook] = []

    def register_alert_hook(self, hook: AlertHook) -> None:
        """Register an alert hook callback triggered whenever a workflow enters the DLQ.

        Hook receives (workflow_id: str, reason: str, error: str | None).
        """
        self._alert_hooks.append(hook)

    async def notify_dead_letter(
        self,
        workflow_id: str,
        reason: str,
        error: str | None = None,
    ) -> None:
        """Trigger all registered alert hooks when a workflow is dead-lettered."""
        logger.error(
            "Workflow '%s' entered Dead-Letter Queue: %s (error: %s)",
            workflow_id,
            reason,
            error,
        )
        for hook in self._alert_hooks:
            try:
                if asyncio.iscoroutinefunction(hook):
                    await hook(workflow_id, reason, error)
                else:
                    hook(workflow_id, reason, error)
            except Exception as exc:
                logger.warning("Alert hook raised an exception: %s", exc)

    async def list_dead_letter_workflows(self) -> list[dict[str, Any]]:
        """Retrieve all workflows currently in the DEAD_LETTER state."""
        from aegisflow.storage.sqlite import SQLiteStorage

        if isinstance(self.event_store, SQLiteStorage):
            conn = await self.event_store._get_conn()
            async with conn.execute(
                """
                SELECT workflow_id, name, status, created_at, updated_at, input_data
                FROM workflows
                WHERE status = ?
                ORDER BY updated_at DESC;
                """,
                (WorkflowState.DEAD_LETTER.value,),
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
        return []
