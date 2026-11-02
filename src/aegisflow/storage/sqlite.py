"""SQLite implementation of the event store and distributed lease manager in WAL mode."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import aiosqlite

from aegisflow.core.events import (
    BaseEvent,
    TaskCompensated,
    TaskCompleted,
    TaskFailed,
    TaskHeartbeat,
    TaskLeased,
    TaskRetried,
    TaskScheduled,
    TaskStarted,
    WorkflowCompensated,
    WorkflowCompensating,
    WorkflowCompleted,
    WorkflowFailed,
    WorkflowInitiated,
    WorkflowRunning,
)
from aegisflow.core.exceptions import (
    ConcurrencyConflictError,
    EventStoreUnavailableError,
    TaskLeaseExpiredError,
    WorkflowAlreadyExistsError,
)
from aegisflow.core.states import WorkflowState
from aegisflow.storage.base import EventStoreInterface, LeaseInfo, LeaseManagerInterface

_EVENT_REGISTRY: dict[str, type[BaseEvent]] = {
    cls.__name__: cls
    for cls in (
        WorkflowInitiated,
        WorkflowRunning,
        WorkflowCompleted,
        WorkflowFailed,
        WorkflowCompensating,
        WorkflowCompensated,
        TaskScheduled,
        TaskLeased,
        TaskStarted,
        TaskHeartbeat,
        TaskCompleted,
        TaskFailed,
        TaskRetried,
        TaskCompensated,
    )
}


class SQLiteStorage(EventStoreInterface, LeaseManagerInterface):
    """Durable, embedded event store and lease manager using SQLite in WAL mode."""

    def __init__(self, database_path: str | Path = ":memory:") -> None:
        self.database_path = str(database_path)
        self._db: aiosqlite.Connection | None = None

    async def _get_conn(self) -> aiosqlite.Connection:
        if self._db is None:
            try:
                self._db = await aiosqlite.connect(self.database_path)
                self._db.row_factory = aiosqlite.Row
            except Exception as e:
                raise EventStoreUnavailableError(f"Cannot connect to SQLite database: {e}") from e
        return self._db

    async def initialize(self) -> None:
        """Initialize the SQLite database with WAL mode and schema."""
        conn = await self._get_conn()

        # Configure high-performance concurrency pragmas
        await conn.execute("PRAGMA journal_mode = WAL;")
        await conn.execute("PRAGMA synchronous = NORMAL;")
        await conn.execute("PRAGMA foreign_keys = ON;")
        await conn.execute("PRAGMA busy_timeout = 5000;")

        # Workflows table
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS workflows (
                workflow_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                input_data TEXT,
                output_data TEXT
            );
            """
        )

        # Append-only events table
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY,
                workflow_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                sequence_number INTEGER NOT NULL,
                FOREIGN KEY (workflow_id) REFERENCES workflows(workflow_id) ON DELETE CASCADE
            );
            """
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_workflow_seq ON events(workflow_id, sequence_number);"
        )

        # Distributed task leases table
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS leases (
                task_id TEXT PRIMARY KEY,
                workflow_id TEXT NOT NULL,
                worker_id TEXT NOT NULL,
                fencing_token INTEGER NOT NULL,
                expires_at TEXT NOT NULL,
                acquired_at TEXT NOT NULL
            );
            """
        )
        await conn.commit()

    async def create_workflow(
        self,
        workflow_id: str,
        name: str,
        status: WorkflowState = WorkflowState.PENDING,
        input_data: dict[str, Any] | None = None,
    ) -> None:
        conn = await self._get_conn()
        now_iso = datetime.now(UTC).isoformat()
        input_json = json.dumps(input_data or {})

        try:
            await conn.execute(
                """
                INSERT INTO workflows (workflow_id, name, status, created_at, updated_at, input_data)
                VALUES (?, ?, ?, ?, ?, ?);
                """,
                (workflow_id, name, status.value, now_iso, now_iso, input_json),
            )
            await conn.commit()
        except aiosqlite.IntegrityError as e:
            raise WorkflowAlreadyExistsError(
                f"Workflow '{workflow_id}' already exists in database"
            ) from e

    async def update_workflow_status(
        self,
        workflow_id: str,
        status: WorkflowState,
        output_data: Any = None,
    ) -> None:
        conn = await self._get_conn()
        now_iso = datetime.now(UTC).isoformat()
        output_json = json.dumps(output_data) if output_data is not None else None

        await conn.execute(
            """
            UPDATE workflows
            SET status = ?, updated_at = ?, output_data = ?
            WHERE workflow_id = ?;
            """,
            (status.value, now_iso, output_json, workflow_id),
        )
        await conn.commit()

    async def get_workflow_record(self, workflow_id: str) -> dict[str, Any] | None:
        conn = await self._get_conn()
        async with conn.execute(
            "SELECT * FROM workflows WHERE workflow_id = ?;", (workflow_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None
            return {
                "workflow_id": row["workflow_id"],
                "name": row["name"],
                "status": WorkflowState(row["status"]),
                "created_at": datetime.fromisoformat(row["created_at"]),
                "updated_at": datetime.fromisoformat(row["updated_at"]),
                "input_data": json.loads(row["input_data"]) if row["input_data"] else {},
                "output_data": json.loads(row["output_data"]) if row["output_data"] else None,
            }

    async def append_event(self, event: BaseEvent) -> None:
        conn = await self._get_conn()

        # Atomic sequence assignment within transaction
        async with conn.execute(
            "SELECT COALESCE(MAX(sequence_number), 0) + 1 AS next_seq FROM events WHERE workflow_id = ?;",
            (event.workflow_id,),
        ) as cursor:
            row = await cursor.fetchone()
            next_seq = row["next_seq"] if row else 1

        payload_json = event.model_dump_json()

        await conn.execute(
            """
            INSERT INTO events (event_id, workflow_id, event_type, payload, timestamp, sequence_number)
            VALUES (?, ?, ?, ?, ?, ?);
            """,
            (
                event.event_id,
                event.workflow_id,
                event.event_type,
                payload_json,
                event.timestamp.isoformat(),
                next_seq,
            ),
        )
        await conn.commit()

    async def get_events(self, workflow_id: str) -> list[BaseEvent]:
        conn = await self._get_conn()
        events: list[BaseEvent] = []

        async with conn.execute(
            "SELECT event_type, payload FROM events WHERE workflow_id = ? ORDER BY sequence_number ASC;",
            (workflow_id,),
        ) as cursor:
            async for row in cursor:
                event_type = row["event_type"]
                payload = row["payload"]
                cls = _EVENT_REGISTRY.get(event_type, BaseEvent)
                events.append(cls.model_validate_json(payload))

        return events

    async def get_active_workflows(self) -> list[str]:
        conn = await self._get_conn()
        query = (
            "SELECT workflow_id FROM workflows WHERE status NOT IN (?, ?, ?, ?);"
        )
        params = (
            WorkflowState.COMPLETED.value,
            WorkflowState.FAILED.value,
            WorkflowState.COMPENSATED.value,
            WorkflowState.CANCELLED.value,
        )

        async with conn.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            return [row["workflow_id"] for row in rows]

    # LeaseManagerInterface Implementation
    async def acquire_lease(
        self,
        task_id: str,
        workflow_id: str,
        worker_id: str,
        ttl_seconds: float,
    ) -> LeaseInfo:
        conn = await self._get_conn()
        now = datetime.now(UTC)
        expires_at = now + timedelta(seconds=ttl_seconds)

        async with conn.execute("SELECT * FROM leases WHERE task_id = ?;", (task_id,)) as cursor:
            existing = await cursor.fetchone()

        if existing is None:
            fencing_token = 1
            await conn.execute(
                """
                INSERT INTO leases (task_id, workflow_id, worker_id, fencing_token, expires_at, acquired_at)
                VALUES (?, ?, ?, ?, ?, ?);
                """,
                (
                    task_id,
                    workflow_id,
                    worker_id,
                    fencing_token,
                    expires_at.isoformat(),
                    now.isoformat(),
                ),
            )
            await conn.commit()
            return LeaseInfo(
                task_id=task_id,
                workflow_id=workflow_id,
                worker_id=worker_id,
                fencing_token=fencing_token,
                expires_at=expires_at,
                acquired_at=now,
            )

        existing_expires = datetime.fromisoformat(existing["expires_at"])
        if existing_expires > now and existing["worker_id"] != worker_id:
            raise ConcurrencyConflictError(
                f"Task '{task_id}' is actively leased by worker '{existing['worker_id']}' until {existing_expires.isoformat()}"
            )

        # Lease is expired or owned by the same worker; advance fencing token monotonically
        new_token = existing["fencing_token"] + 1
        await conn.execute(
            """
            UPDATE leases
            SET worker_id = ?, fencing_token = ?, expires_at = ?, acquired_at = ?
            WHERE task_id = ?;
            """,
            (worker_id, new_token, expires_at.isoformat(), now.isoformat(), task_id),
        )
        await conn.commit()

        return LeaseInfo(
            task_id=task_id,
            workflow_id=workflow_id,
            worker_id=worker_id,
            fencing_token=new_token,
            expires_at=expires_at,
            acquired_at=now,
        )

    async def renew_lease(
        self,
        task_id: str,
        worker_id: str,
        fencing_token: int,
        ttl_seconds: float,
    ) -> LeaseInfo:
        conn = await self._get_conn()
        now = datetime.now(UTC)
        expires_at = now + timedelta(seconds=ttl_seconds)

        async with conn.execute("SELECT * FROM leases WHERE task_id = ?;", (task_id,)) as cursor:
            existing = await cursor.fetchone()

        if existing is None:
            raise TaskLeaseExpiredError(f"Lease for task '{task_id}' does not exist or was reaped")

        if existing["fencing_token"] != fencing_token:
            raise ConcurrencyConflictError(
                f"Fencing token {fencing_token} is obsolete. Current token is {existing['fencing_token']}"
            )

        if existing["worker_id"] != worker_id:
            raise ConcurrencyConflictError(
                f"Lease for task '{task_id}' is held by another worker '{existing['worker_id']}'"
            )

        await conn.execute(
            "UPDATE leases SET expires_at = ? WHERE task_id = ?;",
            (expires_at.isoformat(), task_id),
        )
        await conn.commit()

        return LeaseInfo(
            task_id=task_id,
            workflow_id=existing["workflow_id"],
            worker_id=worker_id,
            fencing_token=fencing_token,
            expires_at=expires_at,
            acquired_at=datetime.fromisoformat(existing["acquired_at"]),
        )

    async def release_lease(
        self,
        task_id: str,
        worker_id: str,
        fencing_token: int,
    ) -> None:
        conn = await self._get_conn()
        now_past = datetime.fromtimestamp(0, UTC).isoformat()
        await conn.execute(
            """
            UPDATE leases
            SET worker_id = '', expires_at = ?
            WHERE task_id = ? AND worker_id = ? AND fencing_token = ?;
            """,
            (now_past, task_id, worker_id, fencing_token),
        )
        await conn.commit()

    async def reap_expired_leases(self) -> list[LeaseInfo]:
        conn = await self._get_conn()
        now = datetime.now(UTC)
        now_iso = now.isoformat()

        async with conn.execute("SELECT * FROM leases WHERE expires_at < ?;", (now_iso,)) as cursor:
            rows = await cursor.fetchall()

        expired = [
            LeaseInfo(
                task_id=row["task_id"],
                workflow_id=row["workflow_id"],
                worker_id=row["worker_id"],
                fencing_token=row["fencing_token"],
                expires_at=datetime.fromisoformat(row["expires_at"]),
                acquired_at=datetime.fromisoformat(row["acquired_at"]),
            )
            for row in rows
        ]

        if expired:
            await conn.execute("DELETE FROM leases WHERE expires_at < ?;", (now_iso,))
            await conn.commit()

        return expired

    async def close(self) -> None:
        if self._db is not None:
            await self._db.close()
            self._db = None
