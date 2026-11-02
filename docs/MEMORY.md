# Project Memory & Engineering Journal (MEMORY.md)
## AegisFlow: Resilient Event-Driven DAG Task Engine

---

### 1. Current Project Status
- **Current Phase:** Phase 4 Complete (Orchestrator, Topological Scheduler & Replay Engine)
- **Active Branch:** `feat/phase-4-orchestrator-replay`
- **Active Workspace Directory:** `C:\Users\DELL\Desktop\AegisFlow\`
- **GitHub Repository:** [github.com/anmolvishvas/AegisFlow](https://github.com/anmolvishvas/AegisFlow)

---

### 2. Completed Milestones

#### Phase 0: Project Architecture & CI/CD Bootstrap
- [x] **PRD.md:** Product vision, problem statement, goals, target users, MVP scope.
- [x] **ARCHITECTURE.md:** 5-layer subsystem architecture, tech stack rationale, directory layout.
- [x] **RULES.md:** Engineering rulebook (`mypy --strict`, async safety, pure state machine).
- [x] **DESIGN.md:** UI/UX specification, dark slate tokens, semantic statuses, Rich CLI.
- [x] **TASKS.md:** 7-phase implementation roadmap.
- [x] **CI/CD Workflows:** 
  - `.github/workflows/ci.yml`: Ruff linting & formatting, Mypy strict type checking, Python 3.11-3.13 test matrix, pip-audit and bandit security scans.
  - `.github/workflows/pr-audit.yml`: Conventional Commits verification and automatic PR path-labeler.
  - `.github/PULL_REQUEST_TEMPLATE.md`: Standardized Senior PR template.

#### Phase 1: Core Foundation & DAG Graph Engine
- [x] **Core Lifecycle States (`states.py`):** `WorkflowState` and `TaskState` with `StrEnum`.
- [x] **Domain Exceptions (`exceptions.py`):** Hierarchical domain exceptions rooted at `AegisFlowError`.
- [x] **Event-Sourced Write-Ahead Log Models (`events.py`):** Immutable Pydantic models with `datetime.UTC`.
- [x] **Domain Models (`models.py`):** `RetryPolicy` with exponential backoff & jitter, `TaskDefinition`, `WorkflowDefinition`.
- [x] **DAG Engine & Cycle Detection (`dsl/graph.py`):** Kahn's topological sort and 3-color DFS cycle detector.
- [x] **Developer DSL (`dsl/task.py`, `dsl/workflow.py`):** `@task` and `@workflow` decorators with `.after()` chaining.
- [x] **Unit Testing:** `test_events.py`, `test_graph.py`, `test_dsl.py`.

#### Phase 2: Storage Layer & SQLite WAL Event Store
- [x] **Storage Abstraction Interfaces (`storage/base.py`):** `EventStoreInterface`, `LeaseManagerInterface`, `LeaseInfo`.
- [x] **SQLite WAL Storage Engine (`storage/sqlite.py`):** Fully async SQLite engine (`aiosqlite`), WAL pragmas, relational tables (`workflows`, `events`, `leases`).
- [x] **Distributed Leases & Fencing Token Persistence:** Monotonic fencing token increments and concurrency conflict detection.
- [x] **Integration Testing (`test_sqlite_store.py`):** Roundtrip event sourcing, lifecycle transitions, lease conflict rejection, and concurrent async appends.

#### Phase 3: Worker Pool, Distributed Leases & Heartbeat System
- [x] **Heartbeat Monitor & Lease Reaper (`engine/lease.py`):**
  - `HeartbeatMonitor`: Asynchronous background lease renewal at `ttl / 3` intervals; triggers task cancellation callback if lease is lost.
  - `LeaseReaper`: Periodic cleaner that detects and purges expired leases across the fleet.
- [x] **Async & Process Execution Runners (`workers/base_runner.py`, `async_runner.py`, `process_runner.py`):**
  - `BaseRunner` & `TaskExecutionResult` outcome contracts.
  - `AsyncRunner`: Direct asyncio coroutine execution with timeout enforcement.
  - `ProcessRunner`: Offloads synchronous / CPU-heavy functions to thread/process executor pools without blocking the main event loop.
- [x] **Worker Supervisor Daemon (`workers/daemon.py`):**
  - Semaphore-guarded concurrency controls (`max_concurrency`).
  - Automated leasing, `TaskStarted`/`TaskCompleted`/`TaskFailed` event recording.
  - Cross-platform graceful shutdown trapping `SIGINT`/`SIGTERM` with in-flight grace periods.
- [x] **Testing Suite:**
  - `tests/unit/test_workers.py`: Unit tests for `AsyncRunner` and `ProcessRunner` (success, failure, timeout).
  - `tests/integration/test_worker_pool.py`: Heartbeat lease extensions, lease reaper cleanup, daemon execution, and graceful shutdown.

#### Phase 4: Orchestrator, Topological Scheduler & Replay Engine
- [x] **Pure Functional State Machine (`engine/state_machine.py`):**
  - `WorkflowExecutionState` immutable snapshot container.
  - Pure function `transition(state, event, workflow_def) -> (new_state, ready_tasks)` with DAG topological resolution.
- [x] **Durable Event Replay & Crash Recovery Engine (`engine/replay.py`):**
  - `replay_workflow`: Deterministic event-log fold reconstructing exact execution state from historical WAL events.
  - `get_resumable_tasks` & `get_runnable_tasks_after_recovery`: Identifies pending work while strictly skipping completed tasks to guarantee idempotency.
- [x] **Topological Dispatcher Loop & Orchestrator (`engine/orchestrator.py`):**
  - `WorkflowOrchestrator`: Coordinates end-to-end execution, task argument injection (positional or parameter-matched upstream outputs), lifecycle events, and crash resumption.
  - `resolve_task_inputs`: Resolves root inputs and feeds parent task outputs into downstream child tasks.
  - `compute_workflow_output`: Extracts final return values from leaf tasks.
- [x] **Testing Suite:**
  - `tests/unit/test_state_machine.py`: Unit tests verifying pure transitions, diamond DAG dependency resolution (A -> B & C -> D), failure state tracking, and event replay.
  - `tests/integration/test_orchestrator.py`: End-to-end linear pipelines, concurrent diamond fan-out/fan-in pipelines, and failure propagation.
  - `tests/integration/test_recovery.py`: Simulated mid-flight worker crash recovery, proving already completed tasks are never re-executed upon resumption.

---

### 3. Progress Summary
```
[====================] 100% Documentation & Architecture (Phase 0)
[====================] 100% Phase 1: Core Foundation & DAG Engine
[====================] 100% Phase 2: Storage Layer & SQLite WAL Event Store
[====================] 100% Phase 3: Worker Pool & Distributed Leases
[====================] 100% Phase 4: Orchestrator, Topological Scheduler & Replay Engine
[                    ]   0% Phase 5: Resilience Features & Saga Rollbacks
```

---

### 4. Key Architectural Decisions (ADR Log)

- **ADR-001:** Event-Sourced Write-Ahead Log (WAL) over In-Place DB Mutation.
- **ADR-002:** SQLite in WAL Mode as Default Embedded Storage.
- **ADR-003:** Distributed Leases with Monotonic Fencing Tokens.
- **ADR-004:** Pure Functional State Machine.
- **ADR-005:** Three-Color DFS for Instant Cycle Detection at DAG Compilation.
- **ADR-006:** Event Registry Type Dispatching for Zero-Loss Event Hydration.
- **ADR-007:** Background Heartbeat Task Cancellation Callback to Prevent Zombie Execution.
- **ADR-008:** Dual Async/Process Runner Architecture for Event-Loop Non-Blocking Execution.
- **ADR-009:** Replay Idempotency via Completed Task Exclusion during Crash Recovery.
- **ADR-010:** Dual Positional and Named Dependency Injection for Flexible Task Composition.

---

### 5. Next Immediate Actions (Phase 5)
1. Implement retry scheduler supporting `max_retries`, `initial_interval`, `multiplier`, and `jitter` with `TaskRetried` events.
2. Implement saga compensation execution engine in `src/aegisflow/engine/saga.py` to dispatch rollback tasks in reverse topological order.
3. Implement Dead-Letter Queue (DLQ) state transitions and alerting hooks.
