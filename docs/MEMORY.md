# Project Memory & Engineering Journal (MEMORY.md)
## AegisFlow: Resilient Event-Driven DAG Task Engine

---

### 1. Current Project Status
- **Current Phase:** Phase 6 Complete (Observability, Rich CLI & Web Dashboard)
- **Active Branch:** `feat/phase-6-observability-cli-dashboard`
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

#### Phase 5: Resilience Features & Saga Rollbacks
- [x] **Retry Scheduler with Backoff & Full Jitter (`engine/retry.py`):**
  - `RetryScheduler`: Calculates delays according to `RetryPolicy` exponential formula with random full jitter.
  - Non-blocking asynchronous delay wait on event loop.
  - Emits `TaskRetried` events with next scheduled attempt timestamps to the WAL.
- [x] **Saga Compensation Coordinator (`engine/saga.py`):**
  - `SagaCoordinator`: Analyzes DAG topology and filters completed tasks with registered compensation handlers.
  - Dispatches compensations in strict reverse topological order (downstream actions compensated before upstream actions).
  - Emits `WorkflowCompensating`, `TaskCompensated`, and `WorkflowCompensated` events to the WAL.
  - Handles compensation failures cleanly by triggering `WorkflowDeadLettered` and raising `SagaCompensationError`.
- [x] **Dead-Letter Queue (DLQ) & Alerting Hooks (`engine/dlq.py`):**
  - `DeadLetterManager`: Collects permanently failed workflows in `DEAD_LETTER` state.
  - Asynchronous and synchronous alert callbacks for incident notifications.
  - Database queries to inspect dead-lettered workflows.
- [x] **Testing Suite:**
  - `tests/unit/test_retry.py`: Unit tests for exponential delay calculations, jitter bounds, and scheduled timestamp computations.
  - `tests/unit/test_saga.py`: Unit tests verifying reverse topological compensation ordering and event stream recording.
  - `tests/integration/test_resilience.py`: Integration tests for transient failure retry recovery, retry exhaustion leading to DLQ and alerts, and complete end-to-end payment saga rollback.

---

### 3. Progress Summary
```
[====================] 100% Documentation & Architecture (Phase 0)
[====================] 100% Phase 1: Core Foundation & DAG Engine
[====================] 100% Phase 2: Storage Layer & SQLite WAL Event Store
#### Phase 6: Observability, Rich CLI & Web Dashboard
- [x] **Distributed Tracing & OpenTelemetry Instrumentation (`observability/telemetry.py`):**
  - Vendor-neutral tracing wrapper `AegisTelemetry` with graceful fallback when OpenTelemetry is not installed.
  - Context managers `trace_workflow` and `trace_task` for propagating spans, status codes, and error exceptions.
  - Direct integration into `WorkflowOrchestrator` execution loops.
- [x] **Interactive Terminal CLI (`cli/main.py`):**
  - Powered by Typer and Rich with color-coded semantic state badges.
  - `aegisflow version`: Displays installed package version.
  - `aegisflow status <workflow_id>`: Renders workflow metadata panel and full WAL chronological event history table.
  - `aegisflow watch <workflow_id>`: Live terminal monitor polling workflow state until terminal status is reached.
  - `aegisflow dlq list`: Lists all workflows residing in the Dead-Letter Queue.
  - CLI entry point `[project.scripts] aegisflow = "aegisflow.cli.main:app"`.
- [x] **Real-time Web Dashboard (`dashboard/app.py`):**
  - FastAPI web application with dark theme matching DESIGN.md specification.
  - HTMX and Tailwind CSS dynamic UI.
  - REST API endpoints:
    - `GET /api/workflows`: Lists recent workflows with JSON-deserialized inputs and outputs.
    - `GET /api/workflows/{workflow_id}`: Fetches full workflow record and event stream.
    - `GET /api/dlq`: Lists dead-letter workflows.
  - Web UI routes:
    - `GET /`: Dashboard homepage listing workflow executions and status pills.
    - `GET /workflows/{workflow_id}`: Detail view showing metadata and WAL events table.
- [x] **Testing Suite:**
  - `tests/unit/test_cli.py`: Unit tests for `version`, `status`, `watch`, and `dlq list` CLI commands.
  - `tests/integration/test_dashboard_and_telemetry.py`: REST and HTML dashboard route testing with FastAPI TestClient; telemetry unit and orchestrator span integration tests.

---

### 3. Progress Summary
```
[====================] 100% Documentation & Architecture (Phase 0)
[====================] 100% Phase 1: Core Foundation & DAG Engine
[====================] 100% Phase 2: Storage Layer & SQLite WAL Event Store
[====================] 100% Phase 3: Worker Pool & Distributed Leases
[====================] 100% Phase 4: Orchestrator, Topological Scheduler & Replay Engine
[====================] 100% Phase 5: Resilience Features & Saga Rollbacks
[====================] 100% Phase 6: Observability, Rich CLI & Web Dashboard
[                    ]   0% Phase 7: Hardening, Benchmarking & Packaging
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
- **ADR-011:** Reverse Topological Saga Compensation to Prevent Partial Transaction Orphaning.
- **ADR-012:** Dead-Letter Queue (DLQ) State and Extensible Alert Hooks for Permanent Failures.
- **ADR-013:** Non-Intrusive OpenTelemetry Tracing with Graceful No-Op Fallback.
- **ADR-014:** Zero-Build-Step Web Dashboard using FastAPI, Tailwind CSS CDN, and HTMX.

---

### 5. Next Immediate Actions (Phase 7)
1. Implement Hypothesis property-based testing in `tests/property/test_hypothesis_dags.py`.
2. Implement Chaos kill-restart and concurrent recovery testing in `tests/chaos/test_chaos_recovery.py`.
3. Implement Performance and throughput benchmark suite in `tests/benchmarks/test_throughput.py`.
4. Finalize root documentation (`README.md`, diagrams, quickstart) and package distribution.

