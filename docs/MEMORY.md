# Project Memory & Engineering Journal (MEMORY.md)
## AegisFlow: Resilient Event-Driven DAG Task Engine

---

### 1. Current Project Status
- **Current Phase:** Phase 2 Complete (Storage Layer & SQLite WAL Event Store)
- **Active Branch:** `feat/phase-2-storage-wal`
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
- [x] **Storage Abstraction Interfaces (`storage/base.py`):**
  - `EventStoreInterface`: `create_workflow`, `update_workflow_status`, `get_workflow_record`, `append_event`, `get_events`, `get_active_workflows`.
  - `LeaseManagerInterface`: `acquire_lease`, `renew_lease`, `release_lease`, `reap_expired_leases`.
  - `LeaseInfo` dataclass tracking task leases, workers, and fencing tokens.
- [x] **SQLite WAL Storage Engine (`storage/sqlite.py`):**
  - Embedded SQLite running in `PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;` with transaction locks and busy timeouts.
  - Relational tables: `workflows`, append-only `events` with indexed sequence ordering, and distributed `leases`.
  - Automatic event deserialization to exact Pydantic event classes.
- [x] **Distributed Leases & Fencing Token Persistence:**
  - Monotonic fencing token increments on lease takeovers.
  - Rejection of expired or conflicting worker lease renewals via `ConcurrencyConflictError`.
- [x] **Integration Testing (`tests/integration/test_sqlite_store.py`):**
  - WAL pragma verification and table initialization.
  - Workflow status transitions and duplicate prevention.
  - Event appending and chronological roundtrip restoration.
  - Active workflow filtering for recovery.
  - Exclusive leasing, fencing token increments, renewal, and reaping.
  - Concurrent async writers test under high write pressure.

---

### 3. Progress Summary
```
[====================] 100% Documentation & Architecture (Phase 0)
[====================] 100% Phase 1: Core Foundation & DAG Engine
[====================] 100% Phase 2: Storage Layer & SQLite WAL Event Store
[                    ]   0% Phase 3: Worker Pool, Distributed Leases & Heartbeat System
```

---

### 4. Key Architectural Decisions (ADR Log)

- **ADR-001:** Event-Sourced Write-Ahead Log (WAL) over In-Place DB Mutation.
- **ADR-002:** SQLite in WAL Mode as Default Embedded Storage.
- **ADR-003:** Distributed Leases with Monotonic Fencing Tokens.
- **ADR-004:** Pure Functional State Machine.
- **ADR-005:** Three-Color DFS for Instant Cycle Detection at DAG Compilation.
- **ADR-006:** Event Registry Type Dispatching for Zero-Loss Event Hydration.

---

### 5. Next Immediate Actions (Phase 3)
1. Implement `WorkerSupervisorDaemon` in `src/aegisflow/workers/daemon.py` with `SIGINT`/`SIGTERM` handling.
2. Build `AsyncRunner` and `ProcessRunner` for isolating IO-bound vs CPU-bound tasks.
3. Build background heartbeat renewal loop and reaper monitor in `src/aegisflow/engine/lease.py`.
