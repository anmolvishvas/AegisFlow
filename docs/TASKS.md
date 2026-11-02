# Implementation Plan & Task Breakdown (TASKS.md)
## AegisFlow: Resilient Event-Driven DAG Task Engine

---

### Project Phases Overview
- **Phase 1:** Core Foundation, Domain Models & DAG Graph Engine
- **Phase 2:** Storage Abstraction & SQLite WAL Event Store
- **Phase 3:** Worker Pool, Distributed Leases & Heartbeat System
- **Phase 4:** Orchestrator, Topological Scheduler & Replay Engine
- **Phase 5:** Resilience Features (Retries, Exponential Backoff, Saga Rollbacks)
- **Phase 6:** Observability, Rich CLI & Web Dashboard
- **Phase 7:** Chaos Testing, Benchmarking & Packaging

---

### Phase 1: Core Foundation & DAG Graph Engine
*Goal: Establish domain types, event models, and the dependency graph compiler with cycle detection.*

- [ ] **Task 1.1: Project Scaffolding & Tooling Setup**
  - Initialize `pyproject.toml` with Ruff, Mypy (`strict = true`), and Pytest configurations.
  - Setup pre-commit hooks and create `src/aegisflow/py.typed`.
  - *Acceptance:* `mypy` and `ruff` pass on the clean skeleton.

- [ ] **Task 1.2: Core Domain Models & Event Schemas**
  - Implement `src/aegisflow/core/states.py`: Workflow and Task state enums.
  - Implement `src/aegisflow/core/events.py`: Immutable event dataclasses/msgspec models (`WorkflowInitiated`, `TaskScheduled`, `TaskCompleted`, `TaskFailed`, etc.).
  - Implement `src/aegisflow/core/exceptions.py`: Complete hierarchical exception tree.
  - *Acceptance:* 100% type coverage, JSON/binary serialization roundtrip tests pass.

- [ ] **Task 1.3: DAG Graph Builder & Cycle Detection**
  - Implement `src/aegisflow/dsl/graph.py`: Node and edge tracking.
  - Implement Kahn’s topological sort and DFS-based cycle detection.
  - Unit tests verifying dynamic fan-out, fan-in, and `CycleDetectedError` on circular definitions.
  - *Acceptance:* Unit test suite covers 100% of graph edge cases.

- [ ] **Task 1.4: Workflow & Task Decorators (DSL)**
  - Implement `@task` decorator (`src/aegisflow/dsl/task.py`): options for retries, timeouts, and compensation hooks.
  - Implement `@workflow` decorator (`src/aegisflow/dsl/workflow.py`): parses function invocation tree into a `WorkflowDefinition`.
  - *Acceptance:* Validates sync and async callables with parameter signature checking.

---

### Phase 2: Storage Abstraction & SQLite WAL Event Store
*Goal: Build the durable persistence layer ensuring zero data loss on process failure.*

- [ ] **Task 2.1: Storage Interfaces**
  - Define `EventStoreInterface` in `src/aegisflow/storage/base.py` (`append_event`, `get_events`, `get_active_workflows`).
  - Define `LeaseManagerInterface` (`acquire_lease`, `renew_lease`, `release_lease`, `reap_expired_leases`).
  - *Acceptance:* Clean ABCs with type annotations and async contracts.

- [ ] **Task 2.2: SQLite WAL Implementation (Embedded Mode)**
  - Implement `src/aegisflow/storage/sqlite.py`.
  - Configure `PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;`.
  - Implement tables: `workflows`, `events` (append-only), `leases`, and `snapshots`.
  - *Acceptance:* Integration tests verifying concurrent writers without database lock errors.

- [ ] **Task 2.3: Idempotency & Fencing Token Persistence**
  - Add atomic transaction guards to prevent duplicate event ingestion.
  - Store and validate monotonically increasing fencing tokens per task.
  - *Acceptance:* Rejection test verifying that an outdated fencing token raises `ConcurrencyConflictError`.

---

### Phase 3: Worker Pool, Distributed Leases & Heartbeat System
*Goal: Enable workers to claim tasks safely, report heartbeats, and recover from hung nodes.*

- [ ] **Task 3.1: Distributed Lease & Heartbeat Manager**
  - Implement `src/aegisflow/engine/lease.py`.
  - Lease acquisition logic with TTL and fencing token generation.
  - Heartbeat background loop running at 1/3 of TTL.
  - *Acceptance:* Lease expires cleanly if heartbeat loop stops; another worker can claim it.

- [ ] **Task 3.2: Async & Process Worker Runners**
  - Implement `AsyncRunner` (`src/aegisflow/workers/async_runner.py`) for coroutine tasks.
  - Implement `ProcessRunner` (`src/aegisflow/workers/process_runner.py`) using `multiprocessing` for CPU-bound tasks.
  - *Acceptance:* Verifies both async and synchronous functions run in their respective isolation environments.

- [ ] **Task 3.3: Worker Supervisor Daemon & Signal Handling**
  - Implement `src/aegisflow/workers/daemon.py`.
  - Trap `SIGTERM` and `SIGINT` for graceful shutdown: stop claiming new work, wait for current tasks up to grace period, release leases.
  - *Acceptance:* Automated test sends `SIGTERM` and asserts active leases are cleanly handled.

---

### Phase 4: Orchestrator, Topological Scheduler & Replay Engine
*Goal: Connect the DAG dependencies to the event stream and enable durable crash-recovery.*

- [ ] **Task 4.1: Pure State Machine**
  - Implement `src/aegisflow/engine/state_machine.py`.
  - Pure function: `transition(workflow_state, event) -> (new_state, tasks_to_dispatch)`.
  - *Acceptance:* Exhaustive unit test suite covering every state transition permutation.

- [ ] **Task 4.2: Topological Dispatcher Loop**
  - Implement `src/aegisflow/engine/orchestrator.py`.
  - Event loop consuming events, updating state machine, and scheduling ready tasks when upstream dependencies succeed.
  - *Acceptance:* Fan-out/fan-in test (A -> B & C -> D) verifies D only executes after both B and C finish.

- [ ] **Task 4.3: Durable Replay & Crash Recovery Engine**
  - Implement `src/aegisflow/engine/replay.py`.
  - On startup or worker takeover, reconstructs workflow DAG from event history, skipping completed tasks and restoring cached outputs.
  - *Acceptance:* Test simulates mid-workflow crash; replayed workflow resumes without re-executing step 1.

---

### Phase 5: Resilience Features & Saga Rollbacks
*Goal: Implement production-grade failure handling, retries, and compensation transactions.*

- [ ] **Task 5.1: Retry Policy with Exponential Backoff & Full Jitter**
  - Implement retry scheduler supporting `max_retries`, `initial_interval`, `multiplier`, and `jitter`.
  - Persist `TaskRetried` events with scheduled execution timestamp.
  - *Acceptance:* Test verifies backoff timing and jitter distribution across consecutive retries.

- [ ] **Task 5.2: Saga Compensation Execution**
  - Implement `src/aegisflow/engine/saga.py`.
  - If a task fails permanently, locate all completed upstream tasks that registered a `compensate` function.
  - Dispatch compensation tasks in reverse topological order.
  - *Acceptance:* End-to-end payment saga test: Task 3 fails, Task 2 and Task 1 compensations run successfully.

- [ ] **Task 5.3: Dead-Letter Queue (DLQ) & Alerting Hooks**
  - Move exhausted workflows to `DEAD_LETTER` state.
  - Provide hooks to replay or manually resolve dead-lettered workflows.
  - *Acceptance:* Workflows failing past max retries enter DLQ and emit alert events.

---

### Phase 6: Observability, Rich CLI & Web Dashboard
*Goal: Provide developer tooling, metrics, and real-time visualization.*

- [x] **Task 6.1: OpenTelemetry Instrumentation**
  - Implement `src/aegisflow/observability/telemetry.py`.
  - Instrument workflows and tasks with OpenTelemetry spans and trace context propagation across worker processes.
  - *Acceptance:* Spans output to standard OTel collector or local console tracer.

- [x] **Task 6.2: Terminal CLI (`aegisflow`)**
  - Implement Typer CLI (`src/aegisflow/cli/main.py`) with commands:
    - `aegisflow run <workflow_file>`
    - `aegisflow status <workflow_id>`
    - `aegisflow watch <workflow_id>` (Live Rich terminal monitor)
    - `aegisflow dlq list` / `aegisflow dlq retry <workflow_id>`
  - *Acceptance:* Live terminal UI renders DAG tree and event ticker seamlessly.

- [x] **Task 6.3: FastAPI & HTMX Real-Time Web Dashboard**
  - Implement `src/aegisflow/dashboard/app.py`.
  - Server-Sent Events (SSE) route streaming live event store changes.
  - HTMX views for workflow list, interactive DAG visualizer, and worker health fleet.
  - *Acceptance:* Web dashboard displays live node status transitions without manual page refresh.

---

### Phase 7: Verification, Benchmarking & Packaging
*Goal: Harden the codebase with chaos tests, performance benchmarks, and publishable distribution.*

- [x] **Task 7.1: Property-Based Testing with Hypothesis**
  - Write property tests generating random DAGs (up to 50 nodes with arbitrary dependencies).
  - Verify that Kahn's algorithm never deadlocks and topological order is strictly respected.
  - *Acceptance:* 1,000 generated DAG test iterations pass without assertion errors.

- [x] **Task 7.2: Chaos Testing Suite (SIGKILL Injection)**
  - Spawn workers running real multi-step workflows.
  - Script randomly fires `kill -9` at worker PIDs mid-task.
  - Verify engine detects lease expiration, recovers state, and completes all workflows with correct output.
  - *Acceptance:* 100% of workflows achieve consistent final state despite crash injections.

- [x] **Task 7.3: Performance Benchmarking**
  - Benchmark dispatch latency (<10ms per task hop).
  - Benchmark SQLite WAL write throughput (>2,000 events/sec on standard SSD).
  - Generate automated benchmark reports.

- [x] **Task 7.4: Packaging & Documentation**
  - Build comprehensive `README.md` with architecture diagrams and quickstart examples.
  - Setup MkDocs documentation site.
  - Publish package build setup via `pyproject.toml` for PyPI distribution.

