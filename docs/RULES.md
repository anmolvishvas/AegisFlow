# Engineering & AI Rulebook (RULES.md)
## AegisFlow: Resilient Event-Driven DAG Task Engine

This document outlines the inviolable architectural principles, technology standards, and coding conventions that all contributors and AI agents must follow when writing code for the AegisFlow project.

---

### 1. General Principles

1. **Durable Execution First:**
   - No task execution or state transition shall occur without prior persistence to the Write-Ahead Log (WAL).
   - Never assume process memory survives. Any state needed for recovery must exist as an event in the event store.

2. **Strict Idempotency & Fencing:**
   - Every task schedule and dispatch must carry an idempotency key and a monotonically increasing fencing token.
   - Workers must never execute duplicate tasks if another worker has already renewed the lease or marked the task complete.

3. **Pure State Machines:**
   - State transition logic (`state_machine.py`) must be pure, deterministic functions without network, disk, or clock side effects.
   - Time and randomness must be passed in as explicit parameters, enabling 100% deterministic replay testing.

4. **Zero Silent Failures:**
   - Never suppress exceptions or use bare `except:` blocks.
   - Any failure during task execution must generate a structured `TaskFailed` event with complete serialized tracebacks and error categorization (transient vs. permanent).

5. **Backward Compatibility of Event Schemas:**
   - Events are immutable. Once written to the WAL, an event schema cannot be modified in a breaking manner. All schema evolution must be additive or versioned.

---

### 2. Technology & Coding Standards

#### 2.1 Python & Environment
- **Target Version:** Python 3.11+ strictly. Code must take advantage of modern features (e.g., `asyncio.TaskGroup`, `ExceptionGroup`, match-case statements, `Self` type).
- **Type Safety:** 
  - Every function, method, parameter, and return value must have explicit type hints.
  - The codebase must pass `mypy --strict` and `pyright` with zero errors or warnings.
  - `Any` is prohibited unless explicitly justified with a comment and wrapped in a localized type guard.
  - Must ship with `src/aegisflow/py.typed` (PEP 561 compliance).

#### 2.2 Concurrency & Async Safety Rules
- **Never Block the Event Loop:**
  - Synchronous file I/O or blocking CPU operations inside async functions are strictly forbidden. Use `asyncio.to_thread` or dispatch to `ProcessPoolExecutor`.
- **Structured Concurrency:**
  - Always prefer `asyncio.TaskGroup()` over detached `asyncio.create_task()` to prevent unhandled background task leaks.
- **Graceful Shutdown & Signal Trapping:**
  - Worker processes must trap `SIGINT` and `SIGTERM`, release active leases, finish in-flight tasks within a grace timeout (default: 30s), or safely yield to the reaper.

#### 2.3 Error Handling & Domain Exceptions
- All domain exceptions must inherit from `AegisFlowError`:
  ```
  AegisFlowError
  ├── WorkflowError
  │   ├── WorkflowNotFoundError
  │   ├── WorkflowAlreadyExistsError
  │   └── CycleDetectedError
  ├── TaskError
  │   ├── TaskNotFoundError
  │   ├── TaskLeaseExpiredError
  │   └── MaxRetriesExceededError
  └── StorageError
      ├── EventStoreUnavailableError
      └── ConcurrencyConflictError
  ```
- Use Python 3.11's `add_note()` to enrich exceptions with execution context (`workflow_id`, `task_id`, `fencing_token`).

#### 2.4 Code Style & Tooling
- **Formatter & Linter:** `ruff format` and `ruff check --select ALL`.
- **Docstrings:** Google-style docstrings for all public classes, functions, and module entrypoints.
- **Logging:** Structured logging only (via standard `logging` configured with JSON formatting or `structlog`). No raw `print()` statements outside the CLI.

---

### 3. Project Structure & Boundary Rules

1. **Strict Layering & No Circular Imports:**
   - `core` cannot import from `dsl`, `engine`, `workers`, or `storage`.
   - `dsl` only depends on `core`.
   - `storage` only depends on `core`.
   - `engine` orchestrates `core` and `storage`.
   - `workers` depend on `core` and `engine`.
   - `cli` and `dashboard` depend on `engine` and `storage`.

2. **Abstract Storage Interfaces:**
   - All persistence must go through `EventStoreInterface` and `LeaseManagerInterface`. The engine must never write directly to SQLite or PostgreSQL specific tables without using the abstraction.

3. **Serialization Integrity:**
   - All task payloads and event arguments must serialize cleanly to JSON or MessagePack. Unpicklable objects (sockets, open file handles, thread locks) cannot be passed across task boundaries.

---

### 4. Testing & Verification Rules

1. **Test Pyramid:**
   - **Unit Tests:** Fast, in-memory, deterministic tests (<50ms each) covering graph traversal, state machine transitions, and decorator syntax.
   - **Integration Tests:** Verifying real SQLite WAL operations, multi-threaded worker pools, and task scheduling.
   - **Property-Based Testing (Hypothesis):** Random DAG generator to verify topological sorting and cycle detection under 1,000+ arbitrary graph permutations.
   - **Chaos Tests:** Spawn workers in separate processes and abruptly send `kill -9` while workflows are running to verify that the replay engine resumes cleanly.

2. **Coverage Standard:**
   - Minimum **90% branch coverage** across all core and engine modules.
