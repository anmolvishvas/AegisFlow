# Project Memory & Engineering Journal (MEMORY.md)
## AegisFlow: Resilient Event-Driven DAG Task Engine

---

### 1. Current Project Status
- **Current Phase:** Phase 1 Initialization & Core Foundation
- **Overall Status:** Specification & Planning Completed. Ready for codebase scaffolding.
- **Active Workspace Directory:** `C:\Users\DELL\.gemini\antigravity\scratch\resilient-dag-engine\`

---

### 2. Completed Tasks
- [x] **Product Requirements Document (`PRD.md`):** Outlined product vision, problem statement, goals, target personas, and MVP scope.
- [x] **System Architecture Document (`ARCHITECTURE.md`):** Defined high-level architecture diagram, 5 core subsystems, technology stack, and folder structure.
- [x] **Engineering Rulebook (`RULES.md`):** Defined strict standards for durable execution, idempotency, pure state machines, async safety, type safety (`mypy --strict`), and project boundaries.
- [x] **UI/UX & Design Tokens (`DESIGN.md`):** Specified visual hierarchy, dark slate & semantic color tokens, typography, web components (DAG visualizer, event ticker, drawers), and Rich CLI layouts.
- [x] **Task Breakdown & Roadmap (`TASKS.md`):** Decomposed the complete project into 7 actionable phases with clear acceptance criteria.
- [x] **Project Memory & Journal (`MEMORY.md`):** Created project memory tracker and Architectural Decision Records (ADRs).

---

### 3. Progress Summary
```
[====================] 100% Documentation & Architecture
[                    ]   0% Core Codebase Implementation
```

---

### 4. Key Architectural Decisions (ADR Log)

#### ADR-001: Event-Sourced Write-Ahead Log (WAL) over In-Place DB Mutation
- **Context:** Standard task queues (like Celery) update row status (`status = 'SUCCESS'`) in-place. If a crash occurs midway, partial state transitions are lost or corrupted.
- **Decision:** All state transitions are modeled as immutable events appended to an append-only WAL. Current state is derived by folding events through a pure state machine function.
- **Consequence:** Full auditability, trivial time-travel debugging, and guaranteed crash recovery via deterministic replay.

#### ADR-002: SQLite in WAL Mode as Default Embedded Storage
- **Context:** Temporal requires operating heavy multi-node backend services (Cassandra/Postgres/Go clusters), making it prohibitive for lightweight projects and local development.
- **Decision:** Support SQLite in WAL mode (`PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;`) out-of-the-box as the default storage engine, with zero external dependencies.
- **Consequence:** Instant zero-setup developer experience for single-node systems, unit tests, and CLI workflows, with a modular interface for PostgreSQL in distributed deployments.

#### ADR-003: Distributed Leases with Monotonic Fencing Tokens
- **Context:** In distributed systems, a worker may pause due to a long garbage collection pause or network blip (zombie worker), causing the orchestrator to assume it is dead and reassign the task. If the zombie worker wakes up and writes its result, state is corrupted.
- **Decision:** Every task lease issuance increments a monotonic fencing token. Storage write operations reject events carrying obsolete fencing tokens.
- **Consequence:** Guarantees safety against split-brain execution and zombie worker races.

#### ADR-004: Pure Functional State Machine
- **Context:** Mixing I/O, timers, or network calls directly into the state transition logic makes testing and replaying nondeterministic.
- **Decision:** The state transition logic (`state_machine.py`) is a pure function: `(State, Event) -> (NewState, [SideEffects])`.
- **Consequence:** Enables 100% test coverage with instantaneous in-memory unit tests and reproducible replay runs.

---

### 5. Immediate Next Actions
1. **Initialize Project Scaffolding:** Create `pyproject.toml` with dependencies (`msgspec`/`pydantic`, `aiosqlite`, `rich`, `typer`, `ruff`, `mypy`).
2. **Implement Core Enums & Exceptions:** Create `states.py`, `events.py`, and `exceptions.py`.
3. **Build the DAG Graph Compiler:** Implement node dependency tracking, cycle detection, and topological sorting in `dsl/graph.py` with unit tests.
