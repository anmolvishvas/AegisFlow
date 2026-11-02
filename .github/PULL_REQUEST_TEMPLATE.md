## 📌 Pull Request Overview

### 1. Summary of Changes
<!-- Provide a concise explanation of what this PR does and why it was built. Focus on the problem solved and design rationale. -->

### 2. Motivation & Context
<!-- Why is this change required? What problem or ticket does it address? Link relevant issues below. -->
- **Fixes Issue:** #
- **Relates to:** #

---

### 3. Type of Change
<!-- Select all that apply using [x] -->
- [ ] 🚀 **`feat`** (A new user-facing or architectural feature)
- [ ] 🐛 **`fix`** (A bug fix)
- [ ] ♻️ **`refactor`** (Code change that neither fixes a bug nor adds a feature)
- [ ] ⚡ **`perf`** (A code optimization that improves performance or latency)
- [ ] 🛡️ **`resilience`** (Improvements to crash recovery, retries, saga rollbacks, or leasing)
- [ ] 📝 **`docs`** (Documentation updates in `docs/` or docstrings)
- [ ] 🧪 **`test`** (Adding or refactoring tests: unit, integration, chaos, property-based)
- [ ] 🔧 **`chore`** (Tooling, dependencies, CI/CD, or repo maintenance)

---

### 4. ⚠️ Breaking Change Warning
<!-- Does this PR introduce a breaking change? (e.g. changing event schema, public API signature, or storage format) -->
- [ ] **No breaking changes** (backward-compatible)
- [ ] **Yes, this is a breaking change!**
  <!-- If YES, provide a detailed migration guide or description of breaking behavior below: -->
  > **Breaking Details:** 

---

### 5. Architectural & System Impact
<!-- Describe any impact on performance, event schemas, storage, or concurrency. -->
- **Subsystem(s) Affected:** `core` | `dsl` | `engine` | `storage` | `workers` | `observability` | `cli` | `dashboard`
- **Event Schema Evolution:** Does this change, add, or deprecate any event in the Write-Ahead Log (WAL)? `[Yes / No]`
- **Concurrency & Async Impact:** Are there new locks, lease mechanisms, background tasks, or sub-processes? `[Yes / No]`

---

### 6. Verification & Test Plan
<!-- How was this tested? Detail tests added and commands run. -->
- [ ] **Unit Tests:** `pytest tests/unit/` (Fast in-memory tests)
- [ ] **Integration Tests:** `pytest tests/integration/` (SQLite WAL / Postgres persistence)
- [ ] **Chaos / Crash Recovery Tests:** `pytest tests/chaos/` (Process kill & replay verification)
- [ ] **Property-Based Tests:** `pytest tests/property/` (Hypothesis random DAG generator)

#### Benchmark / Performance Metrics (if applicable):
```text
# Paste benchmark results or execution timings here:
```

---

### 7. Senior Engineering Quality Checklist
<!-- All items must be checked before merging! -->
- [ ] **Coding Standards:** Code strictly complies with [`docs/RULES.md`](docs/RULES.md).
- [ ] **Strict Type-Safety:** Passes `mypy --strict` and `pyright` with **0 errors and 0 warnings**.
- [ ] **Linting & Formatting:** Passes `ruff check` and `ruff format --check`.
- [ ] **Test Coverage:** Code changes maintain or improve test coverage (>= 90% branch coverage target).
- [ ] **Async Safety:** No blocking synchronous I/O or CPU operations inside `async def` without `asyncio.to_thread`.
- [ ] **Error Handling:** Uses domain exception hierarchy inheriting from `AegisFlowError`. No bare `except:` blocks.
- [ ] **Documentation:** Relevant sections in `docs/` or docstrings have been updated.
- [ ] **Memory & Status:** Updated [`docs/MEMORY.md`](docs/MEMORY.md) if a task or milestone was completed.
