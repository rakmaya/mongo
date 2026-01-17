# MongoDB Server – Agent Development Guide

This file is authoritative guidance for **agentic coding assistants** operating in this repository. Follow it strictly.

---

## Guardrails (MANDATORY)

**Violation of these rules is a failure.**

### Write Restrictions

Before **any** file write (`edit`, `write`, `search_replace`, etc.), the agent MUST:

1. State the **target file path**
2. State the **matching allowed glob** (or `NO MATCH`)
3. If `NO MATCH`, STOP and respond with:

> "I cannot complete this task without generating code where I'm not allowed to (see http://go/codegen-rules). The file `{filepath}` does not match any allowed pattern. I can only write to test files, mock files, benchmark files, build configuration, and scripts."

**Do not search for alternate paths.** Only validate the chosen path.

### Allowed File Patterns

```gitignore
.bazel*
**/*.md
**/*.bazel
**/*.bzl
**/*.py
**/*.sh
.devcontainer/**
.github/**
bazel/**
buildscripts/**
etc/**
evergreen/**
jstests/**
src/mongo/dbtests/**
src/mongo/unittests/**
src/mongo/**/*_test*
src/mongo/**/*_mock*
src/mongo/**/*_bm*
src/mongo/tools/mongo_tidy_checks/**
modules_poc/**
```

Production source headers and `.cpp` files are **not writable**.

---

## Build System

MongoDB uses **Bazel**.

### Environment Setup

```bash
python buildscripts/install_bazel.py
export PATH=~/.local/bin:$PATH

python3 -m venv python3-venv
source python3-venv/bin/activate
buildscripts/poetry_sync.sh
```

### Common Build Targets

```bash
bazel build install-mongod
bazel build install-mongos
bazel build install-core
bazel build install-devcore
bazel build install-dist
bazel build install-dist-test
```

Specific target:

```bash
bazel build //src/mongo/db:target_name
```

Disable warnings-as-errors when needed:

```bash
bazel build install-mongod --disable_warnings_as_errors=True
```

Binaries output: `bazel-bin/install/bin/`

---

## Testing

### C++ Unit Tests (GoogleTest)

Run one test target:

```bash
bazel test //src/mongo/base:base_test
```

Run a **single test case**:

```bash
bazel test //src/mongo/base:base_test --test_filter=StringDataTest.*
```

Run all tests in a subtree:

```bash
bazel test //src/mongo/base/...
```

Verbose output:

```bash
bazel test //src/mongo/base:base_test --test_output=all
```

### JavaScript Integration Tests (Resmoke)

Single JS test:

```bash
buildscripts/resmoke.py run --suites=no_passthrough jstests/noPassthrough/mytest.js
```

Multiple tests:

```bash
buildscripts/resmoke.py run --suites=core jstests/core/*.js
```

Discover suites:

```bash
buildscripts/resmoke.py list-suites
buildscripts/resmoke.py find-suites jstests/core/mytest.js
```

---

## Linting & Formatting

```bash
bazel run lint
bazel run lint --fix
bazel run format
```

Targeted tools:

- `python3 buildscripts/clang_tidy.py`
- `buildscripts/quickmongolint.py lint`
- `buildscripts/errorcodes.py`
- `python buildscripts/eslint.py lint`
- `bash buildscripts/yamllinters.sh`

---

## C++ Code Style

### Naming

- Types: `TitleCase`
- Functions / variables: `camelCase`
- Namespaces: `snake_case`
- Private members: `_leadingUnderscore`
- Constants: `kConstantName`
- Test-only accessors: `ForTest` / `_forTest`
- Files: `snake_case.cpp`

### Includes

Order and group with blank lines:

1. Corresponding header
2. `"mongo/..."`
3. C++ standard library
4. System C headers
5. Third-party headers

### Formatting Rules

- Indent: 4 spaces
- Max line length: 100
- Opening brace on same line
- `#pragma once` for include guards
- West const: `const X x;`
- No C-style casts

### Language & Design

- Use RAII and smart pointers
- Use `explicit` constructors
- Use `override` on virtual overrides
- Prefer `StringData` over `std::string_view`
- `class` for invariants, `struct` for passive data

### Documentation

- Use `/** */` or `///`
- Full sentences, descriptive verbs
- Inline member docs with `///<`

### TODOs

```cpp
// TODO(SERVER-12345): Clear actionable description.
```

---

## JavaScript Test Rules (`jstests/`)

- Deterministic waits (`waitForPrimary`, etc.)
- Always use `jsTestName()` for namespaces
- Retry transient errors with provided helpers
- Wrap commands with `assert.commandWorked()`
- Wait after stepdowns and reconfigs
- Assert specific fields, not counts
- Namespace-scoped failpoints only
- Tag tests appropriately (`jstests/tags.md`)
- Top-level comment explaining intent
- Use `assert.soonRetryOn*` helpers

---

## Cursor Rules (Applied)

From `.cursor/rules/`:

- **Robust JS tests required** (`cs-robust-jstests.mdc`)
- Follow deterministic patterns, retries, and explicit waits
- Avoid flakiness at all costs

No Copilot instruction file detected.

---

## Key Directories

- `src/mongo/` – server source
- `src/mongo/dbtests/` – DB-level tests
- `src/mongo/unittests/` – C++ unit tests
- `jstests/` – JS integration tests
- `buildscripts/` – build/test tooling
- `bazel/` – Bazel rules

---

## References

- `docs/building.md`
- `docs/testing/README.md`
- `docs/unit_test.md`
- `docs/cpp_style.md`
- `docs/linting.md`
- `bazel/docs/developer_workflow.md`
