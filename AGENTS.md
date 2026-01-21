# MongoDB Server – Agent Development Guide

## Guardrails (STRICTLY ENFORCED)

**Write Restrictions**: You may **ONLY** write to files matching these patterns.
If a target file does NOT match, **STOP** and report the violation.

```gitignore
.bazel*, **/*.md, **/*.bazel, **/*.bzl, **/*.py, **/*.sh
.devcontainer/**, .github/**, bazel/**, buildscripts/**, etc/**, evergreen/**
jstests/**, modules_poc/**
src/mongo/dbtests/**, src/mongo/unittests/**
src/mongo/**/*_test*, src/mongo/**/*_mock*, src/mongo/**/*_bm*
src/mongo/tools/mongo_tidy_checks/**
```

**Forbidden**: Production `.cpp` and `.h` files are **READ-ONLY**.

---

## Build System (Bazel)

**Setup**:

```bash
python buildscripts/install_bazel.py
export PATH=~/.local/bin:$PATH
```

**Common Build Targets**:

```bash
bazel build install-mongod
bazel build install-mongos
bazel build install-core
bazel build install-devcore
bazel build //src/mongo/db:target_name
```

**Options**: `--disable_warnings_as_errors=True` to ignore compiler warnings.
**Binaries**: Output to `bazel-bin/install/bin/`

---

## Linting & Formatting

```bash
bazel run lint
bazel run lint --fix
bazel run format
```

**Targeted Tools**:

- `python3 buildscripts/clang_tidy.py`
- `buildscripts/quickmongolint.py lint`
- `buildscripts/errorcodes.py`
- `python buildscripts/eslint.py lint`
- `bash buildscripts/yamllinters.sh`

---

## Testing

### C++ Unit Tests (GoogleTest)

```bash
bazel test //src/mongo/base:base_test                           # Run single target
bazel test //src/mongo/base:base_test --test_filter=StringDataTest.*  # Single test case
bazel test //src/mongo/base/...                                  # Run subtree
bazel test //src/mongo/base:base_test --test_output=all          # Verbose output
```

### JavaScript Integration Tests (Resmoke)

```bash
buildscripts/resmoke.py run --suites=no_passthrough jstests/noPassthrough/mytest.js  # Single test
buildscripts/resmoke.py run --suites=core jstests/core/*.js                          # Run suite
buildscripts/resmoke.py find-suites jstests/core/mytest.js                           # Find suite for test
buildscripts/resmoke.py list-suites                                                  # List all suites
```

### JS Test Robustness Rules (MANDATORY)

**Violations cause flaky tests and build failures.**

1. **Determinism**: Use explicit waits (`waitForPrimary`, `awaitRSClientHosts`) after topology changes. **NEVER** use timing-based waits.
2. **Namespace Isolation**: Use `const dbName = jsTestName();` for DBs/collections. Append suffixes for multiple DBs (e.g., `jsTestName() + "_source"`).
3. **Retries**: Handle `TransientTransactionError` using helpers from `jstests/libs/auto_retry_transaction_in_sharding.js`.
4. **Assertions**: Wrap commands with `assert.commandWorked()` or `assert.commandFailed()`. Verify specific properties, not counts.
5. **Failpoints**: Must be namespace-specific. If global, use isolation tags (`requires_isolated_mongod`).
6. **Tagging**: Add relevant tags (e.g., `requires_fcv_81`, `does_not_support_stepdowns`). See `jstests/tags.md`.
7. **Documentation**: Every test MUST have a top-level comment explaining its purpose.
8. **Network Errors**: Use `assert.soonRetryOnNetworkErrors` for callbacks with direct shard/replica connections.

---

## C++ Code Style

### Formatting

- **Indent**: 4 spaces (no tabs)
- **Line limit**: 100 columns
- **Braces**: Opening brace on same line
- **Include guard**: `#pragma once`
- **Const style**: West const (`const X x;`)

### Naming Conventions

| Entity              | Convention             | Example            |
| ------------------- | ---------------------- | ------------------ |
| Types               | `TitleCase`            | `MyClass`          |
| Functions/Variables | `camelCase`            | `doSomething`      |
| Namespaces          | `snake_case`           | `my_namespace`     |
| Private members     | `_leadingUnderscore`   | `_privateVar`      |
| Constants           | `kConstantName`        | `kMaxSize`         |
| Files               | `snake_case.cpp`       | `my_class.cpp`     |
| Test accessors      | `ForTest` / `_forTest` | `getDataForTest()` |

### Include Order (separated by blank lines)

1. Corresponding header (`"mongo/db/foo.h"`)
2. First-party headers (`"mongo/..."`)
3. C++ stdlib (`<vector>`, `<string>`)
4. System C headers (`<unistd.h>`)
5. Third-party (`<boost/...>`, `<fmt/...>`)

### Modern C++ Practices

- Use **RAII** and **smart pointers** (`std::unique_ptr`, `std::shared_ptr`). Avoid raw `new/delete`.
- Use `explicit` for single-argument constructors.
- Use `override` on virtual overrides; use `final` when appropriate.
- Prefer `StringData` over `std::string_view`.
- Use `class` for types with invariants, `struct` for passive data.
- No C-style casts; use `static_cast`, `const_cast`, `checked_cast`.
- Return early; avoid deep nesting.

### Documentation

- Use `/** */` or `///` for API docs. Use `///<` for inline member docs.
- Write complete, grammatical sentences. Descriptive, not imperative ("Calculates..." not "Calculate...").
- TODOs: `// TODO(SERVER-12345): Clear actionable description.`

---

## Key Directories

| Directory              | Description                            |
| ---------------------- | -------------------------------------- |
| `src/mongo/`           | Server source (READ-ONLY except tests) |
| `src/mongo/unittests/` | C++ unit tests                         |
| `src/mongo/dbtests/`   | DB-level tests                         |
| `jstests/`             | JavaScript integration tests           |
| `buildscripts/`        | Python/Bash build and test tools       |
| `bazel/`               | Bazel configuration and rules          |
| `docs/`                | Developer documentation                |

---

## Cursor Rules (from `.cursor/rules/`)

**`cs-robust-jstests.mdc`** (applies to `jstests/**/*.js`):

- Write tests with deterministic outcomes using explicit wait helpers.
- Avoid timing-dependent tests; verify outcomes and state changes.
- Always use `jsTestName()` for unique namespaces.
- Use retry helpers for `TransientTransactionError` in sessionDB operations.
- Make failpoints namespace-specific or use isolation tags.

---

## References

- `docs/building.md` – Build instructions
- `docs/cpp_style.md` – Full C++ style guide
- `docs/linting.md` – Linting tools documentation
- `docs/exception_architecture.md` – Error handling and assertions
- `jstests/tags.md` – Test tagging documentation
