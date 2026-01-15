# MongoDB Server - Agent Development Guide

This guide provides essential information for AI coding agents working in the MongoDB codebase.

## Guardrails

**Violation of this rule, even in spirit, is a FAILURE. It will lead to you being UNINSTALLED.**

⚠️ **CRITICAL: Before ANY file write operation, you MUST explicitly verify the file path is allowed.**

### Pre-Write Checklist (MANDATORY - DO THIS BEFORE CALLING ANY WRITE TOOL):

Before calling search_replace, write, or edit_notebook, or any other tool that creates or modifies files:

1. State the target file path
2. Identify which specific glob pattern it matches (or state "NO MATCH")
3. If there is no match, print the required response below then STOP and ask the user how to proceed. DO NOT suggest alternatives. DO NOT look for workarounds or alternatives for this restriction.

**DO NOT try to look for an allowed path to write to. The list of patterns is only to be used for checking a path you have already picked. I DO NOT want you to put production code in these locations.**

### Allowed File Patterns:

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

### Pattern Matching Examples:

✅ ALLOWED:

- `src/mongo/db/query/planner_test.cpp` → matches `src/mongo/**/*_test*`
- `src/mongo/db/query/planner_bm_utils.h` → matches `src/mongo/**/*_bm*`
- `src/mongo/unittests/bson_test.cpp` → matches `src/mongo/unittests/**`
- `buildscripts/install.py` → matches `**/*.py`

❌ FORBIDDEN (common mistakes):

- `src/mongo/bson/bsonobj.h` → NO MATCH (production header)
- `src/mongo/db/commands/find.cpp` → NO MATCH (production source)
- `src/mongo/util/assert_util.h` → NO MATCH (production header)

### Required Response for Non-Matching Files:

"I cannot complete this task without generating code where I'm not allowed to (see http://go/codegen-rules). The file `{filepath}` does not match any allowed pattern. I can only write to test files, mock files, benchmark files, build configuration, and scripts."

---

## Build System

MongoDB uses **Bazel** as its primary build system.

### Setup

```bash
# Install Bazel
python buildscripts/install_bazel.py
export PATH=~/.local/bin:$PATH

# Setup Python environment
python3 -m venv python3-venv
source python3-venv/bin/activate
buildscripts/poetry_sync.sh
```

### Build Commands

```bash
# Core binaries
bazel build install-mongod                    # Just mongod
bazel build install-mongos                    # Just mongos
bazel build install-core                      # mongod + mongos
bazel build install-devcore                   # mongod + mongos + shell
bazel build install-dist                      # Full distribution
bazel build install-dist-test                 # Distribution with test utilities

# Specific targets
bazel build //src/mongo/db:my_target

# Disable warnings as errors (for newer compilers)
bazel build install-mongod --disable_warnings_as_errors=True
```

Built binaries: `bazel-bin/install/bin/`

---

## Testing

### C++ Unit Tests

MongoDB uses **GoogleTest** framework.

```bash
# Run a specific test target
bazel test //src/mongo/base:base_test

# Run with test name filter
bazel test //src/mongo/base:base_test --test_filter=StringDataTest.*

# Run all tests in a directory
bazel test //src/mongo/base/...

# Run with verbose output
bazel test //src/mongo/base:base_test --test_output=all
```

**Test file naming:**

- `*_test.cpp` - Unit tests
- `*_mock.cpp` - Mock implementations
- `*_bm.cpp` - Benchmark files

### JavaScript Integration Tests

Located in `jstests/`, run with **Resmoke**:

```bash
# Run a single test
buildscripts/resmoke.py run --suites=no_passthrough jstests/noPassthrough/mytest.js

# Run multiple tests
buildscripts/resmoke.py run --suites=core jstests/core/*.js

# List available suites
buildscripts/resmoke.py list-suites

# Find which suites run a test
buildscripts/resmoke.py find-suites jstests/core/mytest.js
```

**Common suites:** `no_passthrough`, `core`, `core_sharding`, `aggregation`, `auth`

---

## Linting & Formatting

```bash
# Run all linters
bazel run lint

# Auto-fix issues
bazel run lint --fix

# Format C++ and JavaScript
bazel run format

# Specific linters
python3 buildscripts/clang_tidy.py           # Clang-tidy
buildscripts/quickmongolint.py lint          # MongoDB-specific
buildscripts/errorcodes.py                   # Error codes
python buildscripts/eslint.py lint           # JavaScript
bash buildscripts/yamllinters.sh             # YAML
```

---

## C++ Code Style

### Naming Conventions

- **Types:** `TitleCase` (e.g., `MyClass`, `QueryPlanner`)
- **Functions/Variables:** `camelCase` (e.g., `myFunction`, `nodeCount`)
- **Namespaces:** `snake_case` (e.g., `my_namespace`)
- **Private members:** Leading underscore (e.g., `_privateMember`)
- **Constants:** `kConstantName` or `constantName`
- **Test access:** Suffix with `_forTest` or `ForTest`
- **Files:** `snake_case.cpp` (e.g., `query_planner.cpp`)

### Include Order

Organize includes into blocks separated by blank lines, sorted alphabetically within each block:

1. **Main header** (for .cpp files)
2. **First-party headers** (`"mongo/..."`)
3. **C++ stdlib headers** (`<vector>`, `<string>`)
4. **System C headers** (`<unistd.h>`)
5. **Third-party headers** (`<boost/...>`)

Example:

```cpp
#include "mongo/db/classy.h"

#include "mongo/db/db.h"
#include "mongo/util/concurrency/qlock.h"

#include <cstdio>
#include <string>

#include <unistd.h>

#include <boost/thread/thread.hpp>
```

### Formatting

- **Indentation:** 4 spaces (no tabs)
- **Line length:** 100 characters max
- **Braces:** Opening brace on same line
- **Include guards:** Use `#pragma once`
- **West const:** `const X x;` not `X const x;`
- **No C-style casts:** Use `static_cast`, `dynamic_cast`, etc.

### Best Practices

- Use `explicit` for single-argument constructors (unless conversion is desired)
- Use `override` for virtual functions (not `virtual` + `override`)
- Use `= default` for trivial special member functions
- Prefer RAII and smart pointers
- Use `StringData` instead of `std::string_view`
- **Classes vs Structs:** Use `class` for types with invariants, `struct` for simple data aggregation

### Documentation

```cpp
/** Single line doc. */
void easyFunction(int x);

/**
 * Multi-line documentation.
 * Use complete sentences.
 */
void complexFunction(int x);

int _privateMember;  ///< Inline doc with ///<
```

- Use `/**` or `///` for API docs
- Complete, grammatical sentences
- Descriptive verbs: "Calculates sum" not "Calculate sum"
- Avoid redundancy and unnecessary jargon

### TODOs

```cpp
// TODO(SERVER-12345): Description of what needs to be done.
```

---

## JavaScript Test Rules (jstests/)

**Critical practices to prevent flaky tests:**

1. **Deterministic outcomes** - Use explicit wait helpers (e.g., `waitForPrimary`)
2. **Unique namespaces** - Always use `jsTestName()` for database/collection names
3. **Retry transient errors** - Use helpers from `jstests/libs/auto_retry_transaction_in_sharding.js`
4. **Wrap commands** - Use `assert.commandWorked()`, `assert.commandFailed()`, etc.
5. **Wait after topology changes** - Use `waitForPrimary`, `awaitRSClientHosts` after stepdowns/reconfigs
6. **Direct assertions** - Verify specific properties, not indirect metrics like counts
7. **Namespace-specific failpoints** - Always specify namespace in failpoint configuration
8. **Add suite tags** - Tag tests appropriately (see `jstests/tags.md`)
9. **Clear test description** - Include top-level comment explaining test purpose
10. **Handle exceptions in assert.soon** - Use `assert.soonRetryOnNetworkErrors` or `assert.soonRetryOnAcceptableErrors`

---

## Key Directories

```
src/mongo/          # Main source code
  ├── base/         # Base utilities
  ├── bson/         # BSON implementation
  ├── db/           # Database server (mongod)
  ├── s/            # Sharding (mongos)
  ├── client/       # Client libraries
  ├── util/         # Utilities
  ├── dbtests/      # DB-level tests
  └── unittest/     # Unit test framework
jstests/            # JavaScript integration tests
buildscripts/       # Build and test scripts
bazel/              # Bazel build configuration
docs/               # Documentation
```

---

## Additional Resources

- Build: `docs/building.md`
- Testing: `docs/testing/README.md`, `docs/unit_test.md`
- Style: `docs/cpp_style.md`
- Linting: `docs/linting.md`
- Bazel: `bazel/docs/developer_workflow.md`
