# GitHub Copilot Instructions

This file provides context and guidelines for GitHub Copilot when working on the `helix` project.

## Project Overview

Helix is a high-availability dual-database client library designed to support "Shared Nothing" architecture. It provides robustness through active-active dual writes, sticky reads, and asynchronous reconciliation for independent Cassandra clusters.

**Key Features:**
- **Dual Active-Active Writes:** Concurrent writes to two independent clusters for maximum availability
- **Sticky Read Routing:** Per-client sticky reads to maximize cache hits across clusters
- **Active Failover:** Immediate failover to secondary cluster on read failures
- **Replay System:** Asynchronous reconciliation via in-memory queue (Phase 1) or NATS JetStream (Phase N)
- **Drop-in Replacement:** Interface-based design mirrors `gocql` API for minimal migration effort
- **QoS & Priority:** Support for high-priority (critical) and low-priority (best-effort) writes

**Project Details:**
- **Language:** Go >=1.25.0
- **Module:** github.com/arloliu/helix
- **Project Structure:**
  ```
  helix/                        # Root = main public package
  ├── doc.go                    # Package documentation
  ├── client.go                 # Common definitions & interfaces
  ├── cql_client.go             # HelixCQLClient implementation
  ├── sql_client.go             # HelixSQLClient implementation
  ├── *_test.go                 # Unit tests
  ├── types/                    # Shared types & errors (leaf package)
  │   └── types.go              # ClusterID, Consistency, BatchType, errors
  ├── policy/                   # Load balancing & failover policies
  │   ├── read_strategy.go
  │   ├── write_strategy.go
  │   ├── failover_policy.go
  │   └── *_test.go
  ├── replay/                   # Replay system
  │   ├── replayer.go           # Interface
  │   ├── memory.go             # In-memory implementation
  │   ├── nats.go               # NATS JetStream implementation (Phase N)
  │   └── *_test.go
  ├── adapter/                  # Driver adapters
  │   ├── cql/                  # CQL adapter interfaces
  │   │   ├── v1/               # gocql v1 adapter
  │   │   └── v2/               # gocql v2 adapter
  │   └── sql/                  # database/sql adapter
  ├── internal/                 # Private implementation
  │   ├── orchestrator/         # Dual-write orchestration logic
  │   └── circuit/              # Circuit breaker implementation
  ├── test/                     # Integration tests
  │   ├── integration/          # End-to-end scenarios
  │   └── testutil/             # Test utilities
  ├── examples/                 # Example programs
  │   ├── basic/
  │   ├── failover/
  │   └── custom-strategy/
  ├── docs/                     # Design documentation
  │   ├── high_level_design.md
  │   ├── helix_lib_design_draft.md
  │   └── design_plan.md
  └── .github/
      └── copilot-instructions.md
  ```

## Coding Standards & Conventions

### ⚠️ CRITICAL: Always Run Linter After Code Changes

**After modifying ANY code, ALWAYS run the linter before considering the task complete:**

```bash
make lint
```

This is a **mandatory step** - do not skip it! The linter catches:
- Import issues and unused imports
- Type errors and interface mismatches
- Code style violations
- Security issues
- Performance anti-patterns

**If linter fails:** Fix all reported issues before moving on. The CI pipeline will reject code that doesn't pass linting.

### Go Style Guidelines
- Follow the [official Go style guide](https://go.dev/doc/effective_go)
- Use `goimports` for formatting and import management
- Use `golangci-lint` for comprehensive code quality checks
- Use `any` instead of `interface{}` for empty interfaces
- Use `slices` and `maps` packages from the standard library for common operations
- Use `sync` package for synchronization primitives
- Prefer atomic operations from `sync/atomic` for simple counters and flags
- Always use `errors.New` if the error message is static without formatting needed
- Use `fmt.Errorf` with `%w` verb for wrapping errors with context
- Prefer `errors.Is` and `errors.As` for error handling
- Use `context` package for request-scoped values, cancellation, and timeouts
- **Compile-time interface assertions:** Use strategically to avoid import cycles
  - Pattern: `var _ InterfaceName = (*ConcreteType)(nil)`
  - **Use in:** `internal/*` packages only (safe to import root package)
  - **DO NOT use in:** Public subpackages like `policy/`, `replay/`, `adapter/` (causes import cycles)
  - Place immediately after type definition
  - Comment format: `// Compile-time assertion that TypeName implements InterfaceName.`
  - Benefits: Compile-time safety, documentation, refactoring protection
  - Example: `var _ helix.CQLSession = (*v1Adapter)(nil)` in `adapter/cql/v1/session.go`
  - **Alternative:** Add interface tests in `_test.go` files for public subpackages:
    ```go
    func TestImplementsInterface(t *testing.T) {
        var _ helix.ReadStrategy = (*StickyRead)(nil)
    }
    ```
- Follow Go naming conventions:
  - Package names: lowercase, short, descriptive
  - Functions: CamelCase (exported) or camelCase (unexported)
  - Variables: camelCase
  - Constants: CamelCase for package-level constants
  - Receiver names: short and consistent (enforced by receiver-naming rule)

### File Content Order

**All Go source files MUST follow this declaration order:**

| Order | Code Element | Convention/Reason |
|-------|--------------|-------------------|
| 1 | **Package declaration** | `package name` |
| 2 | **Imports** | Grouped: standard library, external, internal |
| 3 | **Constants** (`const`) | Grouped together, exported first |
| 4 | **Variables** (`var`) | Grouped together, exported first |
| 5 | **Types** (`type`) | Structs, interfaces, custom types. Exported first |
| 5.5 | **Interface assertions** | `var _ Interface = (*Type)(nil)` immediately after type (internal packages ONLY) |
| 6 | **Factory Functions** | `NewType() *Type` immediately after type/assertion |
| 7 | **Exported Functions** | Public standalone functions (not methods) |
| 8 | **Unexported Functions** | Private helper functions (not methods) |
| 9 | **Exported Methods** | Methods on types `(t *Type) Method()`. Group by type |
| 10 | **Unexported Methods** | Private methods `(t *Type) helper()`. Group by type |

**Example Structure (internal package):**

```go
package orchestrator

import (
    "context"
    "github.com/arloliu/helix"
)

// DualWriter handles concurrent writes to two clusters.
type DualWriter struct {
    sessionA helix.CQLSession
    sessionB helix.CQLSession
    replayer helix.Replayer
}

// Compile-time assertion that DualWriter implements WriteStrategy.
var _ helix.WriteStrategy = (*DualWriter)(nil)

// NewDualWriter creates a new dual-write orchestrator.
func NewDualWriter(sessionA, sessionB helix.CQLSession, replayer helix.Replayer) *DualWriter {
    // implementation
}

// Execute performs concurrent dual write to both clusters.
func (w *DualWriter) Execute(ctx context.Context, query string, args ...interface{}) error {
    // implementation
}
```

**Key Rules:**
- ✅ Group related items together (all constants, all types, all methods for same receiver)
- ✅ Exported items come before unexported items within each category
- ✅ **Interface assertions come immediately after type definition (internal packages ONLY)**
- ✅ Factory functions (`NewX`) come immediately after the type/assertion
- ✅ Methods are grouped by receiver type, not alphabetically
- ✅ Maintain logical grouping over strict alphabetical ordering

### Loop Patterns (forlooprange rule)
- Use `for i := range slice` when you need the index: `for i := range items { process(i, items[i]) }`
- Use `for range slice` when you don't need the index: `for range items { doSomething() }`
- Use `for b.Loop()` in benchmarks (Go 1.24+): `for b.Loop() { benchmarkedCode() }`
- Use `for range N` (Go 1.22+) for simple iteration: `for range 10 { repeat() }`
- **Key point:** If you're not using the index variable `i`, don't declare it

### Code Organization
- Keep functions small and focused (max 100 lines, prefer under 50)
- Function complexity should not exceed 22 (enforced by cyclop linter)
- Package average complexity should stay under 15.0
- Use meaningful variable and function names
- Group related functionality in the same package
- Separate concerns using interfaces
- Use dependency injection for better testability
- Avoid naked returns in functions longer than 40 lines
- Use struct field tags for marshaling/unmarshaling (enforced by musttag)

### Error Handling
- Always handle errors explicitly (enforced by errcheck)
- Check type assertions with comma ok idiom (check-type-assertions: true)
- Use the standard `error` interface
- Wrap errors with context using `fmt.Errorf` with %w verb for error wrapping
- Return errors as the last return value
- Use early returns to reduce nesting
- Prefix sentinel errors with "Err" and suffix error types with "Error" (errname linter)
- Handle specific error wrapping scenarios properly (errorlint)

Example:
[FILL IN WITH A CODE EXAMPLE IF NEEDED]

### Testing Guidelines

**Ground Rules**
- DON'T USE emojis in test log messages!
- DON'T USE emojis in test log messages!
- DON'T USE emojis in test log messages!

**Test Organization** (Hybrid Approach):
- **Unit tests**: Co-located with implementation (`*_test.go`)
- **Integration tests**: Dedicated directory (`test/integration/`)
- See `docs/design/06-implementation/test-organization.md` for complete details

**Unit Test Guidelines:**
- Write unit tests for all public functions
- **Use table-driven tests ONLY when you have multiple test cases**
- **Avoid over-engineering:** Don't use table-driven structure for single test cases - write simple, direct tests instead
- Place tests in `_test.go` files in the same package
- Use the standard `testing` package
- Use `t.Context()` for context management in tests
- Use `b.Loop()` (introduced in Go 1.24) for benchmarks
- Use `testify` for assertions and mocking
- Mock external dependencies for isolated tests
- Test edge cases and error scenarios
- Aim for high test coverage (>80%)
- Use meaningful test names that describe the scenario
- Use `t.Setenv()` instead of `os.Setenv()` in tests (tenv linter)
- Use `t.Parallel()` appropriately (tparallel linter)
- Ensure examples are testable with expected output (testableexamples linter)
- Test files are excluded from certain linters (bodyclose, dupl, gosec, noctx)

**Testing Asynchronous State Transitions (CRITICAL):**

When testing state machines or asynchronous systems, **NEVER use polling or hardcoded `time.Sleep()` to wait for intermediate states**. This causes flaky tests because fast transitions may complete before polling begins.

**❌ BAD - Polling for intermediate states (FLAKY):**
```go
// DON'T DO THIS - Will fail intermittently on fast machines
stateMachine.EnterScaling(ctx, "test", 10*time.Millisecond)
time.Sleep(15 * time.Millisecond)  // Hope we catch the Rebalancing state
require.Equal(t, StateRebalancing, stateMachine.GetState()) // FLAKY!
```

**✅ GOOD - Event-driven state collection (ROBUST):**
```go
// Use a state transition collector that subscribes to ALL state changes
collector := newStateTransitionCollector(t, stateMachine)
defer collector.Stop()

// Trigger the state transitions
stateMachine.EnterScaling(ctx, "test", 50*time.Millisecond)

// Wait for final state
require.True(t, collector.WaitForState(StateIdle, 2*time.Second))

// Verify the complete transition sequence
collector.RequireContains(StateScaling)
collector.RequireContains(StateRebalancing)
collector.RequireLastState(StateIdle)
```

**Why This Approach Works:**
1. **Subscribe BEFORE triggering** - Captures all states from the start
2. **Event-driven** - Never misses fast transitions regardless of timing
3. **Complete history** - Can verify entire state sequence, not just current state
4. **Deterministic** - No timing dependencies or race conditions
5. **Debuggable** - Full state history available when assertions fail

**Implementation Pattern:**
Create a helper that:
**Integration Test Guidelines:**
- Place in `test/integration/` directory
- Use `t.Context()` for context management in tests
- Package name: `integration_test`
- Always include `testing.Short()` guard
- Use embedded NATS via `testutil.StartEmbeddedNATS(t)` (for replay tests)
- Use test containers or embedded Cassandra for database tests
- Test cross-component scenarios (dual writes, failover, replay reconciliation, etc.)
- Clean up resources with `defer`
- Test partial failure scenarios (one cluster fails, replay kicks in)

**Running Tests:**
```bash
make test-unit          # Fast unit tests only
make test-integration   # Integration tests only
make test-all          # Both unit + integration
make test              # Unit tests with race detector
```bash
make test-unit          # Fast unit tests only
make test-integration   # Integration tests only
make test-all          # Both unit + integration
make test              # Unit tests with race detector
```

**Table-driven test (use when you have multiple test cases):**
```go
func TestFunctionName(t *testing.T) {
    tests := []struct {
        name     string
        input    InputType
        expected ExpectedType
        wantErr  bool
    }{
        {
            name:     "valid input",
            input:    validInput,
            expected: expectedOutput,
            wantErr:  false,
        },
        {
            name:     "invalid input",
            input:    invalidInput,
            expected: nil,
            wantErr:  true,
        },
        // more test cases...
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result, err := FunctionName(tt.input)
            if (err != nil) != tt.wantErr {
                t.Errorf("FunctionName() error = %v, wantErr %v", err, tt.wantErr)
                return
            }
            if !reflect.DeepEqual(result, tt.expected) {
                t.Errorf("FunctionName() = %v, want %v", result, tt.expected)
            }
        })
    }
}
```

**Simple test (use for single test cases):**
```go
func TestGetLittleEndianEngine(t *testing.T) {
    engine := GetLittleEndianEngine()

    require.Equal(t, binary.LittleEndian, engine)
    require.Implements(t, (*EndianEngine)(nil), engine)

    // Test actual behavior
    var testValue uint16 = 0x0102
    bytes := make([]byte, 2)
    engine.PutUint16(bytes, testValue)
    require.Equal(t, byte(0x02), bytes[0]) // LSB first
    require.Equal(t, byte(0x01), bytes[1]) // MSB second
}
```

### Best Practices
- Prefer standard library when possible
- Pre-allocate slices when size is known
- Pre-allocate maps when size is known
- Try to pre-allocate as much as possible
- Use dependency injection for testability
- Document all exported items
- Validate all input data
- **Performance-focused coding:**
  - Prefer branchless code in critical/hot path functions
  - Write code aggressively for inlining potential (small, simple functions)
  - Avoid over-using interfaces when performance is critical (interface calls have overhead)
  - Avoid unnecessary pointer creation for small structs - pass by value unless you need pointer receivers

## Linting Rules & Quality Standards

This project uses a comprehensive `golangci-lint` configuration with strict rules:

### Code Quality Rules
- **Function length**: Maximum 100 lines (revive), prefer shorter functions
- **Cyclomatic complexity**: Maximum 25 per function (cyclop)
- **Package complexity**: Average should be under 15.0
- **Naked returns**: Allowed only in functions ≤40 lines (nakedret)
- **Context handling**: Always pass context as first parameter (context-as-argument)
- **Import shadowing**: Avoid shadowing package names (import-shadowing)

### Security & Safety
- **Type assertions**: Always use comma ok idiom: `val, ok := x.(Type)`
- **SQL operations**: Always close `sql.Rows` and `sql.Stmt` (sqlclosecheck, rowserrcheck)
- **HTTP responses**: Always close response bodies (bodyclose)
- **Nil checks**: Avoid returning nil error with invalid value (nilnil)
- **Unicode safety**: Check for dangerous unicode sequences (bidichk)

### Performance & Memory
- **Pre-allocation**: Consider pre-allocating slices when size is known (prealloc)
- **Unnecessary conversions**: Remove unnecessary type conversions (unconvert)
- **Wasted assignments**: Avoid assignments that are never used (wastedassign)
- **Duration arithmetic**: Be careful with duration multiplications (durationcheck)

### Code Style
- **Variable naming**: Follow Go conventions, avoid stuttering
- **Receiver naming**: Use consistent, short receiver names
- **Comment spacing**: Use proper spacing in comments (comment-spacings)
- **Standard library**: Use standard library variables/constants when available (usestdlibvars)
- **Printf functions**: Name printf-like functions with 'f' suffix (goprintffuncname)

## Documentation Standards

### Code Documentation
- Use clear and concise comments
- Document all exported functions, types, and constants
- Use Go doc comments (start with the name of the item being documented)
- Include examples in documentation when helpful

### Godoc Format for Methods and Functions

**All exported functions and methods MUST follow this standardized format:**

```go
// FunctionName provides a brief one-line description of what the function does.
//
// Optional: More detailed description explaining the purpose, behavior, and usage.
// This section can span multiple paragraphs and include implementation details,
// algorithm descriptions, or other relevant context.
//
// Parameters:
//   - param1: Description of the first parameter and its constraints
//   - param2: Description of the second parameter and its expected values
//   - paramN: Additional parameters with their descriptions
//
// Returns:
//   - returnType1: Description of what the first return value represents
//   - returnType2: Description of the second return value (e.g., error conditions)
//
// Example:
//
//	encoder := NewEncoder()
//	data := []byte("example")
//	result, err := encoder.Process(data)
//	if err != nil {
//	    log.Fatal(err)
//	}
func FunctionName(param1 Type1, param2 Type2) (returnType1, error) {
    // implementation
}
```

**Key Requirements:**
1. **First line:** Brief description starting with the function/method name
2. **Blank line:** Separates the summary from detailed description
3. **Detailed description:** Optional but recommended for complex functions
4. **Blank line:** Before Parameters section
5. **Parameters section:** List all parameters with clear descriptions
   - Use bullet list format with `-` for each parameter
   - Describe constraints, expected values, and special cases
   - For constructors, mention what engine/configuration parameters do
6. **Blank line:** Before Returns section
7. **Returns section:** List all return values with descriptions
   - Describe what each return value represents
   - Explain error conditions for error returns
   - For iterators, mention what the iterator yields
8. **Example section:** Optional but highly recommended
   - Show realistic usage scenarios
   - Include error handling when applicable
   - Use proper indentation (tab character)

**Examples by Function Type:**

**Constructor Function:**
```go
// NewTimestampEncoder creates a new timestamp encoder using the specified endian engine.
//
// The encoder uses delta-of-delta compression to minimize storage space for sequential
// timestamps. This provides 60-87% space savings compared to raw encoding for regular
// interval data.
//
// Parameters:
//   - engine: Endian engine for byte order (typically little-endian)
//
// Returns:
//   - *TimestampEncoder: A new encoder instance ready for timestamp encoding
//
// Example:
//
//	encoder := NewTimestampEncoder(endian.GetLittleEndianEngine())
//	encoder.Write(time.Now().UnixMicro())
//	data := encoder.Bytes()
func NewTimestampEncoder(engine endian.EndianEngine) *TimestampEncoder {
    // implementation
}
```

**Method with Multiple Parameters:**
```go
// Write encodes a single timestamp using delta-of-delta compression.
//
// The timestamp is encoded based on its position:
//   - First timestamp: Full varint-encoded microseconds (5-9 bytes)
//   - Second timestamp: Delta from first (1-9 bytes)
//   - Subsequent timestamps: Delta-of-delta (1-9 bytes)
//
// Parameters:
//   - timestampUs: Timestamp in microseconds since Unix epoch
func (e *TimestampEncoder) Write(timestampUs int64) {
    // implementation
}
```

**Method Returning Values:**
```go
// Bytes returns the encoded byte slice containing all written timestamps.
//
// The returned slice is valid until the next call to Write, WriteSlice, or Reset.
// The caller must not modify the returned slice as it references the internal buffer.
//
// Returns:
//   - []byte: Encoded byte slice (empty if no timestamps written since last Reset)
func (e *TimestampEncoder) Bytes() []byte {
    // implementation
}
```

**Method Returning Multiple Values:**
```go
// At retrieves the timestamp at the specified index from the encoded data.
//
// This method provides efficient random access by decoding only up to the
// target index. For sequential access, use All() iterator instead.
//
// Parameters:
//   - data: Encoded byte slice from TimestampEncoder.Bytes()
//   - index: Zero-based index of the timestamp to retrieve
//   - count: Total number of timestamps in the encoded data
//
// Returns:
//   - int64: The timestamp at the specified index (microseconds since Unix epoch)
//   - bool: true if the index exists and was successfully decoded, false otherwise
//
// Example:
//
//	decoder := NewTimestampDecoder()
//	timestamp, ok := decoder.At(encodedData, 5, 10)
//	if ok {
//	    fmt.Printf("Timestamp at index 5: %v\n", time.UnixMicro(timestamp))
//	}
func (d TimestampDecoder) At(data []byte, index int, count int) (int64, bool) {
    // implementation
}
```

**Method Returning Iterator:**
```go
// All returns an iterator that yields all timestamps from the encoded data.
//
// This method provides zero-allocation iteration using Go's iter.Seq pattern.
// The iterator processes data sequentially without creating intermediate slices.
//
// Parameters:
//   - data: Encoded byte slice from TimestampEncoder.Bytes()
//   - count: Expected number of timestamps (used for optimization)
//
// Returns:
//   - iter.Seq[int64]: Iterator yielding decoded timestamps (microseconds since Unix epoch)
//
// Example:
//
//	decoder := NewTimestampDecoder()
//	for ts := range decoder.All(encodedData, expectedCount) {
//	    fmt.Printf("Timestamp: %v\n", time.UnixMicro(ts))
//	    if someCondition {
//	        break // Can break early if needed
//	    }
//	}
func (d TimestampDecoder) All(data []byte, count int) iter.Seq[int64] {
    // implementation
}
```

**Common Patterns:**
- **No parameters:** Omit Parameters section (e.g., `Bytes()`, `Len()`, `Size()`)
- **No return values:** Omit Returns section (e.g., `Write()`, `Reset()`)
- **Error returns:** Always describe error conditions in Returns section
- **Simple getters:** Can have minimal documentation if self-explanatory
- **Complex algorithms:** Include algorithm description before Parameters section

**Reference Implementation:**
See `encoding/metric_names.go` for the canonical implementation of this format.

### README and Documentation
- Keep README.md up to date with installation and usage instructions
- Document API endpoints if this is a web service
- Include configuration examples
- Provide troubleshooting guides for common issues

## Dependencies

### Dependency Management
- Use Go modules for dependency management
- Keep dependencies minimal and well-maintained
- Prefer standard library when possible
- Pin major versions and update dependencies regularly
### Preferred Libraries
- **Testing:**
  - `github.com/stretchr/testify` for assertions and mocking
  - `github.com/testcontainers/testcontainers-go` for CQL integration tests
  - `github.com/mattn/go-sqlite3` for SQL integration tests (in-memory mode)
- **Cassandra Drivers:**
  - `github.com/gocql/gocql` v1.7.0+ (v1 adapter)
  - `github.com/apache/cassandra-gocql-driver/v2` v2.0.0+ (v2 adapter)
- **NATS:** `github.com/nats-io/nats.go` for JetStream replay (Phase N)
- **Logging:** Interface-based (user provides implementation)
- **Context:** Standard library `context` for cancellation and timeouts

## Security & Performance Guidelines

### Performance Guidelines
- Profile code for performance bottlenecks
- Use goroutines for concurrent dual writes
- Implement proper context handling for timeouts and cancellation
- **Timeout Isolation:** Each cluster operation must have independent timeout contexts
- Consider memory usage and garbage collection impact
- Avoid blocking on slow clusters (concurrent writes with timeout)
- Pre-allocate buffers for replay payloads when possible
- Minimize allocations in hot paths (write/read operations)

### Performance Guidelines
- Profile code for performance bottlenecks
- Use goroutines for concurrent operations when appropriate
- Implement proper context handling for timeouts and cancellation
- Consider memory usage and garbage collection impact
- Use connection pooling for database operations
- Cache expensive operations when possible

## Development Workflow

### Branch Naming
- `feat/` - New features
- `fix/` - Bug fixes
- `docs/` - Documentation updates
- `chore/` - Maintenance tasks
- `test/` - Test-related changes

### Commit Messages
- Use conventional commit format
- Start with a verb in present tense (add, fix, update, remove)
- Keep the first line under 50 characters
- Include detailed description when necessary

### Code Review Guidelines
- Review for correctness, performance, and maintainability
- Check test coverage for new code
- Ensure documentation is updated
- Verify error handling is appropriate

## Environment-Specific Notes

### Development
- Use `go run` for quick testing
- Use `go build` for local builds
- Set up proper IDE configuration for Go development

### Production
- Use proper logging levels
- Implement health checks
- Set up monitoring and alerting
- Use graceful shutdown for services

---

**Note:** Update this file as the project evolves to keep Copilot's suggestions relevant and helpful.