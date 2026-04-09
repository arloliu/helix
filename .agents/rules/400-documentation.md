# 400 - Documentation Standards

## General
- **Godoc:** All exported symbols MUST have doc comments.
- **First Line:** Start with the symbol name. One-line summary.
- **README:** Keep updated with install/usage.

## Godoc Template (MANDATORY)

```go
// FunctionName one-line summary.
//
// Detailed description (optional but recommended).
//
// Parameters:
//   - param1: Description and constraints
//   - param2: Expected values
//
// Returns:
//   - Type: What it represents
//   - error: Conditions that cause errors
//
// Example:
//
//	result, err := FunctionName(input)
//	if err != nil { ... }
func FunctionName(param1 T1, param2 T2) (Result, error) { }
```

## Examples by Type

**Constructor:**
```go
// NewCQLClient creates a dual-cluster CQL client.
//
// Parameters:
//   - sessionA: Primary cluster session (must implement CQLSession)
//   - sessionB: Secondary cluster session (must implement CQLSession)
//   - opts: Functional options (e.g., WithReadStrategy, WithReplayer)
//
// Returns:
//   - *CQLClient: Ready-to-use dual-cluster client
//   - error: If configuration is invalid
func NewCQLClient(sessionA, sessionB CQLSession, opts ...Option) (*CQLClient, error) { }
```

**Method with Multiple Returns:**
```go
// Exec executes the query against both clusters concurrently.
//
// Returns nil if at least one cluster succeeds. Partial failures
// are automatically enqueued for replay if a Replayer is configured.
//
// Returns:
//   - error: nil on partial or full success; *types.DualClusterError if both clusters fail
func (q *Query) Exec() error { }
```

## Omit When Appropriate
- No params → Omit Parameters section.
- No returns → Omit Returns section.
- Simple getters → Minimal doc is OK.
