# Helix — Agent Rules Index

> **CONTEXT**: This is a high-availability dual-database client library (`github.com/arloliu/helix`).
> **ACTION**: Read the files below in order before beginning work.

## Rule Index

### 0. Working Principles
- **[050-principles.md](050-principles.md)**
  *Behavioral guidelines: surface uncertainty, minimize changes, verify with code, define success criteria.*

### 1. Core Directives
- **[100-overview.md](100-overview.md)**
  *Identity, project structure, architecture notes, dependencies, and prime directives.*
- **[150-memex.md](150-memex.md)**
  *Durable implementation-mechanics knowledge bundle and its use.*

### 2. Standards
- **[200-coding-style.md](200-coding-style.md)**
  *Go idioms, error handling, file layout, naming, loop patterns.*
- **[300-testing.md](300-testing.md)**
  *Unit/integration/simulation organization, **CRITICAL** async testing rules, make targets.*
- **[400-documentation.md](400-documentation.md)**
  *Mandatory Godoc format with Helix-specific examples.*

### 3. Workflow & Safety
- **[500-workflow.md](500-workflow.md)**
  *Git conventions, pre-commit checks, make targets reference.*
- **[600-perf-sec.md](600-perf-sec.md)**
  *Performance optimizations (dual-write paths, allocations) and security boundaries.*
- **[700-lint-after-write.md](700-lint-after-write.md)**
  *Automated linting workflow and common fixes.*

---
*Rules are split for readability and context optimization.*
