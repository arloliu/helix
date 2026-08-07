---
type: Unit
title: policy
description: Read, write, and failover policies.
---

# Responsibility

Selects read targets, executes dual writes, and tracks failover state.

# Boundary

The root package applies policy outputs to CQL operations; shared policy interfaces belong to `types`.

# Entries

* None yet.

# Entry points

- read selection: `policy/read_strategy.go` → `NewStickyRead`
- dual-write execution: `policy/write_strategy.go` → `NewConcurrentDualWrite`
- failure tracking: `policy/failover_policy.go` → `NewActiveFailover`
