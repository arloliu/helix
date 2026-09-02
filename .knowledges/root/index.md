---
type: Unit
title: root
description: Public client orchestration and CQL execution behavior.
---

# Responsibility

Owns the public Helix client, its configuration, and orchestration of reads and dual-cluster writes.

# Boundary

Driver adaptation belongs to `adapter/cql`; policy decisions belong to `policy`; durable replay belongs to `replay`; shared contracts belong to `types`.

# Entries

* [Replay outcome classification](/root/replay-outcome-classification.md) - how normal dual writes choose a caller result and replay work.

# Entry points

- CQL client construction: `wiring.go` → `NewCQLClient`
- CQL query execution: `query.go` → `(*CQLClient).Query`
- read routing and classification: `read_path.go` → `resolveReadTarget`, `classifyReadErr`
- dual-write orchestration: `write_path.go` → `executeDualWrite`
