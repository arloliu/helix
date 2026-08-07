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

- client construction: `client.go` → `NewClient`
- CQL client construction: `cql_client.go` → `NewCQLClient`
- CQL query execution: `cql_client.go` → `(*CQLClient).Query`
