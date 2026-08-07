---
type: Unit
title: topology
description: Cluster drain-state topology watchers.
---

# Responsibility

Publishes and watches cluster drain state for routing and write suppression.

# Boundary

The root package applies topology updates; shared topology contracts belong to `types`.

# Entries

* None yet.

# Entry points

- local watcher: `topology/local.go` → `NewLocal`
- NATS watcher: `topology/nats.go` → `NewNATS`
