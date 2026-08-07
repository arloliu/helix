---
type: Unit
title: mirror
description: Asynchronous migration mirror engine.
---

# Responsibility

Owns the bounded in-memory queue and worker pool that mirror successful writes to a destination client.

# Boundary

The root package captures and dispatches writes to the engine; durable retry contracts belong to `types` and `replay`.

# Entries

* None yet.

# Entry points

- mirror engine construction: `mirror/engine.go` → `NewEngine`
