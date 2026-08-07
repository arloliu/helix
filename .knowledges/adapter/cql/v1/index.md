---
type: Unit
title: adapter/cql/v1
description: gocql v1 adapter.
---

# Responsibility

Wraps gocql v1 session, query, batch, and iterator types in the driver-neutral contracts.

# Boundary

The shared contracts belong to `adapter/cql`; public client behavior belongs to the root package.

# Entries

* None yet.

# Entry points

- session wrapper: `adapter/cql/v1/adapter.go` → `NewSession`
