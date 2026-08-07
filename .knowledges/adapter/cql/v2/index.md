---
type: Unit
title: adapter/cql/v2
description: Apache gocql-driver v2 adapter.
---

# Responsibility

Wraps Apache Cassandra gocql-driver v2 types in the driver-neutral contracts.

# Boundary

The shared contracts belong to `adapter/cql`; public client behavior belongs to the root package.

# Entries

* None yet.

# Entry points

- session wrapper: `adapter/cql/v2/adapter.go` → `NewSession`
