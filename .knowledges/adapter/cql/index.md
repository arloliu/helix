---
type: Unit
title: adapter/cql
description: Driver-neutral CQL adapter contracts.
---

# Responsibility

Defines the CQL session, query, batch, and iterator interfaces used by Helix.

# Boundary

Concrete driver wrapping belongs to `adapter/cql/v1` and `adapter/cql/v2`; orchestration belongs to the root package.

# Entries

* None yet.

# Entry points

- adapter contracts: `adapter/cql/adapter.go` → `Session`
