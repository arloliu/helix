---
type: Unit
title: types
description: Shared leaf-package contracts and errors.
---

# Responsibility

Defines contracts, identifiers, errors, payloads, and metrics shared across Helix packages without importing them.

# Boundary

Feature behavior is implemented by the root package and its feature units.

# Entries

* None yet.

# Entry points

- shared contracts: `types/types.go` → `ClusterID`
- cluster events: `types/cluster_event.go` → `ClusterEvent`
