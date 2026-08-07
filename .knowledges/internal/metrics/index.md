---
type: Unit
title: internal/metrics
description: Internal metrics helpers.
---

# Responsibility

Provides internal no-op metrics support.

# Boundary

Public metrics contracts belong to `types` and the VictoriaMetrics implementation belongs to `contrib/metrics/vm`.

# Entries

* None yet.

# Entry points

- no-op collector: `internal/metrics/nop.go` → `NewNopCollector`
