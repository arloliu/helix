---
type: Unit
title: contrib/metrics/vm
description: VictoriaMetrics collector integration.
---

# Responsibility

Implements Helix metrics collection and Prometheus exposition with VictoriaMetrics.

# Boundary

Metric contracts belong to `types`; client behavior that produces metrics belongs to the root package and feature units.

# Entries

* None yet.

# Entry points

- collector construction: `contrib/metrics/vm/vm.go` → `New`
