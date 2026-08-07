---
type: Unit
title: replay
description: Failed-write replay queues and workers.
---

# Responsibility

Owns bounded memory and JetStream replay queues plus workers that retry partial writes.

# Boundary

The root package decides when to enqueue a partial write; replay payload contracts belong to `types`.

# Entries

* None yet.

# Entry points

- memory queue construction: `replay/memory.go` → `NewMemoryReplayer`
- JetStream queue construction: `replay/nats.go` → `NewNATSReplayer`
- replay worker: `replay/worker.go` → `NewWorker`
