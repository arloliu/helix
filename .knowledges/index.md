---
okf_version: "0.2"
swept_at: 864ff2e
---

# Units

* [root](root/) - Public client orchestration and CQL execution behavior.
* [adapter/cql](adapter/cql/) - Driver-neutral CQL adapter contracts.
* [adapter/cql/v1](adapter/cql/v1/) - gocql v1 adapter.
* [adapter/cql/v2](adapter/cql/v2/) - Apache gocql-driver v2 adapter.
* [contrib/metrics/vm](contrib/metrics/vm/) - VictoriaMetrics collector integration.
* [internal/logging](internal/logging/) - Internal logging helpers.
* [internal/metrics](internal/metrics/) - Internal metrics helpers.
* [internal/typeutil](internal/typeutil/) - Internal type helpers.
* [mirror](mirror/) - Asynchronous migration mirror engine.
* [policy](policy/) - Read, write, and failover policies.
* [replay](replay/) - Failed-write replay queues and workers.
* [topology](topology/) - Cluster drain-state topology watchers.
* [types](types/) - Shared leaf-package contracts and errors.
