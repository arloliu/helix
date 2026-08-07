---
type: Bundle Conventions
title: Helix memex conventions
unit: go-package
unit_globs: [".", "adapter/cql", "adapter/cql/v1", "adapter/cql/v2", "contrib/metrics/vm", "internal/logging", "internal/metrics", "internal/typeutil", "mirror", "policy", "replay", "topology", "types"]
exclude: [examples, test, docs, vendor, testdata, .git, .agents, .knowledges]
scope: mechanics
pointer_style: symbol
tracked: true
hook: .agents/rules/150-memex.md
propose_threshold_loc: 150
declined: []
---

# What earns an entry

An entry records one implementation mechanic below Helix's public contract: a control or data flow, state transition, invariant, failure mode, or ownership boundary whose derivation would cost more source reading than the entry. Public API signatures and normal usage stay in Godoc and README; decisions and rationale stay in design documents.

# What does not

Examples, tests, benchmarks, generated code, public API inventories, and ordinary package descriptions do not earn entries on their own. They may be sources when needed to establish a mechanic, but the entry remains about the production behavior.
