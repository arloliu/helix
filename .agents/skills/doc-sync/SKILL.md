---
name: doc-sync
description: Audits and updates docs/ files, README.md and public-package Godoc comments to match the current API, exported symbols, option names, and behavioral contracts in the codebase. Does not invent changes — only syncs what is verifiably out of date.
---

# Doc-Sync Skill

**Assumed Role:** Technical writer / library maintainer who owns the documentation contract.

**Goal:** Ensure every file in `docs/`, `README.md`, and every public-package Godoc comment accurately reflects the *current* source code. Flag discrepancies, then apply targeted fixes. Do not rewrite prose for style — only correct factual errors, stale signatures, missing symbols, and outdated behavioral descriptions.

---

## Scope

By default, audit all docs and public packages. You may narrow scope:

- **docs only:** `docs/` directory (all `.md` files)
- **Godoc only:** exported symbols in `helix/` (root), `adapter/cql/v1/`, `adapter/cql/v2/`, `policy/`, `replay/`, `topology/`, `types/`
- **Specific doc:** e.g., `docs/replay-system.md`, `docs/strategy-policy.md`
- **Specific package:** e.g., `policy/`, `replay/`

---

## Phase 1 — Inventory

### 1a. Collect ground truth from source

For each public package in scope, read:
- All `.go` files (not `_test.go`) to extract exported symbols: types, functions, methods, constants, fields
- Functional option functions (`WithX`) and their parameter types
- Sentinel errors in `types/` and re-exports in root package
- `doc.go` package-level comment

Do **not** read `internal/` packages as facts about the public API — only read them if a public type delegates to an internal one and you need to verify behavioral claims.

### 1b. Collect claims from docs

For each doc file in scope, extract:
- Every code block (` ```go `) — function signatures, type definitions, usage examples
- Every bullet/table row that names an exported symbol, option, error, or configuration field
- Version strings and "Last Updated" dates

---

## Phase 2 — Diff: Docs vs. Source

Compare what docs claim against what source defines. Produce a findings list in this format:

```
[FILE] docs/replay-system.md
  STALE  Line 35: NewNATSReplayer signature missing `opts ...Option` parameter
  STALE  Line 120: WithReplayWorker — option no longer exists; replaced by WithWorker
  MISSING Line 210: NATSReplayer.Stop not documented
  OK     Line 300: MemoryReplayer description matches source

[FILE] policy/doc.go
  STALE  Package summary mentions "pull-based" but AdaptiveDualWrite uses push mode
  MISSING WithAdaptiveThreshold option not mentioned
```

Categories:
- **STALE** — doc describes something that exists but differently (wrong signature, wrong field name, wrong behavior)
- **MISSING** — source has a symbol/option/error the doc does not mention
- **PHANTOM** — doc describes something that no longer exists in source
- **OK** — accurate; no change needed

---

## Phase 3 — Fix

For each finding that is STALE, MISSING, or PHANTOM:

1. **Read the relevant source file** to confirm the current truth before editing.
2. **Apply a targeted edit** — change only the affected lines. Do not reformat surrounding prose.
3. **Preserve intent** — if the old text explained *why* something works, keep that explanation and only correct the *what*.

### Fix rules

- **Signatures:** Match the source exactly. Parameter names, types, and variadic `...` must be verbatim.
- **Option names:** Correct `WithX` names to match source. If an option was renamed, update all occurrences.
- **Removed symbols:** Delete doc sections for PHANTOM symbols. If a replacement exists, add a one-line "renamed to `NewName`" note where the old section was.
- **New symbols (MISSING):** Add a stub entry. For `docs/`-style files, follow the existing section format. For Godoc, add a minimal compliant comment per `400-documentation.md`.
- **Dates/versions:** Update "Last Updated" fields to today's date. Do not change semantic version strings unless the `go.mod` module path or a version constant in source has changed.
- **Do not:** change prose tone, restructure sections, add new examples beyond what the source makes self-evident, or modify files in `docs/design/`.

---

## Phase 4 — Report

After all fixes are applied, output a concise summary:

```
## Doc-Sync Report

### Changes Applied
- docs/replay-system.md: corrected 2 signatures, removed 1 phantom option, added 1 missing entry
- policy/doc.go: updated package summary (AdaptiveDualWrite mode)
- docs/strategy-policy.md: added WithAdaptiveThreshold to options table

### Still Needs Attention (manual review required)
- docs/adaptive-dual-write.md line 88: describes internal latency tracking — verify against source if behavior changed
- README.md line 210: example uses a deleted option constant; replacement unclear — needs author input

### No Changes Needed
- docs/strategy-policy.md — accurate
- types/ Godoc — accurate
```

Mark items as "needs attention" (rather than auto-fixing) when:
- The correct fix requires understanding *intent* that is not derivable from source alone
- The discrepancy is in a design doc or implementation plan
- Multiple plausible fixes exist and the wrong choice could be misleading

---

## Constraints

- **Never fabricate API behavior.** If source is ambiguous, mark the finding as "needs attention" and quote both the doc claim and the source code.
- **Minimal diffs.** Edit only the lines that are wrong. Do not touch accurate adjacent content.
- **Internal packages are off-limits** for doc claims. If a doc says "internally uses X" and X is in `internal/`, do not update that claim based on internal code — flag it as "needs attention."
- **Follow `400-documentation.md`** for any Godoc you write or modify.
