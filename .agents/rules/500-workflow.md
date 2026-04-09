# 500 - Development Workflow

## Before Commit
1. Run `make lint` — Fix all issues.
2. Run `make test` — All unit tests must pass with race detector.
3. If replay message types changed, run `make generate` and commit generated files.
4. Verify docs are updated if API changed.

## Git Conventions
- **Branches:** `feat/`, `fix/`, `docs/`, `chore/`, `test/`.
- **Commits:** Conventional format. Present tense. First line < 50 chars.
    - `feat: add adaptive dual-write strategy`
    - `fix: handle nil replayer on partial write failure`

## Code Review Checklist
- [ ] Correctness
- [ ] Dual-write error semantics preserved (nil if ≥1 cluster succeeds)
- [ ] Performance (no unnecessary allocs in hot paths)
- [ ] Test coverage for new code
- [ ] Docs updated for exported API changes
- [ ] No import cycles introduced (types/ is the leaf)
- [ ] Generated files committed if msgp types changed

## Make Targets Reference
```bash
make help              # Show all targets
make lint              # Run golangci-lint
make fmt               # Format code (gofmt + goimports)
make vet               # Run go vet
make test              # Unit + integration tests with race detector
make test-unit         # Unit tests only
make test-quick        # Unit tests without race detector (fast)
make test-integration  # Integration tests (requires Docker)
make test-all          # Unit + integration
make coverage          # Generate coverage report
make generate          # Run go generate (msgp, etc.)
make gomod-tidy        # Tidy go.mod/go.sum
make ci                # Full CI pipeline (lint + vet + test-all + coverage)
```
