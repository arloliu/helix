---
trigger: always_on
glob: "**/*.go"
description: Run linter after modifying Go files
---

# Lint After Write

After modifying any `.go` file:

1. **Run:** `make lint`
2. **Fix:** All reported issues before committing.
3. **Re-run:** Until clean.
