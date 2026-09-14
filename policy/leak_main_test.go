package policy

import (
	"testing"

	"github.com/arloliu/helix/test/testutil/leak"
)

// TestMain fails this package's test binary if a goroutine started during
// the run is still alive once the tests finish.
// See [leak] for why the check is polled rather than subscribed to.
func TestMain(m *testing.M) {
	leak.TestMain(m)
}
