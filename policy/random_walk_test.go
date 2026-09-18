package policy

import (
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

// Environment variables that steer the seeded random walks in this package.
//
//	HELIX_WALK_SEED=42 go test ./policy -run 'RandomWalk' -v   # one chosen seed
//	HELIX_WALK_STEPS=100000 go test ./policy -run 'RandomWalk' # longer walks
const (
	walkSeedEnv  = "HELIX_WALK_SEED"
	walkStepsEnv = "HELIX_WALK_STEPS"
)

const (
	defaultWalkSteps = 1000
	// walkTraceTail is how many of the steps leading up to a failure are
	// logged; the seed replays the rest.
	walkTraceTail = 80
)

// unknownCluster is an ID no policy knows, to exercise the ignore paths.
const unknownCluster = types.ClusterID("C")

// defaultWalkSeeds run on every test invocation; the fixed list keeps a
// failure in CI reproducible from the subtest name alone.
var defaultWalkSeeds = []uint64{1, 2, 3, 5, 8, 13, 21, 34}

// walk is one seeded random walk: the random source, the step counter and
// the trace of what each step did, logged when an assertion fails.
type walk struct {
	t     *testing.T
	rng   *rand.Rand
	step  int
	trace []string
}

// walkOp is one operation a walk may draw, with its relative weight.
type walkOp struct {
	weight int
	run    func()
}

// walkMachine is a policy under test plus its reference state. step draws
// and applies one operation and checks the invariants; finish runs once
// after the last step.
type walkMachine interface {
	step()
	finish()
}

// runWalk runs one walk per seed as a subtest named seed=N, so the failing
// subtest name is enough to replay it.
func runWalk(t *testing.T, newMachine func(w *walk) walkMachine) {
	t.Helper()

	seeds, steps := walkParams(t)
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			w := &walk{t: t, rng: rand.New(rand.NewPCG(seed, 0))}
			defer w.dumpOnFailure(seed)

			m := newMachine(w)
			for w.step = 1; w.step <= steps; w.step++ {
				m.step()
			}
			w.step = steps + 1
			w.notef("finish")
			m.finish()
		})
	}
}

// walkParams returns the seeds and the steps per seed, from the environment
// when set.
func walkParams(t *testing.T) ([]uint64, int) {
	t.Helper()

	seeds := defaultWalkSeeds
	steps := defaultWalkSteps
	if v, ok := os.LookupEnv(walkSeedEnv); ok {
		seed, err := strconv.ParseUint(v, 10, 64)
		require.NoError(t, err, "%s must be an unsigned integer", walkSeedEnv)
		seeds = []uint64{seed}
	}
	if v, ok := os.LookupEnv(walkStepsEnv); ok {
		n, err := strconv.Atoi(v)
		require.NoError(t, err, "%s must be an integer", walkStepsEnv)
		require.Positive(t, n, "%s must be positive", walkStepsEnv)
		steps = n
	}

	return seeds, steps
}

// dumpOnFailure logs the seed, the command that replays it and the steps
// that led to the failure.
// It runs from a defer, so it also fires after a require has stopped the subtest.
func (w *walk) dumpOnFailure(seed uint64) {
	if !w.t.Failed() {
		return
	}
	trace := w.trace
	omitted := 0
	if len(trace) > walkTraceTail {
		omitted = len(trace) - walkTraceTail
		trace = trace[omitted:]
	}
	top, _, _ := strings.Cut(w.t.Name(), "/")
	w.t.Logf("random walk failed at step %d, seed %d; replay with:\n  %s=%d go test ./policy -run '^%s$/^seed=%d$' -v\n"+
		"steps (%d earlier omitted):\n  %s",
		w.step, seed, walkSeedEnv, seed, top, seed, omitted, strings.Join(trace, "\n  "))
}

// notef appends one line to the step trace.
func (w *walk) notef(format string, args ...any) {
	w.trace = append(w.trace, fmt.Sprintf("%5d  ", w.step)+fmt.Sprintf(format, args...))
}

// intn returns a uniform value in [0, n).
func (w *walk) intn(n int) int {
	return w.rng.IntN(n)
}

// chance reports true with probability 1/n.
func (w *walk) chance(n int) bool {
	return w.rng.IntN(n) == 0
}

// cluster draws cluster A or B.
func (w *walk) cluster() types.ClusterID {
	if w.rng.IntN(2) == 0 {
		return types.ClusterA
	}

	return types.ClusterB
}

// run draws one operation by weight and runs it.
func (w *walk) run(ops []walkOp) {
	total := 0
	for _, op := range ops {
		total += op.weight
	}
	n := w.rng.IntN(total)
	for _, op := range ops {
		if n < op.weight {
			op.run()

			return
		}
		n -= op.weight
	}
}

// countEvents counts the emitted events of kind for cluster.
func countEvents(em *recordingEmitter, kind types.ClusterEventKind, cluster types.ClusterID) int {
	em.mu.Lock()
	defer em.mu.Unlock()
	n := 0
	for _, ev := range em.events {
		if ev.Kind == kind && ev.Cluster == cluster {
			n++
		}
	}

	return n
}
