package replay

// Benchmarks for the alloc-perf findings on the NATS replayer's hot paths:
// Enqueue's subject construction, Dequeue/DequeueByPriority's consumer
// name/filter subject construction, and encodeArgs's buffer growth. These
// isolate the string-building/buffer-growth logic from network I/O so the
// allocation delta is directly comparable before/after each fix.

import (
	"fmt"
	"testing"

	"github.com/tinylib/msgp/msgp"

	"github.com/arloliu/helix/types"
)

// BenchmarkEnqueueSubjectSprintf benchmarks the original fmt.Sprintf-based
// subject construction used by Enqueue.
func BenchmarkEnqueueSubjectSprintf(b *testing.B) {
	prefix := "helix.replay"
	priorityStr := "high"
	cluster := types.ClusterA

	b.ReportAllocs()
	for b.Loop() {
		_ = fmt.Sprintf("%s.%s.%s", prefix, priorityStr, cluster)
	}
}

// BenchmarkEnqueueSubjectConcat benchmarks the plain string-concatenation
// replacement (shipped) for Enqueue's subject construction.
func BenchmarkEnqueueSubjectConcat(b *testing.B) {
	prefix := "helix.replay"
	priorityStr := "high"
	cluster := types.ClusterA

	b.ReportAllocs()
	for b.Loop() {
		_ = prefix + "." + priorityStr + "." + string(cluster)
	}
}

// BenchmarkDequeueNamesSprintf benchmarks the original fmt.Sprintf-based
// consumerName/filterSubject construction used by Dequeue on every call.
func BenchmarkDequeueNamesSprintf(b *testing.B) {
	prefix := "helix.replay"
	cluster := types.ClusterA

	b.ReportAllocs()
	for b.Loop() {
		consumerName := fmt.Sprintf("helix-worker-%s", cluster)
		filterSubject := fmt.Sprintf("%s.*.%s", prefix, cluster)
		_, _ = consumerName, filterSubject
	}
}

// BenchmarkDequeueNamesConcat benchmarks the plain string-concatenation
// replacement (shipped) for Dequeue's consumerName/filterSubject
// construction.
func BenchmarkDequeueNamesConcat(b *testing.B) {
	prefix := "helix.replay"
	cluster := types.ClusterA

	b.ReportAllocs()
	for b.Loop() {
		consumerName := "helix-worker-" + string(cluster)
		filterSubject := prefix + ".*." + string(cluster)
		_, _ = consumerName, filterSubject
	}
}

// BenchmarkDequeueByPriorityNamesSprintf benchmarks the original
// fmt.Sprintf-based consumerName/filterSubject construction used by
// DequeueByPriority on every call.
func BenchmarkDequeueByPriorityNamesSprintf(b *testing.B) {
	prefix := "helix.replay"
	priorityStr := "high"
	cluster := types.ClusterA

	b.ReportAllocs()
	for b.Loop() {
		consumerName := fmt.Sprintf("helix-worker-%s-%s", priorityStr, cluster)
		filterSubject := fmt.Sprintf("%s.%s.%s", prefix, priorityStr, cluster)
		_, _ = consumerName, filterSubject
	}
}

// BenchmarkDequeueByPriorityNamesConcat benchmarks the plain
// string-concatenation replacement (shipped) for DequeueByPriority's
// consumerName/filterSubject construction.
func BenchmarkDequeueByPriorityNamesConcat(b *testing.B) {
	prefix := "helix.replay"
	priorityStr := "high"
	cluster := types.ClusterA

	b.ReportAllocs()
	for b.Loop() {
		consumerName := "helix-worker-" + priorityStr + "-" + string(cluster)
		filterSubject := prefix + "." + priorityStr + "." + string(cluster)
		_, _ = consumerName, filterSubject
	}
}

// benchArgs is a representative set of CQL query arguments (mixed types,
// including a UUID extension) used by the encodeArgs benchmarks below.
func benchArgs() []any {
	var u UUID
	copy(u[:], "1234567890123456")

	return []any{
		"user@example.com",
		int64(42),
		true,
		3.14159,
		u,
		[]byte("some blob payload data"),
	}
}

// benchArgsLarge is a larger, more realistic argument set (e.g. a wide
// INSERT with many columns) used to check how the encodeArgs
// preallocation benefit scales with argument count.
func benchArgsLarge() []any {
	var u UUID
	copy(u[:], "1234567890123456")

	args := make([]any, 0, 20)
	for i := range 15 {
		args = append(args, fmt.Sprintf("column-value-%d", i))
	}
	args = append(args, u, int64(42), true, 3.14159, []byte("blob"))

	return args
}

// BenchmarkEncodeArgsNoPrealloc benchmarks the original encodeArgs, which
// grows its buffer purely via append with no capacity hint.
//
// Two alternative capacity-estimation strategies were tried and rejected
// before landing on the flat per-arg hint benchmarked below:
//   - msgp.GuessSize(arg) per argument: GuessSize doesn't recognize
//     UUID-like values (replay.UUID's msgp.Extension methods have a
//     pointer receiver, and driver UUID types like gocql.UUID aren't
//     msgp builtins), so it falls back to a 512-byte default per UUID --
//     a large overestimate that made this package's typical args slower,
//     not faster.
//   - A UUID-aware per-arg hint (via tryConvertToUUID) that avoided the
//     512-byte misfire: it fixed the memory overestimate, but re-running
//     tryConvertToUUID's type switch (with its BinaryMarshaler/reflect
//     fallback) once per arg just to size the buffer, then again in
//     appendArg to encode it, doubled the per-arg type-detection cost --
//     a net regression once argument counts grew past a handful.
//
// The flat per-arg hint below avoids inspecting each argument's concrete
// type at all, so it adds no type-switch overhead while still cutting
// reallocations.
func BenchmarkEncodeArgsNoPrealloc(b *testing.B) {
	args := benchArgs()

	b.ReportAllocs()
	for b.Loop() {
		var buf []byte
		buf = msgp.AppendArrayHeader(buf, uint32(len(args)))
		for _, arg := range args {
			var err error
			buf, err = appendArg(buf, arg)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkEncodeArgsPrealloc benchmarks encodeArgs (shipped) with a flat,
// type-agnostic per-argument capacity hint.
func BenchmarkEncodeArgsPrealloc(b *testing.B) {
	args := benchArgs()

	b.ReportAllocs()
	for b.Loop() {
		if _, err := encodeArgs(args); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEncodeArgsNoPreallocLarge is BenchmarkEncodeArgsNoPrealloc with
// a larger argument count, to check the fix scales rather than regresses.
func BenchmarkEncodeArgsNoPreallocLarge(b *testing.B) {
	args := benchArgsLarge()

	b.ReportAllocs()
	for b.Loop() {
		var buf []byte
		buf = msgp.AppendArrayHeader(buf, uint32(len(args)))
		for _, arg := range args {
			var err error
			buf, err = appendArg(buf, arg)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkEncodeArgsPreallocLarge is BenchmarkEncodeArgsPrealloc with a
// larger argument count.
func BenchmarkEncodeArgsPreallocLarge(b *testing.B) {
	args := benchArgsLarge()

	b.ReportAllocs()
	for b.Loop() {
		if _, err := encodeArgs(args); err != nil {
			b.Fatal(err)
		}
	}
}
