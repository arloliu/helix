package v2

import (
	"testing"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// BenchmarkAdapterBatchQuery isolates the adapter Batch.Query per-statement
// allocation cost. Building a 100-statement batch over a bare gocql batch measures
// exactly what the adapter adds on top of the driver, guarding against per-statement
// bookkeeping (such as a second entries slice) creeping back into the hot path.
func BenchmarkAdapterBatchQuery(b *testing.B) {
	const stmt = "INSERT INTO tchart (eqp_id, source, svid_name, date, dc_dt, val, data, tag) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
	row := []any{"TOOL_00123", "SECS", "PRESSURE_CHAMBER_A", "2026-07-10", int64(42), 3.14159, "", "OK"}

	b.ReportAllocs()
	for b.Loop() {
		bat := &Batch{batch: &gocql.Batch{}}
		for range 100 {
			bat.Query(stmt, row...)
		}
	}
}
