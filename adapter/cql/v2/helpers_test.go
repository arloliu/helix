package v2_test

import (
	"testing"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/adapter/cql"
	v2 "github.com/arloliu/helix/adapter/cql/v2" //nolint:revive,nolintlint // goimports requires the alias for the v2 path element; revive calls it redundant
)

// The conversion helpers in helpers.go are bare numeric casts:
// they assume the helix constant and the driver constant share a value.
// Nothing in the type system holds that assumption,
// so if the driver renumbers a constant the cast keeps compiling
// and silently maps Quorum onto One.
// The tables below name the driver constant on the right-hand side
// rather than its integer,
// so a renumber on the driver side is what breaks the test.

// TestToGocqlConsistency verifies every helix consistency level maps to the
// gocql v2 constant of the same name.
func TestToGocqlConsistency(t *testing.T) {
	tests := []struct {
		name   string
		helix  cql.Consistency
		driver gocql.Consistency
	}{
		{"Any", cql.Any, gocql.Any},
		{"One", cql.One, gocql.One},
		{"Two", cql.Two, gocql.Two},
		{"Three", cql.Three, gocql.Three},
		{"Quorum", cql.Quorum, gocql.Quorum},
		{"All", cql.All, gocql.All},
		{"LocalQuorum", cql.LocalQuorum, gocql.LocalQuorum},
		{"EachQuorum", cql.EachQuorum, gocql.EachQuorum},
		{"LocalOne", cql.LocalOne, gocql.LocalOne},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.driver, v2.ToGocqlConsistency(tt.helix))
			require.Equal(t, tt.helix, v2.FromGocqlConsistency(tt.driver))
		})
	}
}

// TestToGocqlSerialConsistency verifies the two serial levels
// map to the gocql v2 constants of the same name.
// v2 folded serial consistency back into gocql.Consistency,
// so both helpers work in that type.
func TestToGocqlSerialConsistency(t *testing.T) {
	tests := []struct {
		name   string
		helix  cql.Consistency
		driver gocql.Consistency
	}{
		{"Serial", cql.Serial, gocql.Serial},
		{"LocalSerial", cql.LocalSerial, gocql.LocalSerial},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.driver, v2.ToGocqlSerialConsistency(tt.helix))
			require.Equal(t, tt.helix, v2.FromGocqlSerialConsistency(tt.driver))
		})
	}
}

// TestToGocqlBatchType verifies every helix batch type maps to the gocql v2
// constant of the same name.
// A logged batch silently becoming a counter batch
// would change the atomicity guarantee of every dual-write batch.
func TestToGocqlBatchType(t *testing.T) {
	tests := []struct {
		name   string
		helix  cql.BatchType
		driver gocql.BatchType
	}{
		{"LoggedBatch", cql.LoggedBatch, gocql.LoggedBatch},
		{"UnloggedBatch", cql.UnloggedBatch, gocql.UnloggedBatch},
		{"CounterBatch", cql.CounterBatch, gocql.CounterBatch},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.driver, v2.ToGocqlBatchType(tt.helix))
			require.Equal(t, tt.helix, v2.FromGocqlBatchType(tt.driver))
		})
	}
}
