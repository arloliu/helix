package v1_test

import (
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/adapter/cql"
	v1 "github.com/arloliu/helix/adapter/cql/v1" //nolint:revive,nolintlint // goimports requires the alias for the v1 path element; revive calls it redundant
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
// gocql v1 constant of the same name.
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
			require.Equal(t, tt.driver, v1.ToGocqlConsistency(tt.helix))
			require.Equal(t, tt.helix, v1.FromGocqlConsistency(tt.driver))
		})
	}
}

// TestToGocqlSerialConsistency verifies the two serial levels
// map to the gocql v1 constant of the same name.
// v1 keeps serial consistency in its own type,
// whose values share the numbering of the plain consistency levels.
func TestToGocqlSerialConsistency(t *testing.T) {
	tests := []struct {
		name   string
		helix  cql.Consistency
		driver gocql.SerialConsistency
	}{
		{"Serial", cql.Serial, gocql.Serial},
		{"LocalSerial", cql.LocalSerial, gocql.LocalSerial},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.driver, v1.ToGocqlSerialConsistency(tt.helix))
			require.Equal(t, tt.helix, v1.FromGocqlSerialConsistency(tt.driver))
		})
	}
}

// TestToGocqlBatchType verifies every helix batch type maps to the gocql v1
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
			require.Equal(t, tt.driver, v1.ToGocqlBatchType(tt.helix))
			require.Equal(t, tt.helix, v1.FromGocqlBatchType(tt.driver))
		})
	}
}
