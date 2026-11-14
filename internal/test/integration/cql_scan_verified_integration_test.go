package integration_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/adapter/cql"
	cqlv1 "github.com/arloliu/helix/adapter/cql/v1"
	cqlv2 "github.com/arloliu/helix/adapter/cql/v2"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/types"
)

// TestScanVerified_RealClusters closes the loop the fake-session tests cannot:
// a real driver must hand the callback the row of the cluster being read,
// through an iterator Helix opens and closes itself, on both adapters.
func TestScanVerified_RealClusters(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	rawA, rawB := getSharedSessions(t)
	v2A, v2B := getSharedSessionsV2(t)

	adapters := []struct {
		name string
		a, b cql.Session
	}{
		{name: "v1", a: cqlv1.WrapSession(rawA), b: cqlv1.WrapSession(rawB)},
		{name: "v2", a: cqlv2.NewSession(v2A), b: cqlv2.NewSession(v2B)},
	}

	// onA and onB are the values each cluster holds for the key; empty means no row.
	rows := []struct {
		name     string
		onA, onB string
		wantErr  error
		wantSeen []string
	}{
		{name: "selected has the accepted row", onA: "new", onB: "old", wantSeen: []string{"new"}},
		{name: "alternative has the accepted row", onA: "old", onB: "new", wantSeen: []string{"old", "new"}},
		{name: "alternative has no row", onA: "old", wantErr: types.ErrReadRejected, wantSeen: []string{"old"}},
		{name: "alternative row rejected too", onA: "old", onB: "older", wantErr: types.ErrReadRejected, wantSeen: []string{"old", "older"}},
		{name: "only the alternative has a row", onB: "new", wantSeen: []string{"new"}},
		{name: "neither has a row", wantErr: types.ErrNotFound},
	}

	for _, ad := range adapters {
		table := createTestTableOnBoth(t, "scan_verified_"+ad.name, configTableCQLSchema)

		client, err := helix.NewCQLClient(ad.a, ad.b,
			helix.WithReadStrategy(policy.NewStickyRead(policy.WithPreferredCluster(types.ClusterA))),
		)
		require.NoError(t, err)

		for _, row := range rows {
			t.Run(ad.name+"/"+row.name, func(t *testing.T) {
				insert := "INSERT INTO " + table + " (key, value) VALUES (?, ?)"
				if row.onA != "" {
					require.NoError(t, rawA.Query(insert, row.name, row.onA).Exec())
				}
				if row.onB != "" {
					require.NoError(t, rawB.Query(insert, row.name, row.onB).Exec())
				}

				var got string
				var seen []string
				scanErr := client.Query("SELECT value FROM "+table+" WHERE key = ?", row.name).
					ScanVerifiedContext(t.Context(), func(r helix.RowScanner) bool {
						var value string
						if r.Scan(&value) != nil {
							return false
						}
						seen = append(seen, value)
						if value != "new" {
							return false
						}
						got = value

						return true
					})

				if row.wantErr == nil {
					require.NoError(t, scanErr)
					assert.Equal(t, "new", got)
				} else {
					require.ErrorIs(t, scanErr, row.wantErr)
					assert.Empty(t, got)
				}
				assert.Equal(t, row.wantSeen, seen, "rows the callback was shown")
			})
		}
	}
}
