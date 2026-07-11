package replay

import (
	"math"
	"testing"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestTryConvertToUUID(t *testing.T) {
	t.Run("gocql.UUID", func(t *testing.T) {
		gUUID := gocql.TimeUUID()
		res, ok := tryConvertToUUID(gUUID)
		assert.True(t, ok)
		assert.Equal(t, UUID(gUUID), res)
	})

	t.Run("google/uuid.UUID", func(t *testing.T) {
		uUUID := uuid.New()
		res, ok := tryConvertToUUID(uUUID)
		assert.True(t, ok)
		assert.Equal(t, UUID(uUUID), res)
	})

	t.Run("[16]byte", func(t *testing.T) {
		var b [16]byte
		copy(b[:], "1234567890123456")
		res, ok := tryConvertToUUID(b)
		assert.True(t, ok)
		assert.Equal(t, UUID(b), res)
	})

	t.Run("*[16]byte", func(t *testing.T) {
		var b [16]byte
		copy(b[:], "1234567890123456")
		res, ok := tryConvertToUUID(&b)
		assert.True(t, ok)
		assert.Equal(t, UUID(b), res)
	})

	t.Run("UUID (internal type)", func(t *testing.T) {
		var b [16]byte
		copy(b[:], "1234567890123456")
		u := UUID(b)
		res, ok := tryConvertToUUID(u)
		assert.True(t, ok)
		assert.Equal(t, u, res)
	})

	t.Run("Invalid type", func(t *testing.T) {
		res, ok := tryConvertToUUID("not a uuid")
		assert.False(t, ok)
		assert.Equal(t, UUID{}, res)
	})
}

// TestMsgsToInt verifies that msgsToInt (used by NATSReplayer.Pending)
// propagates a real error instead of fabricating a sentinel value when a
// stream's message count cannot be represented as a platform int.
func TestMsgsToInt(t *testing.T) {
	t.Run("in-range value converts exactly", func(t *testing.T) {
		got, err := msgsToInt(42)
		assert.NoError(t, err)
		assert.Equal(t, 42, got)
	})

	t.Run("zero converts exactly", func(t *testing.T) {
		got, err := msgsToInt(0)
		assert.NoError(t, err)
		assert.Equal(t, 0, got)
	})

	t.Run("out-of-range value returns error, not a fabricated sentinel", func(t *testing.T) {
		got, err := msgsToInt(math.MaxUint64)
		assert.Error(t, err)
		assert.Zero(t, got)
	})
}
