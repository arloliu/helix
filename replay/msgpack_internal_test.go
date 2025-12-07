package replay

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMsgpEncodeDecodeCQLTypes tests msgp encoding/decoding for all common CQL argument types.
// This ensures the replay system can correctly serialize and deserialize CQL query arguments.
func TestMsgpEncodeDecodeCQLTypes(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected any
		desc     string
	}{
		// String types (CQL: text, varchar, ascii)
		{
			name:     "empty string",
			input:    "",
			expected: "",
			desc:     "CQL text/varchar empty",
		},
		{
			name:     "simple string",
			input:    "hello world",
			expected: "hello world",
			desc:     "CQL text/varchar",
		},
		{
			name:     "unicode string",
			input:    "こんにちは世界 🌍",
			expected: "こんにちは世界 🌍",
			desc:     "CQL text with unicode",
		},

		// Integer types (CQL: int, bigint, smallint, tinyint, varint)
		{
			name:     "zero int",
			input:    int(0),
			expected: int64(0),
			desc:     "CQL int zero",
		},
		{
			name:     "positive int",
			input:    int(42),
			expected: int64(42),
			desc:     "CQL int positive",
		},
		{
			name:     "negative int",
			input:    int(-42),
			expected: int64(-42),
			desc:     "CQL int negative",
		},
		{
			name:     "int8",
			input:    int8(127),
			expected: int64(127),
			desc:     "CQL tinyint max",
		},
		{
			name:     "int16",
			input:    int16(32767),
			expected: int64(32767),
			desc:     "CQL smallint max",
		},
		{
			name:     "int32",
			input:    int32(2147483647),
			expected: int64(2147483647),
			desc:     "CQL int max",
		},
		{
			name:     "int64",
			input:    int64(9223372036854775807),
			expected: int64(9223372036854775807),
			desc:     "CQL bigint max",
		},
		{
			name:     "int64 negative",
			input:    int64(-9223372036854775808),
			expected: int64(-9223372036854775808),
			desc:     "CQL bigint min",
		},

		// Unsigned integer types (CQL counter uses bigint internally)
		{
			name:     "uint8",
			input:    uint8(255),
			expected: uint64(255),
			desc:     "unsigned byte",
		},
		{
			name:     "uint16",
			input:    uint16(65535),
			expected: uint64(65535),
			desc:     "unsigned short",
		},
		{
			name:     "uint32",
			input:    uint32(4294967295),
			expected: uint64(4294967295),
			desc:     "unsigned int",
		},
		{
			name:     "uint64",
			input:    uint64(18446744073709551615),
			expected: uint64(18446744073709551615),
			desc:     "unsigned bigint max",
		},

		// Floating point types (CQL: float, double)
		{
			name:     "float32",
			input:    float32(3.14),
			expected: float32(3.14), // msgp preserves float32
			desc:     "CQL float",
		},
		{
			name:     "float64",
			input:    float64(3.141592653589793),
			expected: float64(3.141592653589793),
			desc:     "CQL double",
		},
		{
			name:     "float64 zero",
			input:    float64(0),
			expected: float64(0),
			desc:     "CQL double zero",
		},
		{
			name:     "float64 negative",
			input:    float64(-123.456),
			expected: float64(-123.456),
			desc:     "CQL double negative",
		},

		// Boolean type (CQL: boolean)
		{
			name:     "bool true",
			input:    true,
			expected: true,
			desc:     "CQL boolean true",
		},
		{
			name:     "bool false",
			input:    false,
			expected: false,
			desc:     "CQL boolean false",
		},

		// Binary type (CQL: blob)
		{
			name:     "empty bytes",
			input:    []byte{},
			expected: []byte(nil), // msgp decodes empty bytes as nil
			desc:     "CQL blob empty",
		},
		{
			name:     "binary data",
			input:    []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD},
			expected: []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD},
			desc:     "CQL blob with binary data",
		},
		{
			name:     "large binary",
			input:    make([]byte, 1024), // 1KB of zeros
			expected: make([]byte, 1024),
			desc:     "CQL blob large",
		},

		// Nil/null type
		{
			name:     "nil",
			input:    nil,
			expected: nil,
			desc:     "CQL null",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode args
			args := []any{tt.input}
			encoded, err := encodeArgs(args)
			require.NoError(t, err, "failed to encode %s", tt.desc)

			// Decode args
			decoded, err := decodeArgs(encoded)
			require.NoError(t, err, "failed to decode %s", tt.desc)

			require.Len(t, decoded, 1, "expected 1 decoded argument")
			assert.Equal(t, tt.expected, decoded[0], "value mismatch for %s", tt.desc)
		})
	}
}

// TestMsgpEncodeDecodeComplexCQLTypes tests msgp encoding/decoding for complex CQL types
// like maps, lists, sets, and tuples.
func TestMsgpEncodeDecodeComplexCQLTypes(t *testing.T) {
	t.Run("map string to string", func(t *testing.T) {
		// CQL: map<text, text>
		input := map[string]any{
			"key1": "value1",
			"key2": "value2",
		}
		encoded, err := encodeArgs([]any{input})
		require.NoError(t, err)

		decoded, err := decodeArgs(encoded)
		require.NoError(t, err)
		require.Len(t, decoded, 1)

		result, ok := decoded[0].(map[string]any)
		require.True(t, ok, "expected map[string]any, got %T", decoded[0])
		assert.Equal(t, "value1", result["key1"])
		assert.Equal(t, "value2", result["key2"])
	})

	t.Run("map string to int", func(t *testing.T) {
		// CQL: map<text, int>
		input := map[string]any{
			"count": int64(42),
			"total": int64(100),
		}
		encoded, err := encodeArgs([]any{input})
		require.NoError(t, err)

		decoded, err := decodeArgs(encoded)
		require.NoError(t, err)
		require.Len(t, decoded, 1)

		result, ok := decoded[0].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, int64(42), result["count"])
		assert.Equal(t, int64(100), result["total"])
	})

	t.Run("list of strings", func(t *testing.T) {
		// CQL: list<text>
		input := []any{"a", "b", "c"}
		encoded, err := encodeArgs([]any{input})
		require.NoError(t, err)

		decoded, err := decodeArgs(encoded)
		require.NoError(t, err)
		require.Len(t, decoded, 1)

		result, ok := decoded[0].([]any)
		require.True(t, ok, "expected []any, got %T", decoded[0])
		assert.Equal(t, []any{"a", "b", "c"}, result)
	})

	t.Run("list of ints", func(t *testing.T) {
		// CQL: list<int>
		input := []any{int64(1), int64(2), int64(3)}
		encoded, err := encodeArgs([]any{input})
		require.NoError(t, err)

		decoded, err := decodeArgs(encoded)
		require.NoError(t, err)
		require.Len(t, decoded, 1)

		result, ok := decoded[0].([]any)
		require.True(t, ok)
		assert.Equal(t, []any{int64(1), int64(2), int64(3)}, result)
	})

	t.Run("nested structure", func(t *testing.T) {
		// CQL: map<text, list<int>>
		input := map[string]any{
			"numbers": []any{int64(1), int64(2), int64(3)},
			"more":    []any{int64(4), int64(5)},
		}
		encoded, err := encodeArgs([]any{input})
		require.NoError(t, err)

		decoded, err := decodeArgs(encoded)
		require.NoError(t, err)
		require.Len(t, decoded, 1)

		result, ok := decoded[0].(map[string]any)
		require.True(t, ok)

		numbers, ok := result["numbers"].([]any)
		require.True(t, ok)
		assert.Equal(t, []any{int64(1), int64(2), int64(3)}, numbers)

		more, ok := result["more"].([]any)
		require.True(t, ok)
		assert.Equal(t, []any{int64(4), int64(5)}, more)
	})

	t.Run("empty list", func(t *testing.T) {
		input := []any{}
		encoded, err := encodeArgs([]any{input})
		require.NoError(t, err)

		decoded, err := decodeArgs(encoded)
		require.NoError(t, err)
		require.Len(t, decoded, 1)

		result, ok := decoded[0].([]any)
		require.True(t, ok)
		assert.Empty(t, result)
	})

	t.Run("empty map", func(t *testing.T) {
		input := map[string]any{}
		encoded, err := encodeArgs([]any{input})
		require.NoError(t, err)

		decoded, err := decodeArgs(encoded)
		require.NoError(t, err)
		require.Len(t, decoded, 1)

		result, ok := decoded[0].(map[string]any)
		require.True(t, ok)
		assert.Empty(t, result)
	})
}

// TestMsgpEncodeDecodeMixedArgs tests msgp encoding/decoding with multiple arguments
// of different types, simulating real CQL query arguments.
func TestMsgpEncodeDecodeMixedArgs(t *testing.T) {
	t.Run("typical INSERT args", func(t *testing.T) {
		// Simulates: INSERT INTO users (id, name, age, active, data) VALUES (?, ?, ?, ?, ?)
		args := []any{
			"550e8400-e29b-41d4-a716-446655440000", // UUID as string
			"John Doe",                             // text
			int64(30),                              // int
			true,                                   // boolean
			[]byte{0x01, 0x02, 0x03},               // blob
		}

		encoded, err := encodeArgs(args)
		require.NoError(t, err)

		decoded, err := decodeArgs(encoded)
		require.NoError(t, err)

		require.Len(t, decoded, 5)
		assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", decoded[0])
		assert.Equal(t, "John Doe", decoded[1])
		assert.Equal(t, int64(30), decoded[2])
		assert.Equal(t, true, decoded[3])
		assert.Equal(t, []byte{0x01, 0x02, 0x03}, decoded[4])
	})

	t.Run("SELECT with range args", func(t *testing.T) {
		// Simulates: SELECT * FROM events WHERE ts >= ? AND ts < ? LIMIT ?
		args := []any{
			int64(1699900000000000), // timestamp as microseconds
			int64(1699986400000000),
			int64(100), // limit
		}

		encoded, err := encodeArgs(args)
		require.NoError(t, err)

		decoded, err := decodeArgs(encoded)
		require.NoError(t, err)

		require.Len(t, decoded, 3)
		assert.Equal(t, int64(1699900000000000), decoded[0])
		assert.Equal(t, int64(1699986400000000), decoded[1])
		assert.Equal(t, int64(100), decoded[2])
	})

	t.Run("UPDATE with collection args", func(t *testing.T) {
		// Simulates: UPDATE users SET tags = ?, metadata = ? WHERE id = ?
		args := []any{
			[]any{"tag1", "tag2", "tag3"},                  // set<text> or list<text>
			map[string]any{"key1": "val1", "key2": "val2"}, // map<text, text>
			"550e8400-e29b-41d4-a716-446655440000",         // UUID
		}

		encoded, err := encodeArgs(args)
		require.NoError(t, err)

		decoded, err := decodeArgs(encoded)
		require.NoError(t, err)

		require.Len(t, decoded, 3)

		tags, ok := decoded[0].([]any)
		require.True(t, ok)
		assert.Equal(t, []any{"tag1", "tag2", "tag3"}, tags)

		metadata, ok := decoded[1].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "val1", metadata["key1"])
		assert.Equal(t, "val2", metadata["key2"])

		assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", decoded[2])
	})

	t.Run("args with nil values", func(t *testing.T) {
		// Simulates: INSERT INTO t (a, b, c) VALUES (?, ?, ?) where b is null
		args := []any{
			"value1",
			nil,
			int64(42),
		}

		encoded, err := encodeArgs(args)
		require.NoError(t, err)

		decoded, err := decodeArgs(encoded)
		require.NoError(t, err)

		require.Len(t, decoded, 3)
		assert.Equal(t, "value1", decoded[0])
		assert.Nil(t, decoded[1])
		assert.Equal(t, int64(42), decoded[2])
	})

	t.Run("empty args", func(t *testing.T) {
		args := []any{}

		encoded, err := encodeArgs(args)
		require.NoError(t, err)
		assert.Nil(t, encoded) // Empty args should return nil

		decoded, err := decodeArgs(encoded)
		require.NoError(t, err)
		assert.Nil(t, decoded)
	})
}

// TestMsgpUUIDSerializationBug documents a known limitation: gocql.UUID ([16]byte)
// TestMsgpUUIDExtension tests that UUID types are correctly serialized and
// deserialized through MessagePack using our custom extension.
//
// This verifies the fix for the UUID serialization bug where [16]byte arrays
// were decoded as []interface{} instead of being preserved.
//
// UUIDs are decoded as []byte for compatibility with CQL drivers (gocql accepts
// []byte for UUID columns).
func TestMsgpUUIDExtension(t *testing.T) {
	// Test with [16]byte (what gocql.UUID is)
	t.Run("16-byte array is decoded as byte slice", func(t *testing.T) {
		// Simulate gocql.UUID which is defined as type UUID [16]byte
		uuid := [16]byte{
			0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
			0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
		}

		args := []any{uuid}

		// Encoding succeeds with extension
		encoded, err := encodeArgs(args)
		require.NoError(t, err, "encoding UUID should succeed")
		require.NotNil(t, encoded)

		// Decoding succeeds and returns []byte for driver compatibility
		decoded, err := decodeArgs(encoded)
		require.NoError(t, err, "decoding should succeed")
		require.Len(t, decoded, 1)

		// Should be decoded as []byte for driver compatibility
		decodedBytes, ok := decoded[0].([]byte)
		require.True(t, ok, "should decode as []byte, got %T", decoded[0])
		assert.Equal(t, uuid[:], decodedBytes)

		t.Logf("UUID correctly preserved as []byte: %x", decodedBytes)
	})

	t.Run("replay.UUID type is decoded as byte slice", func(t *testing.T) {
		uuid := UUID{
			0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
			0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
		}

		args := []any{uuid}

		encoded, err := encodeArgs(args)
		require.NoError(t, err)

		decoded, err := decodeArgs(encoded)
		require.NoError(t, err)
		require.Len(t, decoded, 1)

		decodedBytes, ok := decoded[0].([]byte)
		require.True(t, ok, "should decode as []byte, got %T", decoded[0])
		assert.Equal(t, uuid[:], decodedBytes)
	})

	t.Run("pointer to 16-byte array is decoded as byte slice", func(t *testing.T) {
		uuid := &[16]byte{
			0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
			0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
		}

		args := []any{uuid}

		encoded, err := encodeArgs(args)
		require.NoError(t, err)

		decoded, err := decodeArgs(encoded)
		require.NoError(t, err)
		require.Len(t, decoded, 1)

		decodedBytes, ok := decoded[0].([]byte)
		require.True(t, ok, "should decode as []byte, got %T", decoded[0])
		assert.Equal(t, (*uuid)[:], decodedBytes)
	})

	t.Run("multiple UUIDs in args are decoded as byte slices", func(t *testing.T) {
		uuid1 := [16]byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00}
		uuid2 := [16]byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99}

		args := []any{"test", uuid1, int64(42), uuid2, true}

		encoded, err := encodeArgs(args)
		require.NoError(t, err)

		decoded, err := decodeArgs(encoded)
		require.NoError(t, err)
		require.Len(t, decoded, 5)

		assert.Equal(t, "test", decoded[0])

		decodedBytes1, ok := decoded[1].([]byte)
		require.True(t, ok, "should decode as []byte, got %T", decoded[1])
		assert.Equal(t, uuid1[:], decodedBytes1)

		assert.Equal(t, int64(42), decoded[2])

		decodedBytes2, ok := decoded[3].([]byte)
		require.True(t, ok, "should decode as []byte, got %T", decoded[3])
		assert.Equal(t, uuid2[:], decodedBytes2)

		assert.Equal(t, true, decoded[4])
	})
}

// TestMsgpUUIDWorkaround demonstrates that strings can also be used for UUIDs.
// This is an alternative approach that doesn't require the extension.
func TestMsgpUUIDWorkaround(t *testing.T) {
	// Instead of passing gocql.UUID directly, convert to string
	uuidString := "550e8400-e29b-41d4-a716-446655440000"

	args := []any{uuidString}

	encoded, err := encodeArgs(args)
	require.NoError(t, err)

	decoded, err := decodeArgs(encoded)
	require.NoError(t, err)
	require.Len(t, decoded, 1)

	// Strings are correctly preserved through msgpack
	result, ok := decoded[0].(string)
	require.True(t, ok, "should decode as string")
	assert.Equal(t, uuidString, result)

	t.Log("Alternative: Pass UUIDs as strings (e.g., uuid.String()) if extension not needed")
}

// TestUUIDExtensionString verifies the UUID.String() method produces correct output.
func TestUUIDExtensionString(t *testing.T) {
	uuid := UUID{
		0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
		0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
	}

	expected := "550e8400-e29b-41d4-a716-446655440000"
	assert.Equal(t, expected, uuid.String())
}

// TestUUIDFromBytes verifies the UUIDFromBytes helper function.
func TestUUIDFromBytes(t *testing.T) {
	t.Run("valid 16 bytes", func(t *testing.T) {
		bytes := []byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}
		uuid, ok := UUIDFromBytes(bytes)
		require.True(t, ok)
		assert.Equal(t, bytes, uuid.Bytes())
	})

	t.Run("wrong size returns false", func(t *testing.T) {
		bytes := []byte{0x55, 0x0e, 0x84}
		_, ok := UUIDFromBytes(bytes)
		assert.False(t, ok)
	})

	t.Run("empty returns false", func(t *testing.T) {
		_, ok := UUIDFromBytes(nil)
		assert.False(t, ok)
	})
}
