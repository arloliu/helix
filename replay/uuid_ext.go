package replay

import (
	"github.com/tinylib/msgp/msgp"
)

// UUIDExtensionType is the MessagePack extension type for UUIDs.
// We use type 10 which is in the user-defined range (0-127).
// Types 3, 4, 5 are used by msgp for complex64, complex128, and time.Time.
const UUIDExtensionType int8 = 10

// UUIDSize is the fixed size of a UUID (16 bytes).
const UUIDSize = 16

func init() {
	// Register the UUID extension so msgp can decode it back to the correct type
	msgp.RegisterExtension(UUIDExtensionType, func() msgp.Extension {
		return new(UUID)
	})
}

// UUID is a wrapper type for UUID byte arrays that implements msgp.Extension.
// This allows UUIDs to be correctly serialized and deserialized through MessagePack.
//
// When encoding CQL query arguments that contain gocql.UUID values, they should
// be converted to this type first. When decoding, the values can be converted
// back to gocql.UUID or used directly as [16]byte.
type UUID [UUIDSize]byte

// ExtensionType returns the MessagePack extension type for UUID.
//
// Returns:
//   - int8: The extension type identifier (10)
func (u *UUID) ExtensionType() int8 {
	return UUIDExtensionType
}

// Len returns the encoded length of a UUID (always 16 bytes).
//
// Returns:
//   - int: The size in bytes (16)
func (u *UUID) Len() int {
	return UUIDSize
}

// MarshalBinaryTo copies the UUID bytes into the destination buffer.
//
// Parameters:
//   - b: Destination buffer (must be at least 16 bytes)
//
// Returns:
//   - error: nil (never fails for valid input)
func (u *UUID) MarshalBinaryTo(b []byte) error {
	copy(b, u[:])

	return nil
}

// UnmarshalBinary copies bytes from the source buffer into the UUID.
//
// Parameters:
//   - b: Source buffer containing 16 bytes of UUID data
//
// Returns:
//   - error: nil (never fails for valid input)
func (u *UUID) UnmarshalBinary(b []byte) error {
	copy(u[:], b)

	return nil
}

// Bytes returns the UUID as a byte slice.
//
// Returns:
//   - []byte: The 16-byte UUID value
func (u *UUID) Bytes() []byte {
	return u[:]
}

// String returns the UUID in standard hyphenated format.
//
// Returns:
//   - string: UUID string like "550e8400-e29b-41d4-a716-446655440000"
func (u *UUID) String() string {
	return uuidToString(u[:])
}

// uuidToString converts a 16-byte UUID to its string representation.
func uuidToString(b []byte) string {
	if len(b) != UUIDSize {
		return ""
	}

	// Format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
	const hex = "0123456789abcdef"
	buf := make([]byte, 36)

	buf[0] = hex[b[0]>>4]
	buf[1] = hex[b[0]&0x0f]
	buf[2] = hex[b[1]>>4]
	buf[3] = hex[b[1]&0x0f]
	buf[4] = hex[b[2]>>4]
	buf[5] = hex[b[2]&0x0f]
	buf[6] = hex[b[3]>>4]
	buf[7] = hex[b[3]&0x0f]
	buf[8] = '-'
	buf[9] = hex[b[4]>>4]
	buf[10] = hex[b[4]&0x0f]
	buf[11] = hex[b[5]>>4]
	buf[12] = hex[b[5]&0x0f]
	buf[13] = '-'
	buf[14] = hex[b[6]>>4]
	buf[15] = hex[b[6]&0x0f]
	buf[16] = hex[b[7]>>4]
	buf[17] = hex[b[7]&0x0f]
	buf[18] = '-'
	buf[19] = hex[b[8]>>4]
	buf[20] = hex[b[8]&0x0f]
	buf[21] = hex[b[9]>>4]
	buf[22] = hex[b[9]&0x0f]
	buf[23] = '-'
	buf[24] = hex[b[10]>>4]
	buf[25] = hex[b[10]&0x0f]
	buf[26] = hex[b[11]>>4]
	buf[27] = hex[b[11]&0x0f]
	buf[28] = hex[b[12]>>4]
	buf[29] = hex[b[12]&0x0f]
	buf[30] = hex[b[13]>>4]
	buf[31] = hex[b[13]&0x0f]
	buf[32] = hex[b[14]>>4]
	buf[33] = hex[b[14]&0x0f]
	buf[34] = hex[b[15]>>4]
	buf[35] = hex[b[15]&0x0f]

	return string(buf)
}

// UUIDFromBytes creates a UUID from a byte slice.
//
// Parameters:
//   - b: Byte slice (must be exactly 16 bytes)
//
// Returns:
//   - UUID: The UUID value
//   - bool: true if conversion succeeded, false if b is wrong size
func UUIDFromBytes(b []byte) (UUID, bool) {
	var u UUID
	if len(b) != UUIDSize {
		return u, false
	}

	copy(u[:], b)

	return u, true
}
