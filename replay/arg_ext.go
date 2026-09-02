package replay

import (
	"encoding/binary"
	"errors"
	"math/big"
	"net"
	"reflect"

	"github.com/tinylib/msgp/msgp"
	"gopkg.in/inf.v0"

	"github.com/arloliu/helix/types"
)

// MessagePack extension types for CQL argument types that msgp's generic
// encoder cannot carry. They sit in the user-defined range next to
// [UUIDExtensionType].
const (
	// VarintExtensionType carries a *big.Int (CQL varint).
	VarintExtensionType int8 = 11

	// DecimalExtensionType carries a *inf.Dec (CQL decimal).
	DecimalExtensionType int8 = 12

	// InetExtensionType carries a net.IP (CQL inet).
	InetExtensionType int8 = 13

	// DurationExtensionType carries a [types.Duration] (CQL duration).
	DurationExtensionType int8 = 14
)

const durationExtensionSize = 16

var (
	errShortExtension = errors.New("helix: extension payload too short")
	errBadInetLength  = errors.New("helix: inet payload must be 4 or 16 bytes")
)

func init() {
	msgp.RegisterExtension(VarintExtensionType, func() msgp.Extension { return new(varintExt) })
	msgp.RegisterExtension(DecimalExtensionType, func() msgp.Extension { return new(decimalExt) })
	msgp.RegisterExtension(InetExtensionType, func() msgp.Extension { return new(inetExt) })
	msgp.RegisterExtension(DurationExtensionType, func() msgp.Extension { return new(durationExt) })
}

// varintExt encodes a big.Int as a sign byte followed by the big-endian
// magnitude.
type varintExt struct{ value big.Int }

func (e *varintExt) ExtensionType() int8 { return VarintExtensionType }
func (e *varintExt) Len() int            { return 1 + len(e.value.Bytes()) }

func (e *varintExt) MarshalBinaryTo(b []byte) error {
	copy(b, encodeBigInt(&e.value))

	return nil
}

func (e *varintExt) UnmarshalBinary(b []byte) error {
	return decodeBigInt(b, &e.value)
}

func encodeBigInt(v *big.Int) []byte {
	mag := v.Bytes()
	out := make([]byte, 1+len(mag))
	if v.Sign() < 0 {
		out[0] = 1
	}
	copy(out[1:], mag)

	return out
}

func decodeBigInt(b []byte, into *big.Int) error {
	if len(b) < 1 {
		return errShortExtension
	}
	into.SetBytes(b[1:])
	if b[0] == 1 {
		into.Neg(into)
	}

	return nil
}

// decimalExt encodes an inf.Dec as a 4-byte big-endian scale followed by
// the varint encoding of the unscaled value.
type decimalExt struct{ value inf.Dec }

func (e *decimalExt) ExtensionType() int8 { return DecimalExtensionType }
func (e *decimalExt) Len() int            { return 4 + 1 + len(e.value.UnscaledBig().Bytes()) }

func (e *decimalExt) MarshalBinaryTo(b []byte) error {
	//nolint:gosec // inf.Scale is an int32 by definition
	binary.BigEndian.PutUint32(b, uint32(int32(e.value.Scale())))
	copy(b[4:], encodeBigInt(e.value.UnscaledBig()))

	return nil
}

func (e *decimalExt) UnmarshalBinary(b []byte) error {
	if len(b) < 5 {
		return errShortExtension
	}
	//nolint:gosec // the scale was written from an int32
	scale := inf.Scale(int32(binary.BigEndian.Uint32(b)))
	var unscaled big.Int
	if err := decodeBigInt(b[4:], &unscaled); err != nil {
		return err
	}
	e.value.SetUnscaledBig(&unscaled)
	e.value.SetScale(scale)

	return nil
}

// inetExt encodes a net.IP as its 4- or 16-byte form.
type inetExt struct{ value net.IP }

func (e *inetExt) ExtensionType() int8 { return InetExtensionType }
func (e *inetExt) Len() int            { return len(canonicalIP(e.value)) }

func (e *inetExt) MarshalBinaryTo(b []byte) error {
	copy(b, canonicalIP(e.value))

	return nil
}

func (e *inetExt) UnmarshalBinary(b []byte) error {
	if len(b) != net.IPv4len && len(b) != net.IPv6len {
		return errBadInetLength
	}
	e.value = net.IP(append([]byte(nil), b...))

	return nil
}

// canonicalIP returns the 4-byte form for an IPv4 address and the 16-byte
// form otherwise.
func canonicalIP(ip net.IP) []byte {
	if v4 := ip.To4(); v4 != nil {
		return v4
	}

	return ip.To16()
}

// durationExt encodes a CQL duration as months, days, and nanoseconds.
type durationExt struct{ value types.Duration }

func (e *durationExt) ExtensionType() int8 { return DurationExtensionType }
func (e *durationExt) Len() int            { return durationExtensionSize }

func (e *durationExt) MarshalBinaryTo(b []byte) error {
	//nolint:gosec // two's-complement round trip through PutUint32/PutUint64
	binary.BigEndian.PutUint32(b, uint32(e.value.Months))
	//nolint:gosec // two's-complement round trip through PutUint32/PutUint64
	binary.BigEndian.PutUint32(b[4:], uint32(e.value.Days))
	//nolint:gosec // two's-complement round trip through PutUint32/PutUint64
	binary.BigEndian.PutUint64(b[8:], uint64(e.value.Nanoseconds))

	return nil
}

func (e *durationExt) UnmarshalBinary(b []byte) error {
	if len(b) < durationExtensionSize {
		return errShortExtension
	}
	//nolint:gosec // two's-complement round trip through Uint32/Uint64
	e.value = types.Duration{
		Months:      int32(binary.BigEndian.Uint32(b)),
		Days:        int32(binary.BigEndian.Uint32(b[4:])),
		Nanoseconds: int64(binary.BigEndian.Uint64(b[8:])),
	}

	return nil
}

// durationFromValue recognises a CQL duration argument: a [types.Duration],
// or a driver duration struct with the same three fields (gocql v1 and the
// Apache v2 driver both define one), so callers can pass either.
func durationFromValue(arg any) (types.Duration, bool) {
	switch v := arg.(type) {
	case types.Duration:
		return v, true
	case *types.Duration:
		if v != nil {
			return *v, true
		}

		return types.Duration{}, false
	}

	rv := reflect.ValueOf(arg)
	if rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return types.Duration{}, false
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct || rv.NumField() != 3 {
		return types.Duration{}, false
	}
	months, days, nanos := rv.FieldByName("Months"), rv.FieldByName("Days"), rv.FieldByName("Nanoseconds")
	if !months.IsValid() || !days.IsValid() || !nanos.IsValid() ||
		months.Kind() != reflect.Int32 || days.Kind() != reflect.Int32 || nanos.Kind() != reflect.Int64 {
		return types.Duration{}, false
	}

	//nolint:gosec // kinds checked above
	return types.Duration{
		Months:      int32(months.Int()),
		Days:        int32(days.Int()),
		Nanoseconds: nanos.Int(),
	}, true
}
