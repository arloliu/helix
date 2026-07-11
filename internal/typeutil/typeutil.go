// Package typeutil provides small reflection-based helpers shared across
// Helix's internal packages.
package typeutil

import "reflect"

// IsNilInterface reports whether v is either an untyped nil or a typed nil
// wrapped in a non-nil interface value. When a nil concrete pointer is
// assigned to an interface variable (e.g. `var m *T; var i Iface = m`), the
// interface carries a type descriptor alongside the nil pointer, so
// `i == nil` is false even though calling any method on it panics. Go's
// standard `== nil` check misses that case, which is exactly how a
// caller-supplied nil concrete pointer bypasses a `x == nil` fallback guard
// and later panics on first use.
//
// Only kinds that can be nil are dereferenced via reflection; all other
// kinds (e.g. a struct value) can never be nil and short-circuit to false
// without reflection overhead.
//
// Parameters:
//   - v: The value to check, typically a caller-supplied interface implementation
//
// Returns:
//   - bool: true if v is nil, or a typed nil pointer/chan/func/map/slice/unsafe pointer
func IsNilInterface(v any) bool {
	if v == nil {
		return true
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() { //nolint:exhaustive // only nil-able kinds need the check; all others fall through to false
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer, reflect.Slice, reflect.UnsafePointer:
		return rv.IsNil()
	default:
		return false
	}
}
