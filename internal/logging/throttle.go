package logging

// Escalate reports whether the n-th consecutive occurrence of a repeating
// condition should be logged at the escalated level.
//
// It is true for the first occurrence and for every power of two after it
// (1, 2, 4, 8, 16, …), so a condition that persists stays visible in the
// log at a geometrically decreasing rate instead of one line per tick.
// Occurrences that are not escalated are expected to be logged at Debug.
//
// Parameters:
//   - n: One-based count of consecutive occurrences
//
// Returns:
//   - bool: true when the occurrence should be logged at the escalated level
func Escalate(n uint64) bool {
	return n == 1 || (n > 0 && n&(n-1) == 0)
}
