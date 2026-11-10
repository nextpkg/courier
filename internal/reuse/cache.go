// Package reuse contains nonblocking bounded caches, separate from object lifecycle.
package reuse

// Cache stores idle reusable values. A nil cache is disabled.
// It must not be closed; nil pointer values must not be inserted.
type Cache[T any] chan T

func (c Cache[T]) Take() (value T) {
	select {
	case value = <-c:
	default:
	}
	return
}

// Put reports whether the cache accepted the value; full or nil caches reject it.
func (c Cache[T]) Put(value T) bool {
	select {
	case c <- value:
		return true
	default:
		return false
	}
}
