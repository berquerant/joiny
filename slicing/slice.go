package slicing

import "math"

// Flat flattens slices.
func Flat[T any](v ...[]T) []T {
	var size int
	for _, xs := range v {
		size += len(xs)
	}
	r := make([]T, size)

	var i int
	for _, xs := range v {
		for _, x := range xs {
			r[i] = x
			i++
		}
	}
	return r
}

func DriftIndex[T any](v []T, i int) int {
	switch {
	case i < 0:
		return 0
	case i >= len(v):
		return len(v) - 1 // max index
	default:
		return i
	}
}

func DriftLimit[T any](v []T, i int) int {
	switch {
	case i < 0:
		return 0
	case i >= len(v):
		return len(v) // max limit
	default:
		return i
	}
}

// Interval is safe slice[left:right].
// If left is negative then slice[0:X].
// If right is out og range then slice[X:len(slice)].
// If right < left then nil.
func Interval[T any](v []T, left, right int) []T {
	if right < left {
		return nil
	}
	return v[DriftIndex(v, left):DriftLimit(v, right)]
}

// Right is safe slice[:right].
// If right is negative then slice[:0] (=nil).
// If right is out of range then slice[:len(slice)].
func Right[T any](v []T, right int) []T { return Interval(v, 0, right) }

// Left is safe slice[left:].
// If left is negative then slice[0:].
// If left is out of range then slice[len(slice)-1:] (=nil).
func Left[T any](v []T, left int) []T { return Interval(v, left, math.MaxInt) }

// InRange returns true if i is a valid index of v.
func InRange[T any](v []T, i int) bool { return 0 <= i && i < len(v) }

// Uniq uniquify slice elements with key function.
func Uniq[T any, K comparable](v []T, key func(T) K) []T {
	m := make(map[K]T)
	for _, x := range v {
		m[key(x)] = x
	}
	var (
		i int
		r = make([]T, len(m))
	)
	for _, x := range m {
		r[i] = x
		i++
	}
	return r
}
