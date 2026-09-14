//go:build !linux || !cgo

package leakloop

// mallocTrim is glibc's; elsewhere there is nothing to trim.
func mallocTrim() {}
