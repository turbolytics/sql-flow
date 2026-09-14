//go:build linux && cgo

package leakloop

// #include <malloc.h>
import "C"

// mallocTrim returns glibc's free heap pages to the OS. Memory that drops
// after it was freed and retained, not leaked.
func mallocTrim() { C.malloc_trim(0) }
