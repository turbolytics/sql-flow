//go:build !darwin

package turbostats

import "errors"

// residentBytesDarwin is never called off darwin. It exists so the dispatch
// in ResidentAnonBytes compiles everywhere without a build tag of its own.
func residentBytesDarwin() (int64, error) {
	return 0, errors.New("turbostats: not darwin")
}
