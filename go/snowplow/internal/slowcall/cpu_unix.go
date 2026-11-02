//go:build unix

package slowcall

import "syscall"

// processCPU is the process's user+system CPU time.
func processCPU() (int64, bool) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, false
	}
	return ru.Utime.Nano() + ru.Stime.Nano(), true
}
