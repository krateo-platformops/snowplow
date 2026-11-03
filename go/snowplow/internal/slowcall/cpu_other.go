//go:build !unix

package slowcall

// processCPU is unavailable off unix; the record then carries no
// process_cpu_ms.
func processCPU() (int64, bool) { return 0, false }
