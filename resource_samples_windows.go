//go:build windows

package main

import (
	"errors"
	"time"
)

// Windows does not expose /proc or RLIMIT-style soft limits. Return unsupported
// markers so the resource monitor degrades gracefully while still running.
func getProcessCPUTime() (time.Duration, error) {
	return 0, errors.New("process cpu time unsupported on windows")
}

func readSystemCPUSample() (systemCPUSample, error) {
	return systemCPUSample{}, errors.New("system cpu sample unsupported on windows")
}

func readProcSelfIO() (ioSample, error) {
	return ioSample{}, errors.New("process io sample unsupported on windows")
}

func readSystemMemorySample() (uint64, uint64, bool) {
	return 0, 0, false
}
