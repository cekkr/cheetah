//go:build !windows

package main

import (
	"math"
	"syscall"
)

func fileDescriptorSoftLimit() int {
	var rl syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rl); err != nil {
		return 0
	}
	if rl.Cur <= 0 || rl.Cur > math.MaxInt32 {
		return 0
	}
	return int(rl.Cur)
}
