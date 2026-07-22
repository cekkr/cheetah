//go:build windows

package main

// fileDescriptorSoftLimit returns zero on Windows so the database falls back to
// the default pair-table cache limit without relying on RLIMIT_NOFILE.
func fileDescriptorSoftLimit() int {
	return 0
}
