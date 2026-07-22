//go:build !windows

package main

import (
	"bufio"
	"errors"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func getProcessCPUTime() (time.Duration, error) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, err
	}
	user := time.Duration(usage.Utime.Sec)*time.Second + time.Duration(usage.Utime.Usec)*time.Microsecond
	system := time.Duration(usage.Stime.Sec)*time.Second + time.Duration(usage.Stime.Usec)*time.Microsecond
	return user + system, nil
}

func readSystemCPUSample() (systemCPUSample, error) {
	file, err := os.Open("/proc/stat")
	if err != nil {
		return systemCPUSample{}, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "cpu ") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 5 {
			return systemCPUSample{}, errors.New("invalid cpu line")
		}
		var total uint64
		for _, field := range fields[1:] {
			value, err := strconv.ParseUint(field, 10, 64)
			if err != nil {
				return systemCPUSample{}, err
			}
			total += value
		}
		idle, err := strconv.ParseUint(fields[4], 10, 64)
		if err != nil {
			return systemCPUSample{}, err
		}
		return systemCPUSample{Idle: idle, Total: total}, nil
	}
	if err := scanner.Err(); err != nil {
		return systemCPUSample{}, err
	}
	return systemCPUSample{}, errors.New("cpu line not found")
}

func readProcSelfIO() (ioSample, error) {
	data, err := os.ReadFile("/proc/self/io")
	if err != nil {
		return ioSample{}, err
	}
	var sample ioSample
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "read_bytes:"):
			value, err := strconv.ParseUint(strings.TrimSpace(strings.TrimPrefix(line, "read_bytes:")), 10, 64)
			if err == nil {
				sample.ReadBytes = value
			}
		case strings.HasPrefix(line, "write_bytes:"):
			value, err := strconv.ParseUint(strings.TrimSpace(strings.TrimPrefix(line, "write_bytes:")), 10, 64)
			if err == nil {
				sample.WriteBytes = value
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return ioSample{}, err
	}
	return sample, nil
}

func readSystemMemorySample() (uint64, uint64, bool) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, 0, false
	}
	var total, available uint64
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "MemTotal:"):
			if val, err := parseMeminfoValue(line); err == nil {
				total = val
			}
		case strings.HasPrefix(line, "MemAvailable:"):
			if val, err := parseMeminfoValue(line); err == nil {
				available = val
			}
		}
		if total > 0 && available > 0 {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, 0, false
	}
	if total == 0 || available == 0 {
		return total, available, false
	}
	return total, available, true
}

func parseMeminfoValue(line string) (uint64, error) {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return 0, errors.New("invalid meminfo line")
	}
	value, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0, err
	}
	if len(fields) >= 3 && strings.EqualFold(fields[2], "kb") {
		value *= 1024
	}
	return value, nil
}
