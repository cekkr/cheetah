package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

type LogLevel int

const (
	LogLevelError   LogLevel = 1
	LogLevelInfo    LogLevel = 2
	LogLevelVerbose LogLevel = 3
)

var (
	configuredLogLevel = LogLevelInfo
	logSink            = NewLogBuffer(256)
)

func init() {
	configuredLogLevel = parseLogLevel(os.Getenv("CHEETAH_LOG_LEVEL"))
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetOutput(io.MultiWriter(os.Stderr, logSink))
	logInfof("Logger initialized at level=%s", logLevelLabel(configuredLogLevel))
}

type LogBuffer struct {
	mu       sync.Mutex
	entries  []string
	capacity int
}

func NewLogBuffer(capacity int) *LogBuffer {
	if capacity < 1 {
		capacity = 1
	}
	return &LogBuffer{capacity: capacity}
}

func (lb *LogBuffer) Write(p []byte) (int, error) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	text := strings.TrimRight(string(p), "\n")
	for _, line := range strings.Split(text, "\n") {
		if line == "" {
			continue
		}
		lb.entries = append(lb.entries, line)
		if len(lb.entries) > lb.capacity {
			start := len(lb.entries) - lb.capacity
			lb.entries = lb.entries[start:]
		}
	}
	return len(p), nil
}

func (lb *LogBuffer) Flush(limit int) []string {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	entries := lb.entries
	if limit > 0 && limit < len(entries) {
		entries = entries[len(entries)-limit:]
	}
	copied := make([]string, len(entries))
	copy(copied, entries)
	lb.entries = nil
	return copied
}

func parseLogLevel(raw string) LogLevel {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return LogLevelInfo
	}
	if numeric, err := strconv.Atoi(trimmed); err == nil {
		switch {
		case numeric >= int(LogLevelVerbose):
			return LogLevelVerbose
		case numeric <= int(LogLevelError):
			return LogLevelError
		default:
			return LogLevelInfo
		}
	}
	switch strings.ToLower(trimmed) {
	case "error":
		return LogLevelError
	case "debug", "trace", "verbose":
		return LogLevelVerbose
	default:
		return LogLevelInfo
	}
}

func logLevelLabel(level LogLevel) string {
	switch level {
	case LogLevelError:
		return "error"
	case LogLevelVerbose:
		return "verbose"
	default:
		return "info"
	}
}

func logAt(level LogLevel, format string, args ...any) {
	if level > configuredLogLevel {
		return
	}
	prefix := "[INFO]"
	if level == LogLevelError {
		prefix = "[ERROR]"
	} else if level == LogLevelVerbose {
		prefix = "[V3]"
	}
	log.Printf("%s %s", prefix, fmt.Sprintf(format, args...))
}

func logErrorf(format string, args ...any) {
	logAt(LogLevelError, format, args...)
}

func logInfof(format string, args ...any) {
	logAt(LogLevelInfo, format, args...)
}

func logVerbosef(format string, args ...any) {
	logAt(LogLevelVerbose, format, args...)
}
