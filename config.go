package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Config describes server-wide settings loaded from config.ini/environment variables.
type Config struct {
	ListenAddr       string
	DataDir          string
	DefaultDatabase  string
	MaxPairTables    int
	DatabaseDefaults DatabaseConfig
}

// DatabaseConfig holds concrete per-database tunables.
type DatabaseConfig struct {
	PairIndexBytes      int
	PayloadCacheEntries int
	PayloadCacheBytes   int64
}

// DatabaseOverrides carries optional overrides collected via CLI/API commands.
type DatabaseOverrides struct {
	PairIndexBytes      *int
	PayloadCacheEntries *int
	PayloadCacheBytes   *int64
}

func defaultConfig() Config {
	return Config{
		ListenAddr:      "0.0.0.0:4455",
		DataDir:         "cheetah_data",
		DefaultDatabase: "default",
		DatabaseDefaults: DatabaseConfig{
			PairIndexBytes:      1,
			PayloadCacheEntries: defaultPayloadCacheEntries,
			PayloadCacheBytes:   defaultPayloadCacheBytes,
		},
	}
}

func loadConfig() *Config {
	cfg := defaultConfig()
	path := strings.TrimSpace(os.Getenv("CHEETAH_CONFIG_PATH"))
	if path == "" {
		path = "config.ini"
	}
	if abs, err := filepath.Abs(path); err == nil {
		path = abs
	}
	if data, err := os.Open(path); err == nil {
		defer data.Close()
		parseConfigFile(bufio.NewScanner(data), &cfg)
	}
	applyEnvOverrides(&cfg)
	cfg.normalize()
	return &cfg
}

func parseConfigFile(scanner *bufio.Scanner, cfg *Config) {
	section := ""
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.ToLower(strings.TrimSpace(line[1 : len(line)-1]))
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.ToLower(strings.TrimSpace(key))
		val = strings.TrimSpace(val)
		assignConfigValue(section, key, val, cfg)
	}
}

func assignConfigValue(section, key, val string, cfg *Config) {
	switch section {
	case "", "server":
		switch key {
		case "listen_addr":
			if val != "" {
				cfg.ListenAddr = val
			}
		case "data_dir":
			if val != "" {
				cfg.DataDir = val
			}
		case "default_database":
			if val != "" {
				cfg.DefaultDatabase = val
			}
		}
	case "database":
		switch key {
		case "pair_bytes", "pair_index_bytes":
			if v := parsePositiveInt(val); v > 0 {
				cfg.DatabaseDefaults.PairIndexBytes = v
			}
		case "payload_cache_entries":
			cfg.DatabaseDefaults.PayloadCacheEntries = parseIntAllowZero(val, cfg.DatabaseDefaults.PayloadCacheEntries)
		case "payload_cache_mb":
			if v := parseIntAllowZero(val, 0); v >= 0 {
				cfg.DatabaseDefaults.PayloadCacheBytes = int64(v) << 20
			}
		case "payload_cache_bytes":
			if v := parseIntAllowZero(val, int(cfg.DatabaseDefaults.PayloadCacheBytes)); v >= 0 {
				cfg.DatabaseDefaults.PayloadCacheBytes = int64(v)
			}
		}
	case "tuning":
		switch key {
		case "max_pair_tables":
			if v := parsePositiveInt(val); v > 0 {
				cfg.MaxPairTables = v
			}
		}
	}
}

func parsePositiveInt(val string) int {
	if val == "" {
		return 0
	}
	n, err := strconv.Atoi(val)
	if err != nil || n <= 0 {
		return 0
	}
	return n
}

func parseIntAllowZero(val string, fallback int) int {
	if val == "" {
		return fallback
	}
	n, err := strconv.Atoi(val)
	if err != nil {
		return fallback
	}
	if n < 0 {
		return fallback
	}
	return n
}

func parseInt64AllowZero(val string, fallback int64) int64 {
	if val == "" {
		return fallback
	}
	n, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return fallback
	}
	if n < 0 {
		return fallback
	}
	return n
}

func applyEnvOverrides(cfg *Config) {
	if v := strings.TrimSpace(os.Getenv("CHEETAH_LISTEN_ADDR")); v != "" {
		cfg.ListenAddr = v
	}
	if v := strings.TrimSpace(os.Getenv("CHEETAH_DATA_DIR")); v != "" {
		cfg.DataDir = v
	}
	if v := strings.TrimSpace(os.Getenv("CHEETAH_DEFAULT_DB")); v != "" {
		cfg.DefaultDatabase = v
	}
	if v := parsePositiveInt(os.Getenv("CHEETAH_PAIR_INDEX_BYTES")); v > 0 {
		cfg.DatabaseDefaults.PairIndexBytes = v
	}
	if v := parseIntAllowZero(os.Getenv("CHEETAH_PAYLOAD_CACHE_ENTRIES"), cfg.DatabaseDefaults.PayloadCacheEntries); v >= 0 {
		cfg.DatabaseDefaults.PayloadCacheEntries = v
	}
	if raw := strings.TrimSpace(os.Getenv("CHEETAH_PAYLOAD_CACHE_MB")); raw != "" {
		if v := parseIntAllowZero(raw, 0); v >= 0 {
			cfg.DatabaseDefaults.PayloadCacheBytes = int64(v) << 20
		}
	}
	if raw := strings.TrimSpace(os.Getenv("CHEETAH_PAYLOAD_CACHE_BYTES")); raw != "" {
		cfg.DatabaseDefaults.PayloadCacheBytes = parseInt64AllowZero(raw, cfg.DatabaseDefaults.PayloadCacheBytes)
	}
	if v := parsePositiveInt(os.Getenv("CHEETAH_MAX_PAIR_TABLES")); v > 0 {
		cfg.MaxPairTables = v
	}
}

func (cfg *Config) normalize() {
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = "0.0.0.0:4455"
	}
	if cfg.DataDir == "" {
		cfg.DataDir = "cheetah_data"
	}
	if cfg.DefaultDatabase == "" {
		cfg.DefaultDatabase = "default"
	}
	if cfg.DatabaseDefaults.PairIndexBytes <= 0 {
		cfg.DatabaseDefaults.PairIndexBytes = 2
	}
	if cfg.DatabaseDefaults.PairIndexBytes > 2 {
		cfg.DatabaseDefaults.PairIndexBytes = 2
	}
	if cfg.DatabaseDefaults.PayloadCacheBytes <= 0 {
		cfg.DatabaseDefaults.PayloadCacheBytes = defaultPayloadCacheBytes
	}
	if cfg.DatabaseDefaults.PayloadCacheEntries < 0 {
		cfg.DatabaseDefaults.PayloadCacheEntries = defaultPayloadCacheEntries
	}
	if cfg.MaxPairTables < 0 {
		cfg.MaxPairTables = 0
	}
}

func mergeDatabaseConfig(base DatabaseConfig, override DatabaseOverrides) DatabaseConfig {
	result := base
	if override.PairIndexBytes != nil {
		result.PairIndexBytes = *override.PairIndexBytes
	}
	if override.PayloadCacheEntries != nil {
		result.PayloadCacheEntries = *override.PayloadCacheEntries
	}
	if override.PayloadCacheBytes != nil {
		result.PayloadCacheBytes = *override.PayloadCacheBytes
	}
	return result
}

func parseDatabaseOverrideTokens(tokens []string) (DatabaseOverrides, error) {
	var overrides DatabaseOverrides
	for _, token := range tokens {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		key, val, ok := strings.Cut(token, "=")
		if !ok {
			return overrides, fmt.Errorf("invalid override token %q", token)
		}
		key = strings.ToLower(strings.TrimSpace(key))
		val = strings.TrimSpace(val)
		switch key {
		case "pair_bytes", "pair_index_bytes":
			if v := parsePositiveInt(val); v > 0 {
				if v > 2 {
					return overrides, fmt.Errorf("pair_bytes must be 1 or 2")
				}
				overrides.PairIndexBytes = ptrInt(v)
			} else {
				return overrides, fmt.Errorf("pair_bytes must be >0")
			}
		case "payload_cache_entries":
			valParsed := parseIntAllowZero(val, 0)
			overrides.PayloadCacheEntries = ptrInt(valParsed)
		case "payload_cache_mb":
			bytes := int64(parseIntAllowZero(val, 0)) << 20
			overrides.PayloadCacheBytes = ptrInt64(bytes)
		case "payload_cache_bytes":
			parsed := parseInt64AllowZero(val, 0)
			overrides.PayloadCacheBytes = ptrInt64(parsed)
		default:
			return overrides, fmt.Errorf("unknown override %s", key)
		}
	}
	return overrides, nil
}

func ptrInt(v int) *int       { return &v }
func ptrInt64(v int64) *int64 { return &v }

func parseDatabaseTarget(arg string) (string, *DatabaseOverrides, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return "", nil, fmt.Errorf("missing database name")
	}
	tokens := strings.Fields(arg)
	name := tokens[0]
	if len(tokens) == 1 {
		return name, nil, nil
	}
	overrides, err := parseDatabaseOverrideTokens(tokens[1:])
	if err != nil {
		return "", nil, err
	}
	return name, &overrides, nil
}
