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
	ListenAddr          string
	DataDir             string
	DefaultDatabase     string
	MaxPairTables       int
	TCPKeepAliveSeconds int
	DatabaseDefaults    DatabaseConfig
}

// DatabaseConfig holds concrete per-database tunables.
type DatabaseConfig struct {
	PairIndexBytes      int
	PayloadCacheEntries int
	PayloadCacheBytes   int64
	// AdaptivePairIndex enables the adaptive per-node trie container: sparse
	// nodes are stored as a binary-searched sorted list and only densify into a
	// direct-mapped array once populated. Disabling it forces every node to the
	// dense array from creation (legacy performance profile).
	AdaptivePairIndex bool
	// PairListMaxBytes is the sorted-list size (bytes) at which a node densifies.
	// It also decides which nodes use a list at all: a node whose dense array
	// already fits in this budget (any 1-byte-stride node) is dense from creation.
	PairListMaxBytes int
	// PairListMaxFillPercent optionally densifies a list node once it passes this
	// percentage of branch capacity. 0 (default) disables it. Only meaningful for
	// wide (2-byte-stride) nodes, which are the only ones that use lists.
	PairListMaxFillPercent int
}

// DatabaseOverrides carries optional overrides collected via CLI/API commands.
type DatabaseOverrides struct {
	PairIndexBytes      *int
	PayloadCacheEntries *int
	PayloadCacheBytes   *int64
	AdaptivePairIndex   *bool
	PairListMaxBytes    *int
	PairListMaxFillPct  *int
}

const defaultPairListMaxBytes = 4096

func defaultConfig() Config {
	return Config{
		ListenAddr:          "0.0.0.0:4455",
		DataDir:             "cheetah_data",
		DefaultDatabase:     "default",
		TCPKeepAliveSeconds: 60,
		DatabaseDefaults: DatabaseConfig{
			PairIndexBytes:      1,
			PayloadCacheEntries: defaultPayloadCacheEntries,
			PayloadCacheBytes:   defaultPayloadCacheBytes,
			AdaptivePairIndex:   true,
			PairListMaxBytes:    defaultPairListMaxBytes,
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
		case "keepalive_seconds", "tcp_keepalive_seconds":
			cfg.TCPKeepAliveSeconds = parseIntAllowZero(val, cfg.TCPKeepAliveSeconds)
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
		case "adaptive_pair_index":
			cfg.DatabaseDefaults.AdaptivePairIndex = parseBool(val, cfg.DatabaseDefaults.AdaptivePairIndex)
		}
	case "tuning":
		switch key {
		case "max_pair_tables":
			if v := parsePositiveInt(val); v > 0 {
				cfg.MaxPairTables = v
			}
		case "pair_list_max_bytes":
			if v := parsePositiveInt(val); v > 0 {
				cfg.DatabaseDefaults.PairListMaxBytes = v
			}
		case "pair_list_max_fill_percent":
			cfg.DatabaseDefaults.PairListMaxFillPercent = parseIntAllowZero(val, cfg.DatabaseDefaults.PairListMaxFillPercent)
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

func parseBool(val string, fallback bool) bool {
	switch strings.ToLower(strings.TrimSpace(val)) {
	case "1", "true", "yes", "on", "enable", "enabled":
		return true
	case "0", "false", "no", "off", "disable", "disabled":
		return false
	default:
		return fallback
	}
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
	if v := parseIntAllowZero(os.Getenv("CHEETAH_TCP_KEEPALIVE_SECONDS"), cfg.TCPKeepAliveSeconds); v >= 0 {
		cfg.TCPKeepAliveSeconds = v
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
	if raw := strings.TrimSpace(os.Getenv("CHEETAH_ADAPTIVE_PAIR_INDEX")); raw != "" {
		cfg.DatabaseDefaults.AdaptivePairIndex = parseBool(raw, cfg.DatabaseDefaults.AdaptivePairIndex)
	}
	if v := parsePositiveInt(os.Getenv("CHEETAH_PAIR_LIST_MAX_BYTES")); v > 0 {
		cfg.DatabaseDefaults.PairListMaxBytes = v
	}
	if raw := strings.TrimSpace(os.Getenv("CHEETAH_PAIR_LIST_MAX_FILL_PERCENT")); raw != "" {
		cfg.DatabaseDefaults.PairListMaxFillPercent = parseIntAllowZero(raw, cfg.DatabaseDefaults.PairListMaxFillPercent)
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
	if cfg.DatabaseDefaults.PairListMaxBytes <= 0 {
		cfg.DatabaseDefaults.PairListMaxBytes = defaultPairListMaxBytes
	}
	if cfg.DatabaseDefaults.PairListMaxFillPercent < 0 || cfg.DatabaseDefaults.PairListMaxFillPercent > 100 {
		cfg.DatabaseDefaults.PairListMaxFillPercent = 0
	}
	if cfg.MaxPairTables < 0 {
		cfg.MaxPairTables = 0
	}
	if cfg.TCPKeepAliveSeconds < 0 {
		cfg.TCPKeepAliveSeconds = 0
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
	if override.AdaptivePairIndex != nil {
		result.AdaptivePairIndex = *override.AdaptivePairIndex
	}
	if override.PairListMaxBytes != nil {
		result.PairListMaxBytes = *override.PairListMaxBytes
	}
	if override.PairListMaxFillPct != nil {
		result.PairListMaxFillPercent = *override.PairListMaxFillPct
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
		case "adaptive_pair_index":
			overrides.AdaptivePairIndex = ptrBool(parseBool(val, true))
		case "pair_list_max_bytes":
			if v := parsePositiveInt(val); v > 0 {
				overrides.PairListMaxBytes = ptrInt(v)
			} else {
				return overrides, fmt.Errorf("pair_list_max_bytes must be >0")
			}
		case "pair_list_max_fill_percent":
			v := parseIntAllowZero(val, -1)
			if v < 0 || v > 100 {
				return overrides, fmt.Errorf("pair_list_max_fill_percent must be 0..100")
			}
			overrides.PairListMaxFillPct = ptrInt(v)
		default:
			return overrides, fmt.Errorf("unknown override %s", key)
		}
	}
	return overrides, nil
}

func ptrInt(v int) *int       { return &v }
func ptrInt64(v int64) *int64 { return &v }
func ptrBool(v bool) *bool    { return &v }

// --- impostazioni ad hoc per database ---------------------------------------
//
// Gli override di un database non vivono solo in memoria: vengono scritti in
// <data_dir>/<nome>/settings.ini e riletti a ogni apertura. Senza questo, un
// database creato con impostazioni proprie tornava alle impostazioni generali
// del server al primo riavvio — cioè le "impostazioni ad hoc" duravano quanto
// il processo. L'ordine di merge è: [database] del config.ini → settings.ini
// del database → override passati nel comando.
//
// Nota che la geometria del trie resta comunque decisa alla creazione e pinnata
// in pairs/format.dat (pair_format.go): quel marcatore vince su tutto in
// riapertura, e cambiarlo richiede un RESET_DB.

const databaseSettingsFile = "settings.ini"

func (ov DatabaseOverrides) isEmpty() bool {
	return ov.PairIndexBytes == nil &&
		ov.PayloadCacheEntries == nil &&
		ov.PayloadCacheBytes == nil &&
		ov.AdaptivePairIndex == nil &&
		ov.PairListMaxBytes == nil &&
		ov.PairListMaxFillPct == nil
}

// mergeDatabaseOverrides sovrappone extra a base, campo per campo.
func mergeDatabaseOverrides(base DatabaseOverrides, extra DatabaseOverrides) DatabaseOverrides {
	result := base
	if extra.PairIndexBytes != nil {
		result.PairIndexBytes = extra.PairIndexBytes
	}
	if extra.PayloadCacheEntries != nil {
		result.PayloadCacheEntries = extra.PayloadCacheEntries
	}
	if extra.PayloadCacheBytes != nil {
		result.PayloadCacheBytes = extra.PayloadCacheBytes
	}
	if extra.AdaptivePairIndex != nil {
		result.AdaptivePairIndex = extra.AdaptivePairIndex
	}
	if extra.PairListMaxBytes != nil {
		result.PairListMaxBytes = extra.PairListMaxBytes
	}
	if extra.PairListMaxFillPct != nil {
		result.PairListMaxFillPct = extra.PairListMaxFillPct
	}
	return result
}

// renderDatabaseOverrides rende gli override nella stessa forma che il comando
// accetta, così il file è anche la documentazione di come riprodurlo.
func renderDatabaseOverrides(ov DatabaseOverrides) []string {
	var lines []string
	if ov.PairIndexBytes != nil {
		lines = append(lines, fmt.Sprintf("pair_index_bytes=%d", *ov.PairIndexBytes))
	}
	if ov.AdaptivePairIndex != nil {
		lines = append(lines, fmt.Sprintf("adaptive_pair_index=%d", boolToInt(*ov.AdaptivePairIndex)))
	}
	if ov.PairListMaxBytes != nil {
		lines = append(lines, fmt.Sprintf("pair_list_max_bytes=%d", *ov.PairListMaxBytes))
	}
	if ov.PairListMaxFillPct != nil {
		lines = append(lines, fmt.Sprintf("pair_list_max_fill_percent=%d", *ov.PairListMaxFillPct))
	}
	if ov.PayloadCacheEntries != nil {
		lines = append(lines, fmt.Sprintf("payload_cache_entries=%d", *ov.PayloadCacheEntries))
	}
	if ov.PayloadCacheBytes != nil {
		lines = append(lines, fmt.Sprintf("payload_cache_bytes=%d", *ov.PayloadCacheBytes))
	}
	return lines
}

// loadDatabaseSettings legge il file di impostazioni di un database. Un file
// assente non è un errore: significa "nessun override, valgono le generali".
func loadDatabaseSettings(path string) (DatabaseOverrides, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return DatabaseOverrides{}, nil
		}
		return DatabaseOverrides{}, err
	}
	var tokens []string
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		tokens = append(tokens, line)
	}
	return parseDatabaseOverrideTokens(tokens)
}

func saveDatabaseSettings(path string, ov DatabaseOverrides) error {
	lines := renderDatabaseOverrides(ov)
	if len(lines) == 0 {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	body := "# cheetah-db: impostazioni ad hoc di questo database.\n" +
		"# Sovrascrivono la sezione [database] di config.ini a ogni apertura.\n" +
		strings.Join(lines, "\n") + "\n"
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(body), 0644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// databaseSettingTokens è la forma "k=v" ordinata delle impostazioni efficaci,
// usata nelle risposte dei comandi di scope engine.
func databaseSettingTokens(cfg DatabaseConfig) []string {
	return []string{
		fmt.Sprintf("pair_index_bytes=%d", cfg.PairIndexBytes),
		fmt.Sprintf("adaptive_pair_index=%d", boolToInt(cfg.AdaptivePairIndex)),
		fmt.Sprintf("pair_list_max_bytes=%d", cfg.PairListMaxBytes),
		fmt.Sprintf("pair_list_max_fill_percent=%d", cfg.PairListMaxFillPercent),
		fmt.Sprintf("payload_cache_entries=%d", cfg.PayloadCacheEntries),
		fmt.Sprintf("payload_cache_bytes=%d", cfg.PayloadCacheBytes),
	}
}

func databaseSettingMap(cfg DatabaseConfig) map[string]any {
	return map[string]any{
		"pair_index_bytes":           cfg.PairIndexBytes,
		"adaptive_pair_index":        cfg.AdaptivePairIndex,
		"pair_list_max_bytes":        cfg.PairListMaxBytes,
		"pair_list_max_fill_percent": cfg.PairListMaxFillPercent,
		"payload_cache_entries":      cfg.PayloadCacheEntries,
		"payload_cache_bytes":        cfg.PayloadCacheBytes,
	}
}

// validateDatabaseName tiene il nome dentro una singola cartella di data_dir.
// Un nome è un pezzo di percorso: senza questo controllo "../../etc" apriva un
// database fuori da data_dir.
func validateDatabaseName(name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("missing database name")
	}
	if len(name) > maxDatabaseNameLen {
		return fmt.Errorf("database name too long")
	}
	if name == "." || name == ".." {
		return fmt.Errorf("invalid database name %q", name)
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '_' || r == '-' || r == '.':
		default:
			return fmt.Errorf("invalid database name %q", name)
		}
	}
	return nil
}

const maxDatabaseNameLen = 64

func parseDatabaseTarget(arg string) (string, *DatabaseOverrides, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return "", nil, fmt.Errorf("missing database name")
	}
	tokens := strings.Fields(arg)
	name := tokens[0]
	if err := validateDatabaseName(name); err != nil {
		return "", nil, err
	}
	if len(tokens) == 1 {
		return name, nil, nil
	}
	overrides, err := parseDatabaseOverrideTokens(tokens[1:])
	if err != nil {
		return "", nil, err
	}
	return name, &overrides, nil
}
