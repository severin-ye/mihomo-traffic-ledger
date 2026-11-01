package main

import (
	"bytes"
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
)

//go:embed LICENSE web/*
var webAssets embed.FS

const (
	defaultListenAddr      = "127.0.0.1:18080"
	defaultPollInterval    = time.Second
	aggregateFlushInterval = 10 * time.Minute
	summaryBucketSize      = int64(24 * time.Hour / time.Millisecond)
	summaryBuildInterval   = 10 * time.Minute
	defaultRetentionDays   = 30
	defaultAllowedOrigin   = "*"
)

var isContainerRuntime = func() bool {
	_, err := os.Stat("/.dockerenv")
	return err == nil
}

type config struct {
	ListenAddr   string
	MihomoURL    string
	MihomoSecret string
}

type mihomoSettings struct {
	URL    string `json:"url"`
	Secret string `json:"secret"`
}

type autoSwitchSettings struct {
	Enabled                 bool                    `json:"enabled"`
	ThresholdBytesPerMinute int64                   `json:"thresholdBytesPerMinute"`
	CooldownSeconds         int64                   `json:"cooldownSeconds"`
	RestoreEnabled          bool                    `json:"restoreEnabled"`
	RestoreQuietMinutes     int64                   `json:"restoreQuietMinutes"`
	GroupTargets            []autoSwitchGroupTarget `json:"groupTargets"`
}

type autoSwitchGroupTarget struct {
	GroupName   string `json:"groupName"`
	TargetProxy string `json:"targetProxy"`
	Enabled     bool   `json:"enabled"`
}

type autoSwitchExecutionResult struct {
	GroupName   string `json:"groupName"`
	TargetProxy string `json:"targetProxy"`
	Status      string `json:"status"`
	Message     string `json:"message,omitempty"`
}

type autoSwitchEvent struct {
	ID          int64                       `json:"id"`
	TriggeredAt int64                       `json:"triggeredAt"`
	Host        string                      `json:"host"`
	TotalBytes  int64                       `json:"totalBytes"`
	WindowStart int64                       `json:"windowStart"`
	WindowEnd   int64                       `json:"windowEnd"`
	Results     []autoSwitchExecutionResult `json:"results"`
	Error       string                      `json:"error"`
}

type autoRestoreSession struct {
	GroupName          string `json:"groupName"`
	OriginalProxy      string `json:"originalProxy"`
	CurrentProxy       string `json:"currentProxy"`
	LastTriggeredAt    int64  `json:"lastTriggeredAt"`
	LastTriggeredHost  string `json:"lastTriggeredHost"`
	LastTriggeredBytes int64  `json:"lastTriggeredBytes"`
}

type controllableProxyGroup struct {
	Name string   `json:"name"`
	Type string   `json:"type"`
	Now  string   `json:"now"`
	All  []string `json:"all"`
}

type trafficLog struct {
	Timestamp     int64    `json:"timestamp"`
	SourceIP      string   `json:"sourceIP"`
	Host          string   `json:"host"`
	DestinationIP string   `json:"destinationIP"`
	Process       string   `json:"process"`
	ProcessPath   string   `json:"processPath"`
	RouteType     string   `json:"routeType"`
	Outbound      string   `json:"outbound"`
	Chains        []string `json:"chains"`
	Rule          string   `json:"rule"`
	RulePayload   string   `json:"rulePayload"`
	Upload        int64    `json:"upload"`
	Download      int64    `json:"download"`
}

type aggregatedData struct {
	Label    string `json:"label"`
	Upload   int64  `json:"upload"`
	Download int64  `json:"download"`
	Total    int64  `json:"total"`
	Count    int64  `json:"count"`
}

type trendPoint struct {
	Timestamp int64 `json:"timestamp"`
	Upload    int64 `json:"upload"`
	Download  int64 `json:"download"`
}

type connectionDetail struct {
	DestinationIP string   `json:"destinationIP"`
	SourceIP      string   `json:"sourceIP"`
	Process       string   `json:"process"`
	Outbound      string   `json:"outbound"`
	Chains        []string `json:"chains"`
	Upload        int64    `json:"upload"`
	Download      int64    `json:"download"`
	Total         int64    `json:"total"`
	Count         int64    `json:"count"`
}

type connection struct {
	ID              string   `json:"id"`
	Start           string   `json:"start"`
	Upload          int64    `json:"upload"`
	Download        int64    `json:"download"`
	Chains          []string `json:"chains"`
	Rule            string   `json:"rule"`
	RulePayload     string   `json:"rulePayload"`
	Network         string   `json:"-"`
	ConnType        string   `json:"-"`
	SourcePort      string   `json:"-"`
	DestinationPort string   `json:"-"`
	ProcessPath     string   `json:"-"`
	Metadata        struct {
		SourceIP      string `json:"sourceIP"`
		Host          string `json:"host"`
		DestinationIP string `json:"destinationIP"`
		Process       string `json:"process"`
	} `json:"metadata"`
}

func (c *connection) UnmarshalJSON(data []byte) error {
	type connectionAlias connection
	var decoded struct {
		connectionAlias
		Metadata struct {
			Network         string `json:"network"`
			ConnType        string `json:"type"`
			SourceIP        string `json:"sourceIP"`
			SourcePort      string `json:"sourcePort"`
			Host            string `json:"host"`
			DestinationIP   string `json:"destinationIP"`
			DestinationPort string `json:"destinationPort"`
			Process         string `json:"process"`
			ProcessPath     string `json:"processPath"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*c = connection(decoded.connectionAlias)
	c.Network = decoded.Metadata.Network
	c.ConnType = decoded.Metadata.ConnType
	c.SourcePort = decoded.Metadata.SourcePort
	c.DestinationPort = decoded.Metadata.DestinationPort
	c.ProcessPath = decoded.Metadata.ProcessPath
	c.Metadata.SourceIP = decoded.Metadata.SourceIP
	c.Metadata.Host = decoded.Metadata.Host
	c.Metadata.DestinationIP = decoded.Metadata.DestinationIP
	c.Metadata.Process = decoded.Metadata.Process
	return nil
}

type connectionsResponse struct {
	Connections   []connection `json:"connections"`
	UploadTotal   int64        `json:"uploadTotal"`
	DownloadTotal int64        `json:"downloadTotal"`
}

type mihomoProxyInfo struct {
	Type string   `json:"type"`
	Now  string   `json:"now"`
	All  []string `json:"all"`
}

type mihomoProxiesResponse struct {
	Proxies map[string]mihomoProxyInfo `json:"proxies"`
}

type service struct {
	db                      *sql.DB
	client                  *http.Client
	transportMu             sync.Mutex
	mihomoTransportEndpoint string
	mihomoTransportBase     string
	mihomoTransportClient   *http.Client
	now                     func() time.Time
	cfg                     config
	mu                      sync.Mutex
	mihomoSettings          mihomoSettings
	domainGroupingEnabled   bool
	lastConnections         map[string]connection
	activeSessionKeys       map[string]string
	monitorStartedAt        int64
	lastUploadTotal         int64
	lastDownloadTotal       int64
	lastAutoSwitchAt        int64
	lastAutoSwitchMin       int64
	lastCleanup             time.Time
	lastVacuum              time.Time
	lastSummaryBuild        time.Time
	aggregateBuffer         map[string]*aggregatedEntry
	hostMinuteWindows       map[string]*hostTrafficWindow
	aggregateRetentionDays  int
}

type aggregatedEntry struct {
	BucketStart   int64
	BucketEnd     int64
	SourceIP      string
	Host          string
	DestinationIP string
	Process       string
	ProcessPath   string
	RouteType     string
	Outbound      string
	Chains        string
	Rule          string
	RulePayload   string
	Upload        int64
	Download      int64
	Count         int64
}

type hostTrafficWindow struct {
	BucketStart int64
	BucketEnd   int64
	TotalBytes  int64
}

func main() {
	if isShutdownCommand(os.Args) {
		if err := requestExistingShutdown(); err != nil {
			log.Printf("request existing shutdown: %v", err)
		}
		return
	}
	releaseInstance, alreadyRunning, err := acquireSingleInstance()
	if err != nil {
		log.Fatalf("single instance: %v", err)
	}
	if alreadyRunning {
		if err := handleAlreadyRunning(true); err != nil {
			log.Printf("open existing dashboard: %v", err)
		}
		return
	}
	defer releaseInstance()
	closeLog, err := setupFileLogging()
	if err != nil {
		log.Printf("setup file logging: %v", err)
	} else {
		defer closeLog()
	}

	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	db, err := openDatabase(defaultDatabasePath())
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer db.Close()

	runtimeSettings, err := resolveMihomoSettings(db, cfg.MihomoURL, cfg.MihomoSecret)
	if err != nil {
		log.Fatalf("resolve mihomo settings: %v", err)
	}

	domainGroupingEnabled := loadDomainGroupingEnabled(db)

	svc := &service{
		db:                     db,
		client:                 &http.Client{Timeout: 10 * time.Second},
		now:                    time.Now,
		cfg:                    cfg,
		mihomoSettings:         runtimeSettings,
		domainGroupingEnabled:  domainGroupingEnabled,
		aggregateRetentionDays: loadRetentionDays(db),
		lastConnections:        make(map[string]connection),
		monitorStartedAt:       time.Now().UnixMilli(),
		lastVacuum:             time.Time{},
		aggregateBuffer:        make(map[string]*aggregatedEntry),
		hostMinuteWindows:      make(map[string]*hostTrafficWindow),
	}

	defer svc.closeMihomoIdleConnections()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	shutdownRequests, closeShutdownWatcher, err := watchShutdownRequests()
	if err != nil {
		log.Fatalf("watch shutdown requests: %v", err)
	}
	defer closeShutdownWatcher()

	collectorDone := make(chan struct{})
	go func() {
		defer close(collectorDone)
		svc.runCollector(ctx)
	}()

	server := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           svc.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("traffic monitor listening on %s", cfg.ListenAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	quitCh := make(chan struct{}, 1)
	shellDone := make(chan error, 1)
	go func() {
		shellDone <- runDesktopShell(ctx, func() {
			select {
			case quitCh <- struct{}{}:
			default:
			}
		})
	}()

	select {
	case <-sigCh:
	case <-quitCh:
	case <-shutdownRequests:
	case err := <-shellDone:
		if err != nil {
			log.Printf("desktop tray: %v", err)
		}
	}

	cancel()
	select {
	case <-collectorDone:
	case <-time.After(5 * time.Second):
		log.Printf("collector shutdown timed out")
	}
	if err := svc.closeObservedSessions(time.Now()); err != nil {
		log.Printf("close active connection sessions on shutdown: %v", err)
	}
	if err := svc.flushAggregateBuffer(); err != nil {
		log.Printf("flush aggregate buffer on shutdown: %v", err)
	}
	if err := backfillSummary(svc.db, time.Now().UnixMilli()); err != nil {
		log.Printf("build traffic summary on shutdown: %v", err)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown server: %v", err)
	}
}

func loadConfig() (config, error) {
	cfg := config{
		ListenAddr:   getenv("TRAFFIC_MONITOR_LISTEN", defaultListenAddr),
		MihomoURL:    strings.TrimRight(getenv("MIHOMO_URL", ""), "/"),
		MihomoSecret: getenv("MIHOMO_SECRET", ""),
	}

	return cfg, nil
}

func defaultDatabasePath() string {
	if isContainerRuntime() {
		return "/data/traffic_monitor.db"
	}
	if strings.TrimSpace(os.Getenv("LOCALAPPDATA")) != "" {
		return filepath.Join(userDataRoot(), "data", "traffic_monitor.db")
	}
	return "./data/traffic_monitor.db"
}

func openDatabase(path string) (*sql.DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}

	schema := `
	PRAGMA journal_mode=WAL;
	PRAGMA busy_timeout=5000;

	CREATE TABLE IF NOT EXISTS traffic_logs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		timestamp INTEGER NOT NULL,
		source_ip TEXT NOT NULL,
		host TEXT NOT NULL,
		destination_ip TEXT NOT NULL DEFAULT '',
		process TEXT NOT NULL,
		outbound TEXT NOT NULL,
		chains TEXT NOT NULL DEFAULT '[]',
		upload INTEGER NOT NULL,
		download INTEGER NOT NULL
	);

	CREATE TABLE IF NOT EXISTS traffic_aggregated (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		bucket_start INTEGER NOT NULL,
		bucket_end INTEGER NOT NULL,
		source_ip TEXT NOT NULL,
		host TEXT NOT NULL,
		destination_ip TEXT NOT NULL DEFAULT '',
		process TEXT NOT NULL,
		outbound TEXT NOT NULL,
		chains TEXT NOT NULL DEFAULT '[]',
		upload INTEGER NOT NULL,
		download INTEGER NOT NULL,
		count INTEGER NOT NULL,
		UNIQUE(bucket_start, bucket_end, source_ip, host, destination_ip, process, outbound, chains)
	);

	CREATE INDEX IF NOT EXISTS idx_traffic_logs_timestamp ON traffic_logs(timestamp);
	CREATE INDEX IF NOT EXISTS idx_traffic_logs_source_ip ON traffic_logs(source_ip);
	CREATE INDEX IF NOT EXISTS idx_traffic_logs_host ON traffic_logs(host);
	CREATE INDEX IF NOT EXISTS idx_traffic_logs_process ON traffic_logs(process);
	CREATE INDEX IF NOT EXISTS idx_traffic_logs_outbound ON traffic_logs(outbound);

	CREATE INDEX IF NOT EXISTS idx_traffic_aggregated_bucket ON traffic_aggregated(bucket_start, bucket_end);
	CREATE INDEX IF NOT EXISTS idx_traffic_aggregated_source_ip ON traffic_aggregated(source_ip);
	CREATE INDEX IF NOT EXISTS idx_traffic_aggregated_host ON traffic_aggregated(host);
	CREATE INDEX IF NOT EXISTS idx_traffic_aggregated_process ON traffic_aggregated(process);
	CREATE INDEX IF NOT EXISTS idx_traffic_aggregated_outbound ON traffic_aggregated(outbound);

	CREATE TABLE IF NOT EXISTS traffic_summary (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		bucket_start INTEGER NOT NULL,
		bucket_end INTEGER NOT NULL,
		dimension TEXT NOT NULL,
		label TEXT NOT NULL,
		secondary_label TEXT NOT NULL DEFAULT '',
		upload INTEGER NOT NULL,
		download INTEGER NOT NULL,
		count INTEGER NOT NULL,
		UNIQUE(bucket_start, bucket_end, dimension, label, secondary_label)
	);

	CREATE INDEX IF NOT EXISTS idx_traffic_summary_bucket ON traffic_summary(bucket_start, bucket_end);
	CREATE INDEX IF NOT EXISTS idx_traffic_summary_dimension_label ON traffic_summary(dimension, label, bucket_start, bucket_end);
	CREATE INDEX IF NOT EXISTS idx_traffic_summary_secondary_label ON traffic_summary(dimension, secondary_label, bucket_start, bucket_end);

	CREATE TABLE IF NOT EXISTS connection_sessions (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		session_key TEXT NOT NULL UNIQUE,
		connection_id TEXT NOT NULL,
		mihomo_started_at TEXT NOT NULL DEFAULT '',
		started_at INTEGER NOT NULL,
		first_seen_at INTEGER NOT NULL,
		last_seen_at INTEGER NOT NULL,
		ended_at INTEGER,
		network TEXT NOT NULL DEFAULT '',
		connection_type TEXT NOT NULL DEFAULT '',
		source_ip TEXT NOT NULL DEFAULT '',
		source_port TEXT NOT NULL DEFAULT '',
		destination_ip TEXT NOT NULL DEFAULT '',
		destination_port TEXT NOT NULL DEFAULT '',
		host TEXT NOT NULL DEFAULT '',
		process TEXT NOT NULL DEFAULT '',
		process_path TEXT NOT NULL DEFAULT '',
		route_type TEXT NOT NULL DEFAULT 'PROXY',
		outbound TEXT NOT NULL DEFAULT '',
		chains TEXT NOT NULL DEFAULT '[]',
		rule TEXT NOT NULL DEFAULT '',
		rule_payload TEXT NOT NULL DEFAULT '',
		upload INTEGER NOT NULL DEFAULT 0,
		download INTEGER NOT NULL DEFAULT 0
	);

	CREATE INDEX IF NOT EXISTS idx_connection_sessions_started ON connection_sessions(started_at);
	CREATE INDEX IF NOT EXISTS idx_connection_sessions_process ON connection_sessions(process, started_at);
	CREATE INDEX IF NOT EXISTS idx_connection_sessions_outbound ON connection_sessions(outbound, started_at);
	CREATE INDEX IF NOT EXISTS idx_connection_sessions_route_type ON connection_sessions(route_type, started_at);

	CREATE TABLE IF NOT EXISTS traffic_rollups (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		granularity TEXT NOT NULL,
		bucket_start INTEGER NOT NULL,
		bucket_end INTEGER NOT NULL,
		process TEXT NOT NULL DEFAULT '',
		process_path TEXT NOT NULL DEFAULT '',
		host TEXT NOT NULL DEFAULT '',
		destination_ip TEXT NOT NULL DEFAULT '',
		route_type TEXT NOT NULL DEFAULT 'PROXY',
		outbound TEXT NOT NULL DEFAULT '',
		chains TEXT NOT NULL DEFAULT '[]',
		rule TEXT NOT NULL DEFAULT '',
		rule_payload TEXT NOT NULL DEFAULT '',
		upload INTEGER NOT NULL DEFAULT 0,
		download INTEGER NOT NULL DEFAULT 0,
		count INTEGER NOT NULL DEFAULT 0,
		UNIQUE(granularity, bucket_start, process, process_path, host, destination_ip, route_type, outbound, chains, rule, rule_payload)
	);

	CREATE INDEX IF NOT EXISTS idx_traffic_rollups_bucket ON traffic_rollups(granularity, bucket_start, bucket_end);
	CREATE INDEX IF NOT EXISTS idx_traffic_rollups_process ON traffic_rollups(granularity, process, bucket_start);
	CREATE INDEX IF NOT EXISTS idx_traffic_rollups_outbound ON traffic_rollups(granularity, outbound, bucket_start);
	CREATE INDEX IF NOT EXISTS idx_traffic_rollups_route ON traffic_rollups(granularity, route_type, bucket_start);

	CREATE TABLE IF NOT EXISTS app_settings (
		key TEXT PRIMARY KEY,
		value TEXT NOT NULL DEFAULT ''
	);

	CREATE TABLE IF NOT EXISTS auto_switch_group_targets (
		group_name TEXT PRIMARY KEY,
		target_proxy TEXT NOT NULL,
		enabled INTEGER NOT NULL DEFAULT 0
	);

	CREATE TABLE IF NOT EXISTS auto_switch_events (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		triggered_at INTEGER NOT NULL,
		host TEXT NOT NULL,
		total_bytes INTEGER NOT NULL,
		window_start INTEGER NOT NULL,
		window_end INTEGER NOT NULL,
		results_json TEXT NOT NULL DEFAULT '[]',
		error TEXT NOT NULL DEFAULT ''
	);

	CREATE TABLE IF NOT EXISTS auto_restore_sessions (
		group_name TEXT PRIMARY KEY,
		original_proxy TEXT NOT NULL,
		current_proxy TEXT NOT NULL,
		last_triggered_at INTEGER NOT NULL,
		last_triggered_host TEXT NOT NULL DEFAULT '',
		last_triggered_bytes INTEGER NOT NULL DEFAULT 0
	);

	CREATE INDEX IF NOT EXISTS idx_auto_switch_events_triggered_at ON auto_switch_events(triggered_at DESC);
	`

	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, err
	}

	if err := migrateTrafficAggregatedLedgerSchema(db); err != nil {
		db.Close()
		return nil, err
	}

	for _, stmt := range []string{
		`ALTER TABLE traffic_logs ADD COLUMN destination_ip TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE traffic_logs ADD COLUMN chains TEXT NOT NULL DEFAULT '[]'`,
	} {
		if _, err := db.Exec(stmt); err != nil && !strings.Contains(err.Error(), "duplicate column name") {
			db.Close()
			return nil, err
		}
	}

	currentBucketStart := (time.Now().UnixMilli() / 60000) * 60000
	if err := backfillAggregatedLogs(db, currentBucketStart); err != nil {
		db.Close()
		return nil, err
	}
	if err := backfillSummary(db, time.Now().UnixMilli()); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

func migrateTrafficAggregatedLedgerSchema(db *sql.DB) error {
	rows, err := db.Query(`PRAGMA table_info(traffic_aggregated)`)
	if err != nil {
		return err
	}
	columns := make(map[string]bool)
	for rows.Next() {
		var cid, notNull, pk int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &pk); err != nil {
			rows.Close()
			return err
		}
		columns[name] = true
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if columns["process_path"] && columns["route_type"] && columns["rule"] && columns["rule_payload"] {
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`
		CREATE TABLE traffic_aggregated_ledger (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			bucket_start INTEGER NOT NULL,
			bucket_end INTEGER NOT NULL,
			source_ip TEXT NOT NULL,
			host TEXT NOT NULL,
			destination_ip TEXT NOT NULL DEFAULT '',
			process TEXT NOT NULL,
			process_path TEXT NOT NULL DEFAULT '',
			route_type TEXT NOT NULL DEFAULT 'PROXY',
			outbound TEXT NOT NULL,
			chains TEXT NOT NULL DEFAULT '[]',
			rule TEXT NOT NULL DEFAULT '',
			rule_payload TEXT NOT NULL DEFAULT '',
			upload INTEGER NOT NULL,
			download INTEGER NOT NULL,
			count INTEGER NOT NULL,
			UNIQUE(bucket_start, bucket_end, source_ip, host, destination_ip, process, process_path, route_type, outbound, chains, rule, rule_payload)
		);
		INSERT INTO traffic_aggregated_ledger
			(bucket_start, bucket_end, source_ip, host, destination_ip, process, process_path, route_type, outbound, chains, rule, rule_payload, upload, download, count)
		SELECT bucket_start, bucket_end, source_ip, host, destination_ip, process, '',
			CASE WHEN UPPER(outbound) LIKE 'REJECT%' THEN 'REJECT' WHEN UPPER(outbound) = 'DIRECT' THEN 'DIRECT' ELSE 'PROXY' END,
			outbound, chains, '', '', upload, download, count
		FROM traffic_aggregated;
		DROP TABLE traffic_aggregated;
		ALTER TABLE traffic_aggregated_ledger RENAME TO traffic_aggregated;
		CREATE INDEX idx_traffic_aggregated_bucket ON traffic_aggregated(bucket_start, bucket_end);
		CREATE INDEX idx_traffic_aggregated_source_ip ON traffic_aggregated(source_ip);
		CREATE INDEX idx_traffic_aggregated_host ON traffic_aggregated(host);
		CREATE INDEX idx_traffic_aggregated_process ON traffic_aggregated(process);
		CREATE INDEX idx_traffic_aggregated_outbound ON traffic_aggregated(outbound);
		CREATE INDEX idx_traffic_aggregated_route_type ON traffic_aggregated(route_type);
	`); err != nil {
		return err
	}
	return tx.Commit()
}

func routeType(chains []string) string {
	for _, chain := range chains {
		switch strings.ToUpper(strings.TrimSpace(chain)) {
		case "REJECT", "REJECT-DROP":
			return "REJECT"
		case "DIRECT":
			return "DIRECT"
		}
	}
	return "PROXY"
}

func normalizeMihomoSettings(settings mihomoSettings) mihomoSettings {
	settings.URL = strings.TrimRight(strings.TrimSpace(settings.URL), "/")
	settings.Secret = strings.TrimSpace(settings.Secret)
	return settings
}

func loadMihomoSettings(db *sql.DB) (mihomoSettings, error) {
	rows, err := db.Query(`SELECT key, value FROM app_settings WHERE key IN ('mihomo_url', 'mihomo_secret')`)
	if err != nil {
		return mihomoSettings{}, err
	}
	defer rows.Close()

	var settings mihomoSettings
	for rows.Next() {
		var (
			key   string
			value string
		)
		if err := rows.Scan(&key, &value); err != nil {
			return mihomoSettings{}, err
		}
		switch key {
		case "mihomo_url":
			settings.URL = value
		case "mihomo_secret":
			settings.Secret = value
		}
	}
	if err := rows.Err(); err != nil {
		return mihomoSettings{}, err
	}

	return normalizeMihomoSettings(settings), nil
}

func saveMihomoSettings(db *sql.DB, settings mihomoSettings) error {
	settings = normalizeMihomoSettings(settings)

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	for key, value := range map[string]string{
		"mihomo_url":    settings.URL,
		"mihomo_secret": settings.Secret,
	} {
		if _, err := tx.Exec(`
			INSERT INTO app_settings (key, value)
			VALUES (?, ?)
			ON CONFLICT(key) DO UPDATE SET value = excluded.value
		`, key, value); err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

func loadDomainGroupingEnabled(db *sql.DB) bool {
	var value string
	err := db.QueryRow(`SELECT value FROM app_settings WHERE key = 'domain_grouping_enabled'`).Scan(&value)
	if err != nil {
		return true // default on
	}
	return value == "true"
}

func saveDomainGroupingEnabled(db *sql.DB, enabled bool) error {
	value := "false"
	if enabled {
		value = "true"
	}
	_, err := db.Exec(`
		INSERT INTO app_settings (key, value)
		VALUES ('domain_grouping_enabled', ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value
	`, value)
	return err
}

func loadRetentionDays(db *sql.DB) int {
	var value string
	err := db.QueryRow(`SELECT value FROM app_settings WHERE key = 'aggregate_retention_days'`).Scan(&value)
	if err != nil {
		return defaultRetentionDays
	}
	days, err := strconv.Atoi(value)
	if err != nil || days < 1 {
		return defaultRetentionDays
	}
	return days
}

func saveRetentionDays(db *sql.DB, days int) error {
	_, err := db.Exec(`
		INSERT INTO app_settings (key, value)
		VALUES ('aggregate_retention_days', ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value
	`, strconv.Itoa(days))
	return err
}

func resolveMihomoSettings(db *sql.DB, envURL, envSecret string) (mihomoSettings, error) {
	stored, err := loadMihomoSettings(db)
	if err != nil {
		return mihomoSettings{}, err
	}

	resolved := stored
	if strings.TrimSpace(envURL) != "" {
		resolved.URL = envURL
	}
	if strings.TrimSpace(envSecret) != "" {
		resolved.Secret = envSecret
	}
	resolved = normalizeMihomoSettings(resolved)
	if resolved.URL == "" {
		resolved.URL = defaultMihomoEndpoint()
	}

	if strings.TrimSpace(envURL) != "" || strings.TrimSpace(envSecret) != "" || stored.URL == "" {
		if err := saveMihomoSettings(db, resolved); err != nil {
			return mihomoSettings{}, err
		}
	}

	return resolved, nil
}

func loadAutoSwitchSettings(db *sql.DB) (autoSwitchSettings, error) {
	rows, err := db.Query(`
		SELECT key, value
		FROM app_settings
		WHERE key IN (
			'auto_switch_enabled',
			'auto_switch_threshold_bytes_per_minute',
			'auto_switch_cooldown_seconds',
			'auto_restore_enabled',
			'auto_restore_quiet_minutes'
		)
	`)
	if err != nil {
		return autoSwitchSettings{}, err
	}
	defer rows.Close()

	settings := autoSwitchSettings{}
	for rows.Next() {
		var key string
		var value string
		if err := rows.Scan(&key, &value); err != nil {
			return autoSwitchSettings{}, err
		}

		switch key {
		case "auto_switch_enabled":
			settings.Enabled = value == "1"
		case "auto_switch_threshold_bytes_per_minute":
			settings.ThresholdBytesPerMinute, err = strconv.ParseInt(value, 10, 64)
			if err != nil {
				return autoSwitchSettings{}, err
			}
		case "auto_switch_cooldown_seconds":
			settings.CooldownSeconds, err = strconv.ParseInt(value, 10, 64)
			if err != nil {
				return autoSwitchSettings{}, err
			}
		case "auto_restore_enabled":
			settings.RestoreEnabled = value == "1"
		case "auto_restore_quiet_minutes":
			settings.RestoreQuietMinutes, err = strconv.ParseInt(value, 10, 64)
			if err != nil {
				return autoSwitchSettings{}, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return autoSwitchSettings{}, err
	}

	groupRows, err := db.Query(`
		SELECT group_name, target_proxy, enabled
		FROM auto_switch_group_targets
		ORDER BY rowid ASC
	`)
	if err != nil {
		return autoSwitchSettings{}, err
	}
	defer groupRows.Close()

	for groupRows.Next() {
		var target autoSwitchGroupTarget
		var enabled int
		if err := groupRows.Scan(&target.GroupName, &target.TargetProxy, &enabled); err != nil {
			return autoSwitchSettings{}, err
		}
		target.Enabled = enabled == 1
		settings.GroupTargets = append(settings.GroupTargets, target)
	}
	if err := groupRows.Err(); err != nil {
		return autoSwitchSettings{}, err
	}

	return settings, nil
}

func saveAutoSwitchSettings(db *sql.DB, settings autoSwitchSettings) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	values := map[string]string{
		"auto_switch_enabled":                    "0",
		"auto_switch_threshold_bytes_per_minute": strconv.FormatInt(settings.ThresholdBytesPerMinute, 10),
		"auto_switch_cooldown_seconds":           strconv.FormatInt(settings.CooldownSeconds, 10),
		"auto_restore_enabled":                   "0",
		"auto_restore_quiet_minutes":             strconv.FormatInt(settings.RestoreQuietMinutes, 10),
	}
	if settings.Enabled {
		values["auto_switch_enabled"] = "1"
	}
	if settings.RestoreEnabled {
		values["auto_restore_enabled"] = "1"
	}

	for key, value := range values {
		if _, err := tx.Exec(`
			INSERT INTO app_settings (key, value)
			VALUES (?, ?)
			ON CONFLICT(key) DO UPDATE SET value = excluded.value
		`, key, value); err != nil {
			tx.Rollback()
			return err
		}
	}

	if _, err := tx.Exec(`DELETE FROM auto_switch_group_targets`); err != nil {
		tx.Rollback()
		return err
	}

	stmt, err := tx.Prepare(`
		INSERT INTO auto_switch_group_targets (group_name, target_proxy, enabled)
		VALUES (?, ?, ?)
	`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()

	for _, target := range settings.GroupTargets {
		enabled := 0
		if target.Enabled {
			enabled = 1
		}
		if _, err := stmt.Exec(target.GroupName, target.TargetProxy, enabled); err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

func insertAutoSwitchEvent(db *sql.DB, event autoSwitchEvent) error {
	resultsJSON, err := json.Marshal(event.Results)
	if err != nil {
		return err
	}

	_, err = db.Exec(`
		INSERT INTO auto_switch_events
		(triggered_at, host, total_bytes, window_start, window_end, results_json, error)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, event.TriggeredAt, event.Host, event.TotalBytes, event.WindowStart, event.WindowEnd, string(resultsJSON), event.Error)
	return err
}

func upsertAutoRestoreSession(db *sql.DB, session autoRestoreSession) error {
	_, err := db.Exec(`
		INSERT INTO auto_restore_sessions
		(group_name, original_proxy, current_proxy, last_triggered_at, last_triggered_host, last_triggered_bytes)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(group_name) DO UPDATE SET
			original_proxy = excluded.original_proxy,
			current_proxy = excluded.current_proxy,
			last_triggered_at = excluded.last_triggered_at,
			last_triggered_host = excluded.last_triggered_host,
			last_triggered_bytes = excluded.last_triggered_bytes
	`, session.GroupName, session.OriginalProxy, session.CurrentProxy, session.LastTriggeredAt, session.LastTriggeredHost, session.LastTriggeredBytes)
	return err
}

func listAutoRestoreSessions(db *sql.DB) ([]autoRestoreSession, error) {
	rows, err := db.Query(`
		SELECT group_name, original_proxy, current_proxy, last_triggered_at, last_triggered_host, last_triggered_bytes
		FROM auto_restore_sessions
		ORDER BY group_name ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	sessions := make([]autoRestoreSession, 0)
	for rows.Next() {
		var session autoRestoreSession
		if err := rows.Scan(
			&session.GroupName,
			&session.OriginalProxy,
			&session.CurrentProxy,
			&session.LastTriggeredAt,
			&session.LastTriggeredHost,
			&session.LastTriggeredBytes,
		); err != nil {
			return nil, err
		}
		sessions = append(sessions, session)
	}

	return sessions, rows.Err()
}

func deleteAutoRestoreSession(db *sql.DB, groupName string) error {
	_, err := db.Exec(`DELETE FROM auto_restore_sessions WHERE group_name = ?`, groupName)
	return err
}

func getAutoRestoreSession(db *sql.DB, groupName string) (autoRestoreSession, bool, error) {
	row := db.QueryRow(`
		SELECT group_name, original_proxy, current_proxy, last_triggered_at, last_triggered_host, last_triggered_bytes
		FROM auto_restore_sessions
		WHERE group_name = ?
	`, groupName)

	var session autoRestoreSession
	if err := row.Scan(
		&session.GroupName,
		&session.OriginalProxy,
		&session.CurrentProxy,
		&session.LastTriggeredAt,
		&session.LastTriggeredHost,
		&session.LastTriggeredBytes,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return autoRestoreSession{}, false, nil
		}
		return autoRestoreSession{}, false, err
	}

	return session, true, nil
}

func listAutoSwitchEvents(db *sql.DB, limit int) ([]autoSwitchEvent, error) {
	if limit <= 0 {
		limit = 50
	}

	rows, err := db.Query(`
		SELECT id, triggered_at, host, total_bytes, window_start, window_end, results_json, error
		FROM auto_switch_events
		ORDER BY triggered_at DESC, id DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events := make([]autoSwitchEvent, 0)
	for rows.Next() {
		var event autoSwitchEvent
		var resultsJSON string
		if err := rows.Scan(
			&event.ID,
			&event.TriggeredAt,
			&event.Host,
			&event.TotalBytes,
			&event.WindowStart,
			&event.WindowEnd,
			&resultsJSON,
			&event.Error,
		); err != nil {
			return nil, err
		}

		if resultsJSON != "" {
			if err := json.Unmarshal([]byte(resultsJSON), &event.Results); err != nil {
				return nil, err
			}
		}

		events = append(events, event)
	}

	return events, rows.Err()
}

func normalizeProxyGroupType(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "selector", "select":
		return "select"
	case "fallback":
		return "fallback"
	default:
		return ""
	}
}

func (s *service) listControllableProxyGroups(settings mihomoSettings) ([]controllableProxyGroup, error) {
	if settings.URL == "" {
		return nil, errors.New("mihomo url is not configured")
	}

	baseURL, client, err := s.resolveMihomoTransport(settings.URL)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodGet, baseURL+"/proxies", nil)
	if err != nil {
		return nil, err
	}
	if settings.Secret != "" {
		req.Header.Set("Authorization", "Bearer "+settings.Secret)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	var payload mihomoProxiesResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}

	groups := make([]controllableProxyGroup, 0)
	for name, proxy := range payload.Proxies {
		groupType := normalizeProxyGroupType(proxy.Type)
		if groupType == "" {
			continue
		}

		groups = append(groups, controllableProxyGroup{
			Name: name,
			Type: groupType,
			Now:  strings.TrimSpace(proxy.Now),
			All:  append([]string(nil), proxy.All...),
		})
	}

	sort.Slice(groups, func(i, j int) bool {
		return groups[i].Name < groups[j].Name
	})

	return groups, nil
}

func (s *service) switchProxyGroup(settings mihomoSettings, group controllableProxyGroup, targetProxy string) error {
	group.Type = normalizeProxyGroupType(group.Type)
	if group.Type == "" {
		return errors.New("proxy group type is not controllable")
	}

	targetProxy = strings.TrimSpace(targetProxy)
	if targetProxy == "" {
		return errors.New("target proxy is required")
	}
	if !containsString(group.All, targetProxy) {
		return fmt.Errorf("target proxy %q is not available in group %q", targetProxy, group.Name)
	}

	body, err := json.Marshal(map[string]string{"name": targetProxy})
	if err != nil {
		return err
	}

	baseURL, client, err := s.resolveMihomoTransport(settings.URL)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPut, baseURL+"/proxies/"+group.Name, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if settings.Secret != "" {
		req.Header.Set("Authorization", "Bearer "+settings.Secret)
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return nil
}

func backfillAggregatedLogs(db *sql.DB, beforeMS int64) error {
	if beforeMS <= 0 {
		return nil
	}

	var lastBucketEnd sql.NullInt64
	if err := db.QueryRow(`SELECT MAX(bucket_end) FROM traffic_aggregated`).Scan(&lastBucketEnd); err != nil {
		return err
	}

	startMS := int64(0)
	if lastBucketEnd.Valid {
		startMS = lastBucketEnd.Int64
	}
	if startMS >= beforeMS {
		return nil
	}

	_, err := db.Exec(`
		INSERT INTO traffic_aggregated
		(bucket_start, bucket_end, source_ip, host, destination_ip, process, process_path, route_type, outbound, chains, rule, rule_payload, upload, download, count)
		SELECT ((timestamp / 60000) * 60000) AS bucket_start,
		       ((timestamp / 60000) * 60000) + 60000 AS bucket_end,
		       source_ip,
		       host,
		       destination_ip,
		       process,
		       '',
		       CASE WHEN UPPER(outbound) LIKE 'REJECT%' THEN 'REJECT' WHEN UPPER(outbound) = 'DIRECT' THEN 'DIRECT' ELSE 'PROXY' END,
		       outbound,
		       chains,
		       '',
		       '',
		       COALESCE(SUM(upload), 0) AS upload,
		       COALESCE(SUM(download), 0) AS download,
		       COUNT(*) AS count
		FROM traffic_logs
		WHERE timestamp >= ? AND timestamp < ?
		GROUP BY bucket_start, source_ip, host, destination_ip, process, outbound, chains
		ON CONFLICT(bucket_start, bucket_end, source_ip, host, destination_ip, process, process_path, route_type, outbound, chains, rule, rule_payload)
		DO UPDATE SET
			upload = excluded.upload,
			download = excluded.download,
			count = excluded.count
	`, startMS, beforeMS)
	return err
}

func backfillSummary(db *sql.DB, beforeMS int64) error {
	if beforeMS <= 0 {
		return nil
	}

	var lastBucketEnd sql.NullInt64
	if err := db.QueryRow(`SELECT MAX(bucket_end) FROM traffic_summary`).Scan(&lastBucketEnd); err != nil {
		return err
	}

	startMS := int64(0)
	if lastBucketEnd.Valid {
		startMS = lastBucketEnd.Int64
	}
	endExclusive := (beforeMS / summaryBucketSize) * summaryBucketSize
	if startMS >= endExclusive {
		return nil
	}

	_, err := db.Exec(`
		INSERT INTO traffic_summary
		(bucket_start, bucket_end, dimension, label, secondary_label, upload, download, count)
		SELECT ((bucket_start / ?) * ?) AS bucket_start,
		       ((bucket_start / ?) * ?) + ? AS bucket_end,
		       'source_ip',
		       source_ip,
		       host,
		       COALESCE(SUM(upload), 0) AS upload,
		       COALESCE(SUM(download), 0) AS download,
		       COALESCE(SUM(count), 0) AS count
		FROM traffic_aggregated
		WHERE bucket_start >= ? AND bucket_start < ?
		GROUP BY ((bucket_start / ?) * ?), source_ip, host

		UNION ALL

		SELECT ((bucket_start / ?) * ?) AS bucket_start,
		       ((bucket_start / ?) * ?) + ? AS bucket_end,
		       'outbound',
		       outbound,
		       host,
		       COALESCE(SUM(upload), 0) AS upload,
		       COALESCE(SUM(download), 0) AS download,
		       COALESCE(SUM(count), 0) AS count
		FROM traffic_aggregated
		WHERE bucket_start >= ? AND bucket_start < ?
		GROUP BY ((bucket_start / ?) * ?), outbound, host

		UNION ALL

		SELECT ((bucket_start / ?) * ?) AS bucket_start,
		       ((bucket_start / ?) * ?) + ? AS bucket_end,
		       'total',
		       'total',
		       '',
		       COALESCE(SUM(upload), 0) AS upload,
		       COALESCE(SUM(download), 0) AS download,
		       COALESCE(SUM(count), 0) AS count
		FROM traffic_aggregated
		WHERE bucket_start >= ? AND bucket_start < ?
		GROUP BY ((bucket_start / ?) * ?)

		ON CONFLICT(bucket_start, bucket_end, dimension, label, secondary_label)
		DO UPDATE SET
			upload = excluded.upload,
			download = excluded.download,
			count = excluded.count
	`, summaryBucketSize, summaryBucketSize, summaryBucketSize, summaryBucketSize, summaryBucketSize,
		startMS, endExclusive, summaryBucketSize, summaryBucketSize,
		summaryBucketSize, summaryBucketSize, summaryBucketSize, summaryBucketSize, summaryBucketSize,
		startMS, endExclusive, summaryBucketSize, summaryBucketSize,
		summaryBucketSize, summaryBucketSize, summaryBucketSize, summaryBucketSize, summaryBucketSize,
		startMS, endExclusive, summaryBucketSize, summaryBucketSize)
	return err
}

func (s *service) runCollector(ctx context.Context) {
	s.collectOnce(ctx)

	ticker := time.NewTicker(defaultPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.collectOnce(ctx)
		}
	}
}

func (s *service) collectOnce(ctx context.Context) {
	settings := s.currentMihomoSettings()
	if settings.URL == "" {
		return
	}

	resp, err := s.fetchConnections(ctx, settings)
	if err != nil {
		log.Printf("poll Mihomo connections: %v", err)
		return
	}

	if err := s.processConnections(resp); err != nil {
		log.Printf("process connections: %v", err)
	}
}

func (s *service) currentMihomoSettings() mihomoSettings {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mihomoSettings
}

func (s *service) setMihomoSettings(settings mihomoSettings) {
	settings = normalizeMihomoSettings(settings)

	s.mu.Lock()
	defer s.mu.Unlock()

	if settings != s.mihomoSettings {
		s.closeMihomoIdleConnections()
		s.lastConnections = make(map[string]connection)
		s.lastUploadTotal = 0
		s.lastDownloadTotal = 0
	}

	s.mihomoSettings = settings
}

func (s *service) updateMihomoSettings(settings mihomoSettings) error {
	settings = normalizeMihomoSettings(settings)
	if err := saveMihomoSettings(s.db, settings); err != nil {
		return err
	}
	s.setMihomoSettings(settings)
	return nil
}

func (s *service) currentDomainGroupingEnabled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.domainGroupingEnabled
}

func (s *service) updateDomainGroupingEnabled(enabled bool) error {
	if err := saveDomainGroupingEnabled(s.db, enabled); err != nil {
		return err
	}
	s.mu.Lock()
	s.domainGroupingEnabled = enabled
	s.mu.Unlock()
	return nil
}

func (s *service) currentRetentionDays() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.aggregateRetentionDays
}

func (s *service) updateRetentionDays(days int) error {
	if err := saveRetentionDays(s.db, days); err != nil {
		return err
	}
	s.mu.Lock()
	s.aggregateRetentionDays = days
	s.mu.Unlock()
	return nil
}

func (s *service) fetchConnections(ctx context.Context, settings mihomoSettings) (*connectionsResponse, error) {
	if settings.URL == "" {
		return nil, errors.New("mihomo url is not configured")
	}

	baseURL, client, err := s.resolveMihomoTransport(settings.URL)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/connections", nil)
	if err != nil {
		return nil, err
	}

	if settings.Secret != "" {
		req.Header.Set("Authorization", "Bearer "+settings.Secret)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	var payload connectionsResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}

	return &payload, nil
}

func (s *service) currentTime() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

func (s *service) processConnections(payload *connectionsResponse) error {
	now := s.currentTime()
	nowMS := now.UnixMilli()
	logs, err := s.persistConnectionSnapshot(now, payload)
	if err != nil {
		return err
	}

	if len(logs) > 0 {
		if err := s.addToAggregateBuffer(logs, nowMS); err != nil {
			return err
		}
		if err := s.evaluateAutoSwitch(logs, nowMS); err != nil {
			log.Printf("auto switch evaluation: %v", err)
		}
	}
	if err := s.evaluateAutoRestore(nowMS); err != nil {
		log.Printf("auto restore evaluation: %v", err)
	}

	if err := s.flushCompletedAggregateBuckets(nowMS); err != nil {
		log.Printf("flush aggregate buffer: %v", err)
	}

	if time.Since(s.lastSummaryBuild) >= summaryBuildInterval {
		if err := backfillSummary(s.db, nowMS); err != nil {
			log.Printf("build traffic summary: %v", err)
		} else {
			s.lastSummaryBuild = now
		}
	}

	if now.Sub(s.lastCleanup) >= time.Hour {
		if err := s.cleanupOldLogs(nowMS); err != nil {
			log.Printf("cleanup old logs: %v", err)
		} else {
			s.lastCleanup = now
		}
	}

	return nil
}

func (s *service) evaluateAutoSwitch(logs []trafficLog, nowMS int64) error {
	settings, err := loadAutoSwitchSettings(s.db)
	if err != nil {
		return err
	}
	if !settings.Enabled || settings.ThresholdBytesPerMinute <= 0 {
		return nil
	}

	enabledTargets := enabledAutoSwitchTargets(settings)
	if len(enabledTargets) == 0 {
		return nil
	}

	triggerHost, triggerGroup, triggerTotal, bucketStart, bucketEnd := s.updateHostMinuteWindows(
		logs,
		nowMS,
		settings.ThresholdBytesPerMinute,
		enabledTargets,
	)
	if triggerHost == "" {
		return nil
	}

	log.Printf("auto switch debug: trigger reached host=%q group=%q total=%d threshold=%d bucket=%d-%d",
		triggerHost, triggerGroup, triggerTotal, settings.ThresholdBytesPerMinute, bucketStart, bucketEnd)

	if reason, suppressed := s.autoSwitchSuppressionReason(bucketStart, nowMS, settings.CooldownSeconds); suppressed {
		log.Printf("auto switch debug: suppressed trigger host=%q group=%q reason=%s cooldown_seconds=%d",
			triggerHost, triggerGroup, reason, settings.CooldownSeconds)
		return nil
	}

	results, execErr := s.executeAutoSwitch(settings, enabledTargets[triggerGroup], nowMS, triggerHost, triggerTotal)
	event := autoSwitchEvent{
		TriggeredAt: nowMS,
		Host:        triggerHost,
		TotalBytes:  triggerTotal,
		WindowStart: bucketStart,
		WindowEnd:   bucketEnd,
		Results:     results,
	}
	if execErr != nil {
		event.Error = execErr.Error()
	}
	if err := insertAutoSwitchEvent(s.db, event); err != nil {
		return err
	}

	s.mu.Lock()
	s.lastAutoSwitchAt = nowMS
	s.lastAutoSwitchMin = bucketStart
	s.mu.Unlock()
	return execErr
}

func enabledAutoSwitchTargets(settings autoSwitchSettings) map[string]autoSwitchGroupTarget {
	targets := make(map[string]autoSwitchGroupTarget, len(settings.GroupTargets))
	for _, target := range settings.GroupTargets {
		if !target.Enabled {
			continue
		}
		groupName := strings.TrimSpace(target.GroupName)
		if groupName == "" {
			continue
		}
		targets[groupName] = target
	}
	return targets
}

func firstMatchingAutoSwitchGroup(chains []string, enabledTargets map[string]autoSwitchGroupTarget) (autoSwitchGroupTarget, bool) {
	for _, name := range chains {
		target, ok := enabledTargets[strings.TrimSpace(name)]
		if ok {
			return target, true
		}
	}
	return autoSwitchGroupTarget{}, false
}

func (s *service) updateHostMinuteWindows(
	logs []trafficLog,
	nowMS int64,
	threshold int64,
	enabledTargets map[string]autoSwitchGroupTarget,
) (string, string, int64, int64, int64) {
	bucketStart := (nowMS / 60000) * 60000
	bucketEnd := bucketStart + 60000
	if threshold <= 0 {
		return "", "", 0, bucketStart, bucketEnd
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.hostMinuteWindows == nil {
		s.hostMinuteWindows = make(map[string]*hostTrafficWindow)
	}

	for _, entry := range logs {
		total := entry.Upload + entry.Download
		if total <= 0 {
			continue
		}

		target, ok := firstMatchingAutoSwitchGroup(entry.Chains, enabledTargets)
		if !ok {
			continue
		}

		windowKey := fmt.Sprintf("%s\x00%s", entry.Host, target.GroupName)
		window := s.hostMinuteWindows[windowKey]
		if window == nil || window.BucketStart != bucketStart {
			window = &hostTrafficWindow{
				BucketStart: bucketStart,
				BucketEnd:   bucketEnd,
			}
			s.hostMinuteWindows[windowKey] = window
		}

		previousTotal := window.TotalBytes
		window.TotalBytes += total
		if previousTotal < threshold && window.TotalBytes >= threshold && s.lastAutoSwitchMin != bucketStart {
			return entry.Host, target.GroupName, window.TotalBytes, bucketStart, bucketEnd
		}
	}

	return "", "", 0, bucketStart, bucketEnd
}

func (s *service) isAutoSwitchSuppressed(bucketStart, nowMS, cooldownSeconds int64) bool {
	_, suppressed := s.autoSwitchSuppressionReason(bucketStart, nowMS, cooldownSeconds)
	return suppressed
}

func (s *service) autoSwitchSuppressionReason(bucketStart, nowMS, cooldownSeconds int64) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lastAutoSwitchMin == bucketStart {
		return "same_bucket", true
	}
	if cooldownSeconds > 0 && s.lastAutoSwitchAt > 0 && nowMS < s.lastAutoSwitchAt+cooldownSeconds*1000 {
		return "cooldown", true
	}
	return "", false
}

func (s *service) executeAutoSwitch(
	settings autoSwitchSettings,
	target autoSwitchGroupTarget,
	triggeredAt int64,
	triggerHost string,
	triggerTotal int64,
) ([]autoSwitchExecutionResult, error) {
	mihomo := s.currentMihomoSettings()
	groups, err := s.listControllableProxyGroups(mihomo)
	if err != nil {
		return nil, err
	}

	enabledTargets := enabledAutoSwitchTargets(settings)
	groupByName := make(map[string]controllableProxyGroup, len(groups))
	for _, group := range groups {
		groupByName[group.Name] = group
	}

	results := make([]autoSwitchExecutionResult, 0, 1)
	visited := make(map[string]struct{})
	return s.executeAutoSwitchChain(
		settings,
		mihomo,
		groupByName,
		enabledTargets,
		target,
		triggeredAt,
		triggerHost,
		triggerTotal,
		visited,
		results,
	)
}

func (s *service) executeAutoSwitchChain(
	settings autoSwitchSettings,
	mihomo mihomoSettings,
	groupByName map[string]controllableProxyGroup,
	enabledTargets map[string]autoSwitchGroupTarget,
	target autoSwitchGroupTarget,
	triggeredAt int64,
	triggerHost string,
	triggerTotal int64,
	visited map[string]struct{},
	results []autoSwitchExecutionResult,
) ([]autoSwitchExecutionResult, error) {
	if _, seen := visited[target.GroupName]; seen {
		results = append(results, autoSwitchExecutionResult{
			GroupName:   target.GroupName,
			TargetProxy: target.TargetProxy,
			Status:      "error",
			Message:     "cyclic auto switch target chain",
		})
		return results, nil
	}
	visited[target.GroupName] = struct{}{}

	group, ok := groupByName[target.GroupName]
	if !ok {
		results = append(results, autoSwitchExecutionResult{
			GroupName:   target.GroupName,
			TargetProxy: target.TargetProxy,
			Status:      "error",
			Message:     "group not found",
		})
		return results, nil
	}

	session, hasSession, err := getAutoRestoreSession(s.db, target.GroupName)
	if err != nil {
		return results, err
	}

	if group.Now == target.TargetProxy {
		log.Printf("auto switch debug: group=%q already selected target=%q host=%q total=%d",
			target.GroupName, target.TargetProxy, triggerHost, triggerTotal)
		results = append(results, autoSwitchExecutionResult{
			GroupName:   target.GroupName,
			TargetProxy: target.TargetProxy,
			Status:      "skipped",
			Message:     "already selected",
		})
		if settings.RestoreEnabled && settings.RestoreQuietMinutes > 0 && hasSession {
			session.CurrentProxy = target.TargetProxy
			session.LastTriggeredAt = triggeredAt
			session.LastTriggeredHost = triggerHost
			session.LastTriggeredBytes = triggerTotal
			if err := upsertAutoRestoreSession(s.db, session); err != nil {
				return results, err
			}
		}
		return s.continueAutoSwitchChainIfNeeded(
			settings,
			mihomo,
			groupByName,
			enabledTargets,
			target.TargetProxy,
			triggeredAt,
			triggerHost,
			triggerTotal,
			visited,
			results,
		)
	}

	if err := s.switchProxyGroup(mihomo, group, target.TargetProxy); err != nil {
		log.Printf("auto switch debug: switch failed group=%q from=%q to=%q host=%q total=%d err=%v",
			target.GroupName, group.Now, target.TargetProxy, triggerHost, triggerTotal, err)
		results = append(results, autoSwitchExecutionResult{
			GroupName:   target.GroupName,
			TargetProxy: target.TargetProxy,
			Status:      "error",
			Message:     err.Error(),
		})
		return results, nil
	}

	log.Printf("auto switch debug: switched group=%q from=%q to=%q host=%q total=%d",
		target.GroupName, group.Now, target.TargetProxy, triggerHost, triggerTotal)
	results = append(results, autoSwitchExecutionResult{
		GroupName:   target.GroupName,
		TargetProxy: target.TargetProxy,
		Status:      "switched",
	})

	if settings.RestoreEnabled && settings.RestoreQuietMinutes > 0 {
		originalProxy := group.Now
		if hasSession && strings.TrimSpace(session.OriginalProxy) != "" {
			originalProxy = session.OriginalProxy
		}
		if err := upsertAutoRestoreSession(s.db, autoRestoreSession{
			GroupName:          target.GroupName,
			OriginalProxy:      originalProxy,
			CurrentProxy:       target.TargetProxy,
			LastTriggeredAt:    triggeredAt,
			LastTriggeredHost:  triggerHost,
			LastTriggeredBytes: triggerTotal,
		}); err != nil {
			return results, err
		}
	}

	group.Now = target.TargetProxy
	groupByName[target.GroupName] = group
	return s.continueAutoSwitchChainIfNeeded(
		settings,
		mihomo,
		groupByName,
		enabledTargets,
		target.TargetProxy,
		triggeredAt,
		triggerHost,
		triggerTotal,
		visited,
		results,
	)
}

func (s *service) continueAutoSwitchChainIfNeeded(
	settings autoSwitchSettings,
	mihomo mihomoSettings,
	groupByName map[string]controllableProxyGroup,
	enabledTargets map[string]autoSwitchGroupTarget,
	nextGroupName string,
	triggeredAt int64,
	triggerHost string,
	triggerTotal int64,
	visited map[string]struct{},
	results []autoSwitchExecutionResult,
) ([]autoSwitchExecutionResult, error) {
	nextTarget, ok := enabledTargets[strings.TrimSpace(nextGroupName)]
	if !ok {
		return results, nil
	}

	return s.executeAutoSwitchChain(
		settings,
		mihomo,
		groupByName,
		enabledTargets,
		nextTarget,
		triggeredAt,
		triggerHost,
		triggerTotal,
		visited,
		results,
	)
}

func autoRestoreEligible(nowMS, lastTriggeredAt, quietMinutes int64) bool {
	if lastTriggeredAt <= 0 || quietMinutes <= 0 {
		return false
	}

	currentBucketStart := (nowMS / 60000) * 60000
	triggerBucketStart := (lastTriggeredAt / 60000) * 60000
	return currentBucketStart >= triggerBucketStart+quietMinutes*60000
}

func (s *service) evaluateAutoRestore(nowMS int64) error {
	settings, err := loadAutoSwitchSettings(s.db)
	if err != nil {
		return err
	}
	if !settings.RestoreEnabled || settings.RestoreQuietMinutes <= 0 {
		return nil
	}

	sessions, err := listAutoRestoreSessions(s.db)
	if err != nil {
		return err
	}
	if len(sessions) == 0 {
		return nil
	}

	pending := make([]autoRestoreSession, 0, len(sessions))
	for _, session := range sessions {
		if autoRestoreEligible(nowMS, session.LastTriggeredAt, settings.RestoreQuietMinutes) {
			pending = append(pending, session)
		}
	}
	if len(pending) == 0 {
		return nil
	}

	mihomo := s.currentMihomoSettings()
	groups, err := s.listControllableProxyGroups(mihomo)
	if err != nil {
		return err
	}

	groupByName := make(map[string]controllableProxyGroup, len(groups))
	for _, group := range groups {
		groupByName[group.Name] = group
	}

	results := make([]autoSwitchExecutionResult, 0, len(pending))
	var eventErrs []string
	for _, session := range pending {
		group, ok := groupByName[session.GroupName]
		if !ok {
			results = append(results, autoSwitchExecutionResult{
				GroupName:   session.GroupName,
				TargetProxy: session.OriginalProxy,
				Status:      "restore_error",
				Message:     "group not found",
			})
			eventErrs = append(eventErrs, fmt.Sprintf("%s: group not found", session.GroupName))
			if err := deleteAutoRestoreSession(s.db, session.GroupName); err != nil {
				return err
			}
			continue
		}

		if group.Now != session.CurrentProxy {
			results = append(results, autoSwitchExecutionResult{
				GroupName:   session.GroupName,
				TargetProxy: session.OriginalProxy,
				Status:      "restore_skipped",
				Message:     "selection changed manually",
			})
			if err := deleteAutoRestoreSession(s.db, session.GroupName); err != nil {
				return err
			}
			continue
		}

		if !containsString(group.All, session.OriginalProxy) {
			results = append(results, autoSwitchExecutionResult{
				GroupName:   session.GroupName,
				TargetProxy: session.OriginalProxy,
				Status:      "restore_error",
				Message:     "original proxy is no longer available",
			})
			eventErrs = append(eventErrs, fmt.Sprintf("%s: original proxy unavailable", session.GroupName))
			if err := deleteAutoRestoreSession(s.db, session.GroupName); err != nil {
				return err
			}
			continue
		}

		if err := s.switchProxyGroup(mihomo, group, session.OriginalProxy); err != nil {
			results = append(results, autoSwitchExecutionResult{
				GroupName:   session.GroupName,
				TargetProxy: session.OriginalProxy,
				Status:      "restore_error",
				Message:     err.Error(),
			})
			eventErrs = append(eventErrs, fmt.Sprintf("%s: %v", session.GroupName, err))
			if err := deleteAutoRestoreSession(s.db, session.GroupName); err != nil {
				return err
			}
			continue
		}

		results = append(results, autoSwitchExecutionResult{
			GroupName:   session.GroupName,
			TargetProxy: session.OriginalProxy,
			Status:      "restored",
		})
		if err := deleteAutoRestoreSession(s.db, session.GroupName); err != nil {
			return err
		}
	}

	if len(results) == 0 {
		return nil
	}

	bucketStart := (nowMS / 60000) * 60000
	event := autoSwitchEvent{
		TriggeredAt: nowMS,
		Host:        "",
		TotalBytes:  0,
		WindowStart: bucketStart,
		WindowEnd:   bucketStart + 60000,
		Results:     results,
	}
	if len(eventErrs) > 0 {
		event.Error = strings.Join(eventErrs, "; ")
	}

	return insertAutoSwitchEvent(s.db, event)
}

func (s *service) insertLogs(logs []trafficLog) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(`
		INSERT INTO traffic_logs (timestamp, source_ip, host, destination_ip, process, outbound, chains, upload, download)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()

	for _, entry := range logs {
		chainsJSON, err := json.Marshal(sanitizeChains(entry.Chains))
		if err != nil {
			tx.Rollback()
			return err
		}

		if _, err := stmt.Exec(
			entry.Timestamp,
			entry.SourceIP,
			entry.Host,
			entry.DestinationIP,
			entry.Process,
			entry.Outbound,
			string(chainsJSON),
			entry.Upload,
			entry.Download,
		); err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

func (s *service) cleanupOldLogs(nowMS int64) error {
	if _, err := s.db.Exec(`DELETE FROM traffic_logs`); err != nil {
		return err
	}

	// Minute facts, summaries, rollups, and session ledgers are the permanent
	// accounting record. Cleanup only removes the legacy raw-event table.

	if _, err := s.db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		log.Printf("WAL checkpoint failed: %v", err)
	}

	// 定期执行VACUUM（每周一次）
	if time.Since(s.lastVacuum) >= 7*24*time.Hour {
		if _, err := s.db.Exec(`VACUUM`); err != nil {
			log.Printf("VACUUM failed: %v", err)
		} else {
			s.lastVacuum = time.Now()
		}
	}

	return nil
}

// normalizeHost returns the eTLD+1 of host, stripping subdomains.
// Bare IPs and single-label names are returned unchanged.
// This is the sole seam for a future Public Suffix List upgrade.
func normalizeHost(host string) string {
	// Strip port if present (e.g. "example.com:443").
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	// Return bare IPs unchanged.
	if net.ParseIP(host) != nil {
		return host
	}
	parts := strings.Split(host, ".")
	if len(parts) <= 2 {
		return host
	}
	return parts[len(parts)-2] + "." + parts[len(parts)-1]
}

func (s *service) addToAggregateBuffer(logs []trafficLog, nowMS int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	bucketStart := (nowMS / 60000) * 60000 // 1分钟桶
	bucketEnd := bucketStart + 60000

	for _, log := range logs {
		key := fmt.Sprintf("%d-%s-%s-%s-%s-%s-%s-%s-%s-%s-%s", bucketStart, log.SourceIP, log.Host, log.DestinationIP, log.Process, log.ProcessPath, log.RouteType, log.Outbound, strings.Join(log.Chains, ","), log.Rule, log.RulePayload)

		if entry, exists := s.aggregateBuffer[key]; exists {
			entry.Upload += log.Upload
			entry.Download += log.Download
			entry.Count++
		} else {
			chainsJSON, err := json.Marshal(sanitizeChains(log.Chains))
			if err != nil {
				return err
			}
			s.aggregateBuffer[key] = &aggregatedEntry{
				BucketStart:   bucketStart,
				BucketEnd:     bucketEnd,
				SourceIP:      log.SourceIP,
				Host:          log.Host,
				DestinationIP: log.DestinationIP,
				Process:       log.Process,
				ProcessPath:   log.ProcessPath,
				RouteType:     log.RouteType,
				Outbound:      log.Outbound,
				Chains:        string(chainsJSON),
				Rule:          log.Rule,
				RulePayload:   log.RulePayload,
				Upload:        log.Upload,
				Download:      log.Download,
				Count:         1,
			}
		}
	}

	return nil
}

func (s *service) flushCompletedAggregateBuckets(nowMS int64) error {
	currentBucketStart := (nowMS / int64(aggregateFlushInterval/time.Millisecond)) * int64(aggregateFlushInterval/time.Millisecond)
	return s.flushAggregateEntries(func(entry *aggregatedEntry) bool {
		return entry.BucketEnd <= currentBucketStart
	})
}

func (s *service) flushAggregateBuffer() error {
	return s.flushAggregateEntries(func(*aggregatedEntry) bool {
		return true
	})
}

func (s *service) flushAggregateEntries(shouldFlush func(*aggregatedEntry) bool) error {
	buffer := s.snapshotAggregateEntries(shouldFlush)
	if len(buffer) == 0 {
		return nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(`
		INSERT INTO traffic_aggregated
		(bucket_start, bucket_end, source_ip, host, destination_ip, process, process_path, route_type, outbound, chains, rule, rule_payload, upload, download, count)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(bucket_start, bucket_end, source_ip, host, destination_ip, process, process_path, route_type, outbound, chains, rule, rule_payload)
		DO UPDATE SET
			upload = traffic_aggregated.upload + excluded.upload,
			download = traffic_aggregated.download + excluded.download,
			count = traffic_aggregated.count + excluded.count
	`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()

	for _, entry := range buffer {
		if _, err := stmt.Exec(
			entry.BucketStart, entry.BucketEnd, entry.SourceIP, entry.Host, entry.DestinationIP,
			entry.Process, entry.ProcessPath, entry.RouteType, entry.Outbound, entry.Chains, entry.Rule, entry.RulePayload,
			entry.Upload, entry.Download, entry.Count,
		); err != nil {
			tx.Rollback()
			return err
		}
		if err := upsertTrafficRollups(tx, entry); err != nil {
			tx.Rollback()
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for key := range buffer {
		delete(s.aggregateBuffer, key)
	}
	return nil
}

func (s *service) snapshotAggregateEntries(shouldFlush func(*aggregatedEntry) bool) map[string]aggregatedEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	buffer := make(map[string]aggregatedEntry)
	for key, entry := range s.aggregateBuffer {
		if shouldFlush(entry) {
			buffer[key] = *entry
		}
	}
	return buffer
}

func (s *service) routes() http.Handler {
	mux := http.NewServeMux()
	staticFS, err := fs.Sub(webAssets, "web")
	if err != nil {
		panic(err)
	}
	fileServer := http.FileServer(http.FS(staticFS))
	mux.HandleFunc("/LICENSE", s.handleLicense)
	mux.Handle("/", fileServer)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/api/settings/mihomo", s.handleMihomoSettings)
	mux.HandleFunc("/api/settings/domain-grouping", s.handleDomainGroupingSettings)
	mux.HandleFunc("/api/settings/retention", s.handleRetentionSettings)
	mux.HandleFunc("/api/settings/autostart", s.handleAutostartSettings)
	mux.HandleFunc("/api/auto-switch/settings", s.handleAutoSwitchSettings)
	mux.HandleFunc("/api/auto-switch/groups", s.handleAutoSwitchGroups)
	mux.HandleFunc("/api/auto-switch/events", s.handleAutoSwitchEvents)
	mux.HandleFunc("/api/traffic/aggregate", s.handleAggregate)
	mux.HandleFunc("/api/traffic/substats", s.handleSubstats)
	mux.HandleFunc("/api/traffic/proxy-stats", s.handleProxyStats)
	mux.HandleFunc("/api/traffic/devices-by-host", s.handleDevicesByHost)
	mux.HandleFunc("/api/traffic/devices-by-proxy-host", s.handleDevicesByProxyHost)
	mux.HandleFunc("/api/traffic/details", s.handleConnectionDetails)
	mux.HandleFunc("/api/traffic/trend", s.handleTrend)
	mux.HandleFunc("/api/traffic/logs", s.handleLogs)
	mux.HandleFunc("/api/ledger/rollups", s.handleTrafficRollups)
	return s.withCORS(mux)
}

func (s *service) withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", defaultAllowedOrigin)
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, DELETE, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *service) handleLicense(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeMethodNotAllowed(w)
		return
	}

	content, err := webAssets.ReadFile("LICENSE")
	if err != nil {
		http.Error(w, "license unavailable", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	http.ServeContent(w, r, "LICENSE", time.Time{}, bytes.NewReader(content))
}

func (s *service) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *service) handleMihomoSettings(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, s.currentMihomoSettings())
	case http.MethodPut:
		var payload mihomoSettings
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeError(w, http.StatusBadRequest, errors.New("invalid json body"))
			return
		}

		payload = normalizeMihomoSettings(payload)
		if payload.URL == "" {
			writeError(w, http.StatusBadRequest, errors.New("mihomo url is required"))
			return
		}

		if err := s.updateMihomoSettings(payload); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		writeJSON(w, http.StatusOK, s.currentMihomoSettings())
	default:
		writeMethodNotAllowed(w)
	}
}

func (s *service) handleDomainGroupingSettings(w http.ResponseWriter, r *http.Request) {
	type domainGroupingResponse struct {
		Enabled bool `json:"enabled"`
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, domainGroupingResponse{Enabled: s.currentDomainGroupingEnabled()})
	case http.MethodPut:
		var payload domainGroupingResponse
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeError(w, http.StatusBadRequest, errors.New("invalid json body"))
			return
		}
		if err := s.updateDomainGroupingEnabled(payload.Enabled); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, domainGroupingResponse{Enabled: s.currentDomainGroupingEnabled()})
	default:
		writeMethodNotAllowed(w)
	}
}

func (s *service) handleRetentionSettings(w http.ResponseWriter, r *http.Request) {
	type retentionResponse struct {
		Days int `json:"days"`
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, retentionResponse{Days: s.currentRetentionDays()})
	case http.MethodPut:
		var payload retentionResponse
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeError(w, http.StatusBadRequest, errors.New("invalid json body"))
			return
		}
		if payload.Days < 1 || payload.Days > 365 {
			writeError(w, http.StatusBadRequest, errors.New("days must be between 1 and 365"))
			return
		}
		if err := s.updateRetentionDays(payload.Days); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, retentionResponse{Days: s.currentRetentionDays()})
	default:
		writeMethodNotAllowed(w)
	}
}

func validateAutoSwitchSettings(settings autoSwitchSettings) error {
	if settings.Enabled && settings.ThresholdBytesPerMinute <= 0 {
		return errors.New("thresholdBytesPerMinute must be greater than 0 when auto switch is enabled")
	}
	if settings.CooldownSeconds < 0 {
		return errors.New("cooldownSeconds must be 0 or greater")
	}
	if settings.RestoreQuietMinutes < 0 {
		return errors.New("restoreQuietMinutes must be 0 or greater")
	}

	for _, target := range settings.GroupTargets {
		if strings.TrimSpace(target.GroupName) == "" {
			return errors.New("groupName is required")
		}
		if strings.TrimSpace(target.TargetProxy) == "" {
			return errors.New("targetProxy is required")
		}
	}
	return nil
}

func (s *service) handleAutoSwitchSettings(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		settings, err := loadAutoSwitchSettings(s.db)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, settings)
	case http.MethodPut:
		var payload autoSwitchSettings
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeError(w, http.StatusBadRequest, errors.New("invalid json body"))
			return
		}
		if err := validateAutoSwitchSettings(payload); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if err := saveAutoSwitchSettings(s.db, payload); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, payload)
	default:
		writeMethodNotAllowed(w)
	}
}

func (s *service) handleAutoSwitchGroups(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}

	groups, err := s.listControllableProxyGroups(s.currentMihomoSettings())
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, groups)
}

func (s *service) handleAutoSwitchEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}

	events, err := listAutoSwitchEvents(s.db, 50)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, events)
}

func (s *service) handleAggregate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}

	dimension := r.URL.Query().Get("dimension")
	start, end, err := parseTimeRange(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	raw := r.URL.Query().Get("raw") == "1"
	excludeDirect := shouldExcludeDirect(r)
	var data []aggregatedData
	if r.URL.Query().Get("summary") == "1" && !excludeDirect {
		data, err = s.querySummaryAggregate(dimension, start, end, raw)
	} else {
		data, err = s.queryAggregateFiltered(dimension, start, end, raw, excludeDirect)
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	writeJSON(w, http.StatusOK, data)
}

func (s *service) handleSubstats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}

	dimension := r.URL.Query().Get("dimension")
	label := r.URL.Query().Get("label")
	start, end, err := parseTimeRange(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if label == "" {
		writeError(w, http.StatusBadRequest, errors.New("label is required"))
		return
	}

	var data []aggregatedData
	excludeDirect := shouldExcludeDirect(r)
	if r.URL.Query().Get("summary") == "1" && !excludeDirect {
		data, err = s.querySummarySecondary(dimension, label, start, end)
	} else {
		data, err = s.querySubstatsFiltered(dimension, label, start, end, excludeDirect)
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, data)
}

func (s *service) handleProxyStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}

	dimension := r.URL.Query().Get("dimension")
	parentLabel := r.URL.Query().Get("parentLabel")
	host := r.URL.Query().Get("host")
	start, end, err := parseTimeRange(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if parentLabel == "" || host == "" {
		writeError(w, http.StatusBadRequest, errors.New("parentLabel and host are required"))
		return
	}

	var data []aggregatedData
	excludeDirect := shouldExcludeDirect(r)
	if r.URL.Query().Get("summary") == "1" && !excludeDirect {
		data, err = s.querySummaryProxyStats(dimension, parentLabel, host, start, end)
	} else {
		data, err = s.queryProxyStatsFiltered(dimension, parentLabel, host, start, end, excludeDirect)
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, data)
}

func (s *service) handleDevicesByHost(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}

	host := r.URL.Query().Get("host")
	start, end, err := parseTimeRange(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if host == "" {
		writeError(w, http.StatusBadRequest, errors.New("host is required"))
		return
	}

	hostFilter, hostArgs := s.hostFilterArgs(host)
	hostFilter, hostArgs = withDirectExclusion(hostFilter, hostArgs, shouldExcludeDirect(r))
	var data []aggregatedData
	if r.URL.Query().Get("summary") == "1" && !shouldExcludeDirect(r) {
		data, err = s.querySummarySecondary("host", host, start, end)
	} else {
		data, err = s.queryByFilters("source_ip", hostFilter, hostArgs, start, end)
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, data)
}

func (s *service) handleDevicesByProxyHost(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}

	proxy := r.URL.Query().Get("proxy")
	host := r.URL.Query().Get("host")
	start, end, err := parseTimeRange(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if proxy == "" || host == "" {
		writeError(w, http.StatusBadRequest, errors.New("proxy and host are required"))
		return
	}

	filter, args := withDirectExclusion(
		"outbound = ? AND host = ?",
		[]any{proxy, host},
		shouldExcludeDirect(r),
	)
	data, err := s.queryByFilters(
		"source_ip",
		filter,
		args,
		start,
		end,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, data)
}

func (s *service) handleTrend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}

	start, end, err := parseTimeRange(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	bucket := parseInt64(r.URL.Query().Get("bucket"), 60000)
	if bucket <= 0 {
		writeError(w, http.StatusBadRequest, errors.New("bucket must be positive"))
		return
	}

	var data []trendPoint
	excludeDirect := shouldExcludeDirect(r)
	if r.URL.Query().Get("summary") == "1" && !excludeDirect {
		data, err = s.queryTrendSummary(start, end, bucket)
	} else {
		data, err = s.queryTrendFiltered(start, end, bucket, excludeDirect)
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, data)
}

func (s *service) handleConnectionDetails(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}

	dimension := r.URL.Query().Get("dimension")
	primary := r.URL.Query().Get("primary")
	secondary := r.URL.Query().Get("secondary")
	start, end, err := parseTimeRange(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if primary == "" || secondary == "" {
		writeError(w, http.StatusBadRequest, errors.New("primary and secondary are required"))
		return
	}

	if r.URL.Query().Get("summary") == "1" {
		writeJSON(w, http.StatusOK, []connectionDetail{})
		return
	}

	data, err := s.queryConnectionDetailsFiltered(dimension, primary, secondary, start, end, shouldExcludeDirect(r))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, data)
}

func (s *service) handleLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		writeMethodNotAllowed(w)
		return
	}

	tx, err := s.db.Begin()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if _, err := tx.Exec(`DELETE FROM traffic_logs`); err != nil {
		tx.Rollback()
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if _, err := tx.Exec(`DELETE FROM traffic_aggregated`); err != nil {
		tx.Rollback()
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if _, err := tx.Exec(`DELETE FROM traffic_summary`); err != nil {
		tx.Rollback()
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	s.mu.Lock()
	s.aggregateBuffer = make(map[string]*aggregatedEntry)
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *service) queryAggregate(dimension string, start, end int64, raw bool) ([]aggregatedData, error) {
	return s.queryAggregateFiltered(dimension, start, end, raw, false)
}

func (s *service) queryAggregateFiltered(dimension string, start, end int64, raw, excludeDirect bool) ([]aggregatedData, error) {
	column, err := dimensionColumn(dimension)
	if err != nil {
		return nil, err
	}
	filter, args := withDirectExclusion("", nil, excludeDirect)
	rows, err := s.queryByFilters(column, filter, args, start, end)
	if err != nil {
		return nil, err
	}
	if column == "host" && !raw && s.currentDomainGroupingEnabled() {
		rows = groupHostRows(rows)
	}
	return rows, nil
}

func groupHostRows(rows []aggregatedData) []aggregatedData {
	merged := make(map[string]*aggregatedData, len(rows))
	for _, row := range rows {
		key := normalizeHost(row.Label)
		existing, ok := merged[key]
		if !ok {
			copyRow := row
			copyRow.Label = key
			merged[key] = &copyRow
			continue
		}
		existing.Upload += row.Upload
		existing.Download += row.Download
		existing.Total += row.Total
		existing.Count += row.Count
	}
	return sortedAggregatedDataRows(merged)
}

// hostFilterArgs returns a SQL filter and args for matching a host column.
// When domain grouping is enabled, expands a normalized host to also match subdomains via LIKE.
func (s *service) hostFilterArgs(host string) (string, []any) {
	if s.currentDomainGroupingEnabled() {
		return "(host = ? OR host LIKE ?)", []any{host, "%." + host}
	}
	return "host = ?", []any{host}
}

func (s *service) querySubstats(dimension, label string, start, end int64) ([]aggregatedData, error) {
	return s.querySubstatsFiltered(dimension, label, start, end, false)
}

func (s *service) querySubstatsFiltered(dimension, label string, start, end int64, excludeDirect bool) ([]aggregatedData, error) {
	column, err := dimensionColumn(dimension)
	if err != nil {
		return nil, err
	}
	if column == "host" {
		if !s.currentDomainGroupingEnabled() {
			return nil, errors.New("host substats require domain grouping to be enabled")
		}
		hostFilter, hostArgs := s.hostFilterArgs(label)
		hostFilter, hostArgs = withDirectExclusion(hostFilter, hostArgs, excludeDirect)
		return s.queryByFilters("host", hostFilter, hostArgs, start, end)
	}
	filter, args := withDirectExclusion(column+" = ?", []any{label}, excludeDirect)
	return s.queryByFilters("host", filter, args, start, end)
}

func (s *service) queryProxyStats(dimension, parentLabel, host string, start, end int64) ([]aggregatedData, error) {
	return s.queryProxyStatsFiltered(dimension, parentLabel, host, start, end, false)
}

func (s *service) queryProxyStatsFiltered(dimension, parentLabel, host string, start, end int64, excludeDirect bool) ([]aggregatedData, error) {
	column, err := dimensionColumn(dimension)
	if err != nil {
		return nil, err
	}
	if column == "host" {
		return nil, errors.New("host is not supported for proxy stats")
	}
	filter, args := withDirectExclusion(column+" = ? AND host = ?", []any{parentLabel, host}, excludeDirect)
	return s.queryByFilters("outbound", filter, args, start, end)
}

func (s *service) queryByFilters(groupColumn, extraFilter string, extraArgs []any, start, end int64) ([]aggregatedData, error) {
	merged := make(map[string]*aggregatedData)

	items, err := s.queryByFiltersFromAggregates(groupColumn, extraFilter, extraArgs, start, end)
	if err != nil {
		return nil, err
	}
	mergeAggregatedDataRows(merged, items)

	mergeAggregatedDataRows(merged, s.queryByFiltersFromBuffer(groupColumn, extraFilter, extraArgs, start, end))

	return sortedAggregatedDataRows(merged), nil
}

func (s *service) querySummaryAggregate(dimension string, start, end int64, raw bool) ([]aggregatedData, error) {
	column, err := dimensionColumn(dimension)
	if err != nil {
		return nil, err
	}

	data, err := s.querySummaryByDimension(dimension, start, end)
	if err != nil {
		return nil, err
	}
	if column == "host" && !raw && s.currentDomainGroupingEnabled() {
		data = groupHostRows(data)
	}
	return data, nil
}

func (s *service) querySummaryByDimension(dimension string, start, end int64) ([]aggregatedData, error) {
	summaryDimension, labelColumn, err := summaryDimensionParts(dimension)
	if err != nil {
		return nil, err
	}

	rows, err := s.querySummaryRows(summaryDimension, labelColumn, "", nil, start, end)
	if err != nil {
		return nil, err
	}
	merged := make(map[string]*aggregatedData)
	mergeAggregatedDataRows(merged, rows)

	column, err := dimensionColumn(dimension)
	if err != nil {
		return nil, err
	}
	detailStart, err := s.summaryCoveredEnd()
	if err != nil {
		return nil, err
	}
	fallbackStart := detailStart
	if fallbackStart < start {
		fallbackStart = start
	}
	detailItems, err := s.queryByFiltersFromAggregates(column, "", nil, fallbackStart, end)
	if err != nil {
		return nil, err
	}
	mergeAggregatedDataRows(merged, detailItems)
	mergeAggregatedDataRows(merged, s.queryByFiltersFromBuffer(column, "", nil, start, end))
	return sortedAggregatedDataRows(merged), nil
}

func (s *service) querySummaryRows(dimension, labelColumn, filter string, args []any, start, end int64) ([]aggregatedData, error) {
	query := `
		SELECT ` + labelColumn + ` AS label,
		       COALESCE(SUM(upload), 0) AS upload,
		       COALESCE(SUM(download), 0) AS download,
		       COALESCE(SUM(upload + download), 0) AS total,
		       COALESCE(SUM(count), 0) AS count
		FROM traffic_summary
		WHERE dimension = ? AND bucket_end > ? AND bucket_start <= ?` + filter + `
		GROUP BY ` + labelColumn + `
		ORDER BY total DESC, label ASC
	`

	rows, err := s.db.Query(query, append([]any{dimension, start, end}, args...)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]aggregatedData, 0)
	for rows.Next() {
		var item aggregatedData
		if err := rows.Scan(&item.Label, &item.Upload, &item.Download, &item.Total, &item.Count); err != nil {
			return nil, err
		}
		results = append(results, item)
	}
	return results, rows.Err()
}

func (s *service) querySummarySecondary(dimension, primary string, start, end int64) ([]aggregatedData, error) {
	var summaryDimension, labelColumn, filter string
	var args []any
	var bufferGroupColumn, bufferFilter string

	switch dimension {
	case "sourceIP":
		summaryDimension = "source_ip"
		labelColumn = "secondary_label"
		filter = " AND label = ?"
		args = []any{primary}
		bufferGroupColumn = "host"
		bufferFilter = "source_ip = ?"
	case "host":
		summaryDimension = "source_ip"
		labelColumn = "label"
		hostFilter, hostArgs := s.hostFilterArgs(primary)
		filter = " AND (secondary_label = ? OR secondary_label LIKE ?)"
		if !s.currentDomainGroupingEnabled() {
			filter = " AND secondary_label = ?"
		}
		args = hostArgs
		bufferGroupColumn = "source_ip"
		bufferFilter = hostFilter
	case "outbound":
		summaryDimension = "outbound"
		labelColumn = "secondary_label"
		filter = " AND label = ?"
		args = []any{primary}
		bufferGroupColumn = "host"
		bufferFilter = "outbound = ?"
	default:
		return nil, fmt.Errorf("unsupported dimension %q", dimension)
	}

	detailStart, err := s.summaryCoveredEnd()
	if err != nil {
		return nil, err
	}
	fallbackStart := detailStart
	if fallbackStart < start {
		fallbackStart = start
	}
	rows, err := s.querySummaryRows(summaryDimension, labelColumn, filter, args, start, end)
	if err != nil {
		return nil, err
	}
	merged := make(map[string]*aggregatedData)
	mergeAggregatedDataRows(merged, rows)
	detailRows, err := s.queryByFiltersFromAggregates(bufferGroupColumn, bufferFilter, args, fallbackStart, end)
	if err != nil {
		return nil, err
	}
	mergeAggregatedDataRows(merged, detailRows)
	mergeAggregatedDataRows(merged, s.queryByFiltersFromBuffer(bufferGroupColumn, bufferFilter, args, start, end))
	return sortedAggregatedDataRows(merged), nil
}

func summaryDimensionParts(dimension string) (string, string, error) {
	switch dimension {
	case "sourceIP":
		return "source_ip", "label", nil
	case "host":
		return "source_ip", "secondary_label", nil
	case "outbound":
		return "outbound", "label", nil
	default:
		return "", "", fmt.Errorf("unsupported dimension %q", dimension)
	}
}

func (s *service) summaryCoveredEnd() (int64, error) {
	var lastBucketEnd sql.NullInt64
	if err := s.db.QueryRow(`SELECT MAX(bucket_end) FROM traffic_summary`).Scan(&lastBucketEnd); err != nil {
		return 0, err
	}
	if !lastBucketEnd.Valid {
		return 0, nil
	}
	return lastBucketEnd.Int64, nil
}

func (s *service) querySummaryProxyStats(dimension, parentLabel, host string, start, end int64) ([]aggregatedData, error) {
	switch dimension {
	case "sourceIP":
		return s.querySummaryRows("source_ip", "label", " AND label = ? AND secondary_label = ?", []any{parentLabel, host}, start, end)
	case "outbound":
		return s.querySummaryRows("outbound", "label", " AND label = ? AND secondary_label = ?", []any{parentLabel, host}, start, end)
	default:
		return nil, fmt.Errorf("unsupported dimension %q", dimension)
	}
}

func (s *service) queryTrendSummary(start, end, bucket int64) ([]trendPoint, error) {
	detailStart, err := s.summaryCoveredEnd()
	if err != nil {
		return nil, err
	}
	fallbackStart := detailStart
	if fallbackStart < start {
		fallbackStart = start
	}
	rows, err := s.db.Query(`
		SELECT ((bucket_start / ?) * ?) AS bucket_start,
		       COALESCE(SUM(upload), 0) AS upload,
		       COALESCE(SUM(download), 0) AS download
		FROM traffic_summary
		WHERE dimension = 'total' AND bucket_end > ? AND bucket_start <= ?
		GROUP BY bucket_start
		ORDER BY bucket_start ASC
	`, bucket, bucket, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	buckets := make(map[int64]trendPoint)
	for rows.Next() {
		var point trendPoint
		if err := rows.Scan(&point.Timestamp, &point.Upload, &point.Download); err != nil {
			return nil, err
		}
		mergeTrendPoints(buckets, []trendPoint{point})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	detailTrend, err := s.queryTrendFromAggregates(fallbackStart, end, bucket, false)
	if err != nil {
		return nil, err
	}
	mergeTrendPoints(buckets, detailTrend)
	mergeTrendPoints(buckets, s.queryTrendFromBuffer(start, end, bucket, false))

	points := make([]trendPoint, 0, (end-start)/bucket+1)
	for t := start; t <= end; t += bucket {
		key := (t / bucket) * bucket
		if point, ok := buckets[key]; ok {
			points = append(points, point)
			continue
		}
		points = append(points, trendPoint{Timestamp: key})
	}
	return points, nil
}

func (s *service) queryConnectionDetails(dimension, primary, secondary string, start, end int64) ([]connectionDetail, error) {
	return s.queryConnectionDetailsFiltered(dimension, primary, secondary, start, end, false)
}

func (s *service) queryConnectionDetailsFiltered(dimension, primary, secondary string, start, end int64, excludeDirect bool) ([]connectionDetail, error) {
	filter, args, err := detailFilter(dimension, primary, secondary)
	if err != nil {
		return nil, err
	}
	if dimension == "host" && s.currentDomainGroupingEnabled() {
		if net.ParseIP(primary) != nil {
			filter = "host = ?"
			args = []any{secondary}
		} else if net.ParseIP(secondary) == nil {
			// secondary is a raw subdomain — show all source IPs for that host
			filter = "host = ?"
			args = []any{secondary}
		} else {
			hostFilter, hostArgs := s.hostFilterArgs(primary)
			filter = hostFilter + " AND source_ip = ?"
			args = append(hostArgs, secondary)
		}
	}
	filter, args = withDirectExclusion(filter, args, excludeDirect)

	rows, err := s.db.Query(`
		SELECT destination_ip,
		       source_ip,
		       process,
		       outbound,
		       chains,
		       COALESCE(SUM(upload), 0) AS upload,
		       COALESCE(SUM(download), 0) AS download,
		       COALESCE(SUM(upload + download), 0) AS total,
		       COALESCE(SUM(count), 0) AS count
		FROM traffic_aggregated
		WHERE bucket_end > ? AND bucket_start <= ?
		  AND `+filter+`
		GROUP BY destination_ip, source_ip, process, outbound, chains
	`, append([]any{start, end}, args...)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	merged := make(map[string]*connectionDetail)
	for rows.Next() {
		var (
			item      connectionDetail
			chainsRaw string
		)
		if err := rows.Scan(
			&item.DestinationIP,
			&item.SourceIP,
			&item.Process,
			&item.Outbound,
			&chainsRaw,
			&item.Upload,
			&item.Download,
			&item.Total,
			&item.Count,
		); err != nil {
			return nil, err
		}
		item.Chains = parseChains(chainsRaw)
		mergeConnectionDetailRows(merged, []connectionDetail{item})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	mergeConnectionDetailRows(merged, s.queryConnectionDetailsFromBuffer(filter, args, start, end))
	return sortedConnectionDetailRows(merged), nil
}

func (s *service) queryTrend(start, end, bucket int64) ([]trendPoint, error) {
	return s.queryTrendFiltered(start, end, bucket, false)
}

func (s *service) queryTrendFiltered(start, end, bucket int64, excludeDirect bool) ([]trendPoint, error) {
	buckets := make(map[int64]trendPoint)

	items, err := s.queryTrendFromAggregates(start, end, bucket, excludeDirect)
	if err != nil {
		return nil, err
	}
	mergeTrendPoints(buckets, items)
	mergeTrendPoints(buckets, s.queryTrendFromBuffer(start, end, bucket, excludeDirect))

	points := make([]trendPoint, 0, (end-start)/bucket+1)
	for t := start; t <= end; t += bucket {
		key := (t / bucket) * bucket
		if point, ok := buckets[key]; ok {
			points = append(points, point)
			continue
		}
		points = append(points, trendPoint{Timestamp: key})
	}
	return points, nil
}

func (s *service) queryByFiltersFromRaw(groupColumn, extraFilter string, extraArgs []any, start, end int64) ([]aggregatedData, error) {
	return s.queryByFiltersFromTable(
		"traffic_logs",
		"timestamp BETWEEN ? AND ?",
		[]any{start, end},
		groupColumn,
		extraFilter,
		extraArgs,
		"COUNT(*)",
	)
}

func (s *service) queryByFiltersFromAggregates(groupColumn, extraFilter string, extraArgs []any, start, endExclusive int64) ([]aggregatedData, error) {
	return s.queryByFiltersFromTable(
		"traffic_aggregated",
		"bucket_end > ? AND bucket_start <= ?",
		[]any{start, endExclusive},
		groupColumn,
		extraFilter,
		extraArgs,
		"COALESCE(SUM(count), 0)",
	)
}

func (s *service) queryByFiltersFromBuffer(groupColumn, extraFilter string, extraArgs []any, start, end int64) []aggregatedData {
	items := s.snapshotAggregateEntries(func(entry *aggregatedEntry) bool {
		return aggregateEntryOverlapsRange(*entry, start, end)
	})

	merged := make(map[string]*aggregatedData)
	for _, entry := range items {
		if !matchesAggregateEntryFilters(entry, extraFilter, extraArgs) {
			continue
		}
		label := aggregateEntryFieldValue(entry, groupColumn)
		if label == "" {
			continue
		}
		row, ok := merged[label]
		if !ok {
			row = &aggregatedData{Label: label}
			merged[label] = row
		}
		row.Upload += entry.Upload
		row.Download += entry.Download
		row.Total += entry.Upload + entry.Download
		row.Count += entry.Count
	}

	return sortedAggregatedDataRows(merged)
}

func (s *service) queryByFiltersFromTable(table, timeFilter string, timeArgs []any, groupColumn, extraFilter string, extraArgs []any, countExpr string) ([]aggregatedData, error) {
	base := `
		SELECT ` + groupColumn + ` AS label,
		       COALESCE(SUM(upload), 0) AS upload,
		       COALESCE(SUM(download), 0) AS download,
		       COALESCE(SUM(upload + download), 0) AS total,
		       ` + countExpr + ` AS count
		FROM ` + table + `
		WHERE ` + timeFilter

	args := append([]any{}, timeArgs...)
	if extraFilter != "" {
		base += " AND " + extraFilter
		args = append(args, extraArgs...)
	}
	base += `
		GROUP BY ` + groupColumn + `
		ORDER BY total DESC, label ASC
	`

	rows, err := s.db.Query(base, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]aggregatedData, 0)
	for rows.Next() {
		var item aggregatedData
		if err := rows.Scan(&item.Label, &item.Upload, &item.Download, &item.Total, &item.Count); err != nil {
			return nil, err
		}
		results = append(results, item)
	}
	return results, rows.Err()
}

func (s *service) queryTrendFromRaw(start, end, bucket int64) ([]trendPoint, error) {
	rows, err := s.db.Query(`
		SELECT ((timestamp / ?) * ?) AS bucket_start,
		       COALESCE(SUM(upload), 0) AS upload,
		       COALESCE(SUM(download), 0) AS download
		FROM traffic_logs
		WHERE timestamp BETWEEN ? AND ?
		GROUP BY bucket_start
		ORDER BY bucket_start ASC
	`, bucket, bucket, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]trendPoint, 0)
	for rows.Next() {
		var point trendPoint
		if err := rows.Scan(&point.Timestamp, &point.Upload, &point.Download); err != nil {
			return nil, err
		}
		results = append(results, point)
	}
	return results, rows.Err()
}

func (s *service) queryTrendFromAggregates(start, endExclusive, bucket int64, excludeDirect bool) ([]trendPoint, error) {
	directFilter := ""
	args := []any{bucket, bucket, start, endExclusive}
	if excludeDirect {
		directFilter = " AND route_type <> ?"
		args = append(args, "DIRECT")
	}
	rows, err := s.db.Query(`
		SELECT ((bucket_start / ?) * ?) AS bucket_start,
		       COALESCE(SUM(upload), 0) AS upload,
		       COALESCE(SUM(download), 0) AS download
		FROM traffic_aggregated
		WHERE bucket_end > ? AND bucket_start <= ?`+directFilter+`
		GROUP BY bucket_start
		ORDER BY bucket_start ASC
	`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]trendPoint, 0)
	for rows.Next() {
		var point trendPoint
		if err := rows.Scan(&point.Timestamp, &point.Upload, &point.Download); err != nil {
			return nil, err
		}
		results = append(results, point)
	}
	return results, rows.Err()
}

func (s *service) queryTrendFromBuffer(start, end, bucket int64, excludeDirect bool) []trendPoint {
	items := s.snapshotAggregateEntries(func(entry *aggregatedEntry) bool {
		return aggregateEntryOverlapsRange(*entry, start, end) && (!excludeDirect || !strings.EqualFold(entry.RouteType, "DIRECT"))
	})

	merged := make(map[int64]trendPoint)
	for _, entry := range items {
		key := (entry.BucketStart / bucket) * bucket
		point := merged[key]
		point.Timestamp = key
		point.Upload += entry.Upload
		point.Download += entry.Download
		merged[key] = point
	}

	results := make([]trendPoint, 0, len(merged))
	for _, item := range merged {
		results = append(results, item)
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Timestamp < results[j].Timestamp
	})
	return results
}

func (s *service) queryConnectionDetailsFromBuffer(filter string, args []any, start, end int64) []connectionDetail {
	items := s.snapshotAggregateEntries(func(entry *aggregatedEntry) bool {
		return aggregateEntryOverlapsRange(*entry, start, end)
	})

	merged := make(map[string]*connectionDetail)
	for _, entry := range items {
		if !matchesAggregateEntryFilters(entry, filter, args) {
			continue
		}
		item := connectionDetail{
			DestinationIP: entry.DestinationIP,
			SourceIP:      entry.SourceIP,
			Process:       entry.Process,
			Outbound:      entry.Outbound,
			Chains:        parseChains(entry.Chains),
			Upload:        entry.Upload,
			Download:      entry.Download,
			Total:         entry.Upload + entry.Download,
			Count:         entry.Count,
		}
		mergeConnectionDetailRows(merged, []connectionDetail{item})
	}

	return sortedConnectionDetailRows(merged)
}

func aggregateEntryOverlapsRange(entry aggregatedEntry, start, end int64) bool {
	return entry.BucketEnd > start && entry.BucketStart <= end
}

func matchesAggregateEntryFilters(entry aggregatedEntry, filter string, args []any) bool {
	if filter == "" {
		return true
	}

	clauses := strings.Split(filter, " AND ")
	argIdx := 0
	for _, clause := range clauses {
		clause = strings.TrimSpace(clause)
		if argIdx >= len(args) {
			return false
		}
		// Handle compound clause: (col = ? OR col LIKE ?)
		// Used for normalized-host LIKE expansion. Consumes 2 args.
		if strings.HasPrefix(clause, "(") && strings.HasSuffix(clause, ")") {
			if argIdx+1 >= len(args) {
				return false
			}
			inner := clause[1 : len(clause)-1]
			parts := strings.SplitN(inner, " OR ", 2)
			if len(parts) != 2 {
				return false
			}
			col := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(parts[0]), "= ?"))
			col = strings.TrimSpace(strings.TrimSuffix(col, " = ?"))
			fieldVal := aggregateEntryFieldValue(entry, col)
			exactVal := fmt.Sprint(args[argIdx])
			likePattern := fmt.Sprint(args[argIdx+1])
			if fieldVal != exactVal && !matchesLikePattern(fieldVal, likePattern) {
				return false
			}
			argIdx += 2
			continue
		}
		if strings.HasSuffix(clause, "<> ?") {
			column := strings.TrimSpace(strings.TrimSuffix(clause, "<> ?"))
			if strings.EqualFold(aggregateEntryFieldValue(entry, column), fmt.Sprint(args[argIdx])) {
				return false
			}
			argIdx++
			continue
		}
		// Handle simple clause: col = ?
		column := strings.TrimSpace(strings.TrimSuffix(clause, "= ?"))
		column = strings.TrimSpace(strings.TrimSuffix(column, " = ?"))
		if column == "" {
			return false
		}
		if aggregateEntryFieldValue(entry, column) != fmt.Sprint(args[argIdx]) {
			return false
		}
		argIdx++
	}

	return argIdx == len(args)
}

// matchesLikePattern matches the "%.suffix" pattern (prefix wildcard only).
func matchesLikePattern(value, pattern string) bool {
	if strings.HasPrefix(pattern, "%.") {
		return strings.HasSuffix(value, pattern[1:])
	}
	return value == pattern
}

func aggregateEntryFieldValue(entry aggregatedEntry, column string) string {
	switch column {
	case "source_ip":
		return entry.SourceIP
	case "host":
		return entry.Host
	case "destination_ip":
		return entry.DestinationIP
	case "process":
		return entry.Process
	case "outbound":
		return entry.Outbound
	case "route_type":
		return entry.RouteType
	case "chains":
		return entry.Chains
	default:
		return ""
	}
}

func shouldExcludeDirect(r *http.Request) bool {
	return r.URL.Query().Get("excludeDirect") == "1"
}

func withDirectExclusion(filter string, args []any, excludeDirect bool) (string, []any) {
	if !excludeDirect {
		return filter, args
	}
	if filter != "" {
		filter += " AND "
	}
	return filter + "route_type <> ?", append(args, "DIRECT")
}

func fullMinuteBucketRange(start, end int64) (int64, int64) {
	aggregateStart := ((start + 60000 - 1) / 60000) * 60000
	aggregateEndExclusive := ((end + 1) / 60000) * 60000
	if aggregateStart < 0 {
		aggregateStart = 0
	}
	if aggregateEndExclusive < 0 {
		aggregateEndExclusive = 0
	}
	if aggregateStart > aggregateEndExclusive {
		return aggregateStart, aggregateStart
	}
	return aggregateStart, aggregateEndExclusive
}

func mergeAggregatedDataRows(target map[string]*aggregatedData, items []aggregatedData) {
	for _, item := range items {
		existing, ok := target[item.Label]
		if !ok {
			copyItem := item
			target[item.Label] = &copyItem
			continue
		}
		existing.Upload += item.Upload
		existing.Download += item.Download
		existing.Total += item.Total
		existing.Count += item.Count
	}
}

func sortedAggregatedDataRows(items map[string]*aggregatedData) []aggregatedData {
	results := make([]aggregatedData, 0, len(items))
	for _, item := range items {
		results = append(results, *item)
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].Total == results[j].Total {
			return results[i].Label < results[j].Label
		}
		return results[i].Total > results[j].Total
	})
	return results
}

func mergeConnectionDetailRows(target map[string]*connectionDetail, items []connectionDetail) {
	for _, item := range items {
		key := connectionDetailKey(item)
		existing, ok := target[key]
		if !ok {
			copyItem := item
			target[key] = &copyItem
			continue
		}
		existing.Upload += item.Upload
		existing.Download += item.Download
		existing.Total += item.Total
		existing.Count += item.Count
	}
}

func sortedConnectionDetailRows(items map[string]*connectionDetail) []connectionDetail {
	results := make([]connectionDetail, 0, len(items))
	for _, item := range items {
		results = append(results, *item)
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].Total == results[j].Total {
			if results[i].DestinationIP == results[j].DestinationIP {
				if results[i].SourceIP == results[j].SourceIP {
					return results[i].Process < results[j].Process
				}
				return results[i].SourceIP < results[j].SourceIP
			}
			return results[i].DestinationIP < results[j].DestinationIP
		}
		return results[i].Total > results[j].Total
	})
	return results
}

func connectionDetailKey(item connectionDetail) string {
	return strings.Join([]string{
		item.DestinationIP,
		item.SourceIP,
		item.Process,
		item.Outbound,
		strings.Join(item.Chains, "\x1f"),
	}, "\x00")
}

func mergeTrendPoints(target map[int64]trendPoint, items []trendPoint) {
	for _, item := range items {
		existing := target[item.Timestamp]
		existing.Timestamp = item.Timestamp
		existing.Upload += item.Upload
		existing.Download += item.Download
		target[item.Timestamp] = existing
	}
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func parseTimeRange(r *http.Request) (int64, int64, error) {
	start := parseInt64(r.URL.Query().Get("start"), 0)
	end := parseInt64(r.URL.Query().Get("end"), 0)
	if start <= 0 || end <= 0 {
		return 0, 0, errors.New("start and end are required")
	}
	if end < start {
		return 0, 0, errors.New("end must be greater than or equal to start")
	}
	return start, end, nil
}

func dimensionColumn(dimension string) (string, error) {
	switch dimension {
	case "sourceIP":
		return "source_ip", nil
	case "host":
		return "host", nil
	case "process":
		return "process", nil
	case "outbound":
		return "outbound", nil
	default:
		return "", fmt.Errorf("unsupported dimension %q", dimension)
	}
}

func detailFilter(dimension, primary, secondary string) (string, []any, error) {
	switch dimension {
	case "sourceIP":
		return "source_ip = ? AND host = ?", []any{primary, secondary}, nil
	case "host":
		return "host = ? AND source_ip = ?", []any{primary, secondary}, nil
	case "outbound":
		return "outbound = ? AND host = ?", []any{primary, secondary}, nil
	case "process":
		return "process = ? AND host = ?", []any{primary, secondary}, nil
	default:
		return "", nil, fmt.Errorf("unsupported dimension %q", dimension)
	}
}

func outboundName(chains []string) string {
	if len(chains) == 0 || chains[0] == "" {
		return "DIRECT"
	}
	return chains[0]
}

func sanitizeChains(chains []string) []string {
	if len(chains) == 0 {
		return []string{"DIRECT"}
	}

	cleaned := make([]string, 0, len(chains))
	for _, chain := range chains {
		chain = strings.TrimSpace(chain)
		if chain != "" {
			cleaned = append(cleaned, chain)
		}
	}
	if len(cleaned) == 0 {
		return []string{"DIRECT"}
	}
	return cleaned
}

func parseChains(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return []string{"DIRECT"}
	}

	var chains []string
	if err := json.Unmarshal([]byte(raw), &chains); err != nil {
		return []string{raw}
	}
	return sanitizeChains(chains)
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func getenv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func parseInt64(value string, fallback int64) int64 {
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func writeMethodNotAllowed(w http.ResponseWriter) {
	writeError(w, http.StatusMethodNotAllowed, errors.New("method not allowed"))
}
