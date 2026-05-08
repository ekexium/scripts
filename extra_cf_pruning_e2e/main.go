package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
)

const bothModeMismatch = "EXTRA_CF both-mode mismatch"

var validIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type listFlag []string

func (f *listFlag) String() string {
	return strings.Join(*f, ",")
}

func (f *listFlag) Set(value string) error {
	for _, item := range strings.Split(value, ",") {
		item = strings.TrimSpace(item)
		if item != "" {
			*f = append(*f, item)
		}
	}
	return nil
}

type config struct {
	dsn                   string
	table                 string
	reset                 bool
	metricsURLs           listFlag
	tikvLogPatterns       listFlag
	setMode               string
	duration              time.Duration
	drainTimeout          time.Duration
	statusInterval        time.Duration
	probeTimeout          time.Duration
	targets               int
	pendingProbes         int
	fillerRows            int
	fillerBase            int64
	fillerWorkers         int
	minAgeOps             uint64
	maxAgeOps             uint64
	padBytes              int
	requireCoverage       bool
	requireConflictMetric bool
	seed                  int64
}

type counters struct {
	fillerOK       atomic.Uint64
	fillerErr      atomic.Uint64
	seeded         atomic.Uint64
	seedErr        atomic.Uint64
	probeConflict  atomic.Uint64
	probeOtherErr  atomic.Uint64
	falseNegative  atomic.Uint64
	abandonedProbe atomic.Uint64
}

type pendingProbe struct {
	id        int64
	conn      *sql.Conn
	seededAt  time.Time
	seededOps uint64
	dueOps    uint64
	ageOps    uint64
}

type metricSnapshot struct {
	selected          map[string]float64
	bothWriteConflict float64
	bothConflictTotal float64
}

func newMetricSnapshot() metricSnapshot {
	return metricSnapshot{selected: make(map[string]float64)}
}

func main() {
	cfg, err := parseConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid arguments: %v\n", err)
		os.Exit(2)
	}
	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "FAILED: %v\n", err)
		os.Exit(1)
	}
}

func parseConfig() (config, error) {
	cfg := config{}
	flag.StringVar(&cfg.dsn, "dsn", "root@tcp(127.0.0.1:4000)/test?parseTime=true&interpolateParams=true", "TiDB MySQL DSN")
	flag.StringVar(&cfg.table, "table", "extra_cf_pruning_probe", "table used by the workload")
	flag.BoolVar(&cfg.reset, "reset", true, "drop and recreate the workload table before running")
	flag.Var(&cfg.metricsURLs, "metrics-url", "TiKV metrics URL; repeat or comma-separate for multiple endpoints")
	flag.Var(&cfg.tikvLogPatterns, "tikv-log", "TiKV log path or glob; repeat or comma-separate for multiple files")
	flag.StringVar(&cfg.setMode, "set-mode", "both", "set storage.extra_cf_scan_mode on TiKV status endpoints; empty disables")
	flag.DurationVar(&cfg.duration, "duration", 10*time.Minute, "active probe seeding duration")
	flag.DurationVar(&cfg.drainTimeout, "drain-timeout", 3*time.Minute, "extra time to keep filler traffic running until pending probes become due")
	flag.DurationVar(&cfg.statusInterval, "status-interval", 10*time.Second, "progress reporting interval")
	flag.DurationVar(&cfg.probeTimeout, "probe-timeout", 30*time.Second, "timeout for each stale probe update/commit")
	flag.IntVar(&cfg.targets, "targets", 1024, "maximum number of target probe keys")
	flag.IntVar(&cfg.pendingProbes, "pending-probes", 64, "maximum stale optimistic transactions kept open at once")
	flag.IntVar(&cfg.fillerRows, "filler-rows", 10000, "number of filler rows used by lock-only traffic")
	flag.Int64Var(&cfg.fillerBase, "filler-base", 1_000_000_000, "first filler row id")
	flag.IntVar(&cfg.fillerWorkers, "filler-workers", 16, "concurrent filler workers")
	flag.Uint64Var(&cfg.minAgeOps, "min-age-ops", 1, "minimum filler operations before probing a seeded target")
	flag.Uint64Var(&cfg.maxAgeOps, "max-age-ops", 65536, "maximum filler operations before probing a seeded target")
	flag.IntVar(&cfg.padBytes, "pad-bytes", 128, "initial row pad bytes; must be <= 1024")
	flag.BoolVar(&cfg.requireCoverage, "require-coverage", true, "fail unless selected memtable, L0, and stable pruning metrics all increase")
	flag.BoolVar(&cfg.requireConflictMetric, "require-conflict-metric", true, "fail unless both-mode write-conflict metric increases")
	flag.Int64Var(&cfg.seed, "seed", time.Now().UnixNano(), "random seed")
	flag.Parse()

	if len(cfg.metricsURLs) == 0 {
		cfg.metricsURLs = append(cfg.metricsURLs, "http://127.0.0.1:20180/metrics")
	}
	if !validIdentifier.MatchString(cfg.table) {
		return cfg, fmt.Errorf("table name %q is not a simple SQL identifier", cfg.table)
	}
	if cfg.targets <= 0 {
		return cfg, errors.New("--targets must be positive")
	}
	if cfg.pendingProbes <= 0 {
		return cfg, errors.New("--pending-probes must be positive")
	}
	if cfg.fillerRows <= 0 {
		return cfg, errors.New("--filler-rows must be positive")
	}
	if cfg.fillerWorkers <= 0 {
		return cfg, errors.New("--filler-workers must be positive")
	}
	if cfg.minAgeOps == 0 {
		return cfg, errors.New("--min-age-ops must be positive")
	}
	if cfg.maxAgeOps < cfg.minAgeOps {
		return cfg, errors.New("--max-age-ops must be >= --min-age-ops")
	}
	if cfg.padBytes < 0 || cfg.padBytes > 1024 {
		return cfg, errors.New("--pad-bytes must be between 0 and 1024")
	}
	return cfg, nil
}

func run(cfg config) error {
	fmt.Printf("seed=%d\n", cfg.seed)
	fmt.Printf("dsn=%s table=%s duration=%s targets=%d pending=%d filler_workers=%d age_ops=[%d,%d]\n",
		redactDSN(cfg.dsn), cfg.table, cfg.duration, cfg.targets, cfg.pendingProbes, cfg.fillerWorkers, cfg.minAgeOps, cfg.maxAgeOps)

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logFiles, err := expandLogFiles(cfg.tikvLogPatterns)
	if err != nil {
		return err
	}
	logOffsets := captureLogOffsets(logFiles)

	if cfg.setMode != "" {
		if err := setExtraCFScanMode(rootCtx, cfg.metricsURLs, cfg.setMode); err != nil {
			return err
		}
	}

	baseMetrics, err := fetchAllMetrics(rootCtx, cfg.metricsURLs)
	if err != nil {
		return fmt.Errorf("fetch baseline metrics: %w", err)
	}

	db, err := sql.Open("mysql", cfg.dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(cfg.fillerWorkers + cfg.pendingProbes + 16)
	db.SetMaxIdleConns(cfg.fillerWorkers + 8)
	db.SetConnMaxLifetime(30 * time.Minute)
	if err := db.PingContext(rootCtx); err != nil {
		return fmt.Errorf("connect TiDB: %w", err)
	}
	if err := setupTable(rootCtx, db, cfg); err != nil {
		return err
	}

	stats := &counters{}
	fillerCtx, stopFillers := context.WithCancel(rootCtx)
	var fillerWG sync.WaitGroup
	for i := 0; i < cfg.fillerWorkers; i++ {
		fillerWG.Add(1)
		go func(workerID int) {
			defer fillerWG.Done()
			fillerWorker(fillerCtx, db, cfg, stats, cfg.seed+int64(workerID)*7919)
		}(i)
	}

	schedulerErr := runScheduler(rootCtx, db, cfg, stats, baseMetrics)
	stopFillers()
	fillerWG.Wait()

	finalMetrics, metricErr := fetchAllMetrics(context.Background(), cfg.metricsURLs)
	if metricErr != nil {
		return fmt.Errorf("fetch final metrics: %w", metricErr)
	}
	printMetricDelta("final", baseMetrics, finalMetrics)

	var failures []string
	if schedulerErr != nil {
		failures = append(failures, schedulerErr.Error())
	}
	failures = append(failures, verifyMetrics(cfg, baseMetrics, finalMetrics)...)
	failures = append(failures, scanMismatchLogs(logOffsets)...)
	if stats.falseNegative.Load() > 0 {
		failures = append(failures, fmt.Sprintf("false negatives observed: %d", stats.falseNegative.Load()))
	}
	if stats.probeOtherErr.Load() > 0 {
		failures = append(failures, fmt.Sprintf("stale probes failed with non-conflict errors: %d", stats.probeOtherErr.Load()))
	}
	if stats.probeConflict.Load() == 0 {
		failures = append(failures, "no stale probe reached an expected write conflict")
	}

	printSummary(stats)
	if len(failures) > 0 {
		sort.Strings(failures)
		return fmt.Errorf(strings.Join(failures, "; "))
	}
	fmt.Println("PASS")
	return nil
}

func setupTable(ctx context.Context, db *sql.DB, cfg config) error {
	table := quoteIdent(cfg.table)
	if cfg.reset {
		if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
			return fmt.Errorf("drop table: %w", err)
		}
	}
	createSQL := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL, pad VARBINARY(1024) NOT NULL)",
		table,
	)
	if _, err := db.ExecContext(ctx, createSQL); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	pad := bytes.Repeat([]byte("x"), cfg.padBytes)
	targetIDs := make([]int64, 0, cfg.targets)
	for i := 1; i <= cfg.targets; i++ {
		targetIDs = append(targetIDs, int64(i))
	}
	if err := insertRows(ctx, db, table, targetIDs, pad); err != nil {
		return fmt.Errorf("insert target rows: %w", err)
	}

	fillerIDs := make([]int64, 0, cfg.fillerRows)
	for i := 0; i < cfg.fillerRows; i++ {
		fillerIDs = append(fillerIDs, cfg.fillerBase+int64(i))
	}
	if err := insertRows(ctx, db, table, fillerIDs, pad); err != nil {
		return fmt.Errorf("insert filler rows: %w", err)
	}
	return nil
}

func insertRows(ctx context.Context, db *sql.DB, table string, ids []int64, pad []byte) error {
	const batchSize = 500
	for start := 0; start < len(ids); start += batchSize {
		end := start + batchSize
		if end > len(ids) {
			end = len(ids)
		}
		var b strings.Builder
		args := make([]any, 0, (end-start)*3)
		b.WriteString("INSERT IGNORE INTO ")
		b.WriteString(table)
		b.WriteString(" (id, v, pad) VALUES ")
		for i, id := range ids[start:end] {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString("(?, 0, ?)")
			args = append(args, id, pad)
		}
		if _, err := db.ExecContext(ctx, b.String(), args...); err != nil {
			return err
		}
	}
	return nil
}

func runScheduler(ctx context.Context, db *sql.DB, cfg config, stats *counters, baseMetrics metricSnapshot) error {
	rng := rand.New(rand.NewSource(cfg.seed ^ 0x5eed5eed))
	table := quoteIdent(cfg.table)
	start := time.Now()
	seedDeadline := start.Add(cfg.duration)
	drainDeadline := seedDeadline.Add(cfg.drainTimeout)
	nextTarget := int64(1)
	pending := make([]*pendingProbe, 0, cfg.pendingProbes)
	statusTick := time.NewTicker(cfg.statusInterval)
	defer statusTick.Stop()

	for {
		now := time.Now()
		if ctx.Err() != nil {
			rollbackPending(pending)
			return ctx.Err()
		}

		for now.Before(seedDeadline) && nextTarget <= int64(cfg.targets) && len(pending) < cfg.pendingProbes {
			currentOps := stats.fillerOK.Load()
			ageOps := sampleLogUniform(rng, cfg.minAgeOps, cfg.maxAgeOps)
			probe, err := seedProbe(ctx, db, table, nextTarget, currentOps, ageOps)
			if err != nil {
				stats.seedErr.Add(1)
				rollbackPending(pending)
				return fmt.Errorf("seed probe id=%d: %w", nextTarget, err)
			}
			pending = append(pending, probe)
			stats.seeded.Add(1)
			nextTarget++
			now = time.Now()
		}

		currentOps := stats.fillerOK.Load()
		pending = runDueProbes(pending, currentOps, table, cfg.probeTimeout, stats)

		if nextTarget > int64(cfg.targets) && len(pending) == 0 {
			return nil
		}
		if now.After(seedDeadline) && len(pending) == 0 {
			return nil
		}
		if now.After(drainDeadline) {
			abandoned := uint64(len(pending))
			stats.abandonedProbe.Add(abandoned)
			rollbackPending(pending)
			if abandoned > 0 {
				return fmt.Errorf("drain timeout reached with %d pending probes", abandoned)
			}
			return nil
		}

		select {
		case <-ctx.Done():
			rollbackPending(pending)
			return ctx.Err()
		case <-statusTick.C:
			printStatus(stats, pending, start)
			if metrics, err := fetchAllMetrics(ctx, cfg.metricsURLs); err == nil {
				printMetricDelta("current", baseMetrics, metrics)
			} else {
				fmt.Printf("metrics_error=%v\n", err)
			}
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func seedProbe(ctx context.Context, db *sql.DB, table string, id int64, currentOps uint64, ageOps uint64) (*pendingProbe, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
			_ = conn.Close()
		}
	}()

	if _, err := conn.ExecContext(ctx, "SET SESSION tidb_txn_mode='optimistic'"); err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, "BEGIN OPTIMISTIC"); err != nil {
		return nil, err
	}
	var v int64
	if err := conn.QueryRowContext(ctx, "SELECT v FROM "+table+" WHERE id = ?", id).Scan(&v); err != nil {
		return nil, err
	}
	if err := runLockOnlyTxn(ctx, db, table, id); err != nil {
		return nil, err
	}

	cleanup = false
	return &pendingProbe{
		id:        id,
		conn:      conn,
		seededAt:  time.Now(),
		seededOps: currentOps,
		dueOps:    currentOps + ageOps,
		ageOps:    ageOps,
	}, nil
}

func runDueProbes(pending []*pendingProbe, currentOps uint64, table string, probeTimeout time.Duration, stats *counters) []*pendingProbe {
	remaining := pending[:0]
	for _, probe := range pending {
		if currentOps < probe.dueOps {
			remaining = append(remaining, probe)
			continue
		}
		result, err := runProbe(table, probe, probeTimeout)
		switch result {
		case "conflict":
			stats.probeConflict.Add(1)
		case "false_negative":
			stats.falseNegative.Add(1)
		default:
			stats.probeOtherErr.Add(1)
			fmt.Printf("probe_error id=%d err=%v\n", probe.id, err)
		}
	}
	return remaining
}

func runProbe(table string, probe *pendingProbe, timeout time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	_, updateErr := probe.conn.ExecContext(ctx, "UPDATE "+table+" SET v = v + 1 WHERE id = ?", probe.id)
	var commitErr error
	if updateErr == nil {
		_, commitErr = probe.conn.ExecContext(ctx, "COMMIT")
	}
	err := updateErr
	if err == nil {
		err = commitErr
	}
	if err != nil {
		_, _ = probe.conn.ExecContext(context.Background(), "ROLLBACK")
		_ = probe.conn.Close()
		if isWriteConflict(err) {
			return "conflict", nil
		}
		return "other_error", err
	}
	_ = probe.conn.Close()
	fmt.Printf("false_negative id=%d seeded_ops=%d due_ops=%d age_ops=%d held_for=%s\n",
		probe.id, probe.seededOps, probe.dueOps, probe.ageOps, time.Since(probe.seededAt).Round(time.Millisecond))
	return "false_negative", nil
}

func rollbackPending(pending []*pendingProbe) {
	for _, probe := range pending {
		_, _ = probe.conn.ExecContext(context.Background(), "ROLLBACK")
		_ = probe.conn.Close()
	}
}

func fillerWorker(ctx context.Context, db *sql.DB, cfg config, stats *counters, seed int64) {
	rng := rand.New(rand.NewSource(seed))
	table := quoteIdent(cfg.table)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		id := cfg.fillerBase + int64(rng.Intn(cfg.fillerRows))
		if err := runLockOnlyTxn(ctx, db, table, id); err != nil {
			stats.fillerErr.Add(1)
			time.Sleep(20 * time.Millisecond)
			continue
		}
		stats.fillerOK.Add(1)
	}
}

func runLockOnlyTxn(ctx context.Context, db *sql.DB, table string, id int64) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "SET SESSION tidb_txn_mode='pessimistic'"); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "BEGIN PESSIMISTIC"); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		}
	}()

	var v int64
	if err := conn.QueryRowContext(ctx, "SELECT v FROM "+table+" WHERE id = ? FOR UPDATE", id).Scan(&v); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return err
	}
	committed = true
	return nil
}

func isWriteConflict(err error) bool {
	if err == nil {
		return false
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 9007 {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "write conflict") ||
		strings.Contains(msg, "errwriteconflict") ||
		strings.Contains(msg, "[kv:9007]") ||
		strings.Contains(msg, "error 9007") ||
		strings.Contains(msg, "txn conflict")
}

func sampleLogUniform(rng *rand.Rand, minValue, maxValue uint64) uint64 {
	if maxValue <= minValue {
		return minValue
	}
	minExp := math.Log2(float64(minValue))
	maxExp := math.Log2(float64(maxValue))
	value := uint64(math.Pow(2, minExp+rng.Float64()*(maxExp-minExp)))
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

func printStatus(stats *counters, pending []*pendingProbe, start time.Time) {
	oldest := time.Duration(0)
	now := time.Now()
	for _, probe := range pending {
		if age := now.Sub(probe.seededAt); age > oldest {
			oldest = age
		}
	}
	fmt.Printf("status elapsed=%s filler_ok=%d filler_err=%d seeded=%d pending=%d oldest_pending=%s conflicts=%d other_probe_errors=%d false_negatives=%d\n",
		time.Since(start).Round(time.Second),
		stats.fillerOK.Load(),
		stats.fillerErr.Load(),
		stats.seeded.Load(),
		len(pending),
		oldest.Round(time.Second),
		stats.probeConflict.Load(),
		stats.probeOtherErr.Load(),
		stats.falseNegative.Load(),
	)
}

func printSummary(stats *counters) {
	fmt.Printf("summary filler_ok=%d filler_err=%d seeded=%d seed_err=%d conflicts=%d other_probe_errors=%d false_negatives=%d abandoned=%d\n",
		stats.fillerOK.Load(),
		stats.fillerErr.Load(),
		stats.seeded.Load(),
		stats.seedErr.Load(),
		stats.probeConflict.Load(),
		stats.probeOtherErr.Load(),
		stats.falseNegative.Load(),
		stats.abandonedProbe.Load(),
	)
}

func setExtraCFScanMode(ctx context.Context, metricsURLs []string, mode string) error {
	for _, metricsURL := range metricsURLs {
		configURL, err := deriveConfigURL(metricsURL)
		if err != nil {
			return err
		}
		body := []byte(fmt.Sprintf(`{"storage.extra_cf_scan_mode":%q}`, mode))
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, configURL, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return fmt.Errorf("set scan mode via %s: %w", configURL, err)
		}
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("set scan mode via %s: status=%s body=%s", configURL, resp.Status, strings.TrimSpace(string(respBody)))
		}
		fmt.Printf("set_mode endpoint=%s mode=%s\n", configURL, mode)
	}
	return nil
}

func deriveConfigURL(metricsURL string) (string, error) {
	parsed, err := url.Parse(metricsURL)
	if err != nil {
		return "", err
	}
	parsed.Path = "/config"
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}

func fetchAllMetrics(ctx context.Context, metricsURLs []string) (metricSnapshot, error) {
	total := newMetricSnapshot()
	for _, metricsURL := range metricsURLs {
		snapshot, err := fetchMetrics(ctx, metricsURL)
		if err != nil {
			return total, err
		}
		total.add(snapshot)
	}
	return total, nil
}

func fetchMetrics(ctx context.Context, metricsURL string) (metricSnapshot, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metricsURL, nil)
	if err != nil {
		return metricSnapshot{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return metricSnapshot{}, fmt.Errorf("GET %s: %w", metricsURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return metricSnapshot{}, fmt.Errorf("GET %s: status=%s", metricsURL, resp.Status)
	}
	return parseMetrics(resp.Body)
}

func parseMetrics(r io.Reader) (metricSnapshot, error) {
	snapshot := newMetricSnapshot()
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		name, labels, value, ok := parseMetricLine(scanner.Text())
		if !ok {
			continue
		}
		switch name {
		case "tikv_storage_extra_cf_pruning_datasets_total":
			if labels["phase"] == "selected" {
				source := labels["source"]
				if source != "" {
					snapshot.selected[source] += value
				}
			}
		case "tikv_storage_extra_cf_conflict_check_total":
			if labels["mode"] == "both" {
				snapshot.bothConflictTotal += value
				if labels["outcome"] == "write_conflict" {
					snapshot.bothWriteConflict += value
				}
			}
		}
	}
	return snapshot, scanner.Err()
}

func parseMetricLine(line string) (string, map[string]string, float64, bool) {
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "#") {
		return "", nil, 0, false
	}
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return "", nil, 0, false
	}
	value, err := strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return "", nil, 0, false
	}
	metric := fields[0]
	labels := map[string]string{}
	if open := strings.IndexByte(metric, '{'); open >= 0 {
		close := strings.LastIndexByte(metric, '}')
		if close <= open {
			return "", nil, 0, false
		}
		name := metric[:open]
		labels = parseLabels(metric[open+1 : close])
		return name, labels, value, true
	}
	return metric, labels, value, true
}

func parseLabels(input string) map[string]string {
	labels := map[string]string{}
	for _, part := range splitLabels(input) {
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
			unquoted, err := strconv.Unquote(value)
			if err == nil {
				value = unquoted
			} else {
				value = value[1 : len(value)-1]
			}
		}
		labels[key] = value
	}
	return labels
}

func splitLabels(input string) []string {
	var labels []string
	start := 0
	inQuote := false
	escaped := false
	for i, r := range input {
		switch {
		case escaped:
			escaped = false
		case r == '\\':
			escaped = true
		case r == '"':
			inQuote = !inQuote
		case r == ',' && !inQuote:
			labels = append(labels, input[start:i])
			start = i + 1
		}
	}
	if start <= len(input) {
		labels = append(labels, input[start:])
	}
	return labels
}

func (m *metricSnapshot) add(other metricSnapshot) {
	for source, value := range other.selected {
		m.selected[source] += value
	}
	m.bothWriteConflict += other.bothWriteConflict
	m.bothConflictTotal += other.bothConflictTotal
}

func printMetricDelta(label string, base, current metricSnapshot) {
	fmt.Printf("metrics label=%s selected_memtable=%.0f selected_l0=%.0f selected_stable=%.0f both_write_conflicts=%.0f both_conflict_checks=%.0f\n",
		label,
		counterDelta(base.selected["memtable"], current.selected["memtable"]),
		counterDelta(base.selected["l0"], current.selected["l0"]),
		counterDelta(base.selected["stable"], current.selected["stable"]),
		counterDelta(base.bothWriteConflict, current.bothWriteConflict),
		counterDelta(base.bothConflictTotal, current.bothConflictTotal),
	)
}

func verifyMetrics(cfg config, base, current metricSnapshot) []string {
	var failures []string
	if cfg.requireCoverage {
		for _, source := range []string{"memtable", "l0", "stable"} {
			if counterDelta(base.selected[source], current.selected[source]) <= 0 {
				failures = append(failures, fmt.Sprintf("missing selected %s coverage", source))
			}
		}
	}
	if cfg.requireConflictMetric && counterDelta(base.bothWriteConflict, current.bothWriteConflict) <= 0 {
		failures = append(failures, "both-mode write-conflict metric did not increase")
	}
	return failures
}

func counterDelta(base, current float64) float64 {
	if current >= base {
		return current - base
	}
	return current
}

func expandLogFiles(patterns []string) ([]string, error) {
	seen := map[string]struct{}{}
	var files []string
	for _, pattern := range patterns {
		matches := []string{pattern}
		if strings.ContainsAny(pattern, "*?[") {
			globMatches, err := filepath.Glob(pattern)
			if err != nil {
				return nil, fmt.Errorf("invalid log glob %q: %w", pattern, err)
			}
			if len(globMatches) == 0 {
				return nil, fmt.Errorf("log glob %q matched no files", pattern)
			}
			matches = globMatches
		}
		for _, match := range matches {
			if _, ok := seen[match]; ok {
				continue
			}
			seen[match] = struct{}{}
			files = append(files, match)
		}
	}
	sort.Strings(files)
	return files, nil
}

func captureLogOffsets(files []string) map[string]int64 {
	offsets := make(map[string]int64, len(files))
	for _, file := range files {
		info, err := os.Stat(file)
		if err != nil {
			fmt.Printf("log_warning path=%s err=%v\n", file, err)
			offsets[file] = 0
			continue
		}
		offsets[file] = info.Size()
		fmt.Printf("log_watch path=%s offset=%d\n", file, info.Size())
	}
	return offsets
}

func scanMismatchLogs(offsets map[string]int64) []string {
	var failures []string
	for file, offset := range offsets {
		found, err := fileContainsAfterOffset(file, offset, bothModeMismatch)
		if err != nil {
			failures = append(failures, fmt.Sprintf("scan log %s: %v", file, err))
			continue
		}
		if found {
			failures = append(failures, fmt.Sprintf("both-mode mismatch found in %s", file))
		}
	}
	return failures
}

func fileContainsAfterOffset(path string, offset int64, needle string) (bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer file.Close()
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return false, err
	}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), needle) {
			return true, nil
		}
	}
	return false, scanner.Err()
}

func quoteIdent(identifier string) string {
	if !validIdentifier.MatchString(identifier) {
		panic("invalid identifier escaped validation: " + identifier)
	}
	return "`" + identifier + "`"
}

func redactDSN(dsn string) string {
	if at := strings.LastIndex(dsn, "@"); at > 0 {
		prefix := dsn[:at]
		if colon := strings.Index(prefix, ":"); colon >= 0 {
			return prefix[:colon+1] + "xxxxx" + dsn[at:]
		}
	}
	return dsn
}
