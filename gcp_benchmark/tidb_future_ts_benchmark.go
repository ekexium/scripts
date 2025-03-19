package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// TestConfig holds all configuration parameters for the test
type TestConfig struct {
	Host              string
	Port              int
	User              string
	Password          string
	Database          string
	TableName         string
	Rows              int
	RegionCount       int
	Duration          int
	ConcurrencyLevels []int
	FutureTS          int
	Cooldown          int
	Verbose           bool
	SplitRegions      bool // Whether to split the table into regions
	UseFutureTS       bool // Whether to use future timestamp in queries
	// Prometheus configuration
	PrometheusAddr    string // Prometheus server address
	PrometheusPort    int    // Prometheus server port
	CollectMetrics    bool   // Whether to collect Prometheus metrics
}

// TestResult holds the results of a single test run at a specific concurrency level
type TestResult struct {
	Concurrency         int     `json:"concurrency"`
	Duration            float64 `json:"duration"`
	TotalAttempts       int     `json:"total_attempts"`
	TotalQueries        int     `json:"total_queries"`
	TotalErrors         int     `json:"total_errors"`
	TotalRecordsScanned int     `json:"total_records_scanned"`
	AttemptsPerSec      float64 `json:"attempts_per_sec"`
	SuccessfulQPS       float64 `json:"successful_qps"`
	RecordsPerSec       float64 `json:"records_per_sec"`
	ErrorRate           float64 `json:"error_rate"`
	SplitRegions        bool    `json:"split_regions"`
	UseFutureTS         bool    `json:"use_future_ts"`
	TSORequests         float64 `json:"tso_requests"`
}

// ClientResult holds the results from a single test client
type ClientResult struct {
	Queries        int
	Errors         int
	RecordsScanned int
}

// TestRunner orchestrates the entire test process
type TestRunner struct {
	Config      TestConfig
	DB          *sql.DB
	TestResults map[int]TestResult
}

// TestRunnerGroup holds all test runners for different configurations
type TestRunnerGroup struct {
	Runners []*TestRunner
}

// NewTestRunner creates a new test runner with the given configuration
func NewTestRunner(config TestConfig) *TestRunner {
	return &TestRunner{
		Config:      config,
		TestResults: make(map[int]TestResult),
	}
}

// NewTestRunnerGroup creates a new group of test runners
func NewTestRunnerGroup() *TestRunnerGroup {
	return &TestRunnerGroup{
		Runners: make([]*TestRunner, 0),
	}
}

// AddRunner adds a test runner to the group
func (g *TestRunnerGroup) AddRunner(runner *TestRunner) {
	g.Runners = append(g.Runners, runner)
}

// Connect establishes a connection to the TiDB database
func (t *TestRunner) Connect() error {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?timeout=10s&autocommit=true",
		t.Config.User, t.Config.Password, t.Config.Host, t.Config.Port, t.Config.Database)

	var err error
	t.DB, err = sql.Open("mysql", dsn)
	if err != nil {
		return err
	}

	// Configure connection pool
	t.DB.SetMaxOpenConns(200) // Adjust based on your system capacity
	t.DB.SetMaxIdleConns(100)
	t.DB.SetConnMaxLifetime(time.Hour)

	// Test connection
	err = t.DB.Ping()
	if err != nil {
		return err
	}

	fmt.Printf("Connected to TiDB: %s:%d\n", t.Config.Host, t.Config.Port)
	return nil
}

// Close closes the database connection
func (t *TestRunner) Close() {
	if t.DB != nil {
		t.DB.Close()
		fmt.Println("Database connection closed")
	}
}

// SetupTable creates and populates the test table
func (t *TestRunner) SetupTable() error {
	// Drop existing table if any
	_, err := t.DB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", t.Config.TableName))
	if err != nil {
		return fmt.Errorf("failed to drop table: %v", err)
	}

	// Create test table
	fmt.Printf("Creating test table %s...\n", t.Config.TableName)
	_, err = t.DB.Exec(fmt.Sprintf(`
        CREATE TABLE %s (
            id INT PRIMARY KEY
        )
    `, t.Config.TableName))
	if err != nil {
		return fmt.Errorf("failed to create table: %v", err)
	}

	// Insert test data
	fmt.Printf("Inserting %d rows of test data...\n", t.Config.Rows)
	batchSize := 10000
	progressStep := t.Config.Rows / 20
	if progressStep < 1 {
		progressStep = 1
	}

	for i := 0; i < t.Config.Rows; i += batchSize {
		end := i + batchSize
		if end > t.Config.Rows {
			end = t.Config.Rows
		}

		tx, err := t.DB.Begin()
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %v", err)
		}

		var values string
		for j := i; j < end; j++ {
			if j > i {
				values += ","
			}
			values += fmt.Sprintf("(%d)", j)
		}

		query := fmt.Sprintf("INSERT INTO %s (id) VALUES %s", t.Config.TableName, values)
		_, err = tx.Exec(query)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to insert data: %v", err)
		}

		err = tx.Commit()
		if err != nil {
			return fmt.Errorf("failed to commit transaction: %v", err)
		}

		if i%progressStep < batchSize {
			progress := float64(end) / float64(t.Config.Rows) * 100
			fmt.Printf("Inserted %d / %d rows (%.0f%%)\n", end, t.Config.Rows, progress)
		}
	}

	// Split table into regions if configured
	if t.Config.SplitRegions {
		err = t.SplitTable()
		if err != nil {
			return err
		}
	} else {
		fmt.Println("Skipping table split as per configuration")
	}

	return nil
}

// SplitTable splits the test table into multiple regions
func (t *TestRunner) SplitTable() error {
	fmt.Printf("Splitting table into %d regions...\n", t.Config.RegionCount)

	// Use a separate connection for SPLIT operations
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?timeout=300s&autocommit=true",
		t.Config.User, t.Config.Password, t.Config.Host, t.Config.Port, t.Config.Database)

	splitDB, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to open split connection: %v", err)
	}
	defer splitDB.Close()

	// Execute the SPLIT TABLE command
	_, err = splitDB.Exec(fmt.Sprintf(
		"SPLIT TABLE %s BETWEEN (0) AND (%d) REGIONS %d",
		t.Config.TableName, t.Config.Rows, t.Config.RegionCount))
	if err != nil {
		return fmt.Errorf("failed to split table: %v", err)
	}

	// Verify the split worked
	rows, err := splitDB.Query(fmt.Sprintf("SHOW TABLE %s REGIONS", t.Config.TableName))
	if err != nil {
		return fmt.Errorf("failed to show regions: %v", err)
	}
	defer rows.Close()

	var regionCount int
	for rows.Next() {
		regionCount++
	}

	fmt.Printf("Table %s now has %d regions\n", t.Config.TableName, regionCount)
	return nil
}

// RunClient executes the test queries for a single client
func (t *TestRunner) RunClient(clientID int, duration time.Duration, resultChan chan<- ClientResult) {
	// Create a new connection for this client
	if t.Config.Verbose {
		fmt.Printf("Client %d: Connecting to TiDB...\n", clientID)
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?timeout=300s&autocommit=true",
		t.Config.User, t.Config.Password, t.Config.Host, t.Config.Port, t.Config.Database)

	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Printf("Client %d: Failed to connect: %v\n", clientID, err)
		resultChan <- ClientResult{0, 1, 0}
		return
	}
	defer conn.Close()

	// Test connection and table existence
	var count int
	err = conn.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s LIMIT 1", t.Config.TableName)).Scan(&count)
	if err != nil {
		fmt.Printf("Client %d: ERROR - Cannot access test table: %v\n", clientID, err)
		resultChan <- ClientResult{0, 1, 0}
		return
	}

	if t.Config.Verbose || clientID == 0 {
		fmt.Printf("Client %d: Table verification successful. Found %d total rows.\n", clientID, count)
		
		// Log query type only once at the beginning
		if clientID == 0 {
			if t.Config.UseFutureTS {
				fmt.Printf("Using queries with future timestamp (AS OF TIMESTAMP) of %d ms\n", t.Config.FutureTS)
			} else {
				fmt.Printf("Using regular queries (without AS OF TIMESTAMP)\n")
			}
		}
	}

	// Run the test queries
	startTime := time.Now()
	endTime := startTime.Add(duration)

	queryCount := 0
	errorCount := 0
	recordsScanned := 0

	for time.Now().Before(endTime) {
		var query string
		
		if t.Config.UseFutureTS {
			// Use fixed future timestamp
			microseconds := t.Config.FutureTS * 1000
			futureTS := fmt.Sprintf("NOW() + INTERVAL %d MICROSECOND", microseconds)
			query = fmt.Sprintf("SELECT COUNT(*) FROM %s AS OF TIMESTAMP %s", t.Config.TableName, futureTS)
		} else {
			// Regular query without future timestamp
			query = fmt.Sprintf("SELECT COUNT(*) FROM %s", t.Config.TableName)
		}

		var recordCount int
		err := conn.QueryRow(query).Scan(&recordCount)
		if err != nil {
			errorCount++
			if t.Config.Verbose && (errorCount == 1 || (errorCount <= 10 && errorCount%5 == 0) || errorCount%100 == 0) {
				fmt.Printf("Client %d: Query failed (%d times): %v...\n", clientID, errorCount, err)
				if errorCount == 1 {
					fmt.Printf("Query was: %s\n", query)
				}
			} else if !t.Config.Verbose && (errorCount == 1) {
				fmt.Printf("Client %d: Query failed (%d times): %v...\n", clientID, errorCount, err)
				if errorCount == 1 {
					fmt.Printf("Query was: %s\n", query)
				}
			}

			continue
		}

		queryCount++
		recordsScanned += recordCount

		// Progress reporting
		if t.Config.Verbose {
			if queryCount%1000 == 0 && clientID < 3 {
				elapsed := time.Since(startTime).Seconds()
				fmt.Printf("Client %d: Progress: %d queries in %.1fs (%.1f qps)\n",
					clientID, queryCount, elapsed, float64(queryCount)/elapsed)
			}
		} else if clientID == 0 && queryCount%5000 == 0 {
			elapsed := time.Since(startTime).Seconds()
			fmt.Printf("Progress: %d queries in %.1fs (%.1f qps)\n",
				queryCount, elapsed, float64(queryCount)/elapsed)
		}
	}

	fmt.Printf("Client %d completed: %d queries, %d errors, %d records\n",
		clientID, queryCount, errorCount, recordsScanned)

	resultChan <- ClientResult{queryCount, errorCount, recordsScanned}
}

// RunTest runs a test with a specific concurrency level
func (t *TestRunner) RunTest(concurrency int) TestResult {
	fmt.Printf("\nStarting test with concurrency %d...\n", concurrency)

	// Collect counter value before test
	var beforeCounter float64
	var err error
	if t.Config.CollectMetrics {
		fmt.Println("Collecting counter value before test...")
		beforeCounter, err = t.queryPrometheusCounter()
		if err != nil {
			fmt.Printf("WARNING: Failed to collect pre-test counter: %v\n", err)
		} else {
			fmt.Printf("Initial TSO counter value: %.0f\n", beforeCounter)
		}
	}
	
	resultChan := make(chan ClientResult, concurrency)
	var wg sync.WaitGroup

	startTime := time.Now()

	// Start client goroutines
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			t.RunClient(clientID, time.Duration(t.Config.Duration)*time.Second, resultChan)
		}(i)

		// Progress for large concurrency values
		if concurrency > 50 && i > 0 && i%50 == 0 {
			fmt.Printf("Started %d of %d clients...\n", i, concurrency)
		}
	}

	// Wait for all clients in a separate goroutine
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	totalQueries := 0
	totalErrors := 0
	totalRecords := 0

	for result := range resultChan {
		totalQueries += result.Queries
		totalErrors += result.Errors
		totalRecords += result.RecordsScanned
	}

	endTime := time.Now()
	actualDuration := endTime.Sub(startTime).Seconds()

	totalAttempts := totalQueries + totalErrors

	// Calculate metrics
	attemptsPerSec := float64(totalAttempts) / actualDuration
	successfulQPS := float64(totalQueries) / actualDuration
	recordsPerSec := float64(totalRecords) / actualDuration
	errorRate := 0.0
	if totalAttempts > 0 {
		errorRate = float64(totalErrors) / float64(totalAttempts)
	}

	// Collect counter value after test
	var afterCounter float64
	var tsoRequestCount float64
	if t.Config.CollectMetrics {
		fmt.Println("Collecting counter value after test...")
		afterCounter, err = t.queryPrometheusCounter()
		if err != nil {
			fmt.Printf("WARNING: Failed to collect post-test counter: %v\n", err)
		} else {
			tsoRequestCount = afterCounter - beforeCounter
			fmt.Printf("Final TSO counter value: %.0f\n", afterCounter)
			fmt.Printf("TSO requests during test: %.0f\n", tsoRequestCount)
			fmt.Printf("TSO requests per second: %.2f\n", tsoRequestCount/actualDuration)
		}
	}

	// Create result
	result := TestResult{
		Concurrency:         concurrency,
		Duration:            actualDuration,
		TotalAttempts:       totalAttempts,
		TotalQueries:        totalQueries,
		TotalErrors:         totalErrors,
		TotalRecordsScanned: totalRecords,
		AttemptsPerSec:      attemptsPerSec,
		SuccessfulQPS:       successfulQPS,
		RecordsPerSec:       recordsPerSec,
		ErrorRate:           errorRate,
		SplitRegions:        t.Config.SplitRegions,
		UseFutureTS:         t.Config.UseFutureTS,
		TSORequests:         tsoRequestCount,
	}

	t.TestResults[concurrency] = result

	// Print results
	fmt.Printf("Test results for concurrency %d:\n", concurrency)
	fmt.Printf("  Actual test duration: %.2f seconds\n", actualDuration)
	fmt.Printf("  Total query attempts: %d\n", totalAttempts)
	fmt.Printf("  Successful queries: %d\n", totalQueries)
	fmt.Printf("  Failed queries: %d\n", totalErrors)
	fmt.Printf("  Total records scanned: %d\n", totalRecords)
	fmt.Printf("  Attempts/sec: %.2f\n", attemptsPerSec)
	if totalQueries > 0 {
		fmt.Printf("  Successful QPS: %.2f\n", successfulQPS)
	}
	fmt.Printf("  Error rate: %.2f%%\n", errorRate*100)

	// Cooldown
	fmt.Printf("Waiting for system cooldown, %d seconds...\n", t.Config.Cooldown)
	time.Sleep(time.Duration(t.Config.Cooldown) * time.Second)

	return result
}

// RunAllTests runs tests for all specified concurrency levels
func (t *TestRunner) RunAllTests() {
	for _, concurrency := range t.Config.ConcurrencyLevels {
		t.RunTest(concurrency)
	}

	t.GenerateReport()
}

// GenerateReport creates a summary report of all test results
func (t *TestRunner) GenerateReport() {
	if len(t.TestResults) == 0 {
		fmt.Println("No test results available for report")
		return
	}

	// Create report strings
	var report strings.Builder
	
	report.WriteString("\n===== TEST REPORT =====\n")
	report.WriteString(fmt.Sprintf("Test table: %s\n", t.Config.TableName))
	report.WriteString(fmt.Sprintf("Row count: %d\n", t.Config.Rows))
	report.WriteString(fmt.Sprintf("Test duration per concurrency: %d seconds\n", t.Config.Duration))
	report.WriteString(fmt.Sprintf("Split regions: %v (count: %d)\n", t.Config.SplitRegions, t.Config.RegionCount))
	report.WriteString(fmt.Sprintf("Use future timestamp: %v (%d milliseconds)\n\n", t.Config.UseFutureTS, t.Config.FutureTS))

	// Headers
	report.WriteString("Test Results:\n")
	report.WriteString("--------------------------------------------------------------------------------\n")
	report.WriteString(fmt.Sprintf("%-10s %-15s %-15s %-10s %-12s %-15s\n",
		"Concurrency", "Total Attempts", "Success", "Errors", "Attempts/sec", "Error Rate(%)"))
	report.WriteString("--------------------------------------------------------------------------------\n")

	// Sort concurrency levels
	var levels []int
	for level := range t.TestResults {
		levels = append(levels, level)
	}
	sort.Ints(levels)

	// Print results
	for _, concurrency := range levels {
		result := t.TestResults[concurrency]
		report.WriteString(fmt.Sprintf("%-10d %-15d %-15d %-10d %-12.2f %-15.2f%%\n",
			concurrency,
			result.TotalAttempts,
			result.TotalQueries,
			result.TotalErrors,
			result.AttemptsPerSec,
			result.ErrorRate*100))
	}
	report.WriteString("--------------------------------------------------------------------------------\n")

	// If we have metrics data, generate metrics report
	if t.Config.CollectMetrics {
		report.WriteString("\nPrometheus Metrics Summary:\n")
		report.WriteString("--------------------------------------------------------------------------------\n")
		
		// Print TSO request information for each concurrency
		for _, level := range levels {
			result := t.TestResults[level]
			report.WriteString(fmt.Sprintf("\nConcurrency %d TSO metrics:\n", level))
			report.WriteString(fmt.Sprintf("  Total TSO requests: %.0f\n", result.TSORequests))
			report.WriteString(fmt.Sprintf("  TSO requests per second: %.2f\n", result.TSORequests/result.Duration))
		}
		
		report.WriteString("--------------------------------------------------------------------------------\n")
		
		// Calculate average TSO requests across levels
		report.WriteString("\nAverage TSO Metrics (across all concurrency levels):\n")
		report.WriteString("--------------------------------------------------------------------------------\n")
		
		var totalTSORequests float64
		var totalDuration float64
		var count int
		
		for _, level := range levels {
			result := t.TestResults[level]
			if result.TSORequests > 0 {
				totalTSORequests += result.TSORequests
				totalDuration += result.Duration
				count++
			}
		}
		
		if count > 0 {
			avgTSORequests := totalTSORequests / float64(count)
			avgTSORate := totalTSORequests / totalDuration
			
			report.WriteString(fmt.Sprintf("Average TSO requests per test: %.2f\n", avgTSORequests))
			report.WriteString(fmt.Sprintf("Average TSO requests per second: %.2f\n", avgTSORate))
		} else {
			report.WriteString("No TSO metrics collected\n")
		}
		
		report.WriteString("--------------------------------------------------------------------------------\n")
	}

	// Print to console
	fmt.Print(report.String())

	// Save test results as JSON
	jsonData, err := json.MarshalIndent(t.TestResults, "", "  ")
	if err != nil {
		fmt.Printf("Failed to serialize test results: %v\n", err)
		return
	}

	// Create a filename that includes test configuration
	splitStatus := "nosplit"
	if t.Config.SplitRegions {
		splitStatus = fmt.Sprintf("split%d", t.Config.RegionCount)
	}
	
	tsStatus := "regular"
	if t.Config.UseFutureTS {
		tsStatus = fmt.Sprintf("futurets%d", t.Config.FutureTS)
	}
	
	// Generate timestamp for files
	timestamp := time.Now().Format("20060102_150405")
	jsonFilename := fmt.Sprintf("tidb_test_%s_%s_%s.json", splitStatus, tsStatus, timestamp)
	
	// Save the report text to a file
	reportFilename := fmt.Sprintf("tidb_test_%s_%s_%s_report.txt", splitStatus, tsStatus, timestamp)
	
	// Write JSON results
	err = os.WriteFile(jsonFilename, jsonData, 0644)
	if err != nil {
		fmt.Printf("Failed to save test results: %v\n", err)
		return
	}
	
	// Write text report
	err = os.WriteFile(reportFilename, []byte(report.String()), 0644)
	if err != nil {
		fmt.Printf("Failed to save test report: %v\n", err)
		return
	}

	fmt.Printf("Test results saved to %s and %s\n", jsonFilename, reportFilename)
}

// GenerateComparisonReport generates a comparison report across all test combinations
func (g *TestRunnerGroup) GenerateComparisonReport() {
	if len(g.Runners) == 0 {
		fmt.Println("No test results available for comparison report")
		return
	}

	// Create report strings
	var report strings.Builder
	
	report.WriteString("\n===============================================================\n")
	report.WriteString("              COMPARISON ACROSS ALL TEST CONFIGURATIONS              \n")
	report.WriteString("===============================================================\n")

	// Get a list of all concurrency levels across all runners
	concurrencyLevels := make(map[int]bool)
	for _, runner := range g.Runners {
		for level := range runner.TestResults {
			concurrencyLevels[level] = true
		}
	}

	// Convert to sorted slice
	var levels []int
	for level := range concurrencyLevels {
		levels = append(levels, level)
	}
	sort.Ints(levels)

	// Create a table comparing QPS and error rates
	report.WriteString("\n1. Performance Comparison (QPS):\n")
	report.WriteString("--------------------------------------------------------------------------------\n")
	
	// Print header
	report.WriteString(fmt.Sprintf("%-10s ", "Concurr."))
	for _, runner := range g.Runners {
		config := runner.Config
		splitStatus := "No Split"
		if config.SplitRegions {
			splitStatus = fmt.Sprintf("Split(%d)", config.RegionCount)
		}
		
		tsStatus := "Regular"
		if config.UseFutureTS {
			tsStatus = fmt.Sprintf("Future(%d)", config.FutureTS)
		}
		
		title := fmt.Sprintf("%s,%s", splitStatus, tsStatus)
		report.WriteString(fmt.Sprintf("%-20s ", title))
	}
	report.WriteString("\n")
	report.WriteString("--------------------------------------------------------------------------------\n")

	// Print data rows
	for _, level := range levels {
		report.WriteString(fmt.Sprintf("%-10d ", level))
		
		for _, runner := range g.Runners {
			result, ok := runner.TestResults[level]
			if ok {
				report.WriteString(fmt.Sprintf("%-20.2f ", result.SuccessfulQPS))
			} else {
				report.WriteString(fmt.Sprintf("%-20s ", "N/A"))
			}
		}
		report.WriteString("\n")
	}
	report.WriteString("--------------------------------------------------------------------------------\n")

	// Create a table comparing error rates
	report.WriteString("\n2. Error Rate Comparison (%):\n")
	report.WriteString("--------------------------------------------------------------------------------\n")
	
	// Print header
	report.WriteString(fmt.Sprintf("%-10s ", "Concurr."))
	for _, runner := range g.Runners {
		config := runner.Config
		splitStatus := "No Split"
		if config.SplitRegions {
			splitStatus = fmt.Sprintf("Split(%d)", config.RegionCount)
		}
		
		tsStatus := "Regular"
		if config.UseFutureTS {
			tsStatus = fmt.Sprintf("Future(%d)", config.FutureTS)
		}
		
		title := fmt.Sprintf("%s,%s", splitStatus, tsStatus)
		report.WriteString(fmt.Sprintf("%-20s ", title))
	}
	report.WriteString("\n")
	report.WriteString("--------------------------------------------------------------------------------\n")

	// Print data rows
	for _, level := range levels {
		report.WriteString(fmt.Sprintf("%-10d ", level))
		
		for _, runner := range g.Runners {
			result, ok := runner.TestResults[level]
			if ok {
				report.WriteString(fmt.Sprintf("%-20.2f%% ", result.ErrorRate*100))
			} else {
				report.WriteString(fmt.Sprintf("%-20s ", "N/A"))
			}
		}
		report.WriteString("\n")
	}
	report.WriteString("--------------------------------------------------------------------------------\n")

	// Generate a table comparing TSO requests
	report.WriteString("\n3. TSO Requests Comparison:\n")
	report.WriteString("--------------------------------------------------------------------------------\n")

	// Print header
	report.WriteString(fmt.Sprintf("%-10s ", "Concurr."))
	for _, runner := range g.Runners {
		config := runner.Config
		splitStatus := "No Split"
		if config.SplitRegions {
			splitStatus = fmt.Sprintf("Split(%d)", config.RegionCount)
		}
		
		tsStatus := "Regular"
		if config.UseFutureTS {
			tsStatus = fmt.Sprintf("Future(%d)", config.FutureTS)
		}
		
		title := fmt.Sprintf("%s,%s", splitStatus, tsStatus)
		report.WriteString(fmt.Sprintf("%-20s ", title))
	}
	report.WriteString("\n")
	report.WriteString("--------------------------------------------------------------------------------\n")

	// Print data rows
	for _, level := range levels {
		report.WriteString(fmt.Sprintf("%-10d ", level))
		
		for _, runner := range g.Runners {
			result, ok := runner.TestResults[level]
			if ok {
				report.WriteString(fmt.Sprintf("%-20.0f ", result.TSORequests))
			} else {
				report.WriteString(fmt.Sprintf("%-20s ", "N/A"))
			}
		}
		report.WriteString("\n")
	}
	report.WriteString("--------------------------------------------------------------------------------\n")

	// Calculate average metrics for each runner
	hasMetrics := false
	for _, runner := range g.Runners {
		if runner.Config.CollectMetrics {
			hasMetrics = true
			break
		}
	}
	
	if hasMetrics {
		// Print summary of TSO request comparison
		report.WriteString("\n4. TSO Requests Analysis:\n")
		report.WriteString("--------------------------------------------------------------------------------\n")
		
		// Calculate average TSO requests for each configuration
		report.WriteString(fmt.Sprintf("%-25s %-20s %-20s\n", "Configuration", "Avg TSO Requests", "Avg TSO Reqs/sec"))
		report.WriteString("--------------------------------------------------------------------------------\n")
		
		for _, runner := range g.Runners {
			config := runner.Config
			
			splitStatus := "No Split"
			if config.SplitRegions {
				splitStatus = fmt.Sprintf("Split(%d)", config.RegionCount)
			}
			
			tsStatus := "Regular"
			if config.UseFutureTS {
				tsStatus = fmt.Sprintf("Future(%d)", config.FutureTS)
			}
			
			title := fmt.Sprintf("%s + %s", splitStatus, tsStatus)
			
			// Calculate averages
			var totalTSO, totalDuration float64
			var count int
			
			for _, result := range runner.TestResults {
				if result.TSORequests > 0 {
					totalTSO += result.TSORequests
					totalDuration += result.Duration
					count++
				}
			}
			
			if count > 0 {
				avgTSO := totalTSO / float64(count)
				avgTSORate := totalTSO / totalDuration
				report.WriteString(fmt.Sprintf("%-25s %-20.0f %-20.2f\n", title, avgTSO, avgTSORate))
			} else {
				report.WriteString(fmt.Sprintf("%-25s %-20s %-20s\n", title, "N/A", "N/A"))
			}
		}
		
		report.WriteString("--------------------------------------------------------------------------------\n")
	}

	report.WriteString("\nSummary of Findings:\n")
	report.WriteString("--------------------------------------------------------------------------------\n")
	
	// Find the best configuration for QPS
	type ConfigPerf struct {
		SplitRegions bool
		UseFutureTS  bool
		AvgQPS       float64
		AvgError     float64
	}
	
	// Calculate average QPS and error rate for each configuration
	var configPerfs []ConfigPerf
	for _, runner := range g.Runners {
		var sumQPS, sumError float64
		var count int
		
		for _, result := range runner.TestResults {
			sumQPS += result.SuccessfulQPS
			sumError += result.ErrorRate
			count++
		}
		
		if count > 0 {
			configPerfs = append(configPerfs, ConfigPerf{
				SplitRegions: runner.Config.SplitRegions,
				UseFutureTS:  runner.Config.UseFutureTS,
				AvgQPS:       sumQPS / float64(count),
				AvgError:     sumError / float64(count) * 100,
			})
		}
	}
	
	// Sort by QPS (descending)
	sort.Slice(configPerfs, func(i, j int) bool {
		return configPerfs[i].AvgQPS > configPerfs[j].AvgQPS
	})
	
	// Print summary
	report.WriteString("Performance ranking (best to worst):\n")
	for i, perf := range configPerfs {
		splitStatus := "No Split"
		if perf.SplitRegions {
			splitStatus = "Split Regions"
		}
		
		tsStatus := "Regular Queries"
		if perf.UseFutureTS {
			tsStatus = "Future TS Queries"
		}
		
		report.WriteString(fmt.Sprintf("%d. %s + %s: Avg QPS = %.2f, Avg Error Rate = %.2f%%\n", 
			i+1, splitStatus, tsStatus, perf.AvgQPS, perf.AvgError))
	}
	
	report.WriteString("--------------------------------------------------------------------------------\n")
	report.WriteString("End of comparison report\n")

	// Print to console
	fmt.Print(report.String())
	
	// Save report to file
	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("tidb_test_comparison_%s.txt", timestamp)
	
	// Save all test results as a single JSON file
	resultsMap := make(map[string]map[int]TestResult)
	for _, runner := range g.Runners {
		config := runner.Config
		configKey := fmt.Sprintf("split_%v_futurets_%v", config.SplitRegions, config.UseFutureTS)
		resultsMap[configKey] = runner.TestResults
	}
	
	jsonData, err := json.MarshalIndent(resultsMap, "", "  ")
	if err != nil {
		fmt.Printf("Failed to serialize comparison results: %v\n", err)
	} else {
		jsonFilename := fmt.Sprintf("tidb_test_comparison_%s.json", timestamp)
		if err = os.WriteFile(jsonFilename, jsonData, 0644); err != nil {
			fmt.Printf("Failed to save comparison results: %v\n", err)
		} else {
			fmt.Printf("Comparison results saved to %s\n", jsonFilename)
		}
	}
	
	// Write text report
	if err = os.WriteFile(filename, []byte(report.String()), 0644); err != nil {
		fmt.Printf("Failed to save comparison report: %v\n", err)
		return
	}
	
	fmt.Printf("Comparison report saved to %s\n", filename)
}

// parseConcurrencyLevels parses the concurrency levels from a string
func parseConcurrencyLevels(s string) ([]int, error) {
	parts := strings.Split(s, ",")
	var result []int
	for _, p := range parts {
		// Trim whitespace
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		
		// Convert to int
		i, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("invalid concurrency level '%s': %v", p, err)
		}
		if i <= 0 {
			return nil, fmt.Errorf("concurrency level must be positive: %d", i)
		}
		result = append(result, i)
	}
	if len(result) == 0 {
		return []int{1, 5, 10, 20, 50, 100}, nil // Default levels
	}
	return result, nil
}

// queryPrometheusCounter queries the Prometheus server and returns the raw counter value
func (t *TestRunner) queryPrometheusCounter() (float64, error) {
	if !t.Config.CollectMetrics {
		return 0, nil
	}

	// Check if Prometheus address is provided
	if t.Config.PrometheusAddr == "" {
		return 0, fmt.Errorf("Prometheus address not provided")
	}

	// Query the raw counter value directly
	query := `sum(pd_client_request_handle_requests_duration_seconds_count{type="tso"})`

	// Build Prometheus API query URL
	queryURL := fmt.Sprintf("http://%s:%d/api/v1/query?query=%s",
		t.Config.PrometheusAddr, t.Config.PrometheusPort, url.QueryEscape(query))

	// Make HTTP request
	resp, err := http.Get(queryURL)
	if err != nil {
		return 0, fmt.Errorf("failed to query Prometheus: %v", err)
	}
	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("failed to read Prometheus response: %v", err)
	}

	// Parse JSON response
	var promResp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Value  []interface{}     `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}

	if err := json.Unmarshal(body, &promResp); err != nil {
		return 0, fmt.Errorf("failed to parse Prometheus response: %v", err)
	}

	// Check if response is successful
	if promResp.Status != "success" {
		return 0, fmt.Errorf("Prometheus query failed with status: %s", promResp.Status)
	}

	// Extract counter value
	if len(promResp.Data.Result) == 0 {
		return 0, fmt.Errorf("no results returned for counter query")
	}

	// Extract timestamp and value
	valueStr := promResp.Data.Result[0].Value[1].(string)
	value, err := strconv.ParseFloat(valueStr, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse metric value: %v", err)
	}

	return value, nil
}

func main() {
	// Parse command line arguments
	host := flag.String("host", "127.0.0.1", "TiDB host")
	port := flag.Int("port", 4000, "TiDB port")
	user := flag.String("user", "root", "TiDB username")
	password := flag.String("password", "", "TiDB password")
	database := flag.String("database", "test", "Database name")
	tableName := flag.String("table-name", "future_ts_test", "Test table name")
	rows := flag.Int("rows", 1000000, "Number of rows in test table")
	regionCount := flag.Int("region-count", 1000, "Number of regions to split table into")
	duration := flag.Int("duration", 60, "Duration of each test (seconds)")
	concurrencyStr := flag.String("concurrency", "16", "Comma-separated list of concurrency levels to test")
	futureTS := flag.Int("future-ts", 1000, "Fixed future timestamp in milliseconds")
	cooldown := flag.Int("cooldown", 60, "Cooldown time between tests (seconds)")
	verbose := flag.Bool("verbose", false, "Enable verbose logging")
	splitRegions := flag.Bool("split-regions", true, "Whether to split the table into regions")
	useFutureTS := flag.Bool("use-future-ts", true, "Whether to use future timestamp in queries")
	runAllTests := flag.Bool("run-all-tests", true, "Run tests for all combinations of split-regions and use-future-ts")
	prometheusAddr := flag.String("prometheus-addr", "127.0.0.1", "Prometheus server address")
	prometheusPort := flag.Int("prometheus-port", 9090, "Prometheus server port")
	collectMetrics := flag.Bool("collect-metrics", true, "Whether to collect Prometheus metrics")
	showHelp := flag.Bool("help-prometheus", false, "Show help about Prometheus metrics collection")

	flag.Parse()

	// Display Prometheus help if requested
	if *showHelp {
		fmt.Println("\nPrometheus Metrics Collection Help:")
		fmt.Println("====================================")
		fmt.Println("This tool collects TSO handling metrics from Prometheus to analyze")
		fmt.Println("how different test configurations affect PD's timestamp oracle (TSO).")
		fmt.Println("\nTo enable metrics collection, use the following flags:")
		fmt.Println("  -collect-metrics      : Enable metrics collection (default: true)")
		fmt.Println("  -prometheus-addr      : Prometheus server address (required)")
		fmt.Println("  -prometheus-port      : Prometheus server port (default: 9090)")
		fmt.Println("\nExample:")
		fmt.Println("  ./tidb_future_ts -collect-metrics -prometheus-addr=\"10.0.0.1\"")
		fmt.Println("\nMetrics collected:")
		fmt.Println("  sum(pd_client_request_handle_requests_duration_seconds_count{type=\"tso\"})")
		fmt.Println("\nThis tool uses the counter difference method to calculate TSO requests.")
		fmt.Println("It queries the raw counter value before and after each test run,")
		fmt.Println("then calculates the difference to determine the exact number of TSO")
		fmt.Println("requests that occurred during the test period.")
		fmt.Println("====================================")
		os.Exit(0)
	}

	// Parse concurrency levels
	concurrencyLevels, err := parseConcurrencyLevels(*concurrencyStr)
	if err != nil {
		log.Fatalf("Invalid concurrency levels: %v", err)
	}

	// Check Prometheus parameters
	if *collectMetrics {
		if *prometheusAddr == "" {
			log.Fatalf("Prometheus address (-prometheus-addr) is required when -collect-metrics is enabled")
		}
	}

	// Setup signal handler
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("Test interrupted")
		os.Exit(0)
	}()

	// Create base config
	baseConfig := TestConfig{
		Host:              *host,
		Port:              *port,
		User:              *user,
		Password:          *password,
		Database:          *database,
		TableName:         *tableName,
		Rows:              *rows,
		RegionCount:       *regionCount,
		Duration:          *duration,
		ConcurrencyLevels: concurrencyLevels,
		FutureTS:          *futureTS,
		Cooldown:          *cooldown,
		Verbose:           *verbose,
		SplitRegions:      *splitRegions,
		UseFutureTS:       *useFutureTS,
		PrometheusAddr:    *prometheusAddr,
		PrometheusPort:    *prometheusPort,
		CollectMetrics:    *collectMetrics,
	}

	// Create a test runner group to collect all runners
	runnerGroup := NewTestRunnerGroup()
	
	// Run tests based on configuration
	if *runAllTests {
		// Run all combinations
		combinations := []struct {
			split      bool
			useFutureTS bool
		}{
			{false, false},
			{false, true},
			{true, false},
			{true, true},
		}

		for i, combo := range combinations {
			config := baseConfig
			config.SplitRegions = combo.split
			config.UseFutureTS = combo.useFutureTS

			fmt.Printf("\n========================================================\n")
			fmt.Printf("RUNNING TEST COMBINATION %d of %d\n", i+1, len(combinations))
			fmt.Printf("Split Regions: %v, Use Future TS: %v\n", combo.split, combo.useFutureTS)
			fmt.Printf("========================================================\n")

			runner := NewTestRunner(config)
			if err := runner.Connect(); err != nil {
				log.Fatalf("Failed to connect to database: %v", err)
			}

			// Always setup the table for each test combination to ensure proper region configuration
			fmt.Printf("Setting up new table for test combination %d...\n", i+1)
			if err := runner.SetupTable(); err != nil {
				log.Fatalf("Failed to setup table: %v", err)
			}

			// For individual test runs, don't generate the reports to file
			// This improves performance and reduces file clutter
			for _, concurrency := range config.ConcurrencyLevels {
				runner.RunTest(concurrency)
			}
			
			runner.Close()
			
			// Add runner to the group for comparison report
			runnerGroup.AddRunner(runner)

			// Extra cooldown between test combinations
			fmt.Printf("Waiting for extra cooldown between test combinations, %d seconds...\n", *cooldown)
			time.Sleep(time.Duration(*cooldown) * time.Second)
		}
		
		// Generate comparison report after all tests - this will be our single output file
		fmt.Println("\nGenerating comprehensive report for all test configurations...")
		runnerGroup.GenerateComparisonReport()
		
		fmt.Println("\nAll tests completed successfully!")
	} else {
		// Run a single test with the specified configuration
		config := baseConfig
		config.SplitRegions = *splitRegions
		config.UseFutureTS = *useFutureTS

		runner := NewTestRunner(config)
		if err := runner.Connect(); err != nil {
			log.Fatalf("Failed to connect to database: %v", err)
		}
		defer runner.Close()

		// Always setup the table before running tests
		fmt.Println("Setting up test table...")
		if err := runner.SetupTable(); err != nil {
			log.Fatalf("Failed to setup table: %v", err)
		}

		// For single configuration, run all tests and generate the report
		for _, concurrency := range config.ConcurrencyLevels {
			runner.RunTest(concurrency)
		}
		
		// Generate report for this single configuration
		runner.GenerateReport()
		
		// Add runner to the group for the report
		runnerGroup.AddRunner(runner)
		
		fmt.Println("\nTest completed successfully!")
	}
}
