package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
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

// NewTestRunner creates a new test runner with the given configuration
func NewTestRunner(config TestConfig) *TestRunner {
	return &TestRunner{
		Config:      config,
		TestResults: make(map[int]TestResult),
	}
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
	batchSize := 5000
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

	// Split table into regions
	err = t.SplitTable()
	if err != nil {
		return err
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
	}

	// Run the test queries
	startTime := time.Now()
	endTime := startTime.Add(duration)

	queryCount := 0
	errorCount := 0
	recordsScanned := 0

	for time.Now().Before(endTime) {
		// Use fixed future timestamp
		microseconds := t.Config.FutureTS * 1000
		futureTS := fmt.Sprintf("NOW() + INTERVAL %d MICROSECOND", microseconds)
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s AS OF TIMESTAMP %s", t.Config.TableName, futureTS)

		var recordCount int
		err := conn.QueryRow(query).Scan(&recordCount)
		if err != nil {
			errorCount++
			if t.Config.Verbose && (errorCount == 1 || (errorCount <= 10 && errorCount%5 == 0) || errorCount%100 == 0) {
				fmt.Printf("Client %d: Query failed (%d times): %v...\n", clientID, errorCount, err)
			} else if !t.Config.Verbose && (errorCount == 1 || errorCount%500 == 0) {
				fmt.Printf("Client %d: Query failed (%d times): %v...\n", clientID, errorCount, err)
			}

			time.Sleep(100 * time.Millisecond) // Avoid immediate retry
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

	fmt.Println("\n===== TEST REPORT =====")
	fmt.Printf("Test table: %s\n", t.Config.TableName)
	fmt.Printf("Row count: %d\n", t.Config.Rows)
	fmt.Printf("Test duration per concurrency: %d seconds\n", t.Config.Duration)
	fmt.Printf("Region count: %d\n", t.Config.RegionCount)
	fmt.Printf("Future timestamp: %d milliseconds\n\n", t.Config.FutureTS)

	// Headers
	fmt.Println("Test Results:")
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Printf("%-10s %-15s %-15s %-10s %-12s %-15s\n",
		"Concurrency", "Total Attempts", "Success", "Errors", "Attempts/sec", "Error Rate(%)")
	fmt.Println("--------------------------------------------------------------------------------")

	// Sort concurrency levels
	var levels []int
	for level := range t.TestResults {
		levels = append(levels, level)
	}
	sort.Ints(levels)

	// Print results
	for _, concurrency := range levels {
		result := t.TestResults[concurrency]
		fmt.Printf("%-10d %-15d %-15d %-10d %-12.2f %-15.2f%%\n",
			concurrency,
			result.TotalAttempts,
			result.TotalQueries,
			result.TotalErrors,
			result.AttemptsPerSec,
			result.ErrorRate*100)
	}
	fmt.Println("--------------------------------------------------------------------------------")

	// Save raw results
	jsonData, err := json.MarshalIndent(t.TestResults, "", "  ")
	if err != nil {
		fmt.Printf("Failed to serialize test results: %v\n", err)
		return
	}

	err = os.WriteFile("future_ts_test_results.json", jsonData, 0644)
	if err != nil {
		fmt.Printf("Failed to save test results: %v\n", err)
		return
	}

	fmt.Println("Test results saved to future_ts_test_results.json")
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

func main() {
	// Parse command line arguments
	host := flag.String("host", "127.0.0.1", "TiDB host")
	port := flag.Int("port", 4000, "TiDB port")
	user := flag.String("user", "root", "TiDB username")
	password := flag.String("password", "", "TiDB password")
	database := flag.String("database", "test", "Database name")
	tableName := flag.String("table-name", "future_ts_test", "Test table name")
	rows := flag.Int("rows", 1000000, "Number of rows in test table")
	regionCount := flag.Int("region-count", 10, "Number of regions to split table into")
	duration := flag.Int("duration", 60, "Duration of each test (seconds)")
	concurrencyStr := flag.String("concurrency", "1,5,10,20,50,100", "Comma-separated list of concurrency levels to test")
	futureTS := flag.Int("future-ts", 1000, "Fixed future timestamp in milliseconds")
	cooldown := flag.Int("cooldown", 60, "Cooldown time between tests (seconds)")
	verbose := flag.Bool("verbose", false, "Enable verbose logging")

	flag.Parse()

	// Parse concurrency levels
	concurrencyLevels, err := parseConcurrencyLevels(*concurrencyStr)
	if err != nil {
		log.Fatalf("Invalid concurrency levels: %v", err)
	}

	config := TestConfig{
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
	}

	// Setup signal handler
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("Test interrupted")
		os.Exit(0)
	}()

	// Run test
	runner := NewTestRunner(config)
	if err := runner.Connect(); err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer runner.Close()

	// Ask about table setup
	var setup string
	fmt.Print("Re-create test table (y/n)? [y]: ")
	fmt.Scanln(&setup)
	if setup == "" || setup == "y" || setup == "Y" {
		if err := runner.SetupTable(); err != nil {
			log.Fatalf("Failed to setup table: %v", err)
		}
	}

	runner.RunAllTests()
}
