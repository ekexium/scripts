import subprocess
import time
import mysql.connector
import sys
import os
import signal
import threading # Import threading module
import argparse # Import argparse

# --- Configuration ---
TIUP_BIN = "tiup"  # Path to tiup binary if not in PATH
CONFIG_FILES = ["tikv_gzip.toml", "tikv_deflate.toml"]
TIDB_HOST = "127.0.0.1"
TIDB_PORT = 4000  # Default TiDB port for tiup playground
TIDB_USER = "root"
TIDB_PASSWORD = ""  # Default password is empty
DB_NAME = "test_grpc_compression"
TABLE_NAME = "test_table"

# Global flag for debug logging
DEBUG = False

# Timeouts and Delays
PLAYGROUND_STARTUP_TIMEOUT = 120  # Max seconds to wait for playground to start
CONNECTION_RETRY_INTERVAL = 5    # Seconds between connection attempts
SQL_TIMEOUT = 30                 # Max seconds for SQL operations

# --- Helper Functions ---

def log_debug(message):
    """Prints a message only if DEBUG mode is enabled."""
    if DEBUG:
        print(f"[DEBUG] {message}", flush=True)

def run_command(command, timeout=None, capture_output=True, check=False, **kwargs):
    """Runs a shell command (mostly for synchronous commands like clean)."""
    log_debug(f"Executing command: {' '.join(command)}")
    process = None # Initialize process to None
    try:
        log_debug(f"About to call subprocess.run for: {' '.join(command)}") # <-- Add this
        process = subprocess.run(
            command,
            timeout=timeout,
            capture_output=capture_output,
            text=True,
            check=check,
            **kwargs
        )
        log_debug(f"subprocess.run finished for: {' '.join(command)} with code: {process.returncode}") # <-- Add this
        # Print output if debug is enabled or if the command failed
        if DEBUG or process.returncode != 0:
            if capture_output:
                # Only print if there's significant output or an error occurred
                if process.stdout or process.stderr or process.returncode != 0:
                    print(f"Command stdout:{process.stdout}")
                    print(f"Command stderr:{process.stderr}")
        if process.returncode != 0 and not check: # Log non-zero exit code if check=False
             log_debug(f"Command finished with non-zero exit code: {process.returncode}")
        return process
    except subprocess.TimeoutExpired:
        print(f"Command timed out after {timeout} seconds: {' '.join(command)}")
        return None
    except subprocess.CalledProcessError as e:
        print(f"Command failed with exit code {e.returncode}: {' '.join(command)}")
        if capture_output:
            print(f"Stdout:{e.stdout}")
            print(f"Stderr:{e.stderr}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred while running command: {' '.join(command)}")
        print(f"Error: {e}")
        return None
    finally: # Add a finally block for certainty
         if process:
             log_debug(f"run_command finally block. Process for {' '.join(command)} completed with code: {process.returncode}")
         else:
             log_debug(f"run_command finally block. Process for {' '.join(command)} did not complete successfully (likely exception).")

def stream_output(stream, prefix):
    """Reads and prints lines from a stream (stdout/stderr) with a prefix."""
    try:
        for line in iter(stream.readline, ''):
            if DEBUG: # Only print if debug mode is enabled
                print(f"[{prefix}] {line.strip()}", flush=True) # Print immediately
            # If not DEBUG, we still read the line to consume the pipe, but don't print it.
    except Exception as e:
        # Avoid printing error message if it's just about reading from a closed pipe after process termination
        if isinstance(e, ValueError) and "I/O operation on closed file" in str(e):
             log_debug(f"Stream [{prefix}] closed.")
        else:
            print(f"Error reading stream [{prefix}]: {e}", flush=True)
    finally:
        log_debug(f"Closing stream reader for [{prefix}]")
        # stream.close() # Don't close here, Popen object handles it

def cleanup_playground():
    """Cleans up any existing tiup playground processes."""
    print("Cleaning up previous playground instance (if any)...")
    command = [TIUP_BIN, "clean", "--all"]
    run_command(command, timeout=60)
    log_debug("Attempting to kill potentially lingering processes...")

    # Add logging around each pkill
    try:
        log_debug("Running pkill for tikv-server...")
        # Use a more specific pattern to avoid matching the script itself via --kv-binpath
        subprocess.run(["pkill", "-f", "bin/tikv-server"], capture_output=not DEBUG, text=True)
        log_debug("Finished pkill for tikv-server.")

        log_debug("Running pkill for pd-server...")
        subprocess.run(["pkill", "-f", "bin/pd-server"], capture_output=not DEBUG, text=True)
        log_debug("Finished pkill for pd-server.")

        log_debug("Running pkill for tidb-server...")
        subprocess.run(["pkill", "-f", "bin/tidb-server"], capture_output=not DEBUG, text=True)
        log_debug("Finished pkill for tidb-server.")

        log_debug("Running pkill for tiup playground...")
        # This pattern might still be risky if the script path contains 'tiup playground'
        subprocess.run(["pkill", "-f", "tiup playground"], capture_output=not DEBUG, text=True)
        log_debug("Finished pkill for tiup playground.")

    except Exception as pkill_err:
        log_debug(f"Error during pkill execution: {pkill_err}")

    # Use run_command with capture_output=False if you want to see pkill output directly
    # Suppress output unless debugging
    # subprocess.run(["pkill", "-f", "tikv-server"], capture_output=not DEBUG, text=True)
    # subprocess.run(["pkill", "-f", "pd-server"], capture_output=not DEBUG, text=True)
    # subprocess.run(["pkill", "-f", "tidb-server"], capture_output=not DEBUG, text=True)
    # subprocess.run(["pkill", "-f", "tiup playground"], capture_output=not DEBUG, text=True) # Also try to kill the tiup command itself
    log_debug("Waiting after pkill...")
    time.sleep(5)
    log_debug("Finished cleanup_playground.") # Add a final marker

def wait_for_tidb(host, port, timeout):
    """Waits for the TiDB server to become available."""
    print(f"Waiting for TiDB server at {host}:{port} to become available (max {timeout}s)...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            log_debug(f"Attempting connection to {host}:{port}...")
            conn = mysql.connector.connect(
                host=host,
                port=port,
                user=TIDB_USER,
                password=TIDB_PASSWORD,
                connection_timeout=3 # Use a shorter timeout for polling
            )
            conn.close()
            print("TiDB server is up!")
            return True
        except mysql.connector.Error as err:
            # Only print connection errors periodically to avoid flooding logs
            # if (time.time() - start_time) % (CONNECTION_RETRY_INTERVAL * 2) < CONNECTION_RETRY_INTERVAL:
            # Always log debug, print periodically for normal mode
            log_debug(f"TiDB connection attempt failed: {err}.")
            # Print only first error and then periodically to avoid spam
            if 'printed_connection_error' not in locals() or (time.time() - start_time) % (CONNECTION_RETRY_INTERVAL * 4) < CONNECTION_RETRY_INTERVAL:
                 print(f"TiDB connection attempt failed. Retrying...")
                 printed_connection_error = True # Flag to track if initial error was printed

            time.sleep(CONNECTION_RETRY_INTERVAL)
        except Exception as e:
            log_debug(f"Unexpected error during connection attempt: {e}. Retrying...")
            time.sleep(CONNECTION_RETRY_INTERVAL)

    print(f"Error: TiDB server did not become available within {timeout} seconds.")
    return False

def run_sql_test(host, port, user, password):
    """Connects to TiDB and performs a simple write/read test."""
    print("Running SQL write/read test...")
    conn = None
    cursor = None
    try:
        log_debug(f"Connecting to database {DB_NAME} at {host}:{port}...")
        conn = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=DB_NAME,
            connection_timeout=SQL_TIMEOUT
        )
        conn.autocommit = True
        cursor = conn.cursor()

        log_debug(f"Creating table {TABLE_NAME} if not exists...")
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} (id INT PRIMARY KEY, value VARCHAR(50));")

        log_debug("Inserting data...")
        test_id = int(time.time())
        test_value = "hello_grpc_test"
        sql_insert = f"INSERT INTO {TABLE_NAME} (id, value) VALUES (%s, %s) ON DUPLICATE KEY UPDATE value = %s;"
        log_debug(f"Executing SQL: {sql_insert} with values ({test_id}, {test_value}, {test_value})")
        cursor.execute(sql_insert, (test_id, test_value, test_value))

        log_debug("Reading data back...")
        sql_select = f"SELECT value FROM {TABLE_NAME} WHERE id = %s;"
        log_debug(f"Executing SQL: {sql_select} with value ({test_id},)")
        cursor.execute(sql_select, (test_id,))
        result = cursor.fetchone()
        log_debug(f"SQL query result: {result}")

        if result and result[0] == test_value:
            print("SQL Read successful, value matches.")
            print("SQL test PASSED.")
            return True
        elif result:
            print(f"SQL Read successful, but value mismatch! Expected: '{test_value}', Got: '{result[0]}'")
            print("SQL test FAILED.")
            return False
        else:
            print("SQL Read failed: No data found for the inserted ID.")
            print("SQL test FAILED.")
            return False

    except mysql.connector.Error as err:
        print(f"SQL Error: {err}")
        print("SQL test FAILED.")
        return False
    except Exception as e:
        print(f"An unexpected error occurred during SQL test: {e}")
        print("SQL test FAILED.")
        return False
    finally:
        if cursor:
            log_debug("Closing cursor.")
            cursor.close()
        if conn and conn.is_connected():
            log_debug("Closing database connection.")
            conn.close()
            print("Database connection closed.")
        else:
            log_debug("No active connection to close or cursor.")

# --- Main Test Logic ---

def main():
    global DEBUG # Allow modifying the global DEBUG flag

    parser = argparse.ArgumentParser(description="Test gRPC compression settings in TiKV via TiUP Playground.")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging.")
    parser.add_argument("--version", type=str, default=None, help="Specify the TiUP playground version (e.g., 'v7.5.0').")
    parser.add_argument("--kv-binpath", type=str, default=None, help="Specify the path to the tikv-server binary.")
    args = parser.parse_args()

    DEBUG = args.debug
    if DEBUG:
        print("Debug mode enabled.")
        log_debug(f"Arguments received: {args}")


    results = {}
    overall_success = True

    for config_file in CONFIG_FILES:
        if not os.path.exists(config_file):
            print(f"Error: Configuration file not found: {config_file}")
            sys.exit(1)

    for config_file in CONFIG_FILES:
        print(f"{'='*20} Testing with {config_file} {'='*20}")
        results[config_file] = "FAILED" # Default to FAILED
        playground_process = None
        stdout_thread = None
        stderr_thread = None

        try:
            # 1. Cleanup before starting
            cleanup_playground()

            # 2. Start tiup playground with the specific config and optional version/binpath
            print(f"Starting tiup playground with config: {config_file}...")
            command = [TIUP_BIN, "playground"]
            if args.version:
                command.append(args.version)
            command.extend([
                "--kv.config", config_file,
                "--db", "1", "--pd", "1", "--kv", "1", "--tiflash", "0",
                "--db.port", str(TIDB_PORT),
                # "--without-monitor" # Uncomment if needed
            ])
            if args.kv_binpath:
                 # Check if binpath exists
                 if not os.path.exists(args.kv_binpath) or not os.path.isfile(args.kv_binpath):
                      print(f"Warning: Specified --kv-binpath '{args.kv_binpath}' does not exist or is not a file. TiUP might fail.")
                 command.extend(["--kv.binpath", args.kv_binpath])

            # Print the command clearly before execution
            print(f"Executing command: {' '.join(command)}")

            # Use Popen to run in background and capture streams
            playground_process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,  # Line buffered
                preexec_fn=os.setsid if os.name != 'nt' else None # Create process group for easier termination
            )
            print(f"Playground process started with PID: {playground_process.pid}")

            # 3. Start threads to read and print tiup output in real-time
            stdout_thread = threading.Thread(target=stream_output, args=(playground_process.stdout, "tiup-stdout"), daemon=True)
            stderr_thread = threading.Thread(target=stream_output, args=(playground_process.stderr, "tiup-stderr"), daemon=True)
            stdout_thread.start()
            stderr_thread.start()
            log_debug("stdout and stderr reader threads started.")

            # 4. Wait for TiDB to be ready (while output is being streamed)
            if not wait_for_tidb(TIDB_HOST, TIDB_PORT, PLAYGROUND_STARTUP_TIMEOUT):
                print(f"Error: Playground failed to start properly or TiDB didn't become available for {config_file}.")
                # Check if the process exited unexpectedly
                if playground_process.poll() is not None:
                     print(f"TiUP playground process exited prematurely with code: {playground_process.returncode}")
                raise RuntimeError("Playground startup failed or timed out")


            # 5. Prepare Database
            try:
                print(f"Ensuring database '{DB_NAME}' exists...")
                log_debug(f"Connecting to {TIDB_HOST}:{TIDB_PORT} to create database...")
                conn_setup = mysql.connector.connect(
                    host=TIDB_HOST, port=TIDB_PORT, user=TIDB_USER, password=TIDB_PASSWORD,
                    connection_timeout=10
                )
                cursor_setup = conn_setup.cursor()
                create_db_sql = f"CREATE DATABASE IF NOT EXISTS {DB_NAME};"
                log_debug(f"Executing SQL: {create_db_sql}")
                cursor_setup.execute(create_db_sql)
                cursor_setup.close()
                conn_setup.close()
                print(f"Database '{DB_NAME}' is ready.")
                log_debug("Setup connection closed.")
            except mysql.connector.Error as db_err:
                print(f"Error creating/checking database '{DB_NAME}': {db_err}")
                raise

            # 6. Run the SQL test
            if run_sql_test(TIDB_HOST, TIDB_PORT, TIDB_USER, TIDB_PASSWORD):
                results[config_file] = "PASSED"
            else:
                # Already marked as FAILED by default
                overall_success = False

        except Exception as e:
            print(f"An error occurred during the test for {config_file}: {e}")
            results[config_file] = "ERROR"
            overall_success = False
        finally:
            # 7. Stop and cleanup the playground
            if playground_process:
                print(f"Terminating playground process group (PID: {playground_process.pid})...")
                # Check if process is still running before trying to kill
                if playground_process.poll() is None:
                    try:
                        log_debug(f"Sending SIGTERM to process group {os.getpgid(playground_process.pid)} (if applicable).")
                        if os.name != 'nt':
                            # Kill the entire process group created by preexec_fn=os.setsid
                            os.killpg(os.getpgid(playground_process.pid), signal.SIGTERM)
                        else:
                            # Fallback for Windows (might not kill all children)
                            playground_process.terminate()

                        # Wait for process to terminate
                        try:
                             log_debug("Waiting for playground process to terminate (30s timeout)...")
                             playground_process.wait(timeout=30)
                             log_debug("Playground process terminated gracefully.")
                        except subprocess.TimeoutExpired:
                            print("Playground process did not terminate gracefully after SIGTERM, sending SIGKILL...")
                            log_debug("Timeout expired, sending SIGKILL.")
                            if os.name != 'nt':
                                # Force kill the process group
                                os.killpg(os.getpgid(playground_process.pid), signal.SIGKILL)
                            else:
                                playground_process.kill()
                            try:
                                log_debug("Waiting for playground process to terminate after SIGKILL (10s timeout)...")
                                playground_process.wait(timeout=10) # Wait after kill
                                log_debug("Playground process killed.")
                            except subprocess.TimeoutExpired:
                                print("Failed to kill playground process even with SIGKILL.")
                                log_debug("SIGKILL wait timed out.")
                        except ProcessLookupError:
                             log_debug("Playground process already terminated before wait (SIGTERM race condition).") # Handle race condition

                    except ProcessLookupError:
                        # This can happen if the process terminated very quickly after the poll() check
                        print("Playground process group likely already terminated.")
                        log_debug("ProcessLookupError during termination attempt.")
                    except Exception as term_err:
                        print(f"Error during playground termination: {term_err}")
                        log_debug(f"Unexpected error during termination: {term_err}")
                else:
                    print(f"Playground process (PID: {playground_process.pid}) already exited with code: {playground_process.returncode}")
                    log_debug("Playground process already exited before final cleanup block.")


            # Wait for reader threads to finish (they should exit when the process pipes close)
            if stdout_thread and stdout_thread.is_alive():
                log_debug("Waiting for stdout reader thread to finish...")
                stdout_thread.join(timeout=5)
            if stderr_thread and stderr_thread.is_alive():
                log_debug("Waiting for stderr reader thread to finish...")
                stderr_thread.join(timeout=5)
            log_debug("Reader threads joined or timed out.")

            # Run tiup clean again for thorough cleanup
            cleanup_playground()
            print(f"Finished testing with {config_file}")

    # --- Report Results ---
    print("\n" + "="*40)
    print("        Test Results Summary")
    print("="*40)
    for config, result in results.items():
        print(f"{config}: {result}")
    print("="*40)

    if overall_success:
        print("\nAll tests PASSED!")
        sys.exit(0)
    else:
        print("\nSome tests FAILED or encountered errors.")
        sys.exit(1)

if __name__ == "__main__":
    main()
