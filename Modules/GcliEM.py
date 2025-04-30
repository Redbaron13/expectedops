# GcliEM.py
"""
Handles CLI. Calls scheduler loop when --force is not used.
Includes command to reset run counter.
"""
import argparse
import logging
import time
import threading
import sys
import os
import re
from queue import Queue, Empty

import GscraperEM
import GdbEM
import GstatusEM
import GschedulerEM # Import the scheduler module
import GconfigEM

log = logging.getLogger(__name__)

# --- Input Handling ---
def get_input_with_timeout(prompt, timeout, input_queue):
    """Target function for input thread."""
    try:
        print(prompt, end='', flush=True) # Print prompt without newline
        user_input = input()
        input_queue.put(user_input)
    except EOFError: # Handle case where input stream is closed
        input_queue.put(None)
    except Exception as e:
        log.error(f"Error reading input: {e}", exc_info=True)
        input_queue.put(None)


def prompt_with_timeout(prompt_text, timeout_seconds):
    """Prompts user for input with a timeout."""
    input_queue = Queue()
    input_thread = threading.Thread(target=get_input_with_timeout,
                                    args=(prompt_text, timeout_seconds, input_queue),
                                    daemon=True) # Daemon thread exits if main thread exits
    input_thread.start()
    input_thread.join(timeout=timeout_seconds)

    if input_thread.is_alive():
        # Timeout occurred
        print("\nTimeout occurred.")
        # Attempt to interrupt the input() call if possible (OS-dependent)
        # This is tricky in Python, often requires platform-specific solutions.
        # For simplicity, we'll just let the daemon thread terminate eventually.
        return None
    else:
        # Thread finished within timeout
        try:
            user_input = input_queue.get_nowait()
            return user_input.strip() if user_input is not None else None
        except Empty:
             log.warning("Input thread finished but queue was empty.")
             # This might happen if the input call was interrupted externally
             return None

# --- Command Handler Functions ---

def handle_run_command(args):
    """Handles the 'run' command (immediate or scheduled)."""
    log.info(f"Handling 'run' command. Force: {args.force}")

    if not args.force:
        log.info("No force flag. Starting scheduler loop...")
        try:
            print("Initializing databases before starting scheduler...")
            GdbEM.initialize_all_databases()
            GschedulerEM.start_schedule_loop()
        except Exception as e:
            log.critical(f"Failed to start scheduler: {e}", exc_info=True)
            print(f"Error starting scheduler: {e}")
        return

    # --- Forced Run Logic ---
    log.info("Force flag set. Running scrape immediately.")
    print("Forcing immediate run...")
    try:
        print("Initializing databases for forced run...")
        GdbEM.initialize_all_databases()

        # db_files = GconfigEM.get_db_filenames() # Not directly needed here now

        print(f"--- Scraping Primary Source ({GscraperEM.PAGE_URL}) ---")
        log.info(f"Initiating immediate primary scrape from {GscraperEM.PAGE_URL}")
        scraped_opinions, release_date = GscraperEM.fetch_and_parse_opinions(url=GscraperEM.PAGE_URL)

        if not scraped_opinions:
            print("No opinions found in this immediate run.")
            log.info("Immediate run finished. No opinions found.")
            # Increment counter even if no data? Let's rely on save_opinions_to_dbs
            return

        print(f"\n--- Scraped Data ({len(scraped_opinions)} entries) ---")
        for i, opinion in enumerate(scraped_opinions):
             print(f"\nEntry {i+1}:")
             for key, value in opinion.items():
                 print(f"  {key:<18}: {value if value is not None else 'N/A'}")
        print("----------------------------")

        validation_timeout = 300
        user_response = prompt_with_timeout(
            f"\nIs the extracted data above correct? (y = Yes, n = No, s = Skip validation) [Auto-skip in {validation_timeout}s]: ",
            validation_timeout
        )
        is_validated = False
        proceed_to_save = True

        if user_response is None:
            print("Proceeding without validation (timeout). Data will be marked as unvalidated.")
            log.info("Validation prompt timed out. Data will be saved as unvalidated.")
            is_validated = False
        elif user_response.lower() == 'y':
            print("Data marked as validated by user.")
            log.info("User confirmed data is correct (validated).")
            is_validated = True
        elif user_response.lower() == 's':
            print("Skipping validation. Data will be marked as unvalidated.")
            log.info("User skipped validation. Data will be saved as unvalidated.")
            is_validated = False
        elif user_response.lower() == 'n':
            print("Data marked as incorrect by user. Data will NOT be saved.")
            log.info("User marked data as incorrect. Discarding data.")
            proceed_to_save = False
            # No edit option currently implemented
        else:
            print("Invalid input. Proceeding without validation. Data will be marked as unvalidated.")
            log.warning(f"Invalid input '{user_response}' to validation prompt. Saving as unvalidated.")
            is_validated = False
            proceed_to_save = True

        if proceed_to_save:
            print("\nProcessing entries for databases...")
            run_type = 'manual-immediate'
            # Call the updated save function - it handles all 4 DBs for this run_type
            save_results = GdbEM.save_opinions_to_dbs(scraped_opinions, is_validated, run_type)
            print("\nDatabase processing complete:")
            db_files = GconfigEM.get_db_filenames() # Get names for display
            for db_key, counts in save_results.items():
                if counts["total"] > 0:
                    print(f"  Database '{db_key}' ({db_files.get(db_key, 'N/A')}):")
                    print(f"    Inserted: {counts['inserted']}")
                    print(f"    Updated:  {counts['updated']}")
                    print(f"    Skipped:  {counts['skipped']}")
                    print(f"    Errors:   {counts['error']}")
        else:
            print("Data from this run was discarded based on user input.")
            log.info("Immediate run data discarded based on user input.")

        print("\nImmediate run finished.")
        log.info("Immediate run finished.")

    except FileNotFoundError as e:
         log.error(f"Configuration file not found during run: {e}", exc_info=True)
         config_path = GconfigEM._get_config_path() # Get expected path
         print(f"Error: Configuration file '{os.path.basename(config_path)}' not found at expected location '{config_path}'.")
    except KeyError as e:
         log.error(f"Configuration key missing during run: {e}", exc_info=True)
         print(f"Error: Required setting '{e}' missing in configuration file.")
    except ConnectionError as e:
         log.error(f"Database connection failed during run: {e}", exc_info=True)
         print(f"Error: Could not connect to the database: {e}")
    except Exception as e:
        log.critical(f"An unexpected error occurred during the forced run: {e}", exc_info=True)
        print(f"An unexpected error occurred: {e}")


def handle_status_command(args):
    log.info("Handling 'status' command.")
    GstatusEM.display_status() # Call the updated status display

def handle_validate_command(args):
    log.info(f"Handling 'validate' command. ID: {args.id}, Show: {args.show}")
    print("Validation functionality is not yet fully implemented.")
    # Placeholder: Would call functions from GvalidatorEM if implemented
    # if args.show: GvalidatorEM.list_unvalidated()
    # if args.id: GvalidatorEM.validate_case(args.id)
    log.warning("Validation command called but not fully implemented.")

def handle_configure_command(args):
    log.info(f"Handling 'configure' command. Args: {args}")
    try:
        config = GconfigEM.load_config()
        updated = False
        if args.schedule:
            times = [t.strip() for t in args.schedule.split(',')]
            if len(times) == 2 and all(re.match(r"^\d{2}:\d{2}$", t) for t in times):
                config['schedule'] = {'primary': times[0], 'backup': times[1]}
                print(f"Schedule updated: Primary={times[0]}, Backup={times[1]}")
                log.info(f"Configuration updated - Schedule: Primary={times[0]}, Backup={times[1]}")
                updated = True
            else:
                print("Error: Invalid schedule format. Use HH:MM,HH:MM (e.g., '10:10,09:30').")
                log.warning(f"Invalid schedule format provided: {args.schedule}")

        db_config_updated = False
        for db_type in GconfigEM.DEFAULT_DB_NAMES.keys():
             arg_name = f"db_{db_type}"
             new_filename = getattr(args, arg_name, None)
             if new_filename:
                 # Use the pattern from GconfigEM for validation
                 if GconfigEM.DB_FILENAME_PATTERN.match(new_filename):
                     config['db_files'][db_type] = new_filename
                     print(f"{db_type.capitalize()} database filename updated to: {new_filename}")
                     log.info(f"Configuration updated - DB File ({db_type}): {new_filename}")
                     db_config_updated = True
                 else:
                     print(f"Error: Invalid filename format for {db_type} DB: '{new_filename}'. Must match 'G[Name]EM.db'.")
                     log.warning(f"Invalid DB filename format: {new_filename}")

        updated = updated or db_config_updated
        if updated: GconfigEM.save_config(config)
        else: print("No valid configuration options provided to update.")

    except Exception as e:
        log.error(f"Error during configure command: {e}", exc_info=True)
        print(f"An error occurred during configuration: {e}")

def handle_reset_counter_command(args):
    """Handles the 'reset-counter' command."""
    log.info("Handling 'reset-counter' command.")
    try:
        confirm = input("Are you sure you want to reset the run counter to 0? (y/n): ").strip().lower()
        if confirm == 'y':
            GconfigEM.reset_run_counter()
            print("Run counter has been reset to 0.")
        else:
            print("Run counter reset cancelled.")
            log.info("User cancelled run counter reset.")
    except Exception as e:
        log.error(f"Error resetting run counter: {e}", exc_info=True)
        print(f"An error occurred while resetting the counter: {e}")


def handle_exit_command(args):
    log.info("Handling 'exit' command.")
    print("Exit command received. Stopping scheduler if running...")
    # This doesn't forcefully stop the scheduler thread from here easily.
    # The typical way is Ctrl+C in the terminal running the scheduler.
    # For a programmatic exit, inter-thread communication (e.g., an event) is needed.
    log.warning("Exit command called but cannot programmatically stop scheduler from here.")
    sys.exit(0) # Exit the CLI script itself

# --- Argument Parser Setup ---
def parse_arguments():
    parser = argparse.ArgumentParser(
        prog='GmainEM',
        description='NJ Court Expected Opinions Extractor and Tracker'
    )
    parser.add_argument('--version', action='version', version='%(prog)s 0.5.0') # Version bump
    subparsers = parser.add_subparsers(dest='command', help='Available commands', required=True)

    # --- Run Command ---
    run_parser = subparsers.add_parser('run', help='Run data extraction (use --force for immediate run, targets all DBs)')
    run_parser.add_argument('--force', action='store_true', help='Run immediately with interactive validation')
    run_parser.set_defaults(func=handle_run_command)

    # --- Status Command ---
    status_parser = subparsers.add_parser('status', help='Show application status and DB stats')
    status_parser.set_defaults(func=handle_status_command)

    # --- Configure Command ---
    config_parser = subparsers.add_parser('configure', help='Configure settings')
    config_parser.add_argument('--schedule', type=str, help='Set schedule times (HH:MM,HH:MM e.g., "10:10,09:30")')
    config_parser.add_argument('--db-primary', type=str, help='Set primary DB file (e.g., GPrimaryOpsEM.db)')
    config_parser.add_argument('--db-backup', type=str, help='Set backup DB file (e.g., GBackupOpsEM.db)')
    config_parser.add_argument('--db-all-runs', type=str, help='Set all_runs DB file (e.g., GAllRunsOpsEM.db)')
    config_parser.add_argument('--db-test', type=str, help='Set test DB file (e.g., GTestOpsEM.db)')
    config_parser.set_defaults(func=handle_configure_command)

    # --- Reset Counter Command ---
    reset_parser = subparsers.add_parser('reset-counter', help='Reset the application run counter to 0')
    reset_parser.set_defaults(func=handle_reset_counter_command)

    # --- Validate Command (Still Placeholder) ---
    validate_parser = subparsers.add_parser('validate', help='Review and mark data as validated (Not Implemented)')
    validate_parser.add_argument('--show', action='store_true', help='Show unvalidated entries')
    validate_parser.add_argument('--id', type=str, help='Mark specific UniqueID as validated')
    validate_parser.set_defaults(func=handle_validate_command)

    # --- Exit Command ---
    exit_parser = subparsers.add_parser('exit', help='Signal application exit (intended for scheduler)')
    exit_parser.set_defaults(func=handle_exit_command)

    args = parser.parse_args()
    if hasattr(args, 'func') and callable(args.func):
        args.func(args) # Call the appropriate handler function
    else:
        # Should not happen if subparsers are required=True
        log.error(f"No handler function defined for command: {args.command}")
        parser.print_help()

# === End of GcliEM.py ===