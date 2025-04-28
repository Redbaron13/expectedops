#a GcliEM.py
"""
Handles CLI. Calls scheduler loop when --force is not used.
"""
import argparse
import logging
import time
import threading
import sys
import os
import re
from queue import Queue

import GscraperEM
import GdbEM
import GstatusEM
import GschedulerEM # Import the scheduler module
import GconfigEM

log = logging.getLogger(__name__)

# --- Input Handling (Unchanged) ---
# ... (get_input_with_timeout and prompt_with_timeout functions remain the same) ...
def get_input_with_timeout(prompt, timeout, input_queue):
    try:
        user_input = input(prompt)
        input_queue.put(user_input)
    except EOFError:
        input_queue.put(None)

def prompt_with_timeout(prompt_text, timeout_seconds):
    input_queue = Queue()
    input_thread = threading.Thread(target=get_input_with_timeout,
                                    args=(prompt_text, timeout_seconds, input_queue),
                                    daemon=True)
    input_thread.start()
    input_thread.join(timeout=timeout_seconds)
    if input_thread.is_alive():
        print("\nTimeout occurred.")
        return None
    else:
        try:
            user_input = input_queue.get_nowait()
            return user_input.strip() if user_input is not None else None
        except Queue.Empty:
             log.warning("Input thread finished but queue was empty.")
             return None

# --- Command Handler Functions ---

def handle_run_command(args):
    """Handles the 'run' command (immediate or scheduled)."""
    log.info(f"Handling 'run' command. Force: {args.force}")

    if not args.force:
        # --- Start the scheduler ---
        log.info("No force flag. Starting scheduler loop...")
        try:
            # Initialize DBs before starting scheduler to ensure tables exist
            print("Initializing databases before starting scheduler...")
            GdbEM.initialize_all_databases()
            # Call the function from GschedulerEM to start the loop
            GschedulerEM.start_schedule_loop()
        except Exception as e:
            log.critical(f"Failed to start scheduler: {e}", exc_info=True)
            print(f"Error starting scheduler: {e}")
        return # Exit after attempting to start scheduler

    # --- Forced Run Logic ---
    log.info("Force flag set. Running scrape immediately.")
    print("Forcing immediate run...")
    try:
        # Initialize DBs first
        print("Initializing databases for forced run...")
        GdbEM.initialize_all_databases()

        db_files = GconfigEM.get_db_filenames()
        primary_db_file = db_files.get("primary")
        all_runs_db_file = db_files.get("all_runs")
        if not primary_db_file or not all_runs_db_file:
             log.error("Primary and/or All_Runs DB filenames not configured.")
             print("Error: Ensure 'primary' and 'all_runs' database files are configured.")
             return

        print(f"--- Scraping Primary Source ({GscraperEM.PAGE_URL}) ---")
        log.info(f"Initiating immediate primary scrape from {GscraperEM.PAGE_URL}")
        scraped_opinions, release_date = GscraperEM.fetch_and_parse_opinions(url=GscraperEM.PAGE_URL)

        if not scraped_opinions:
            print("No opinions found in this immediate run.")
            log.info("Immediate run finished. No opinions found.")
            return

        # Display data
        print(f"\n--- Scraped Data ({len(scraped_opinions)} entries) ---")
        for i, opinion in enumerate(scraped_opinions):
            print(f"\nEntry {i+1}:")
            print(f"  AppDocketID:       {opinion.get('AppDocketID', 'N/A')}")
            print(f"  LinkedDocketIDs:   {opinion.get('LinkedDocketIDs', 'N/A')}")
            print(f"  CaseName:          {opinion.get('CaseName', 'N/A')}")
            print(f"  ReleaseDate:       {opinion.get('ReleaseDate', 'N/A')}")
            print(f"  Venue:             {opinion.get('Venue', 'N/A')}")
            print(f"  DecisionType Code: {opinion.get('DecisionTypeCode', 'N/A')}")
            print(f"  DecisionType Text: {opinion.get('DecisionTypeText', 'N/A')}")
            print(f"  LCCounty:          {opinion.get('LCCounty', 'N/A')}")
            print(f"  LCdocketID:        {opinion.get('LCdocketID', 'N/A')}")
            print(f"  LC Venue:          {opinion.get('LowerCourtVenue', 'N/A')}")
            print(f"  LC SubType:        {opinion.get('LowerCourtSubCaseType', 'N/A')}")
            print(f"  OPJURISAPP:        {opinion.get('OPJURISAPP', 'N/A')}")
            print(f"  StateAgency1:      {opinion.get('StateAgency1', 'N/A')}")
            print(f"  StateAgency2:      {opinion.get('StateAgency2', 'N/A')}")
            print(f"  CaseNotes:         {opinion.get('CaseNotes', 'N/A')}")
        print("----------------------------")

        # Interactive Validation Prompt
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
            print("Data marked as incorrect by user.")
            log.info("User marked data as incorrect.")
            proceed_to_save = False
            edit_timeout = 300
            edit_response = prompt_with_timeout(
                f"Delete this run's data or attempt edit (d = Delete, e = Edit)? [Auto-delete in {edit_timeout}s]: ",
                edit_timeout
            )
            if edit_response is None or edit_response.lower() == 'd':
                print("Data from this run will be discarded.")
                log.info("User chose to delete data or timeout occurred.")
            elif edit_response.lower() == 'e':
                print("Manual editing is not yet implemented. Data will be discarded.")
                log.warning("User chose edit, but manual mode not implemented. Discarding data.")
            else:
                print("Invalid input. Data will be discarded.")
                log.warning(f"Invalid input '{edit_response}' to edit prompt. Discarding data.")
        else:
            print("Invalid input. Proceeding without validation. Data will be marked as unvalidated.")
            log.warning(f"Invalid input '{user_response}' to validation prompt. Saving as unvalidated.")
            is_validated = False
            proceed_to_save = True

        # Save to Databases
        if proceed_to_save:
            print("\nProcessing entries for databases...")
            run_type = 'manual-immediate'
            save_results = GdbEM.save_opinions_to_dbs(scraped_opinions, is_validated, run_type) # Saves to Primary and AllRuns
            print("\nDatabase processing complete:")
            for db_key, counts in save_results.items():
                if counts["total"] > 0:
                    print(f"  Database '{db_key}':")
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
         config_path = GconfigEM._get_config_path()
         print(f"Error: Configuration file '{os.path.basename(config_path)}' not found.")
    except KeyError as e:
         log.error(f"Configuration key missing during run: {e}", exc_info=True)
         print(f"Error: Required setting '{e}' missing in configuration file.")
    except ConnectionError as e:
         log.error(f"Database connection failed during run: {e}", exc_info=True)
         print(f"Error: Could not connect to the database: {e}")
    except Exception as e:
        log.critical(f"An unexpected error occurred during the forced run: {e}", exc_info=True)
        print(f"An unexpected error occurred: {e}")

# --- Other Command Handlers (Unchanged) ---
# ... (handle_status_command, handle_validate_command, handle_configure_command, etc. remain the same) ...
def handle_status_command(args):
    log.info("Handling 'status' command.")
    GstatusEM.display_status()

def handle_validate_command(args):
    log.info(f"Handling 'validate' command. ID: {args.id}, Show: {args.show}")
    print("Validation functionality is not yet fully implemented.")
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

def handle_manual_command(args):
    log.info("Handling 'manual' command.")
    print("Manual mode is not yet implemented.")
    log.warning("Manual command called but not implemented.")

def handle_exit_command(args):
    log.info("Handling 'exit' command.")
    print("Exit command - Currently does nothing unless scheduler is running.")
    log.warning("Exit command called but not fully implemented.")

# --- Argument Parser Setup (Unchanged) ---
def parse_arguments():
    # ... (parser setup remains the same) ...
    parser = argparse.ArgumentParser(
        prog='GmainEM',
        description='NJ Court Expected Opinions Extractor and Tracker'
    )
    parser.add_argument('--version', action='version', version='%(prog)s 0.4.0') # Version bump
    subparsers = parser.add_subparsers(dest='command', help='Available commands', required=True)

    run_parser = subparsers.add_parser('run', help='Run data extraction (use --force for immediate run)')
    run_parser.add_argument('--force', action='store_true', help='Run immediately with interactive validation')
    run_parser.set_defaults(func=handle_run_command)

    status_parser = subparsers.add_parser('status', help='Show application status and DB stats')
    status_parser.set_defaults(func=handle_status_command)

    config_parser = subparsers.add_parser('configure', help='Configure settings')
    config_parser.add_argument('--schedule', type=str, help='Set schedule times (HH:MM,HH:MM e.g., "10:10,09:30")')
    config_parser.add_argument('--db-primary', type=str, help='Set primary DB file (e.g., GPrimaryOpsEM.db)')
    config_parser.add_argument('--db-backup', type=str, help='Set backup DB file (e.g., GBackupOpsEM.db)')
    config_parser.add_argument('--db-all-runs', type=str, help='Set all_runs DB file (e.g., GAllRunsOpsEM.db)')
    config_parser.add_argument('--db-test', type=str, help='Set test DB file (e.g., GTestOpsEM.db)')
    config_parser.set_defaults(func=handle_configure_command)

    manual_parser = subparsers.add_parser('manual', help='Open interactive manual mode wizard (Not Implemented)')
    manual_parser.set_defaults(func=handle_manual_command)

    validate_parser = subparsers.add_parser('validate', help='Review and mark data as validated (Not Implemented)')
    validate_parser.add_argument('--show', action='store_true', help='Show unvalidated entries')
    validate_parser.add_argument('--id', type=str, help='Mark specific UniqueID as validated')
    validate_parser.set_defaults(func=handle_validate_command)

    exit_parser = subparsers.add_parser('exit', help='Exit the application (used for scheduler)')
    exit_parser.set_defaults(func=handle_exit_command)

    args = parser.parse_args()
    if hasattr(args, 'func') and callable(args.func):
        args.func(args)
    else:
        log.error(f"No handler function defined for command: {args.command}")
        parser.print_help()

# === End of GcliEM.py ===