# GcliEM.py
# V3: Added --test flag, modified --force, added build-combo-db command
"""
Handles CLI commands.
- `run`: Starts scheduler (default)
- `run --force`: Runs immediate primary scrape, saves to primary & all_runs.
- `run --force --test`: Simulates Primary1, Primary2, Backup runs, saves to ALL DBs.
- `status`: Shows status.
- `configure`: Configures settings.
- `reset-counter`: Resets run counter.
- `validate`: Lists or validates entries.
- `updater`: (Informational) Explains manual update process.
- `build-combo-db`: Manually rebuilds the combo DB from primary and backup.
- `exit`: Stops the application.
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
import GvalidatorEM # Import the validator module

log = logging.getLogger(__name__)

# --- Input Handling (Unchanged) ---
# ... (get_input_with_timeout and prompt_with_timeout functions) ...
def get_input_with_timeout(prompt, timeout, input_queue):
    """Target function for input thread."""
    try:
        print(prompt, end='', flush=True)
        user_input = input()
        input_queue.put(user_input)
    except EOFError:
        input_queue.put(None) # Handle Ctrl+D
    except Exception as e:
        log.error(f"Error reading input: {e}", exc_info=True)
        input_queue.put(None)

def prompt_with_timeout(prompt_text, timeout_seconds=300): # Default timeout 5 mins
    """Prompts user for input with a timeout."""
    input_queue = Queue()
    input_thread = threading.Thread(target=get_input_with_timeout,
                                    args=(prompt_text, timeout_seconds, input_queue),
                                    daemon=True)
    input_thread.start()
    input_thread.join(timeout=timeout_seconds)

    if input_thread.is_alive():
        print("\nTimeout occurred waiting for user input.")
        return None # Indicate timeout
    else:
        try:
            user_input = input_queue.get_nowait()
            return user_input.strip() if user_input is not None else None
        except Empty:
             log.warning("Input thread finished but queue was empty.")
             return None


# --- Command Handler Functions ---

def handle_run_command(args):
    """Handles the 'run' command based on --force and --test flags."""
    log.info(f"Handling 'run' command. Force: {args.force}, Test: {args.test}")

    if not args.force:
        # --- Start Scheduler ---
        log.info("No force flag. Starting scheduler loop...")
        try:
            print("Initializing databases before starting scheduler...")
            GdbEM.initialize_all_databases()
            GschedulerEM.start_schedule_loop() # Contains the loop
        except Exception as e:
            log.critical(f"Failed to start scheduler: {e}", exc_info=True)
            print(f"Error starting scheduler: {e}")
        return

    # --- Forced Run Logic ---
    try:
        print("Initializing all databases for forced run...")
        GdbEM.initialize_all_databases() # Ensure all schemas are ready

        if args.test:
            # --- Test Run (--force --test) ---
            log.info("Test run requested (--force --test). Simulating all runs.")
            print("--- Starting Test Run (Simulating Primary 1, Primary 2, Backup) ---")
            run_type_test = 'manual-test'
            is_validated_test = False # Test runs are not validated

            # Simulate Primary 1
            print("\n[Test] Simulating Primary Run 1...")
            opinions_p1, date_p1 = GscraperEM.fetch_and_parse_opinions()
            if opinions_p1:
                print(f"[Test] Saving {len(opinions_p1)} opinions from simulated P1 run.")
                GdbEM.save_opinions_to_dbs(opinions_p1, is_validated_test, run_type_test + "-p1") # Add sub-tag
            else: print("[Test] No opinions found for simulated P1.")

            # Simulate Primary 2
            print("\n[Test] Simulating Primary Run 2...")
            opinions_p2, date_p2 = GscraperEM.fetch_and_parse_opinions() # Re-scrape
            if opinions_p2:
                 print(f"[Test] Saving {len(opinions_p2)} opinions from simulated P2 run.")
                 GdbEM.save_opinions_to_dbs(opinions_p2, is_validated_test, run_type_test + "-p2")
            else: print("[Test] No opinions found for simulated P2.")

            # Simulate Backup
            print("\n[Test] Simulating Backup Run...")
            opinions_b, date_b = GscraperEM.fetch_and_parse_opinions() # Re-scrape
            if opinions_b:
                 print(f"[Test] Saving {len(opinions_b)} opinions from simulated Backup run.")
                 GdbEM.save_opinions_to_dbs(opinions_b, is_validated_test, run_type_test + "-bk")
            else: print("[Test] No opinions found for simulated Backup.")

            print("\n--- Test Run Simulation Complete ---")
            log.info("Test run simulation finished.")

        else:
            # --- Standard Force Run (--force only) ---
            log.info("Standard force run requested (--force). Running primary scrape.")
            print("--- Starting Forced Primary Run ---")
            run_type_force = 'manual-primary-force' # Specific type for this run

            print(f"Scraping {GscraperEM.PAGE_URL}...")
            scraped_opinions, release_date = GscraperEM.fetch_and_parse_opinions()

            if not scraped_opinions:
                print("No opinions found in this forced run.")
                log.info("Forced run finished. No opinions found.")
                return

            # Display limited data
            print(f"\n--- Scraped Data ({len(scraped_opinions)} entries) ---")
            display_limit = 5
            for i, opinion in enumerate(scraped_opinions):
                 if i >= display_limit: print(f"\n... (display limited to first {display_limit}) ..."); break
                 print(f"\nEntry {i+1}: AppDocket={opinion.get('AppDocketID','N/A')}, Case={opinion.get('CaseName','N/A')[:40]}...") # Brief display
            print("----------------------------")

            # Validation Prompt
            validation_timeout = 120 # 2 minutes for force run?
            user_response = prompt_with_timeout(
                f"\nValidate this data? (y=Yes, n=No/Discard, s/Enter=Skip/Save Unvalidated) [{validation_timeout}s timeout]: ",
                validation_timeout
            )
            is_validated = False
            proceed_to_save = True

            if user_response is None or user_response.lower() == 's' or user_response == '':
                print("\nSkipping validation. Data will be marked as unvalidated.")
                log.info("Forced run validation skipped or timed out. Saving as unvalidated.")
                is_validated = False
            elif user_response.lower() == 'y':
                print("Data marked as validated by user.")
                log.info("User confirmed data as correct (validated) for forced run.")
                is_validated = True
            elif user_response.lower() == 'n':
                print("Data marked as incorrect by user. Data will NOT be saved.")
                log.info("User marked data as incorrect for forced run. Discarding data.")
                proceed_to_save = False
            else: # Invalid input
                print("Invalid input. Skipping validation. Data will be marked as unvalidated.")
                log.warning(f"Invalid input '{user_response}' to forced run validation. Saving as unvalidated.")
                is_validated = False

            # Save (only to primary and all_runs for this run type)
            if proceed_to_save:
                print("\nProcessing entries for Primary and AllRuns databases...")
                # save_opinions_to_dbs uses run_type to determine targets
                save_results = GdbEM.save_opinions_to_dbs(scraped_opinions, is_validated, run_type_force)
                print("\nDatabase processing complete:")
                # Log results for the targeted DBs
                db_files = GconfigEM.get_db_filenames()
                for db_key in ["primary", "all_runs"]: # Only show these targets
                    if db_key in save_results and (save_results[db_key]['total'] > 0 or save_results[db_key]['error'] != 0):
                         counts = save_results[db_key]
                         db_path = db_files.get(db_key, "N/A")
                         if counts["error"] == -1: log.error(f"DB '{db_key}' ({db_path}): Skipped due to initialization error.")
                         elif db_key == "all_runs": # History specific logging
                              log.info(f"DB 'all_runs' ({db_path}): Processed {counts['total']} -> History Inserted: {counts.get('inserted_history',0)}, History Errors: {counts.get('error_history',0)}")
                              print(f"  AllRuns DB: History Inserted={counts.get('inserted_history',0)}, Errors={counts.get('error_history',0)}")
                         else: # Standard DB logging
                              log.info(f"DB '{db_key}' ({db_path}): Processed {counts['total']} -> Inserted: {counts['inserted']}, Updated: {counts['updated']}, Skipped: {counts['skipped']}, Errors: {counts['error']}")
                              print(f"  {db_key.capitalize()} DB: Inserted={counts['inserted']}, Updated={counts['updated']}, Skipped={counts['skipped']}, Errors={counts['error']}")

            else: # User chose not to save
                print("Data from this forced run was discarded based on user input.")
                log.info("Forced run data discarded based on user input.")

            print("\nForced primary run finished.")
            log.info("Forced primary run finished.")

    # --- Error Handling for Forced Runs ---
    except FileNotFoundError as e:
         log.error(f"Configuration file not found during forced run: {e}", exc_info=True)
         print(f"Error: Configuration file not found. Please ensure 'config.json' exists.")
    except KeyError as e:
         log.error(f"Configuration key missing during forced run: {e}", exc_info=True)
         print(f"Error: Required setting '{e}' missing in configuration file.")
    except ConnectionError as e:
         log.error(f"Database connection failed during forced run: {e}", exc_info=True)
         print(f"Error: Could not connect to the database: {e}")
    except Exception as e:
        log.critical(f"An unexpected error occurred during the forced run: {e}", exc_info=True)
        print(f"An unexpected error occurred: {e}")


def handle_status_command(args):
    """Handles the 'status' command."""
    log.info("Handling 'status' command.")
    GstatusEM.display_status() # Assumes GstatusEM is updated if needed


def handle_validate_command(args):
    """Handles the 'validate' command for listing or validating entries."""
    log.info(f"Handling 'validate' command. Validate ID: {args.validate_id}, List Unvalidated: {args.list_unvalidated}, List Missing LC: {args.list_missing_lc}")
    action_taken = False
    db_key_target = args.db if args.db else "primary" # Default to primary if not specified

    if args.list_unvalidated:
        print(f"Listing unvalidated entries from '{db_key_target}' DB...")
        GvalidatorEM.list_entries(db_key=db_key_target, list_type="unvalidated")
        action_taken = True
    if args.list_missing_lc:
        print(f"Listing entries potentially missing LC Docket ID from '{db_key_target}' DB...")
        GvalidatorEM.list_entries(db_key=db_key_target, list_type="missing_lc_docket")
        action_taken = True
    if args.validate_id:
         # Ensure validation happens on the primary DB for consistency? Or allow target?
         # Let's allow target for now, but maybe warn user?
         if db_key_target != "primary":
              print(f"Warning: Running validation on '{db_key_target}' DB. Changes here might not reflect in other DBs until next runs.")
         print(f"Starting interactive validation for UniqueID: {args.validate_id} in '{db_key_target}' DB...")
         GvalidatorEM.validate_case(args.validate_id, db_key_target) # Pass DB key
         action_taken = True

    if not action_taken:
        print("No action specified for 'validate'. Use flags like --list-unvalidated, --list-missing-lc, or --validate-id <UniqueID>.")
        log.warning("Validate command called without specific action flags.")


def handle_configure_command(args):
    """Handles the 'configure' command."""
    # This function remains largely the same, just ensures all DB keys are handled
    log.info(f"Handling 'configure' command. Args: {args}")
    try:
        config = GconfigEM.load_config()
        updated = False

        # DB Filename Updates (Iterate through ALL default names)
        db_config_updated = False
        for db_type in GconfigEM.DEFAULT_DB_NAMES.keys():
             arg_name = f"db_{db_type}".replace("_", "-") # Arg names use hyphens
             new_filename = getattr(args, arg_name, None)
             if new_filename:
                 if GconfigEM.DB_FILENAME_PATTERN.match(new_filename):
                     # Check if value is actually changing
                     if config['db_files'].get(db_type) != new_filename:
                         config['db_files'][db_type] = new_filename
                         print(f"{db_type.capitalize()} database filename updated to: {new_filename}")
                         log.info(f"Configuration updated - DB File ({db_type}): {new_filename}")
                         db_config_updated = True
                     else:
                          print(f"{db_type.capitalize()} database filename is already set to {new_filename}.")
                 else:
                     print(f"Error: Invalid filename format for {db_type} DB: '{new_filename}'. Must match 'G[Name]EM.db'.")
                     log.warning(f"Invalid DB filename format provided for {db_type}: {new_filename}")

        updated = updated or db_config_updated

        # Logging Toggle
        if args.toggle_logging is not None: # Check if the argument was provided
            if config['logging'] != args.toggle_logging:
                 config['logging'] = args.toggle_logging
                 status = "enabled" if args.toggle_logging else "disabled"
                 print(f"File logging has been {status}.")
                 log.info(f"Configuration updated - Logging: {status}")
                 updated = True
            else:
                 print(f"File logging is already {'enabled' if config['logging'] else 'disabled'}.")


        if updated:
            GconfigEM.save_config(config)
            print("Configuration saved.")
        else:
             print("No valid configuration options provided or values match current config.")

    except Exception as e:
        log.error(f"Error during configure command: {e}", exc_info=True)
        print(f"An error occurred during configuration: {e}")

def handle_reset_counter_command(args):
    """Handles the 'reset-counter' command."""
    # (Unchanged from previous version)
    log.info("Handling 'reset-counter' command.")
    try:
        confirm = prompt_with_timeout(
            "Are you sure you want to reset the run counter to 0? This cannot be undone. (y/n): ",
             timeout_seconds=60
             )
        if confirm and confirm.lower() == 'y':
            GconfigEM.reset_run_counter()
            print("Run counter has been reset to 0.")
            log.info("Run counter reset to 0 by user.")
        elif confirm is None:
            print("Reset counter cancelled (timeout).")
            log.info("Run counter reset timed out.")
        else:
            print("Run counter reset cancelled.")
            log.info("User cancelled run counter reset.")
    except Exception as e:
        log.error(f"Error resetting run counter: {e}", exc_info=True)
        print(f"An error occurred while resetting the counter: {e}")


# --- Handler for Building Combo DB ---
def handle_build_combo_db(args):
    """Handles the 'build-combo-db' command."""
    log.info("Handling 'build-combo-db' command.")
    print("Attempting to rebuild the Combo database from Primary and Backup...")
    try:
        db_files = GconfigEM.get_db_filenames()
        combo_db = db_files.get("combo")
        primary_db = db_files.get("primary")
        backup_db = db_files.get("backup")

        if not combo_db or not primary_db or not backup_db:
            print("Error: Database files for combo, primary, or backup are not defined in config.")
            log.error("Build Combo DB failed: Missing DB filenames in configuration.")
            return

        success, error_msg = GdbEM.build_combo_db(combo_db, primary_db, backup_db)

        if success:
            print(f"Successfully rebuilt '{combo_db}'.")
        else:
            print(f"Failed to rebuild Combo DB: {error_msg}")

    except Exception as e:
        log.error(f"Unexpected error during 'build-combo-db': {e}", exc_info=True)
        print(f"An unexpected error occurred: {e}")


# --- Placeholder Handler for 'Updater' Concept ---
def handle_updater_command(args):
    """Handles the conceptual 'updater' command."""
     # (Unchanged from previous version - still informational)
    log.info(f"Handling 'updater' command. Args: {args}")
    print("\n--- Database Updater Information ---")
    # ... (explanation text remains the same) ...
    print("The 'updater' module concept, designed to automatically fetch and parse")
    print("decision PDFs from njcourts.gov to verify/update database records,")
    print("cannot be fully implemented with the currently available tools.")
    print("Specifically, fetching arbitrary web URLs and parsing PDF content is not supported.")
    print("\nRecommended Workflow for Updating/Correcting Data:")
    print("1. Identify records needing review:")
    print("   - Use `validate --list-unvalidated` or `validate --list-missing-lc`.")
    print("2. Manually review an entry:")
    print("   - Use `validate --validate-id <UniqueID>`.")
    print("3. Use the displayed 'Potential PDF URL' in the validator.")
    print("4. Compare PDF info with validator data.")
    print("5. Edit fields directly within the validator interface.")
    print("6. Mark the entry as validated.")

    log.warning("Updater command invoked, but functionality is limited. Directing user to manual validation workflow.")
    if args.update_id: print(f"\nTo manually review/update docket {args.update_id}, please use: `validate --validate-id {args.update_id}`")


def handle_exit_command(args):
    """Handles the 'exit' command."""
    # (Unchanged from previous version)
    log.info("Handling 'exit' command.")
    print("Exit command received. Stopping application...")
    sys.exit(0)


# --- Argument Parser Setup ---
def parse_arguments():
    parser = argparse.ArgumentParser(
        prog='GmainEM',
        description='NJ Court Expected Opinions Extractor and Tracker',
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('--version', action='version', version='%(prog)s 0.8.0') # Version bump
    subparsers = parser.add_subparsers(dest='command', help='Available commands', required=True)

    # --- Run Command ---
    run_parser = subparsers.add_parser('run',
                                       help='Run data extraction or start scheduler.',
                                       description='Default: Start scheduler.\n'
                                                   '--force: Run immediate primary scrape -> primary, all_runs.\n'
                                                   '--force --test: Simulate P1, P2, Backup runs -> all DBs.')
    run_parser.add_argument('--force', action='store_true', help='Run immediately instead of starting scheduler.')
    run_parser.add_argument('--test', action='store_true', help='If used with --force, runs test simulation hitting all DBs.')
    run_parser.set_defaults(func=handle_run_command)

    # --- Status Command ---
    status_parser = subparsers.add_parser('status', help='Show application status and DB stats')
    status_parser.set_defaults(func=handle_status_command)

    # --- Configure Command ---
    config_parser = subparsers.add_parser('configure', help='Configure settings like DB filenames or logging.')
    # Add args for ALL db types from config
    for db_type, default_name in GconfigEM.DEFAULT_DB_NAMES.items():
        arg_name = f"--db-{db_type.replace('_', '-')}"
        config_parser.add_argument(arg_name, type=str, metavar='FILENAME', help=f'Set {db_type} DB file (e.g., {default_name})')
    config_parser.add_argument('--toggle-logging', type=lambda x: x.lower() == 'true', metavar='true|false', help='Enable or disable file logging')
    config_parser.set_defaults(func=handle_configure_command)

    # --- Reset Counter Command ---
    reset_parser = subparsers.add_parser('reset-counter', help='Reset the application run counter to 0')
    reset_parser.set_defaults(func=handle_reset_counter_command)

    # --- Validate Command ---
    validate_parser = subparsers.add_parser('validate',
                                            help='Review, list, or manually validate database entries.',
                                            description='Use list flags to find entries, then --validate-id to edit/validate.')
    validate_group = validate_parser.add_mutually_exclusive_group(required=True) # Require one action
    validate_group.add_argument('--list-unvalidated', action='store_true', help='List entries marked as unvalidated')
    validate_group.add_argument('--list-missing-lc', action='store_true', help='List entries potentially missing LC Docket ID')
    validate_group.add_argument('--validate-id', type=str, metavar='UniqueID', help='Interactively review and validate specific UniqueID')
    validate_parser.add_argument('--db', choices=GconfigEM.DEFAULT_DB_NAMES.keys(), default='primary', help='Specify DB target for list/validate (default: primary)')
    validate_parser.set_defaults(func=handle_validate_command)


    # --- Updater Command (Informational) ---
    updater_parser = subparsers.add_parser('updater',
                                           help='(Informational) Explains manual update process using Validator.',
                                           description='Provides guidance on updating records manually using the validate command.')
    updater_parser.add_argument('--update-id', type=str, metavar='UniqueID', help='(For info only) Specify an ID you wish to update manually.')
    # Add other conceptual args if desired for help text
    updater_parser.set_defaults(func=handle_updater_command)

    # --- Build Combo DB Command ---
    combo_parser = subparsers.add_parser('build-combo-db', help='Manually rebuild the Combo DB from Primary and Backup DBs.')
    combo_parser.set_defaults(func=handle_build_combo_db)

    # --- Exit Command ---
    exit_parser = subparsers.add_parser('exit', help='Stop the application (useful if run without --force)')
    exit_parser.set_defaults(func=handle_exit_command)

    # --- Parse Arguments and Execute ---
    try:
        args = parser.parse_args()
        if hasattr(args, 'func') and callable(args.func):
            args.func(args)
        else:
            log.error("Command not recognized or handler missing.")
            parser.print_help()
            sys.exit(1)
    except Exception as e:
         log.critical(f"An error occurred during argument parsing or command execution: {e}", exc_info=True)
         print(f"An error occurred: {e}")
         sys.exit(1)

# === End of GcliEM.py ===