# GcliEM.py
# V4: Fixed result logging error, added build-combo-db
"""
Handles CLI commands.
- `run`: Starts scheduler (default)
- `run --force`: Runs immediate primary scrape -> primary, all_runs.
- `run --force --test`: Simulates Primary1, Primary2, Backup runs -> all DBs.
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
    try: print(prompt, end='', flush=True); user_input = input(); input_queue.put(user_input)
    except EOFError: input_queue.put(None)
    except Exception as e: log.error(f"Error reading input: {e}", exc_info=True); input_queue.put(None)

def prompt_with_timeout(prompt_text, timeout_seconds=300):
    """Prompts user for input with a timeout."""
    input_queue = Queue(); thread = threading.Thread(target=get_input_with_timeout, args=(prompt_text, timeout_seconds, input_queue), daemon=True); thread.start(); thread.join(timeout=timeout_seconds)
    if thread.is_alive(): print("\nTimeout occurred waiting for user input."); return None
    else:
        try: user_input = input_queue.get_nowait(); return user_input.strip() if user_input is not None else None
        except Empty: log.warning("Input thread finished but queue was empty."); return None


# --- Command Handler Functions ---

def handle_run_command(args):
    """Handles the 'run' command based on --force and --test flags."""
    log.info(f"Handling 'run' command. Force: {args.force}, Test: {args.test}")

    if not args.force:
        # Start Scheduler (Code unchanged)
        log.info("No force flag. Starting scheduler loop...")
        try: print("Initializing databases..."); GdbEM.initialize_all_databases(); GschedulerEM.start_schedule_loop()
        except Exception as e: log.critical(f"Failed to start scheduler: {e}", exc_info=True); print(f"Error starting scheduler: {e}")
        return

    # --- Forced Run Logic ---
    try:
        print("Initializing all databases for forced run...")
        GdbEM.initialize_all_databases() # Ensure all schemas are ready

        if args.test:
            # --- Test Run (--force --test) ---
            log.info("Test run requested (--force --test). Simulating all runs.")
            print("--- Starting Test Run (Simulating Primary 1, Primary 2, Backup) ---")
            # Use a distinct run_type for test simulation results
            run_type_test_base = 'manual-test'
            is_validated_test = False # Test runs are not validated

            # Simulate Primary 1
            print("\n[Test] Simulating Primary Run 1...")
            opinions_p1, _ = GscraperEM.fetch_and_parse_opinions()
            if opinions_p1: print(f"[Test] Saving {len(opinions_p1)} from P1."); GdbEM.save_opinions_to_dbs(opinions_p1, is_validated_test, run_type_test_base + "-p1")
            else: print("[Test] No opinions found for P1.")
            time.sleep(0.5) # Small delay between simulated runs

            # Simulate Primary 2
            print("\n[Test] Simulating Primary Run 2...")
            opinions_p2, _ = GscraperEM.fetch_and_parse_opinions()
            if opinions_p2: print(f"[Test] Saving {len(opinions_p2)} from P2."); GdbEM.save_opinions_to_dbs(opinions_p2, is_validated_test, run_type_test_base + "-p2")
            else: print("[Test] No opinions found for P2.")
            time.sleep(0.5)

            # Simulate Backup
            print("\n[Test] Simulating Backup Run...")
            opinions_b, _ = GscraperEM.fetch_and_parse_opinions()
            if opinions_b: print(f"[Test] Saving {len(opinions_b)} from Backup."); GdbEM.save_opinions_to_dbs(opinions_b, is_validated_test, run_type_test_base + "-bk")
            else: print("[Test] No opinions found for Backup.")

            print("\n--- Test Run Simulation Complete ---")
            log.info("Test run simulation finished.")
            # Note: Results dict isn't aggregated across simulations here, logged per save call in GdbEM

        else:
            # --- Standard Force Run (--force only) ---
            log.info("Standard force run requested (--force). Running primary scrape.")
            print("--- Starting Forced Primary Run ---")
            run_type_force = 'manual-primary-force' # Specific type

            print(f"Scraping {GscraperEM.PAGE_URL}...")
            scraped_opinions, release_date = GscraperEM.fetch_and_parse_opinions()

            if not scraped_opinions: print("No opinions found."); log.info("Forced run: No opinions."); return

            # Display limited data (Unchanged)
            print(f"\n--- Scraped Data ({len(scraped_opinions)} entries) ---"); display_limit = 5
            for i, o in enumerate(scraped_opinions):
                if i >= display_limit: print(f"\n... (limit {display_limit}) ..."); break
                print(f"\nEntry {i+1}: AppDocket={o.get('AppDocketID','N/A')}, Case={o.get('CaseName','N/A')[:40]}...")
            print("----------------------------")

            # Validation Prompt (Unchanged)
            validation_timeout = 120
            user_response = prompt_with_timeout(f"\nValidate? (y=Yes, n=No/Discard, s/Enter=Skip) [{validation_timeout}s timeout]: ", validation_timeout)
            is_validated = False; proceed_to_save = True
            if user_response is None or user_response.lower() == 's' or user_response == '': print("\nSkipping validation."); log.info("Forced run validation skipped/timeout."); is_validated = False
            elif user_response.lower() == 'y': print("Marked as validated."); log.info("User validated data."); is_validated = True
            elif user_response.lower() == 'n': print("Discarding data."); log.info("User discarded data."); proceed_to_save = False
            else: print("Invalid input. Skipping validation."); log.warning(f"Invalid input '{user_response}'."); is_validated = False

            # Save (only to primary and all_runs)
            if proceed_to_save:
                print("\nProcessing for Primary and AllRuns DBs...")
                save_results = GdbEM.save_opinions_to_dbs(scraped_opinions, is_validated, run_type_force)
                print("\nDatabase processing complete:")

                # --- Corrected Result Logging ---
                db_files = GconfigEM.get_db_filenames()
                # Log primary results
                db_key_primary = "primary"
                if db_key_primary in save_results:
                    counts_p = save_results[db_key_primary]
                    db_path_p = db_files.get(db_key_primary, "N/A")
                    if counts_p.get("error") == -1: # Check for init error
                         log.error(f"DB '{db_key_primary}' ({db_path_p}): Skipped due to initialization error.")
                         print(f"  Primary DB: Skipped due to initialization error.")
                    elif counts_p.get("total", 0) > 0: # Check if any processing happened
                         log.info(f"DB '{db_key_primary}' ({db_path_p}): Processed {counts_p.get('total',0)} -> Inserted: {counts_p.get('inserted',0)}, Updated: {counts_p.get('updated',0)}, Skipped: {counts_p.get('skipped',0)}, Errors: {counts_p.get('error',0)}")
                         print(f"  Primary DB: Inserted={counts_p.get('inserted',0)}, Updated={counts_p.get('updated',0)}, Skipped={counts_p.get('skipped',0)}, Errors={counts_p.get('error',0)}")

                # Log all_runs results (using history keys)
                db_key_allruns = "all_runs"
                if db_key_allruns in save_results:
                     counts_ar = save_results[db_key_allruns]
                     db_path_ar = db_files.get(db_key_allruns, "N/A")
                     if counts_ar.get("error_history") == -1: # Check for init error (-1 used in GdbEM)
                          log.error(f"DB '{db_key_allruns}' ({db_path_ar}): Skipped due to initialization error.")
                          print(f"  AllRuns DB: Skipped due to initialization error.")
                     elif counts_ar.get("total", 0) > 0:
                          log.info(f"DB '{db_key_allruns}' ({db_path_ar}): Processed {counts_ar.get('total',0)} -> History Inserted: {counts_ar.get('inserted_history',0)}, History Errors: {counts_ar.get('error_history',0)}")
                          print(f"  AllRuns DB: History Inserted={counts_ar.get('inserted_history',0)}, Errors={counts_ar.get('error_history',0)}")

            else: # User chose not to save
                print("Data discarded."); log.info("Forced run data discarded.")

            print("\nForced primary run finished.")
            log.info("Forced primary run finished.")

    # --- Error Handling for Forced Runs (Unchanged) ---
    except FileNotFoundError as e: log.error(f"Config file not found: {e}", exc_info=True); print("Error: Config file missing.")
    except KeyError as e: log.error(f"Config key missing: {e}", exc_info=True); print(f"Error: Setting '{e}' missing in config.")
    except ConnectionError as e: log.error(f"DB connection failed: {e}", exc_info=True); print(f"Error: DB connection failed: {e}")
    except Exception as e: log.critical(f"Unexpected error during forced run: {e}", exc_info=True); print(f"Unexpected error: {e}")


# --- handle_status_command, handle_validate_command, handle_configure_command, handle_reset_counter_command (Unchanged) ---
# ... (code remains the same) ...
def handle_status_command(args):
    """Handles the 'status' command."""
    log.info("Handling 'status' command.")
    GstatusEM.display_status() # Assumes GstatusEM uses new get_db_stats

def handle_validate_command(args):
    """Handles the 'validate' command for listing or validating entries."""
    log.info(f"Handling 'validate' command. Validate ID: {args.validate_id}, List Unvalidated: {args.list_unvalidated}, List Missing LC: {args.list_missing_lc}, DB: {args.db}")
    action_taken = False
    db_key_target = args.db # Uses default='primary' from argparser

    # Ensure target DB is valid for listing/validation
    db_files = GconfigEM.get_db_filenames()
    db_filename = db_files.get(db_key_target)
    db_basename = os.path.basename(db_filename) if db_filename else None
    allowed_db_keys = ["primary", "backup", "test"]
    allowed_db_files = [GconfigEM.DEFAULT_DB_NAMES.get(k) for k in allowed_db_keys]

    if db_basename not in allowed_db_files:
         print(f"Error: Validation/Listing can only target Primary, Backup, or Test DBs. '{db_key_target}' is not suitable.")
         log.error(f"Validate command failed: Invalid target DB '{db_key_target}'.")
         return

    if args.list_unvalidated:
        print(f"Listing unvalidated entries from '{db_key_target}' DB...")
        GvalidatorEM.list_entries(db_key=db_key_target, list_type="unvalidated")
        action_taken = True
    if args.list_missing_lc:
        print(f"Listing entries potentially missing LC Docket ID from '{db_key_target}' DB...")
        GvalidatorEM.list_entries(db_key=db_key_target, list_type="missing_lc_docket")
        action_taken = True
    if args.validate_id:
         if db_key_target != "primary": print(f"Warning: Validating in '{db_key_target}' DB.")
         print(f"Starting interactive validation for UniqueID: {args.validate_id} in '{db_key_target}' DB...")
         GvalidatorEM.validate_case(args.validate_id, db_key_target) # Pass DB key
         action_taken = True

    if not action_taken: # Should not happen due to required=True group
        print("Error: No action specified for 'validate'.")
        log.error("Validate command handler reached without action flag.")

def handle_configure_command(args):
    """Handles the 'configure' command."""
    # (Code unchanged from V4)
    log.info(f"Handling 'configure' command. Args: {args}")
    try:
        config = GconfigEM.load_config()
        updated = False
        db_config_updated = False
        for db_type in GconfigEM.DEFAULT_DB_NAMES.keys():
             arg_name = f"db_{db_type}".replace("_", "-")
             new_filename = getattr(args, arg_name, None)
             if new_filename:
                 if GconfigEM.DB_FILENAME_PATTERN.match(new_filename):
                     if config['db_files'].get(db_type) != new_filename:
                         config['db_files'][db_type] = new_filename; print(f"{db_type.capitalize()} DB updated: {new_filename}"); log.info(f"Config updated - DB ({db_type}): {new_filename}"); db_config_updated = True
                     else: print(f"{db_type.capitalize()} DB already set to {new_filename}.")
                 else: print(f"Error: Invalid filename for {db_type}: '{new_filename}'. Use 'G[Name]EM.db'."); log.warning(f"Invalid DB filename for {db_type}: {new_filename}")
        updated = updated or db_config_updated
        if args.toggle_logging is not None:
            if config['logging'] != args.toggle_logging:
                 config['logging'] = args.toggle_logging; status = "enabled" if args.toggle_logging else "disabled"; print(f"File logging {status}."); log.info(f"Config updated - Logging: {status}"); updated = True
            else: print(f"File logging already {'enabled' if config['logging'] else 'disabled'}.")
        if updated: GconfigEM.save_config(config); print("Configuration saved.")
        else: print("No valid configuration options provided or values match current config.")
    except Exception as e: log.error(f"Error during configure: {e}", exc_info=True); print(f"Error during configuration: {e}")

def handle_reset_counter_command(args):
    """Handles the 'reset-counter' command."""
    # (Code unchanged from V4)
    log.info("Handling 'reset-counter' command.")
    try:
        confirm = prompt_with_timeout("Reset run counter to 0? (y/n): ", 60)
        if confirm and confirm.lower() == 'y': GconfigEM.reset_run_counter(); print("Run counter reset to 0."); log.info("Counter reset by user.")
        elif confirm is None: print("Reset cancelled (timeout)."); log.info("Counter reset timed out.")
        else: print("Reset cancelled."); log.info("User cancelled counter reset.")
    except Exception as e: log.error(f"Error resetting counter: {e}", exc_info=True); print(f"Error resetting counter: {e}")

# --- handle_build_combo_db (Unchanged) ---
def handle_build_combo_db(args):
    """Handles the 'build-combo-db' command."""
    # ... (code remains the same) ...
    log.info("Handling 'build-combo-db' command.")
    print("Attempting to rebuild the Combo database from Primary and Backup...")
    try:
        db_files = GconfigEM.get_db_filenames()
        combo_db, primary_db, backup_db = db_files.get("combo"), db_files.get("primary"), db_files.get("backup")
        if not all([combo_db, primary_db, backup_db]): print("Error: DB files for combo, primary, or backup missing in config."); log.error("Build Combo DB failed: Missing DB names."); return
        success, error_msg = GdbEM.build_combo_db(combo_db, primary_db, backup_db)
        if success: print(f"Successfully rebuilt '{combo_db}'.")
        else: print(f"Failed to rebuild Combo DB: {error_msg}")
    except Exception as e: log.error(f"Unexpected error during 'build-combo-db': {e}", exc_info=True); print(f"Unexpected error: {e}")

# --- handle_updater_command (Unchanged) ---
def handle_updater_command(args):
    """Handles the conceptual 'updater' command."""
    # ... (code remains the same - informational only) ...
    log.info(f"Handling 'updater' command. Args: {args}")
    print("\n--- Database Updater Information ---")
    print("Automatic fetching/parsing of PDF decisions is not currently supported.")
    print("\nRecommended Workflow for Updating/Correcting Data:")
    print("1. Identify records: Use `validate --list-unvalidated` or `validate --list-missing-lc`.")
    print("2. Review entry: Use `validate --validate-id <UniqueID>`.")
    print("3. Use displayed 'Potential PDF URL'.")
    print("4. Compare PDF info with validator data.")
    print("5. Edit fields in validator interface.")
    print("6. Mark as validated.")
    log.warning("Updater command invoked, functionality limited.")
    if args.update_id: print(f"\nTo manually update {args.update_id}, use: `validate --validate-id {args.update_id}`")


# --- handle_exit_command (Unchanged) ---
def handle_exit_command(args):
    """Handles the 'exit' command."""
    # ... (code remains the same) ...
    log.info("Handling 'exit' command."); print("Exit command received. Stopping..."); sys.exit(0)


# --- Argument Parser Setup (Updated run, configure, validate) ---
def parse_arguments():
    parser = argparse.ArgumentParser(prog='GmainEM', description='NJ Court Opinions Extractor', formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--version', action='version', version='%(prog)s 0.9.0') # Version bump
    subparsers = parser.add_subparsers(dest='command', help='Available commands', required=True)

    # --- Run Command ---
    run_parser = subparsers.add_parser('run', help='Run data extraction or start scheduler.', description='Default: Start scheduler.\n--force: Run immediate primary scrape -> primary, all_runs.\n--force --test: Simulate P1, P2, Backup runs -> all DBs.')
    run_parser.add_argument('--force', action='store_true', help='Run immediately instead of starting scheduler.')
    run_parser.add_argument('--test', action='store_true', help='With --force, runs test simulation hitting all DBs.')
    run_parser.set_defaults(func=handle_run_command)

    # --- Status Command ---
    status_parser = subparsers.add_parser('status', help='Show application status and DB stats')
    status_parser.set_defaults(func=handle_status_command)

    # --- Configure Command ---
    config_parser = subparsers.add_parser('configure', help='Configure settings like DB filenames or logging.')
    # Dynamically add args for all known DB types
    for db_type, default_name in GconfigEM.DEFAULT_DB_NAMES.items():
        arg_name = f"--db-{db_type.replace('_', '-')}"
        config_parser.add_argument(arg_name, type=str, metavar='FILENAME', help=f'Set {db_type} DB file (e.g., {default_name})')
    config_parser.add_argument('--toggle-logging', type=lambda x: x.lower() == 'true', metavar='true|false', help='Enable or disable file logging')
    config_parser.set_defaults(func=handle_configure_command)

    # --- Reset Counter Command ---
    reset_parser = subparsers.add_parser('reset-counter', help='Reset the application run counter to 0')
    reset_parser.set_defaults(func=handle_reset_counter_command)

    # --- Validate Command ---
    validate_parser = subparsers.add_parser('validate', help='Review, list, or manually validate database entries.', description='Use list flags or --validate-id.')
    validate_group = validate_parser.add_mutually_exclusive_group(required=True) # Require one action
    validate_group.add_argument('--list-unvalidated', action='store_true', help='List unvalidated entries')
    validate_group.add_argument('--list-missing-lc', action='store_true', help='List entries potentially missing LC Docket ID')
    validate_group.add_argument('--validate-id', type=str, metavar='UniqueID', help='Interactively review and validate specific UniqueID')
    validate_parser.add_argument('--db', choices=GconfigEM.DEFAULT_DB_NAMES.keys(), default='primary', help='Specify DB target for list/validate (default: primary)')
    validate_parser.set_defaults(func=handle_validate_command)

    # --- Updater Command (Informational) ---
    updater_parser = subparsers.add_parser('updater', help='(Info) Explains manual update process via Validator.', description='Provides guidance on updating records manually.')
    updater_parser.add_argument('--update-id', type=str, metavar='UniqueID', help='(Info only) Specify an ID.')
    updater_parser.set_defaults(func=handle_updater_command)

    # --- Build Combo DB Command ---
    combo_parser = subparsers.add_parser('build-combo-db', help='Manually rebuild Combo DB from Primary and Backup.')
    combo_parser.set_defaults(func=handle_build_combo_db)

    # --- Exit Command ---
    exit_parser = subparsers.add_parser('exit', help='Stop the application.')
    exit_parser.set_defaults(func=handle_exit_command)

    # --- Parse Arguments and Execute ---
    try:
        args = parser.parse_args()
        if hasattr(args, 'func') and callable(args.func): args.func(args)
        else: log.error("Command not recognized."); parser.print_help(); sys.exit(1)
    except Exception as e: log.critical(f"Arg parse/command error: {e}", exc_info=True); print(f"Error: {e}"); sys.exit(1)

# === End of GcliEM.py ===