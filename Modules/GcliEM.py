# GcliEM.py
# V8: Interactive merge-db command with safety checks; Fixed result logging indent.
"""
Handles CLI commands.
- `run`: Starts scheduler (default)
- `run --force`: Runs immediate primary scrape -> primary, all_runs.
- `run --force --test`: Simulates P1, P2, Backup runs -> all DBs.
- `status`: Shows status.
- `configure`: Configures settings.
- `reset-counter`: Resets run counter.
- `validate`: Lists or validates entries.
- `updater`: (Informational) Explains manual update process.
- `build-combo-db`: Manually rebuilds the combo DB.
- `merge-db`: Interactively merges data from an older DB file. # Interactive V8
- `exit`: Stops the application.
"""
import argparse
import logging
import time
import threading
import sys
import os
import re
import shutil # For file operations (copy, move)
import datetime
from queue import Queue, Empty

# Import project modules
import GscraperEM
import GdbEM
import GstatusEM
import GschedulerEM
import GconfigEM
import GvalidatorEM
import GmergerEM # For merge utility

log = logging.getLogger(__name__)

# --- Database Roles Clarification ---
# primary: Holds the most current, validated (or awaiting validation) production dataset, updated by scheduled runs 1 & 2 and manual force runs.
# backup: Holds a recent production dataset, updated independently by the scheduled backup run. Serves as a secondary source/comparison point.
# combo: A manually generated combination of the current primary and backup databases for a unified view (built using build-combo-db command). Considered production view.
# all_runs: Append-only historical archive. Stores a JSON snapshot of every record processed by any run targeting it (scheduled, manual, test, merge). NOT for direct production use, but for historical analysis.
# test: Non-production database for testing development changes. Data is volatile and not reliable.

# --- Input Handling Helpers ---
def get_input_with_timeout(prompt, timeout, input_queue):
    """Target function for input thread."""
    try:
        print(prompt, end='', flush=True)
        user_input = input()
        input_queue.put(user_input)
    except EOFError:
        input_queue.put(None) # Handle Ctrl+D
    except Exception as e:
        log.error(f"Input error: {e}", exc_info=True) # Log traceback
        input_queue.put(None)

def prompt_with_timeout(prompt_text, timeout_seconds=300):
    """Prompts user for input with a timeout."""
    q = Queue()
    t = threading.Thread(target=get_input_with_timeout, args=(prompt_text, timeout_seconds, q), daemon=True)
    t.start()
    t.join(timeout=timeout_seconds)
    if t.is_alive():
        print("\nTimeout waiting for input.")
        return None
    else:
        try:
            # Use get_nowait to avoid blocking if queue is somehow empty after thread joins
            data = q.get_nowait()
            return data.strip() if data is not None else None
        except Empty:
            log.warning("Input thread finished but queue was empty.")
            return None

def prompt_for_value(prompt_text, validation_func=None, error_msg="Invalid input.", default=None):
    """Helper to repeatedly prompt until valid input is received."""
    while True:
        prompt_full = prompt_text
        if default is not None: # Show default clearly
            prompt_full += f" [{default}]"
        prompt_full += ": "

        # Use raw input, handle default after stripping
        raw_input = input(prompt_full)
        user_input = raw_input.strip()

        if not user_input and default is not None:
            user_input = str(default) # Apply default if user just pressed Enter

        if validation_func:
            is_valid, value_or_error = validation_func(user_input)
            if is_valid:
                return value_or_error # Return the validated/converted value
            else:
                # Print specific error from validation func if provided
                print(f"Error: {value_or_error}. {error_msg}")
                # Loop again
        elif user_input: # If no validator, just need non-empty input
             return user_input
        elif default is not None and not raw_input: # If user hit enter accepting default, and no validator needed
             return str(default)
        else: # Input was empty, and no default was applicable
             print(f"Error: Input cannot be empty. {error_msg}")
             # Loop again


# --- Command Handler Functions ---

def handle_run_command(args):
    """Handles the 'run' command based on --force and --test flags."""
    log.info(f"Handling 'run' command. Force: {args.force}, Test: {args.test}")

    if not args.force:
        # Start Scheduler
        log.info("Starting scheduler..."); print("Initializing DBs...")
        try: GdbEM.initialize_all_databases(); GschedulerEM.start_schedule_loop()
        except Exception as e: log.critical(f"Scheduler start failed: {e}", exc_info=True); print(f"Error starting scheduler: {e}")
        return

    # --- Forced Run Logic ---
    try:
        print("Initializing all databases..."); GdbEM.initialize_all_databases()

        if args.test:
            # --- Test Run (--force --test) ---
            log.info("Test run requested."); print("--- Starting Test Run Simulation ---")
            run_type_base = 'manual-test'; is_validated = False
            # Simulate runs sequentially
            print("\n[Test] Simulating Primary Run 1...")
            ops1, _ = GscraperEM.fetch_and_parse_opinions()
            if ops1: print(f"[Test] Saving {len(ops1)} from P1."); GdbEM.save_opinions_to_dbs(ops1, is_validated, run_type_base + "-p1")
            else: print("[Test] No opinions found for P1.")
            time.sleep(0.5)
            print("\n[Test] Simulating Primary Run 2...")
            ops2, _ = GscraperEM.fetch_and_parse_opinions()
            if ops2: print(f"[Test] Saving {len(ops2)} from P2."); GdbEM.save_opinions_to_dbs(ops2, is_validated, run_type_base + "-p2")
            else: print("[Test] No opinions found for P2.")
            time.sleep(0.5)
            print("\n[Test] Simulating Backup Run...")
            opsB, _ = GscraperEM.fetch_and_parse_opinions()
            if opsB: print(f"[Test] Saving {len(opsB)} from Backup."); GdbEM.save_opinions_to_dbs(opsB, is_validated, run_type_base + "-bk")
            else: print("[Test] No opinions found for Backup.")
            print("\n--- Test Run Complete ---"); log.info("Test run finished.")

        else:
            # --- Standard Force Run (--force only) ---
            log.info("Standard force run requested."); print("--- Starting Forced Primary Run ---")
            run_type_force = 'manual-primary-force'

            print(f"Scraping {GscraperEM.PAGE_URL}..."); scraped_opinions, release_date = GscraperEM.fetch_and_parse_opinions()
            if not scraped_opinions: print("No opinions found."); log.info("Forced run: No opinions."); return

            # Detailed Output Format
            print(f"\n--- Scraped Data ({len(scraped_opinions)} entries) ---")
            display_fields = [
                ('ReleaseDate', '[ReleaseDate] Opinion Release Date'),
                ('opinionstatus', '[OpinionStatus] Opinion Status'),
                ('Venue', '[Venue] Current Venue'),
                ('CaseName', '[CaseName] Case Caption'),
                ('AppDocketID', '[AppDocketID] Appellate Division (A.D.) Docket No.'),
                ('LinkedDocketIDs', '[LinkedDocketIDs] Related A.D. Case No(s).'),
                ('DecisionTypeCode', '[DecisionTypeCode] Opinion Type Code'),
                ('DecisionTypeText', '[DecisionTypeText] Opinion Type Text'),
                ('LCCounty', '[LCCounty] Lower Court County'),
                ('LCdocketID', '[LCdocketID] LC Docket No(s).'),
                ('LowerCourtVenue', '[LowerCourtVenue] LC Venue'),
                ('LowerCourtSubCaseType', '[LowerCourtSubCaseType] LC Sub-Case Type'),
                ('StateAgency1', '[StateAgency1] State Agency Involved'),
                ('StateAgency2', '[StateAgency2] Other State Agency Involved'),
                ('CaseNotes', '[CaseNotes] Case Notes'),
                ('caseconsolidated', '[caseconsolidated] Consolidated Matter'),
                ('recordimpounded', '[recordimpounded] Record Impounded'),
            ]
            for i, opinion in enumerate(scraped_opinions):
                print(f"\n--- Scraped Entry #{i+1} ---") # Clear header for each entry
                for db_col, label in display_fields:
                    value = opinion.get(db_col)
                    display_value = "N/A" # Default if value is None or missing
                    if value is not None: # Format specific fields if value exists
                        if db_col == 'caseconsolidated': display_value = "Consolidated" if value else "Not Consolidated"
                        elif db_col == 'recordimpounded': display_value = "Record Impounded" if value else "Record Public"
                        elif db_col == 'opinionstatus': display_value = "Opinion Released" if value else "Opinion Expected"
                        elif isinstance(value, (list, tuple)): display_value = ", ".join(map(str, value)) if value else "N/A" # Handle empty lists/tuples
                        else: display_value = str(value)
                    print(f"  {label}: {display_value}") # Print label and formatted value
                print("-" * 30) # Separator line between entries
            print("--- End of Scraped Data ---")

            # Validation Prompt
            validation_timeout = 180; prompt_msg = f"\nValidate {len(scraped_opinions)} entries? (y=Yes, n=No/Discard, s/Enter=Skip) [{validation_timeout}s timeout]: "; user_response = prompt_with_timeout(prompt_msg, validation_timeout)
            is_validated, proceed_to_save = False, True
            if user_response is None or user_response.lower() == 's' or user_response == '': print("\nSkipping validation."); log.info("Validation skipped/timeout.")
            elif user_response.lower() == 'y': print("Marked as validated."); log.info("User validated data."); is_validated = True
            elif user_response.lower() == 'n': print("Discarding data."); log.info("User discarded data."); proceed_to_save = False
            else: print("Invalid input. Skipping validation."); log.warning(f"Invalid input '{user_response}'.")

            # Save (only to primary and all_runs)
            if proceed_to_save:
                print("\nProcessing for Primary and AllRuns databases..."); save_results = GdbEM.save_opinions_to_dbs(scraped_opinions, is_validated, run_type_force); print("\nDatabase processing complete:")
                db_files = GconfigEM.get_db_filenames()
                # Loop for logging results (Corrected Indentation & Logic)
                for db_key in ["primary", "all_runs"]:
                    if db_key in save_results:
                        counts = save_results[db_key]
                        db_path = db_files.get(db_key, "N/A")
                        init_error_std = counts.get("error") == -1
                        init_error_hist = counts.get("error_history") == -1

                        # Use consistent indent level here
                        if init_error_std or init_error_hist:
                            log.error(f"DB '{db_key}' ({db_path}): Skipped due to initialization error.")
                            print(f"  {db_key.capitalize()} DB: Skipped (Initialization Error)")
                        elif counts.get("total", 0) > 0:
                            if db_key == "all_runs":
                                ins_h = counts.get('inserted_history', 0); err_h = counts.get('error_history', 0)
                                log.info(f"DB 'all_runs' ({db_path}): Processed {counts['total']} -> Hist Ins: {ins_h}, Hist Err: {err_h}")
                                print(f"  AllRuns DB: History Inserted={ins_h}, Errors={err_h}")
                            else: # Standard DB (primary)
                                ins_s = counts.get('inserted', 0); upd_s = counts.get('updated', 0); skp_s = counts.get('skipped', 0); err_s = counts.get('error', 0)
                                log.info(f"DB '{db_key}' ({db_path}): Processed {counts['total']} -> Ins: {ins_s}, Upd: {upd_s}, Skp: {skp_s}, Err: {err_s}")
                                print(f"  {db_key.capitalize()} DB: Inserted={ins_s}, Updated={upd_s}, Skipped={skp_s}, Errors={err_s}")

            else: # User chose not to save
                print("Data discarded."); log.info("Forced run data discarded.")

            print("\nForced primary run finished."); log.info("Forced primary run finished.")

    # Error Handling
    except FileNotFoundError as e: log.error(f"Config missing: {e}", exc_info=True); print("Error: Config file missing.")
    except KeyError as e: log.error(f"Config key missing: {e}", exc_info=True); print(f"Error: Setting '{e}' missing.")
    except ConnectionError as e: log.error(f"DB connection fail: {e}", exc_info=True); print(f"Error: DB connection failed: {e}")
    except Exception as e: log.critical(f"Unexpected error during forced run: {e}", exc_info=True); print(f"Unexpected error: {e}")


def handle_status_command(args):
    """Handles the 'status' command."""
    log.info("Handling 'status' command.")
    GstatusEM.display_status() # Assumes GstatusEM uses new get_db_stats

def handle_validate_command(args):
    """Handles the 'validate' command for listing or validating entries."""
    log.info(f"Handling 'validate' command: {args}")
    db_key = args.db # Uses default='primary' from argparser
    action_taken = False
    db_files = GconfigEM.get_db_filenames()
    db_path = db_files.get(db_key)
    allowed_keys = ["primary", "backup", "test"] # Target DBs must use the 'opinions' schema

    if db_key not in allowed_keys:
         print(f"Error: Validation/Listing can only target Primary, Backup, or Test DBs. '{db_key}' is not suitable.")
         log.error(f"Validate command failed: Invalid target DB '{db_key}'.")
         return

    if args.list_unvalidated:
        print(f"Listing unvalidated entries from '{db_key}' DB...")
        GvalidatorEM.list_entries(db_key=db_key, list_type="unvalidated")
        action_taken = True
    if args.list_missing_lc:
        print(f"Listing entries potentially missing LC Docket ID from '{db_key}' DB...")
        GvalidatorEM.list_entries(db_key=db_key, list_type="missing_lc_docket")
        action_taken = True
    if args.validate_id:
         if db_key != "primary": # Optional warning if not validating primary
              print(f"Warning: Validating entry in non-primary database ('{db_key}'). Changes are isolated.")
         print(f"Starting interactive validation for UniqueID: {args.validate_id} in '{db_key}' DB...")
         GvalidatorEM.validate_case(args.validate_id, db_key) # Pass DB key to validator
         action_taken = True

    if not action_taken: # Should not happen due to required=True group
        print("Error: No action specified for 'validate'. Use --list-unvalidated, --list-missing-lc, or --validate-id.")
        log.error("Validate command handler reached without action flag.")

def handle_configure_command(args):
    """Handles the 'configure' command."""
    log.info(f"Handling 'configure' command: {args}")
    try:
        config = GconfigEM.load_config(); updated = False; db_updated = False
        # DB File Updates
        for db_type in GconfigEM.DEFAULT_DB_NAMES.keys():
             arg_name = f"db_{db_type}".replace("_", "-")
             new_filename = getattr(args, arg_name, None)
             if new_filename:
                 if GconfigEM.DB_FILENAME_PATTERN.match(new_filename):
                     if config['db_files'].get(db_type) != new_filename:
                         config['db_files'][db_type] = new_filename; print(f"{db_type.capitalize()} DB updated."); db_updated = True
                 else: print(f"Error: Invalid format for {db_type}: {new_filename}")
        updated = updated or db_updated
        # Logging Toggle
        if args.toggle_logging is not None:
            if config['logging'] != args.toggle_logging:
                config['logging'] = args.toggle_logging; print(f"Logging set to {args.toggle_logging}."); updated = True
        # Save if changed
        if updated: GconfigEM.save_config(config); print("Configuration saved.")
        else: print("No valid configuration changes provided.")
    except Exception as e: log.error(f"Configure error: {e}", exc_info=True); print("Configure error.")


def validate_schedule_entry(entry_str):
    """Validates a schedule entry string (HH:MM,type,days)."""
    parts = entry_str.split(",")
    if len(parts) != 3:
        return False, "Invalid format. Use 'HH:MM,type,days' (e.g., '14:00,primary-1,Mon-Fri')."

    time_str, run_type, days_str = parts
    if not re.match(r"^\d{2}:\d{2}$", time_str):
        return False, "Invalid time format. Use HH:MM."

    allowed_types = ["primary-1", "primary-2", "backup", "other"]  # Add "other" for custom types
    if run_type not in allowed_types:
        return False, f"Invalid run type. Choose from: {', '.join(allowed_types)}"

    allowed_days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    days = days_str.split("-")
    if not all(day.capitalize() in allowed_days for day in days):
        return False, f"Invalid day(s). Use abbreviations (e.g., 'Mon-Fri', 'Sun')."

    return True, {"time": time_str, "type": run_type, "days": days_str}


def handle_add_schedule_command(args):
    """Handles the 'add-schedule' command."""
    log.info("Handling 'add-schedule' command")
    entry_str = prompt_for_value("Enter new schedule entry (HH:MM,type,days)", validate_schedule_entry)
    if entry_str:
        config = GconfigEM.load_config()
        config["schedule"].append(entry_str)
        GconfigEM.save_config(config)

def handle_reset_counter_command(args):
    """Handles the 'reset-counter' command."""
    log.info("Handling 'reset-counter'")
    confirm = prompt_with_timeout("Reset run counter to 0? (y/n): ", 60)
    if confirm and confirm.lower() == 'y': GconfigEM.reset_run_counter(); print("Counter reset."); log.info("Counter reset.")
    else: print("Reset cancelled."); log.info("Reset cancelled.")

def handle_build_combo_db(args):
    """Handles the 'build-combo-db' command."""
    log.info("Handling 'build-combo-db'"); print("Rebuilding Combo DB...")
    try:
        db_files = GconfigEM.get_db_filenames(); combo, primary, backup = db_files.get("combo"), db_files.get("primary"), db_files.get("backup")
        if not all([combo, primary, backup]): print("Error: DB files missing in config."); return
        success, msg = GdbEM.build_combo_db(combo, primary, backup)
        if success: print(f"Rebuilt '{combo}'.")
        else: print(f"Failed: {msg}")
    except Exception as e: log.error(f"Build combo error: {e}", exc_info=True); print("Error building combo DB.")

def handle_updater_command(args):
    """Handles the conceptual 'updater' command (Informational Only)."""
    log.info("Handling 'updater'"); print("\n--- Updater Info ---"); print("Auto PDF parsing not available. Use 'validate'."); log.warning("Updater cmd limited.")
    if args.update_id: print(f" To manually update: `validate --validate-id {args.update_id}`")

def handle_exit_command(args):
    """Handles the 'exit' command."""
    log.info("Handling 'exit'"); print("Exiting..."); sys.exit(0)

# --- UPDATED Interactive Handler for Merge DB command ---
def handle_merge_db(args):
    """Handles the 'merge-db' command interactively with safety checks."""
    log.info("Handling 'merge-db' command (Interactive Mode).")
    print("\n--- Database Merge Utility ---")
    print("Merges data from an older DB file into primary, backup, or test.")
    print("NOTE: Target duplicates (same content hash) are SKIPPED.")
    print("      ALL source records are logged to AllRuns history DB.")

    # --- Get Inputs Interactively ---
    def validate_source_path(path):
        abs_path = os.path.abspath(os.path.expanduser(path))
        return (True, abs_path) if os.path.exists(abs_path) and os.path.isfile(abs_path) and abs_path.lower().endswith('.db') else (False, f"Valid .db file not found: '{path}'")
    source_path = prompt_for_value("Enter FULL path to SOURCE (older) .db file", validate_source_path)
    if not source_path: print("Cancelled."); return

    allowed_targets = ["primary", "backup", "test"]; target_prompt = f"Enter TARGET DB key ({'/'.join(allowed_targets)})"
    def validate_target_key(key): 
        k = key.lower()
        return (True, k) if k in allowed_targets else (False, f"Choose from: {', '.join(allowed_targets)}")
    target_key = prompt_for_value(target_prompt, validate_target_key, default="test") # Default to test
    if not target_key: print("Cancelled."); return

    def validate_version(v_str):
        try:
            v = int(v_str)
            if v >= 0:
                return (True, v)
            else:
                return (False, "Must be >= 0.")
        except ValueError:
            return (False, "Must be an integer.")
    source_ver = prompt_for_value("Enter SCHEMA VERSION of SOURCE DB (e.g., 1, 2)", validate_version)
    if source_ver is None: print("Cancelled."); return

    # --- Display Plan and Confirm ---
    db_files = GconfigEM.get_db_filenames(); target_path = db_files.get(target_key); all_runs_path = db_files.get("all_runs")
    if not target_path: print(f"Error: Target DB '{target_key}' path missing."); return

    print("\n--- Merge Plan ---"); print(f"Source : {source_path} (V{source_ver})"); print(f"Target : {target_path} ({target_key})"); print(f"History: {all_runs_path or '(Not Configured)'}"); print("Process: 1.Backup 2.TempCopy 3.Merge->Temp 4.Log->History 5.ConfirmReplace"); print("-" * 20)
    confirm1 = prompt_with_timeout("Proceed with backup & merge to TEMP file? (y/n): ", 60)
    if not confirm1 or confirm1.lower() != 'y': print("Merge cancelled."); log.info("Merge cancelled (stage 1)."); return

    # --- Execute Safe Merge ---
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    source_backup = f"{source_path}.{timestamp}.bak"; target_backup = f"{target_path}.{timestamp}.bak"; temp_target = f"{target_path}.merge_candidate.{timestamp}.tmp"
    merge_job_success = False # Tracks if the merge function itself ran ok
    try: # File Ops & Merge Call
        print(f"Backing up source -> {os.path.basename(source_backup)}"); shutil.copy2(source_path, source_backup); log.info(f"Src backup: {source_backup}")
        print(f"Backing up target -> {os.path.basename(target_backup)}"); shutil.copy2(target_path, target_backup); log.info(f"Tgt backup: {target_backup}")
        print(f"Creating temp target -> {os.path.basename(temp_target)}"); shutil.copy2(target_path, temp_target); log.info(f"Temp target: {temp_target}")
        print(f"\nAttempting merge into temporary file..."); time.sleep(0.5)
        # Pass the TEMP path to the merge function
        merge_job_success = GmergerEM.merge_old_database(source_path, temp_target, source_ver) # Pass temp path
    except Exception as e: log.error(f"Pre-merge/Merge call error: {e}", exc_info=True); print(f"Error during setup/merge call: {e}"); merge_job_success = False

    # --- Handle Merge Result ---
    if merge_job_success: # Check if merge function completed without critical failure
        log.info(f"Merge function completed for temp file '{temp_target}'."); print("\nMerge function completed (check summary above).")
        print("-" * 20); print(f"Merged : {os.path.basename(temp_target)}"); print(f"Original: {os.path.basename(target_path)}"); print("-" * 20)
        confirm2 = prompt_with_timeout(f"REPLACE original target '{target_path}' with merged file? (y/n): ", 120)
        if confirm2 and confirm2.lower() == 'y':
            try:
                target_old = f"{target_path}.pre_merge_{timestamp}.old"; print(f"Moving original -> {os.path.basename(target_old)}"); os.rename(target_path, target_old); log.info(f"Original -> {target_old}")
                print(f"Moving merged -> {os.path.basename(target_path)}"); os.rename(temp_target, target_path); log.info(f"Temp -> {target_path}"); print("\nMerge complete. Target file replaced.")
            except OSError as e:
                log.error(f"Error replacing target: {e}", exc_info=True)
                print(f"Error replacing file: {e}.")
        else:
            print("Replace cancelled. Deleting temp."); log.info("User cancelled replace. Deleting temp.")
            try:
                os.remove(temp_target)
            except OSError as e:
                log.warning(f"Cannot delete temp {temp_target}: {e}")
    else: # Merge function failed or setup failed
        log.error(f"Merge process failed before completion for '{temp_target}'."); print("\nMerge process failed. Check logs.")
        print("Deleting temporary merge file (if exists).")
        if os.path.exists(temp_target): 
            try:
                os.remove(temp_target)
            except OSError as e:
                log.warning(f"Cannot delete failed temp {temp_target}: {e}")
    # Cleanup message if necessary
    if not merge_job_success or (confirm2 and confirm2.lower() != 'y'): print("Original target file unchanged. Backups preserved.")


# --- Argument Parser Setup ---
def parse_arguments():
    parser = argparse.ArgumentParser(prog='GmainEM', description='NJ Court Opinions Extractor', formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--version', action='version', version='%(prog)s 1.0.1')
    subparsers = parser.add_subparsers(dest='command', help='Available commands', required=True)
    # Run Command
    run_parser = subparsers.add_parser('run', help='Run extraction or scheduler.', description='Default: Scheduler.\n--force: Immediate primary->P+AR.\n--force --test: Simulate runs->All DBs.'); run_parser.add_argument('--force', action='store_true'); run_parser.add_argument('--test', action='store_true'); run_parser.set_defaults(func=handle_run_command)
    # Status, Configure, Reset, Validate, Updater, Build-Combo, Exit...
    status_parser = subparsers.add_parser('status', help='Show status'); status_parser.set_defaults(func=handle_status_command)
    config_parser = subparsers.add_parser('configure', help='Configure settings'); config_parser.set_defaults(func=handle_configure_command)
    for db_t, def_n in GconfigEM.DEFAULT_DB_NAMES.items(): config_parser.add_argument(f"--db-{db_t.replace('_','-')}", type=str, metavar='FN', help=f'Set {db_t} DB ({def_n})')
    config_parser.add_argument('--toggle-logging', type=lambda x: x.lower()=='true', metavar='t/f', help='Enable/disable file logging')

    reset_parser = subparsers.add_parser('reset-counter', help='Reset run counter'); reset_parser.set_defaults(func=handle_reset_counter_command)
    add_schedule_parser = subparsers.add_parser('add-schedule', help='Add a new schedule entry'); add_schedule_parser.set_defaults(func=handle_add_schedule_command)

    validate_parser = subparsers.add_parser('validate', help='List/validate entries'); validate_parser.set_defaults(func=handle_validate_command)
    v_group = validate_parser.add_mutually_exclusive_group(required=True); v_group.add_argument('--list-unvalidated', action='store_true'); v_group.add_argument('--list-missing-lc', action='store_true'); v_group.add_argument('--validate-id', type=str, metavar='UID')
    validate_parser.add_argument('--db', choices=GconfigEM.DEFAULT_DB_NAMES.keys(), default='primary', help='Target DB (default: primary)')
    updater_parser = subparsers.add_parser('updater', help='(Info) Manual update'); updater_parser.set_defaults(func=handle_updater_command); updater_parser.add_argument('--update-id', type=str, metavar='UID', help='(Info only)')
    combo_parser = subparsers.add_parser('build-combo-db', help='Rebuild Combo DB'); combo_parser.set_defaults(func=handle_build_combo_db)
    exit_parser = subparsers.add_parser('exit', help='Stop application'); exit_parser.set_defaults(func=handle_exit_command)
    # Merge DB Command (No Args)
    merge_parser = subparsers.add_parser('merge-db', help='Interactively merge data from an older DB.', description='Prompts for inputs. Uses safe backup/temp process.')
    merge_parser.set_defaults(func=handle_merge_db)
    # Parse and Execute
    try: args = parser.parse_args(); args.func(args)
    except Exception as e: log.critical(f"Arg parse/command error: {e}", exc_info=True); print(f"Error: {e}"); sys.exit(1)

# === End of GcliEM.py ===