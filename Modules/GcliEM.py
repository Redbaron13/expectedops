# GcliEM.py
# V6: Added merge-db command
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
- `merge-db`: Manually merges data from an older DB file. # NEW
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
import datetime

import GscraperEM
import GdbEM
import GstatusEM
import GschedulerEM
import GconfigEM
import GvalidatorEM
import GmergerEM # NEW Import for merge utility

log = logging.getLogger(__name__)

# --- Input Handling (Unchanged) ---
# ... (code remains the same) ...
def get_input_with_timeout(prompt, timeout, input_queue): # ...
    try: print(prompt, end='', flush=True); user_input = input(); input_queue.put(user_input)
    except EOFError: input_queue.put(None)
    except Exception as e: log.error(f"Input error: {e}"); input_queue.put(None)
def prompt_with_timeout(prompt_text, timeout_seconds=300): # ...
    q=Queue();t=threading.Thread(target=get_input_with_timeout,args=(prompt_text,timeout_seconds,q),daemon=True);t.start();t.join(timeout_seconds)
    if t.is_alive(): print("\nTimeout."); return None
    else:
        try: data = q.get_nowait(); return data.strip() if data is not None else None
        except Empty: log.warning("Input queue empty."); return None

# --- Command Handler Functions ---

# ... (handle_run_command, handle_status_command, handle_validate_command, handle_configure_command, handle_reset_counter_command, handle_build_combo_db, handle_updater_command, handle_exit_command - remain the same as V6/V7) ...
def handle_run_command(args): # ... (code remains same as V6) ...
    log.info(f"Handling 'run' command. Force: {args.force}, Test: {args.test}")
    if not args.force: log.info("Starting scheduler..."); print("Initializing DBs..."); GdbEM.initialize_all_databases(); GschedulerEM.start_schedule_loop(); return
    try:
        print("Initializing all databases..."); GdbEM.initialize_all_databases()
        if args.test: # Test Run
            log.info("Test run requested."); print("--- Starting Test Run Simulation ---"); run_type_base = 'manual-test'; is_validated = False
            print("\n[Test] Simulating P1..."); ops1, _ = GscraperEM.fetch_and_parse_opinions();
            if ops1: print(f"[Test] Saving {len(ops1)} from P1."); GdbEM.save_opinions_to_dbs(ops1, is_validated, run_type_base + "-p1")
            else: print("[Test] No ops P1."); time.sleep(0.5)
            print("\n[Test] Simulating P2..."); ops2, _ = GscraperEM.fetch_and_parse_opinions();
            if ops2: print(f"[Test] Saving {len(ops2)} from P2."); GdbEM.save_opinions_to_dbs(ops2, is_validated, run_type_base + "-p2")
            else: print("[Test] No ops P2."); time.sleep(0.5)
            print("\n[Test] Simulating Backup..."); opsB, _ = GscraperEM.fetch_and_parse_opinions();
            if opsB: print(f"[Test] Saving {len(opsB)} from Backup."); GdbEM.save_opinions_to_dbs(opsB, is_validated, run_type_base + "-bk")
            else: print("[Test] No ops Backup."); print("\n--- Test Run Complete ---"); log.info("Test run finished.")
        else: # Standard Force Run
            log.info("Standard force run requested."); print("--- Starting Forced Primary Run ---"); run_type_force = 'manual-primary-force'
            print(f"Scraping {GscraperEM.PAGE_URL}..."); ops, _ = GscraperEM.fetch_and_parse_opinions()
            if not ops: print("No opinions found."); log.info("Forced run: No opinions."); return
            print(f"\n--- Scraped Data ({len(ops)} entries) ---") # Detailed Print Logic
            display_fields = [('ReleaseDate','[ReleaseDate] Opinion Release Date'), ('opinionstatus','[OpinionStatus] Opinion Status'), ('Venue','[Venue] Current Venue'), ('CaseName','[CaseName] Case Caption'), ('AppDocketID','[AppDocketID] Appellate Division (A.D.) Docket No.'), ('LinkedDocketIDs','[LinkedDocketIDs] Related A.D. Case No(s).'), ('DecisionTypeCode','[DecisionTypeCode] Opinion Type Code'), ('DecisionTypeText','[DecisionTypeText] Opinion Type Text'), ('LCCounty','[LCCounty] Lower Court County'), ('LCdocketID','[LCdocketID] LC Docket No(s).'), ('LowerCourtVenue','[LowerCourtVenue] LC Venue'), ('LowerCourtSubCaseType','[LowerCourtSubCaseType] LC Sub-Case Type'), ('StateAgency1','[StateAgency1] State Agency Involved'), ('StateAgency2','[StateAgency2] Other State Agency Involved'), ('CaseNotes','[CaseNotes] Case Notes'), ('caseconsolidated','[caseconsolidated] Consolidated Matter'), ('recordimpounded','[recordimpounded] Record Impounded')]
            for i, o in enumerate(ops):
                print(f"\n--- Scraped Entry #{i+1} ---")
                for db_col, label in display_fields:
                    v = o.get(db_col); dv = "N/A"
                    if v is not None:
                        if db_col=='caseconsolidated': dv="Consolidated" if v else "Not Consolidated"
                        elif db_col=='recordimpounded': dv="Record Impounded" if v else "Record Public"
                        elif db_col=='opinionstatus': dv="Opinion Released" if v else "Opinion Expected"
                        elif isinstance(v,(list,tuple)): dv=", ".join(map(str,v))
                        else: dv=str(v)
                    print(f"  {label}: {dv}")
                print("-" * 30)
            print("--- End of Scraped Data ---")
            # Validation Prompt
            prompt_msg = f"\nValidate {len(ops)} entries? (y=Yes, n=No/Discard, s/Enter=Skip) [180s timeout]: "; user_response = prompt_with_timeout(prompt_msg, 180)
            is_validated, proceed = False, True
            if user_response is None or user_response.lower()=='s' or user_response=='': print("\nSkipping validation."); log.info("Validation skipped/timeout.")
            elif user_response.lower()=='y': print("Marked as validated."); log.info("User validated."); is_validated=True
            elif user_response.lower()=='n': print("Discarding data."); log.info("User discarded."); proceed=False
            else: print("Invalid input. Skipping validation."); log.warning(f"Invalid input '{user_response}'.")
            # Save
            if proceed:
                print("\nProcessing for Primary and AllRuns DBs..."); save_results=GdbEM.save_opinions_to_dbs(ops, is_validated, run_type_force); print("\nDB processing complete:")
                db_files=GconfigEM.get_db_filenames() # Log results (corrected logic)
                for db_key in ["primary", "all_runs"]:
                    if db_key in save_results: counts=save_results[db_key]; db_path=db_files.get(db_key,"N/A")
                                              if counts.get("error",0)==-1 or counts.get("error_history",0)==-1: log.error(f"DB '{db_key}' Skipped: init error."); print(f"  {db_key.capitalize()} DB: Skipped (init error)")
                                              elif counts.get("total",0)>0:
                                                  if db_key=="all_runs": log.info(f"DB AR: Proc {counts['total']}->HistIns:{counts['inserted_history']}, HistErr:{counts['error_history']}"); print(f"  AllRuns: Hist Ins={counts['inserted_history']}, Err={counts['error_history']}")
                                                  else: log.info(f"DB '{db_key}': Proc {counts['total']}->Ins:{counts['inserted']}, Upd:{counts['updated']}, Skp:{counts['skipped']}, Err:{counts['error']}"); print(f"  {db_key.capitalize()}: Ins={counts['inserted']}, Upd={counts['updated']}, Skp={counts['skipped']}, Err={counts['error']}")
            else: print("Data discarded."); log.info("Forced run data discarded.")
            print("\nForced primary run finished."); log.info("Forced primary run finished.")
    except Exception as e: log.critical(f"Forced run error: {e}", exc_info=True); print(f"Error during forced run: {e}")

def handle_status_command(args): log.info("Handling 'status'"); GstatusEM.display_status()
def handle_validate_command(args):
    log.info(f"Handling 'validate' command: {args}"); db_key=args.db; action=False; db_files=GconfigEM.get_db_filenames(); db_path=db_files.get(db_key)
    allowed=[db_files.get(k) for k in ["primary","backup","test"]];
    if os.path.basename(db_path) not in allowed: print(f"Error: Validate only on P/B/T DBs."); return
    if args.list_unvalidated: print(f"Listing unvalidated {db_key}..."); GvalidatorEM.list_entries(db_key=db_key, list_type="unvalidated"); action=True
    if args.list_missing_lc: print(f"Listing missing LC {db_key}..."); GvalidatorEM.list_entries(db_key=db_key, list_type="missing_lc_docket"); action=True
    if args.validate_id: print(f"Validating {args.validate_id} in {db_key}..."); GvalidatorEM.validate_case(args.validate_id, db_key); action=True
    if not action: print("Validate: No action specified.")
def handle_configure_command(args):
    log.info(f"Handling 'configure' command: {args}")
    try: # Code unchanged from V6
        config=GconfigEM.load_config(); updated=False; db_updated=False
        for db_type in GconfigEM.DEFAULT_DB_NAMES.keys():
             arg_name=f"db_{db_type}".replace("_","-"); fn=getattr(args, arg_name, None)
             if fn:
                 if GconfigEM.DB_FILENAME_PATTERN.match(fn):
                     if config['db_files'].get(db_type)!=fn: config['db_files'][db_type]=fn; print(f"{db_type} DB updated."); db_updated=True
                 else: print(f"Error: Invalid format for {db_type}: {fn}")
        updated=updated or db_updated
        if args.toggle_logging is not None:
            if config['logging']!=args.toggle_logging: config['logging']=args.toggle_logging; print(f"Logging {args.toggle_logging}."); updated=True
        if updated: GconfigEM.save_config(config); print("Config saved.")
        else: print("No valid changes.")
    except Exception as e: log.error(f"Configure error: {e}", exc_info=True); print("Configure error.")
def handle_reset_counter_command(args):
    log.info("Handling 'reset-counter'"); confirm=prompt_with_timeout("Reset counter? (y/n): ", 60)
    if confirm and confirm.lower()=='y': GconfigEM.reset_run_counter(); print("Counter reset."); log.info("Counter reset.")
    else: print("Reset cancelled."); log.info("Reset cancelled.")
def handle_build_combo_db(args):
    log.info("Handling 'build-combo-db'"); print("Rebuilding Combo DB...")
    try: # Code unchanged from V6
        db_files=GconfigEM.get_db_filenames(); combo, primary, backup = db_files.get("combo"), db_files.get("primary"), db_files.get("backup")
        if not all([combo, primary, backup]): print("Error: DB files missing in config."); return
        success, msg = GdbEM.build_combo_db(combo, primary, backup)
        if success: print(f"Rebuilt '{combo}'.")
        else: print(f"Failed: {msg}")
    except Exception as e: log.error(f"Build combo error: {e}", exc_info=True); print("Error building combo DB.")
def handle_updater_command(args): # Informational only
    log.info("Handling 'updater'"); print("\n--- Updater Info ---"); print("Auto PDF parsing not available. Use 'validate'."); log.warning("Updater cmd limited.")
    if args.update_id: print(f" To manually update: `validate --validate-id {args.update_id}`")
def handle_exit_command(args): log.info("Handling 'exit'"); print("Exiting..."); sys.exit(0)


# --- NEW: Handler for Merge DB command ---
def handle_merge_db(args):
    """Handles the 'merge-db' command."""
    log.info(f"Handling 'merge-db': Source={args.source}, Target={args.target_db}, SourceVer={args.source_version}")

    source_path = args.source
    target_key = args.target_db
    source_ver = args.source_version

    if not os.path.exists(source_path):
        print(f"Error: Source file not found: {source_path}")
        log.error(f"Merge DB error: Source file '{source_path}' not found.")
        return

    # Basic validation for source version
    if not isinstance(source_ver, int) or source_ver < 0:
         print(f"Error: Invalid source schema version '{source_ver}'. Must be a non-negative integer.")
         log.error(f"Merge DB error: Invalid source version '{source_ver}'.")
         return

    # Confirmation prompt
    confirm_prompt = f"\nWARNING: This will merge data from:\n  Source: {source_path} (Schema V{source_ver})\n  Target: {target_key} database\n\nExisting records in '{target_key}' with the same UniqueID (content hash) as source records will be SKIPPED.\nThis operation cannot be easily undone. Backup your target DB first!\n\nProceed with merge? (y/n): "
    confirm = prompt_with_timeout(confirm_prompt, timeout_seconds=60)

    if confirm and confirm.lower() == 'y':
        print(f"Starting merge process...")
        try:
            success = GmergerEM.merge_old_database(source_path, target_key, source_ver)
            if success:
                print("Merge process completed successfully.")
            else:
                print("Merge process finished with errors. Check logs for details.")
        except Exception as e:
            log.error(f"Unexpected error during merge command execution: {e}", exc_info=True)
            print(f"An unexpected error occurred during the merge: {e}")
    else:
        print("Merge cancelled by user or timeout.")
        log.info("Merge operation cancelled.")


# --- Argument Parser Setup ---
def parse_arguments():
    parser = argparse.ArgumentParser(prog='GmainEM', description='NJ Court Opinions Extractor', formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--version', action='version', version='%(prog)s 1.0.0') # Version bump
    subparsers = parser.add_subparsers(dest='command', help='Available commands', required=True)

    # --- Run Command (Unchanged definition) ---
    run_parser = subparsers.add_parser('run', help='Run extraction or start scheduler.', description='Default: Start scheduler.\n--force: Run immediate primary scrape -> primary, all_runs.\n--force --test: Simulate P1, P2, Backup runs -> all DBs.')
    run_parser.add_argument('--force', action='store_true', help='Run immediately.')
    run_parser.add_argument('--test', action='store_true', help='With --force, runs test simulation.')
    run_parser.set_defaults(func=handle_run_command)

    # --- Status Command (Unchanged definition) ---
    status_parser = subparsers.add_parser('status', help='Show status'); status_parser.set_defaults(func=handle_status_command)

    # --- Configure Command (Unchanged definition) ---
    config_parser = subparsers.add_parser('configure', help='Configure settings'); config_parser.set_defaults(func=handle_configure_command)
    for db_t, def_n in GconfigEM.DEFAULT_DB_NAMES.items(): config_parser.add_argument(f"--db-{db_t.replace('_','-')}", type=str, metavar='FN', help=f'Set {db_t} DB ({def_n})')
    config_parser.add_argument('--toggle-logging', type=lambda x: x.lower()=='true', metavar='t/f', help='Enable/disable file logging')

    # --- Reset Counter Command (Unchanged definition) ---
    reset_parser = subparsers.add_parser('reset-counter', help='Reset run counter'); reset_parser.set_defaults(func=handle_reset_counter_command)

    # --- Validate Command (Unchanged definition) ---
    validate_parser = subparsers.add_parser('validate', help='List or validate entries'); validate_parser.set_defaults(func=handle_validate_command)
    v_group = validate_parser.add_mutually_exclusive_group(required=True); v_group.add_argument('--list-unvalidated', action='store_true', help='List unvalidated')
    v_group.add_argument('--list-missing-lc', action='store_true', help='List missing LC dockets'); v_group.add_argument('--validate-id', type=str, metavar='UID', help='Validate specific UniqueID')
    validate_parser.add_argument('--db', choices=GconfigEM.DEFAULT_DB_NAMES.keys(), default='primary', help='Target DB (default: primary)')

    # --- Updater Command (Unchanged definition) ---
    updater_parser = subparsers.add_parser('updater', help='(Info) Explains manual update'); updater_parser.set_defaults(func=handle_updater_command)
    updater_parser.add_argument('--update-id', type=str, metavar='UID', help='(Info only)')

    # --- Build Combo DB Command (Unchanged definition) ---
    combo_parser = subparsers.add_parser('build-combo-db', help='Rebuild Combo DB'); combo_parser.set_defaults(func=handle_build_combo_db)

    # --- NEW: Merge DB Command ---
    merge_parser = subparsers.add_parser('merge-db', help='Merge data from an older DB file into a target DB.')
    merge_parser.add_argument('--source', type=str, required=True, metavar='PATH', help='Path to the source (older) database file.')
    merge_parser.add_argument('--target-db', choices=GconfigEM.DEFAULT_DB_NAMES.keys(), default='primary', help='Target DB key (primary, backup, test; default: primary).')
    merge_parser.add_argument('--source-version', type=int, required=True, metavar='VER', help='Schema version number of the source DB (e.g., 1, 2).')
    merge_parser.set_defaults(func=handle_merge_db)

    # --- Exit Command (Unchanged definition) ---
    exit_parser = subparsers.add_parser('exit', help='Stop application'); exit_parser.set_defaults(func=handle_exit_command)

    # --- Parse Arguments and Execute ---
    try: args = parser.parse_args(); args.func(args)
    except Exception as e: log.critical(f"Arg parse/command error: {e}", exc_info=True); print(f"Error: {e}"); sys.exit(1)

# === End of GcliEM.py ===