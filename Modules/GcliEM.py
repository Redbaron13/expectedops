# GcliEM.py
# V9: Adapted for Supabase, added calendar processing command.
"""
Handles CLI commands for the ExpectedOps tool using Supabase backend.
- `run`: Starts scheduler (default) or forces opinion scraping.
- `process-calendars`: Parses PDFs from a folder and saves to Supabase.
- `status`: Shows application status.
- `configure`: Configures non-sensitive settings (schedule, logging).
- `reset-counter`: Resets run counter.
- `validate`: Lists or validates opinion entries in Supabase.
- `supreme`: Test Supreme Court docket search (DB saving needs review).
- `exit`: Stops the application.

Removed SQLite-specific commands: `build-combo-db`, `merge-db`.
Removed `updater` command (needs rethink for Supabase).
"""
import argparse
import logging
import time
import threading
import sys
import os
import re
import shutil # Keep for potential future file ops
import datetime
from queue import Queue, Empty

# Import project modules
import GscraperEM
import GdbEM # Supabase version
import GstatusEM # Supabase version
import GschedulerEM
import GconfigEM # Supabase version
import GvalidatorEM # Needs Supabase update
import GsupremetestEM # Needs Supabase update if saving
import GcalendarParserEM # New
import GcalendarDbEM # New

log = logging.getLogger(__name__)

# --- Input Handling Helpers (Unchanged) ---
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
    t.join(timeout_seconds)
    if t.is_alive():
        print("\nTimeout waiting for input.")
        return None
    else:
        try:
            data = q.get_nowait()
            return data.strip() if data is not None else None
        except Empty:
            log.warning("Input thread finished but queue was empty.")
            return None

# --- Command Handler Functions ---

def handle_run_command(args):
    """Handles the 'run' command (Scheduler or Forced Opinion Scrape)."""
    log.info(f"Handling 'run' command. Force: {args.force}")

    # --- Check Supabase Connectivity ---
    try:
        log.info("Checking Supabase connection...")
        client = GdbEM.get_supabase_client()
        if not client:
            print("Error: Cannot connect to Supabase. Check credentials and network.")
            log.critical("Aborting run command due to Supabase connection failure.")
            return
        log.info("Supabase connection check passed.")
    except Exception as e:
        print(f"Error: Failed to initialize Supabase client: {e}")
        log.critical(f"Aborting run command due to Supabase initialization error: {e}", exc_info=True)
        return

    if not args.force:
        # Start Scheduler
        log.info("No force flag. Starting scheduler loop...")
        # Initialization is handled within scheduler loop now potentially
        GschedulerEM.start_schedule_loop()
        return

    # --- Forced Run Logic (Opinion Scrape) ---
    try:
        log.info("Force flag set. Running immediate primary opinion scrape.")
        print("--- Starting Forced Primary Opinion Run ---")
        run_type_force = 'manual-primary-force'

        print(f"Scraping {GscraperEM.PAGE_URL}...")
        scraped_opinions, release_date = GscraperEM.fetch_and_parse_opinions()
        if not scraped_opinions:
            print("No opinions found during scrape.")
            log.info("Forced run: No opinions found.")
            return

        # --- Display Scraped Data (Detailed) ---
        print(f"\n--- Scraped Opinion Data ({len(scraped_opinions)} entries) ---")
        display_fields = [ # Adjusted field names slightly if needed
            ('ReleaseDate', '[ReleaseDate] Opinion Release Date'),
            ('opinionstatus', '[OpinionStatus] Opinion Status'), # 0=Expected, 1=Released
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
            ('caseconsolidated', '[caseconsolidated] Consolidated Matter'), # Boolean
            ('recordimpounded', '[recordimpounded] Record Impounded'), # Boolean
        ]
        for i, opinion in enumerate(scraped_opinions):
            print(f"\n--- Scraped Entry #{i+1} ---")
            for db_col, label in display_fields:
                value = opinion.get(db_col)
                display_value = "N/A"
                if value is not None:
                    if db_col in ['caseconsolidated', 'recordimpounded']: display_value = str(bool(value))
                    elif db_col == 'opinionstatus': display_value = "Released" if int(value) == 1 else "Expected"
                    else: display_value = str(value)
                print(f"  {label}: {display_value}")
            print("-" * 30)
        print("--- End of Scraped Opinion Data ---")

        # --- Validation Prompt ---
        validation_timeout = 180
        prompt_msg = f"\nValidate {len(scraped_opinions)} entries? (y=Yes, n=No/Discard, s/Enter=Skip) [{validation_timeout}s timeout]: "
        user_response = prompt_with_timeout(prompt_msg, validation_timeout)
        is_validated, proceed_to_save = False, True

        if user_response is None or user_response.lower() == 's' or user_response == '':
            print("\nSkipping validation. Data will be saved as unvalidated.")
            log.info("Validation skipped/timeout during forced run.")
        elif user_response.lower() == 'y':
            print("Marked as validated.")
            log.info("User confirmed data is correct (validated) during forced run.")
            is_validated = True
        elif user_response.lower() == 'n':
            print("Discarding scraped data.")
            log.info("User discarded scraped data during forced run.")
            proceed_to_save = False
        else:
            print("Invalid input. Skipping validation. Data will be saved as unvalidated.")
            log.warning(f"Invalid validation input '{user_response}' during forced run.")

        # --- Save to Supabase ---
        if proceed_to_save:
            print("\nSaving opinions to Supabase ('opinions' and 'opinion_history')...")
            save_results = GdbEM.save_opinions_to_db(scraped_opinions, is_validated, run_type_force)

            print("\nDatabase processing complete:")
            print(f"  Opinions Processed: {save_results.get('processed', 0)}")
            print(f"  Upserted to 'opinions': {save_results.get('upserted', 0)}")
            print(f"  Errors during upsert: {save_results.get('error', 0)}")
            print(f"  Saved to 'opinion_history': {save_results.get('history_saved', 0)}")
            print(f"  Errors during history save: {save_results.get('history_error', 0)}")
        else:
            print("Data discarded.")

        print("\nForced primary opinion run finished.")
        log.info("Forced primary opinion run finished.")

    except Exception as e:
        log.critical(f"Unexpected error during forced run: {e}", exc_info=True)
        print(f"An unexpected error occurred during the forced run: {e}")


def handle_process_calendars_command(args):
    """Handles parsing calendar PDFs and saving to Supabase."""
    log.info("Handling 'process-calendars' command.")
    input_folder = args.folder

    if not os.path.isdir(input_folder):
        print(f"Error: Input folder not found or is not a directory: {input_folder}")
        log.error(f"Calendar processing failed: Invalid input folder '{input_folder}'.")
        return

    # --- Check Supabase Connectivity ---
    try:
        log.info("Checking Supabase connection...")
        client = GdbEM.get_supabase_client()
        if not client:
            print("Error: Cannot connect to Supabase. Check credentials and network.")
            log.critical("Aborting calendar processing due to Supabase connection failure.")
            return
        log.info("Supabase connection check passed.")
    except Exception as e:
        print(f"Error: Failed to initialize Supabase client: {e}")
        log.critical(f"Aborting calendar processing due to Supabase initialization error: {e}", exc_info=True)
        return

    output_folder = os.path.join(input_folder, "processed_calendars") # Store processed PDFs in subfolder
    os.makedirs(output_folder, exist_ok=True)

    pdf_files = [f for f in os.listdir(input_folder) if f.lower().endswith('.pdf') and os.path.isfile(os.path.join(input_folder, f))]

    if not pdf_files:
        print(f"No PDF files found in folder: {input_folder}")
        log.info("No PDF calendars found to process.")
        return

    print(f"Found {len(pdf_files)} PDF files to process in '{input_folder}'.")
    total_entries_saved = 0
    total_errors = 0
    processed_files = 0
    failed_files = 0

    for pdf_file in pdf_files:
        full_path = os.path.join(input_folder, pdf_file)
        print(f"\n--- Processing {pdf_file} ---")
        try:
            # Parse the PDF
            parsed_cases, new_filename = GcalendarParserEM.parse_calendar_pdf(full_path)

            if parsed_cases is None:
                log.error(f"Failed to parse {pdf_file}. Skipping.")
                failed_files += 1
                continue

            if not parsed_cases:
                log.info(f"No case entries extracted from {pdf_file}. Skipping database save.")
                # Move file even if no entries extracted, assuming it was processed
                new_full_path = os.path.join(output_folder, new_filename if new_filename else pdf_file + ".empty")
                try:
                    shutil.move(full_path, new_full_path)
                    log.info(f"Moved empty/processed file {pdf_file} to {output_folder}")
                except Exception as move_err:
                    log.error(f"Error moving file {pdf_file} after empty parse: {move_err}")
                processed_files += 1
                continue

            # Save extracted data to Supabase
            print(f"  Extracted {len(parsed_cases)} entries. Saving to Supabase...")
            save_results = GcalendarDbEM.save_calendar_entries(parsed_cases)

            total_entries_saved += save_results.get('inserted', 0)
            total_errors += save_results.get('error', 0)

            # Rename and move the processed file
            if new_filename:
                new_full_path = os.path.join(output_folder, new_filename)
                try:
                    shutil.move(full_path, new_full_path) # Use move instead of rename
                    print(f"  Successfully processed and moved to {new_filename}")
                    log.info(f"Processed and moved '{pdf_file}' to '{new_filename}' in {output_folder}.")
                    processed_files += 1
                except Exception as move_err:
                    log.error(f"Error moving processed file {pdf_file} to {new_full_path}: {move_err}")
                    failed_files += 1 # Count as failure if move fails
            else:
                log.error(f"Could not determine new filename for {pdf_file}. File not moved.")
                failed_files += 1

        except Exception as e:
            log.error(f"Critical error processing file {pdf_file}: {e}", exc_info=True)
            print(f"  Error processing file: {e}")
            failed_files += 1

    print("\n--- Calendar Processing Summary ---")
    print(f"  PDF Files Processed: {processed_files}")
    print(f"  PDF Files Failed:    {failed_files}")
    print(f"  Total Entries Saved: {total_entries_saved}")
    print(f"  Total DB Errors:     {total_errors}")
    log.info(f"Calendar processing finished. Files Processed: {processed_files}, Failed: {failed_files}, Entries Saved: {total_entries_saved}, DB Errors: {total_errors}")


def handle_status_command(args):
    """Handles the 'status' command."""
    log.info("Handling 'status' command.")
    GstatusEM.display_status() # Assumes GstatusEM uses Supabase now

def handle_validate_command(args):
    """Handles the 'validate' command for listing or validating opinion entries."""
    log.info(f"Handling 'validate' command: {args}")
    action_taken = False

    # --- Check Supabase Connectivity ---
    try:
        client = GdbEM.get_supabase_client()
        if not client:
            print("Error: Cannot connect to Supabase. Check credentials and network.")
            return
    except Exception as e:
        print(f"Error: Failed to initialize Supabase client: {e}")
        return

    if args.list_unvalidated:
        print("Listing unvalidated opinion entries from Supabase...")
        # GvalidatorEM.list_entries needs update for Supabase
        GvalidatorEM.list_entries_supabase(list_type="unvalidated")
        action_taken = True
    if args.list_missing_lc:
        print("Listing unvalidated opinions potentially missing LC Docket ID...")
        # GvalidatorEM.list_entries needs update for Supabase
        GvalidatorEM.list_entries_supabase(list_type="missing_lc_docket")
        action_taken = True
    if args.validate_id:
         print(f"Starting interactive validation for Opinion UniqueID: {args.validate_id}...")
         # GvalidatorEM.validate_case needs update for Supabase
         GvalidatorEM.validate_case_supabase(args.validate_id)
         action_taken = True

    if not action_taken:
        # This part of the parser setup needs adjustment if validate is optional
        print("Error: No action specified for 'validate'. Use --list-unvalidated, --list-missing-lc, or --validate-id <ID>.")
        log.error("Validate command handler reached without required action flag.")


def handle_configure_command(args):
    """Handles the 'configure' command (Schedule, Logging)."""
    log.info(f"Handling 'configure' command: {args}")
    try:
        config = GconfigEM.load_config()
        updated = False

        # Logging Toggle
        if args.toggle_logging is not None:
            # Convert string 'true'/'false' from argparse to boolean
            new_logging_state = str(args.toggle_logging).lower() == 'true'
            if config['logging'] != new_logging_state:
                config['logging'] = new_logging_state
                print(f"Logging set to {config['logging']}.")
                updated = True

        # Schedule modification would be more complex - requires adding/removing entries.
        # Keep it simple for now: only logging toggle via CLI. Schedule managed via config file.
        if args.add_schedule or args.remove_schedule is not None:
             print("Info: Schedule modification via CLI is not currently supported.")
             print("Please edit the 'schedule' section in 'config.json' manually.")
             log.warning("Schedule modification attempted via CLI - not implemented.")

        # Save if changed
        if updated:
            GconfigEM.save_config(config)
            print("Configuration saved.")
        else:
            print("No valid configuration changes provided or applied.")
    except Exception as e:
        log.error(f"Configure error: {e}", exc_info=True)
        print(f"Error during configuration: {e}")

def handle_reset_counter_command(args):
    """Handles the 'reset-counter' command."""
    log.info("Handling 'reset-counter'")
    confirm = prompt_with_timeout("Reset run counter to 0? (y/n): ", 60)
    if confirm and confirm.lower() == 'y':
        GconfigEM.reset_run_counter()
        print("Run counter reset to 0.")
        log.info("Run counter reset by user.")
    else:
        print("Reset cancelled.")
        log.info("Run counter reset cancelled by user.")

def handle_supreme_command(args):
    """Handles the supreme search command."""
    # Note: GsupremetestEM saving to SQLite DB needs removal or update for Supabase
    log.warning("Supreme Court scraper testing initiated. Database saving behavior might be outdated (SQLite).")
    try:
        # The search logic itself might work, but saving needs review
        results = GsupremetestEM.search_supreme_docket(
            args.docket,
            save_results=False # Disable saving for now until updated
        )

        if results:
            print("\nSearch Results (from Web Scrape):")
            print(f"  Supreme Docket: {results.get('sc_docket')}")
            print(f"  Appellate Docket: {results.get('app_docket')}")
            print(f"  Case Caption: {results.get('case_name')}")
            # Add other fields if returned by scraper
        else:
            print(f"\nNo results found via web scrape for docket: {args.docket}")

    except Exception as e:
        log.error(f"Supreme search failed: {e}", exc_info=True)
        print(f"Error during Supreme Court search: {e}")
        # sys.exit(1) # Don't exit the whole app if test fails

def handle_exit_command(args):
    """Handles the 'exit' command."""
    log.info("Handling 'exit'"); print("Exiting..."); sys.exit(0)

# --- Argument Parser Setup ---
def setup_parser():
    """Sets up command line argument parser."""
    parser = argparse.ArgumentParser(
        description='ExpectedOps CLI Tool - Manage Court Opinion and Calendar Data (Supabase Backend)',
        formatter_class=argparse.RawTextHelpFormatter # Preserve formatting in help messages
        )
    subparsers = parser.add_subparsers(dest='command', help='Available commands', required=False) # Make command optional for default run

    # --- Run Command ---
    run_parser = subparsers.add_parser('run', help='Run opinion scraping scheduler (default) or force an immediate scrape.')
    run_parser.add_argument('--force', action='store_true', help='Force immediate opinion scrape and save to Supabase.')
    # run_parser.add_argument('--test', action='store_true', help='Run a test simulation (Obsolete with Supabase focus).') # Removed test flag

    # --- Process Calendars Command ---
    calendar_parser = subparsers.add_parser('process-calendars', help='Parse calendar PDFs from a specified folder and save to Supabase.')
    calendar_parser.add_argument('folder', help='Path to the folder containing calendar PDF files.')

    # --- Status Command ---
    status_parser = subparsers.add_parser('status', help='Display application status, DB stats, and checks.')

    # --- Validate Command ---
    validate_parser = subparsers.add_parser('validate', help='List or interactively validate opinion entries.')
    validate_group = validate_parser.add_mutually_exclusive_group(required=True) # Must choose one action
    validate_group.add_argument('--list-unvalidated', action='store_true', help='List unvalidated opinions.')
    validate_group.add_argument('--list-missing-lc', action='store_true', help='List unvalidated opinions potentially missing LC Docket ID.')
    validate_group.add_argument('--validate-id', metavar='UNIQUE_ID', help='Interactively validate a specific opinion by its UniqueID.')
    # Removed --db flag as Supabase is the single source now

    # --- Configure Command ---
    config_parser = subparsers.add_parser('configure', help='Configure application settings (Logging).')
    config_parser.add_argument('--toggle-logging', choices=['true', 'false'], help='Enable or disable file logging (true/false).')
    config_parser.add_argument('--add-schedule', help='(Not Implemented) Add schedule entry.', action='store_true')
    config_parser.add_argument('--remove-schedule', type=int, help='(Not Implemented) Remove schedule entry by index.')

    # --- Reset Counter Command ---
    reset_parser = subparsers.add_parser('reset-counter', help='Reset the run counter in config.json to 0.')

    # --- Supreme Command ---
    supreme_parser = subparsers.add_parser('supreme', help='Test Supreme Court docket web search (DB saving disabled).')
    supreme_parser.add_argument('docket', help='Supreme Court docket number (e.g., A-XX-YY format).')
    # supreme_parser.add_argument('--no-save', action='store_true', help='(Defaulted) Do not save results to database.') # Keep flag for consistency?

    # --- Exit Command ---
    exit_parser = subparsers.add_parser('exit', help='Exit the application.')

    return parser

# --- Main Execution ---
def main():
    """Main entry point for CLI."""
    # Setup logging first
    try:
        # Load config minimally to check logging setting
        temp_config = GconfigEM.load_config()
        log_enabled = temp_config.get('logging', True)
        # Setup logger (module GloggerEM needs to exist)
        import GloggerEM
        GloggerEM.setup_logging() # Setup based on config
        log.info("--- CLI Application Started ---")
    except Exception as log_e:
        # Fallback basic logging if setup fails
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        log.error(f"Initial logging setup failed: {log_e}", exc_info=True)
        print(f"Warning: Logging setup failed: {log_e}")

    try:
        parser = setup_parser()
        args = parser.parse_args()

        # Default action: Run scheduler if no command is given
        if args.command is None:
            log.info("No command specified, defaulting to 'run' (scheduler mode).")
            # Simulate args for the run command without force
            class RunArgs: force = False
            handle_run_command(RunArgs())
            return

        # --- Command Dispatch ---
        if args.command == 'run': handle_run_command(args)
        elif args.command == 'process-calendars': handle_process_calendars_command(args)
        elif args.command == 'status': handle_status_command(args)
        elif args.command == 'validate': handle_validate_command(args)
        elif args.command == 'configure': handle_configure_command(args)
        elif args.command == 'reset-counter': handle_reset_counter_command(args)
        elif args.command == 'supreme': handle_supreme_command(args)
        elif args.command == 'exit': handle_exit_command(args)
        else: # Should not happen if command is required, but good fallback
            parser.print_help()

    except Exception as e:
        log.critical(f"Unhandled error in CLI main: {e}", exc_info=True)
        print(f"\nAn unexpected critical error occurred: {e}")
        sys.exit(1)
    finally:
        log.info("--- CLI Application Finished ---")


if __name__ == "__main__":
    main()

# === End of GcliEM.py ===
