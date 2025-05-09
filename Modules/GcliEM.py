# Modules/GcliEM.py
# V10: Added archive-html command and --source-file option for run --force.
"""
Handles CLI commands for the ExpectedOps tool using Supabase backend.
- `run`: Starts scheduler (default) or forces opinion scraping.
- `process-calendars`: Parses PDFs from a folder and saves to Supabase.
- `archive-html`: Fetches and saves HTML from configured court pages.
- `status`: Shows application status.
- `configure`: Configures non-sensitive settings (schedule, logging).
- `reset-counter`: Resets run counter.
- `validate`: Lists or validates opinion entries in Supabase.
- `supreme`: Test Supreme Court docket search (DB saving needs review).
- `exit`: Stops the application.
"""
import argparse
import logging
# import time # Not directly used in this version of GcliEM main logic
# import threading # Not directly used
import sys
import os
# import re # Not directly used
import shutil
import datetime
from queue import Queue, Empty # Kept if prompt_with_timeout is still needed somewhere else

# Import project modules
import GscraperEM
import GdbEM
import GstatusEM
import GschedulerEM
import GconfigEM
import GvalidatorEM
import GsupremetestEM
import GcalendarParserEM
import GcalendarDbEM
import GhtmlArchiverEM # New import

log = logging.getLogger(__name__)

# --- Input Handling Helpers (Keep if used, e.g., by GvalidatorEM indirectly or for other prompts) ---
def get_input_with_timeout(prompt, timeout, input_queue):
    """Target function for input thread."""
    try:
        print(prompt, end='', flush=True) # Ensure prompt is displayed before input()
        user_input = input()
        input_queue.put(user_input)
    except EOFError:
        input_queue.put(None)
    except Exception as e:
        log.error(f"Input error in get_input_with_timeout: {e}", exc_info=True)
        input_queue.put(None)

def prompt_with_timeout(prompt_text, timeout_seconds=300):
    """Prompts user for input with a timeout."""
    # This function relies on threading, which might be complex for simple CLI.
    # Consider replacing with simpler input() if timeouts are not strictly needed for all prompts.
    # For now, keeping it as it was in your provided code.
    # If not using threading, ensure 'threading' is removed from imports.
    import threading # Moved import here to be conditional
    q = Queue()
    # Use a try-except for thread creation if issues arise in some environments
    try:
        t = threading.Thread(target=get_input_with_timeout, args=(prompt_text, timeout_seconds, q), daemon=True)
        t.start()
        t.join(timeout_seconds)
        if t.is_alive():
            print("\nTimeout waiting for input.")
            # Attempt to interrupt the input() call if possible (platform dependent)
            # This is hard to do reliably across platforms for console input.
            # The daemon thread will exit when main thread exits.
            return None
        else:
            try:
                data = q.get_nowait()
                return data.strip() if data is not None else None
            except Empty:
                log.warning("Input thread finished for prompt_with_timeout but queue was empty.")
                return None
    except RuntimeError as e:
        log.error(f"RuntimeError creating input thread: {e}. Falling back to standard input without timeout.")
        print(f"(Timeout feature for input failed: {e})")
        print(prompt_text, end='', flush=True)
        try:
            return input().strip()
        except EOFError:
            return None


# --- Command Handler Functions ---

def handle_run_command(args):
    log.info(f"Handling 'run' command. Force: {args.force}, Source File: {args.source_file}")
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
        log.info("No force flag. Starting scheduler loop...")
        GschedulerEM.start_schedule_loop() # Assumes GschedulerEM handles its own DB init checks
        return

    # --- Forced Run Logic (Opinion Scrape) ---
    try:
        log.info("Force flag set. Running immediate primary opinion scrape.")
        print("--- Starting Forced Primary Opinion Run ---")
        run_type_force = 'manual-primary-force' # Consistent run type

        source_to_scrape = GscraperEM.DEFAULT_SCRAPE_SOURCE # Default to live URL
        if args.source_file:
            if os.path.exists(args.source_file) and os.path.isfile(args.source_file):
                source_to_scrape = args.source_file
                print(f"Scraping from local HTML file: {source_to_scrape}...")
            else:
                print(f"Warning: Source file '{args.source_file}' not found or not a file. Defaulting to live URL: {GscraperEM.DEFAULT_SCRAPE_SOURCE}")
                log.warning(f"Source file '{args.source_file}' for forced run not found/invalid. Using live URL.")
                # source_to_scrape remains DEFAULT_SCRAPE_SOURCE
        else:
            print(f"Scraping from live URL: {source_to_scrape}...")

        scraped_opinions, release_date = GscraperEM.fetch_and_parse_opinions(url_or_file_path=source_to_scrape)
        if not scraped_opinions:
            print("No opinions found during scrape.")
            log.info("Forced run: No opinions found.")
            return

        print(f"\n--- Scraped Opinion Data ({len(scraped_opinions)} entries) ---")
        # (Display logic remains the same as your GcliEM.py)
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


        validation_timeout = 180 # seconds
        prompt_msg = f"\nValidate {len(scraped_opinions)} entries? (y=Yes, n=No/Discard, s/Enter=Skip validation) [{validation_timeout}s timeout]: "
        user_response = prompt_with_timeout(prompt_msg, validation_timeout)
        is_validated, proceed_to_save = False, True

        if user_response is None or user_response.lower() == 's' or user_response == '':
            print("\nSkipping validation. Data will be saved as unvalidated.")
            log.info("Validation skipped or timed out by user during forced run.")
        elif user_response.lower() == 'y':
            print("Data will be marked as validated.")
            log.info("User confirmed data is correct (validated) during forced run.")
            is_validated = True
        elif user_response.lower() == 'n':
            print("Discarding scraped data. No changes will be saved.")
            log.info("User chose to discard scraped data during forced run.")
            proceed_to_save = False
        else:
            print("Invalid input. Skipping validation. Data will be saved as unvalidated by default.")
            log.warning(f"Invalid validation input '{user_response}' during forced run. Defaulting to unvalidated save.")

        if proceed_to_save:
            print("\nSaving opinions to Supabase ('opinions' and 'opinion_history')...")
            # GdbEM.save_opinions_to_db is expected to handle Supabase
            save_results = GdbEM.save_opinions_to_db(scraped_opinions, is_validated, run_type_force)
            print("\nDatabase processing complete:")
            print(f"  Opinions Processed by GdbEM: {save_results.get('processed', 0)}") # Renamed for clarity
            print(f"  Upserted to 'opinions' table: {save_results.get('upserted', 0)}")
            print(f"  Errors during 'opinions' upsert: {save_results.get('error', 0)}")
            print(f"  Saved to 'opinion_history': {save_results.get('history_saved', 0)}")
            print(f"  Errors during 'opinion_history' save: {save_results.get('history_error', 0)}")
        else:
            print("Data discarded as per user instruction.")

        print("\nForced primary opinion run finished.")
        log.info("Forced primary opinion run finished.")

    except Exception as e:
        log.critical(f"Unexpected error during forced run: {e}", exc_info=True)
        print(f"An unexpected error occurred during the forced run: {e}")

def handle_process_calendars_command(args):
    # (This function remains largely the same as your GcliEM.py version,
    # as it interacts with GcalendarParserEM and GcalendarDbEM which are assumed
    # to be correctly set up for their respective tasks. shutil is used here.)
    log.info("Handling 'process-calendars' command.")
    input_folder = args.folder

    if not os.path.isdir(input_folder):
        print(f"Error: Input folder not found or is not a directory: {input_folder}")
        log.error(f"Calendar processing failed: Invalid input folder '{input_folder}'.")
        return

    try:
        log.info("Checking Supabase connection for calendar processing...")
        client = GdbEM.get_supabase_client()
        if not client:
            print("Error: Cannot connect to Supabase for calendar processing. Check credentials and network.")
            log.critical("Aborting calendar processing due to Supabase connection failure.")
            return
        log.info("Supabase connection check passed for calendar processing.")
    except Exception as e:
        print(f"Error: Failed to initialize Supabase client for calendar processing: {e}")
        log.critical(f"Aborting calendar processing due to Supabase initialization error: {e}", exc_info=True)
        return

    output_folder = os.path.join(input_folder, "processed_calendars")
    os.makedirs(output_folder, exist_ok=True)

    pdf_files = [f for f in os.listdir(input_folder) if f.lower().endswith('.pdf') and os.path.isfile(os.path.join(input_folder, f))]

    if not pdf_files:
        print(f"No PDF files found in folder: {input_folder}")
        log.info("No PDF calendars found to process.")
        return

    print(f"Found {len(pdf_files)} PDF files to process in '{input_folder}'.")
    total_entries_saved = 0
    total_db_errors = 0 # Renamed for clarity
    processed_files_count = 0 # Renamed for clarity
    failed_files_count = 0 # Renamed for clarity

    for pdf_file in pdf_files:
        full_path = os.path.join(input_folder, pdf_file)
        print(f"\n--- Processing {pdf_file} ---")
        try:
            parsed_cases, new_filename = GcalendarParserEM.parse_calendar_pdf(full_path)

            if parsed_cases is None: # Indicates a critical parsing failure
                log.error(f"Failed to parse {pdf_file} (parser returned None). Skipping.")
                failed_files_count += 1
                continue

            if not parsed_cases: # Parsed successfully, but no entries found
                log.info(f"No case entries extracted from {pdf_file}. Moving file.")
                # Attempt to move even if empty, using original or a modified name
                processed_filename_for_move = new_filename if new_filename else pdf_file + ".empty_or_processed"
                new_full_path = os.path.join(output_folder, processed_filename_for_move)
                try:
                    shutil.move(full_path, new_full_path)
                    log.info(f"Moved empty/processed file {pdf_file} to {new_full_path}")
                except Exception as move_err:
                    log.error(f"Error moving file {pdf_file} after empty parse: {move_err}")
                processed_files_count += 1 # Count as processed as parsing was attempted
                continue

            print(f"  Extracted {len(parsed_cases)} entries. Saving to Supabase...")
            save_results = GcalendarDbEM.save_calendar_entries(parsed_cases) # Assumed to use Supabase

            total_entries_saved += save_results.get('inserted', 0)
            total_db_errors += save_results.get('error', 0) # Accumulate DB errors

            if new_filename: # If parser provided a new filename (e.g., date-based)
                new_full_path = os.path.join(output_folder, new_filename)
                try:
                    shutil.move(full_path, new_full_path)
                    print(f"  Successfully processed and moved to {new_filename}")
                    log.info(f"Processed and moved '{pdf_file}' to '{new_filename}' in {output_folder}.")
                    processed_files_count += 1
                except Exception as move_err:
                    log.error(f"Error moving processed file {pdf_file} to {new_full_path}: {move_err}")
                    failed_files_count += 1 # Count as failure if move fails post-processing
            else: # Should ideally always have a new_filename or handle this case better
                log.warning(f"Could not determine new filename for {pdf_file} from parser. File not moved from source.")
                # Consider if this is a processed or failed file. Let's say processed if data was saved.
                if save_results.get('inserted', 0) > 0 : processed_files_count += 1
                else: failed_files_count +=1
        
        except Exception as e:
            log.error(f"Critical error processing file {pdf_file}: {e}", exc_info=True)
            print(f"  Error processing file: {e}")
            failed_files_count += 1

    print("\n--- Calendar Processing Summary ---")
    print(f"  PDF Files Attempted: {len(pdf_files)}")
    print(f"  PDF Files Processed (moved/analyzed): {processed_files_count}")
    print(f"  PDF Files Failed Processing:    {failed_files_count}")
    print(f"  Total Calendar Entries Saved to DB: {total_entries_saved}")
    print(f"  Total Database Errors During Save:     {total_db_errors}")
    log.info(f"Calendar processing finished. Attempted: {len(pdf_files)}, Processed: {processed_files_count}, Failed: {failed_files_count}, Entries Saved: {total_entries_saved}, DB Errors: {total_db_errors}")

def handle_archive_html_command(args):
    """Handles the 'archive-html' command."""
    log.info("Handling 'archive-html' command.")
    # Ensure Supabase client can be initialized, e.g. for config that might depend on it
    try:
        GdbEM.get_supabase_client() 
    except Exception as e:
        print(f"Warning: Could not initialize Supabase client (may affect config loading if it depends on Supabase): {e}")
        # Proceeding as HTML archival might not directly need DB for its own operation, only for config.

    if args.page_key:
        pages_config = GconfigEM.get_archive_pages_config()
        page_to_archive = next((p for p in pages_config if p["key"] == args.page_key), None)
        if page_to_archive:
            print(f"Archiving specific page: {args.page_key}")
            GhtmlArchiverEM.fetch_and_save_html(page_to_archive)
        else:
            print(f"Error: Page key '{args.page_key}' not found in configuration.")
            log.error(f"Page key '{args.page_key}' for archival not found in config.")
    else:
        print("Archiving all configured HTML pages...")
        GhtmlArchiverEM.archive_all_configured_pages()

def handle_status_command(args):
    log.info("Handling 'status' command.")
    GstatusEM.display_status()

def handle_validate_command(args):
    log.info(f"Handling 'validate' command with args: {args}")
    action_taken = False
    try:
        client = GdbEM.get_supabase_client()
        if not client:
            print("Error: Cannot connect to Supabase for validation. Check credentials and network.")
            return
    except Exception as e:
        print(f"Error: Failed to initialize Supabase client for validation: {e}")
        return

    if args.list_unvalidated:
        print("Listing unvalidated opinion entries from Supabase...")
        GvalidatorEM.list_entries_supabase(list_type="unvalidated")
        action_taken = True
    if args.list_missing_lc: # This was part of a mutually exclusive group, check logic
        print("Listing unvalidated opinions potentially missing LC Docket ID...")
        GvalidatorEM.list_entries_supabase(list_type="missing_lc_docket")
        action_taken = True
    if args.validate_id:
         print(f"Starting interactive validation for Opinion UniqueID: {args.validate_id}...")
         GvalidatorEM.validate_case_supabase(args.validate_id)
         action_taken = True

    if not action_taken and not (args.list_unvalidated or args.list_missing_lc or args.validate_id):
        # This condition might be redundant if argparse `required=True` is on the group
        print("Error: No action specified for 'validate'. Use --list-unvalidated, --list-missing-lc, or --validate-id <ID>.")
        log.error("Validate command handler reached without required action flag from parser.")

def handle_configure_command(args):
    log.info(f"Handling 'configure' command: {args}")
    try:
        config = GconfigEM.load_config() # Loads current or default config
        updated = False

        if args.toggle_logging is not None: # Check if the argument was provided
            new_logging_state = str(args.toggle_logging).lower() == 'true'
            if config.get('logging') != new_logging_state:
                config['logging'] = new_logging_state
                print(f"Logging set to {config['logging']}.")
                updated = True
            else:
                print(f"Logging is already set to {new_logging_state}.")
        
        # Add schedule modification logic here if desired in future
        if args.add_schedule or args.remove_schedule is not None:
             print("Info: Schedule modification via CLI for 'add_schedule' or 'remove_schedule' is not currently implemented.")
             print("Please edit the 'schedule' section in 'config.json' manually if needed.")
             log.warning("Schedule modification (add/remove) attempted via CLI - not implemented.")

        if updated:
            GconfigEM.save_config(config)
            print("Configuration saved.")
            # Re-initialize logging if it was changed
            if 'logging' in config: # Check if logging key was actually changed
                print("Re-initializing logging with new setting...")
                import GloggerEM # Assuming GloggerEM.setup_logging() reads the config
                GloggerEM.setup_logging()
        else:
            print("No valid configuration changes were specified or applied.")
            
    except Exception as e:
        log.error(f"Error during 'configure' command: {e}", exc_info=True)
        print(f"Error during configuration: {e}")


def handle_reset_counter_command(args):
    log.info("Handling 'reset-counter'")
    confirm = prompt_with_timeout("Are you sure you want to reset the run counter to 0? (y/n): ", 60)
    if confirm and confirm.lower() == 'y':
        GconfigEM.reset_run_counter()
        print("Run counter has been reset to 0.")
        log.info("Run counter reset to 0 by user.")
    else:
        print("Run counter reset cancelled.")
        log.info("Run counter reset cancelled by user.")

def handle_supreme_command(args):
    log.warning("Supreme Court scraper testing ('supreme' command) initiated. Database saving from this test is currently disabled/needs review for Supabase.")
    try:
        # GsupremetestEM.search_supreme_docket is expected to handle its own logic
        results = GsupremetestEM.search_supreme_docket(
            args.docket,
            save_results=False # Explicitly disable saving for now until GsupremetestEM is updated for Supabase
        )
        if results:
            print("\nSupreme Search Results (from Web Scrape - not saved to DB by this test command):")
            print(f"  Supreme Docket Searched: {args.docket}")
            print(f"  Found SC Docket in results: {results.get('sc_docket')}")
            print(f"  Found Appellate Docket: {results.get('app_docket')}")
            print(f"  Found Case Caption: {results.get('case_name')}")
            # Add other relevant fields if GsupremetestEM.search_supreme_docket returns them
        else:
            print(f"\nNo results found via web scrape for Supreme Court docket: {args.docket}")

    except Exception as e:
        log.error(f"Supreme Court search command failed: {e}", exc_info=True)
        print(f"Error during Supreme Court search: {e}")


def handle_exit_command(args):
    log.info("Handling 'exit' command. Application will terminate.")
    print("Exiting application...")
    sys.exit(0)

# --- Argument Parser Setup ---
def setup_parser():
    parser = argparse.ArgumentParser(
        description='ExpectedOps CLI Tool - Manage Court Opinion and Calendar Data (Supabase Backend)',
        formatter_class=argparse.RawTextHelpFormatter
    )
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    # Make 'command' optional to default to 'run' (scheduler) if no command is given
    # subparsers.required = False # Handled in main() by checking if args.command is None

    # --- Run Command ---
    run_parser = subparsers.add_parser('run', help='Run opinion scraping scheduler (default if no command) or force an immediate scrape.')
    run_parser.add_argument('--force', action='store_true', help='Force immediate opinion scrape using the default or specified source.')
    run_parser.add_argument(
        '--source-file', 
        metavar='FILEPATH', 
        help='(Used with --force) Use a local HTML file as the source for opinion scraping instead of fetching the live URL.'
    )
    run_parser.set_defaults(func=handle_run_command)

    # --- Process Calendars Command ---
    calendar_parser = subparsers.add_parser('process-calendars', help='Parse calendar PDFs from a specified folder and save to Supabase.')
    calendar_parser.add_argument('folder', help='Path to the folder containing calendar PDF files to process.')
    calendar_parser.set_defaults(func=handle_process_calendars_command)

    # --- Archive HTML Command ---
    archive_parser = subparsers.add_parser('archive-html', help='Fetch and save HTML content from configured court pages.')
    archive_parser.add_argument(
        '--page-key', 
        metavar='KEY',
        type=str, # Explicitly type
        help='Archive only a specific page by its key (e.g., "expected_opinions" from config). Archives all configured pages if not specified.'
    )
    archive_parser.set_defaults(func=handle_archive_html_command)

    # --- Status Command ---
    status_parser = subparsers.add_parser('status', help='Display application status, DB stats, and checks.')
    status_parser.set_defaults(func=handle_status_command)

    # --- Validate Command ---
    validate_parser = subparsers.add_parser('validate', help='List or interactively validate opinion entries from Supabase.')
    validate_action_group = validate_parser.add_mutually_exclusive_group(required=True)
    validate_action_group.add_argument('--list-unvalidated', action='store_true', help='List unvalidated opinions.')
    validate_action_group.add_argument('--list-missing-lc', action='store_true', help='List unvalidated opinions potentially missing LC Docket ID.')
    validate_action_group.add_argument('--validate-id', metavar='UNIQUE_ID', type=str, help='Interactively validate a specific opinion by its UniqueID.')
    validate_parser.set_defaults(func=handle_validate_command)

    # --- Configure Command ---
    config_parser = subparsers.add_parser('configure', help='Configure application settings (e.g., logging).')
    config_parser.add_argument(
        '--toggle-logging', 
        choices=['true', 'false'], 
        type=str.lower, # Ensure input is lowercase for comparison
        help='Enable or disable file logging (true/false).'
    )
    # Placeholders for future schedule modifications via CLI if needed
    config_parser.add_argument('--add-schedule', help='(Not Implemented) Add a schedule entry.', action='store_true')
    config_parser.add_argument('--remove-schedule', type=int, metavar='INDEX', help='(Not Implemented) Remove a schedule entry by its index.')
    config_parser.set_defaults(func=handle_configure_command)

    # --- Reset Counter Command ---
    reset_parser = subparsers.add_parser('reset-counter', help='Reset the run counter in config.json to 0.')
    reset_parser.set_defaults(func=handle_reset_counter_command)

    # --- Supreme Command ---
    supreme_parser = subparsers.add_parser('supreme', help='Test Supreme Court docket web search (experimental, DB saving disabled).')
    supreme_parser.add_argument('docket', help='Supreme Court docket number to search (e.g., A-XX-YY format).')
    supreme_parser.set_defaults(func=handle_supreme_command)

    # --- Exit Command ---
    exit_parser = subparsers.add_parser('exit', help='Exit the application.')
    exit_parser.set_defaults(func=handle_exit_command)
    
    return parser

# --- Main Execution ---
def main():
    try:
        # Minimal config load just for logging setup if needed by GloggerEM
        # GconfigEM.load_env() # Ensure .env is loaded for Supabase creds if GloggerEM needs them
        # temp_config_for_log = GconfigEM.load_config() # Load once for logging
        import GloggerEM # Assuming GloggerEM uses GconfigEM internally
        GloggerEM.setup_logging() # Setup based on config
        log.info("--- CLI Application Started ---")
    except Exception as log_e:
        logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
        log.error(f"Initial logging setup failed: {log_e}", exc_info=True)
        print(f"Warning: Critical logging setup failed: {log_e}. Using basic logging.")

    try:
        parser = setup_parser()
        # If no arguments are provided, parse_args() will result in args.command being None
        # if subparsers.required is False (or not set, as it defaults to False if no default func).
        # However, if each subparser has set_defaults(func=...), then args.func will exist.
        args = parser.parse_args()

        if hasattr(args, 'func'):
            args.func(args) # Call the function associated with the subparser
        else:
            # Default action: Run scheduler if no command is given
            log.info("No command specified or no handler found, defaulting to 'run' (scheduler mode).")
            # Simulate args for the run command without force or source_file
            class DefaultRunArgs:
                force = False
                source_file = None
            handle_run_command(DefaultRunArgs())
            
    except SystemExit: # Allow sys.exit() to pass through from handlers
        log.info("Application exiting via SystemExit.")
    except KeyboardInterrupt:
        log.info("Application interrupted by user (KeyboardInterrupt).")
        print("\nOperation cancelled by user.")
        sys.exit(130) # Standard exit code for Ctrl+C
    except Exception as e:
        log.critical(f"Unhandled error in CLI main: {e}", exc_info=True)
        print(f"\nAn unexpected critical error occurred: {e}")
        sys.exit(1)
    finally:
        log.info("--- CLI Application Finished ---")

if __name__ == "__main__":
    main()

# === End of GcliEM.py ===