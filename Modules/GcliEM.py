# GcliEM.py
# V9: Adapted for Supabase, added calendar processing command.
"""
Command-Line Interface (CLI) handler for the G-EntryPoint-Manager (GEM) application.

This module is responsible for:
1.  Defining and parsing command-line arguments using the `argparse` module.
2.  Orchestrating application tasks by dispatching commands to their respective
    handler functions (e.g., `handle_run_command`, `handle_process_calendars_command`).
3.  Interacting with other modules (e.g., GscraperEM, GdbEM, GschedulerEM, GconfigEM)
    to perform actions based on parsed CLI arguments.
4.  Providing user feedback and logging information about CLI operations.
5.  Handling user input with timeouts for interactive prompts.

Available commands include running the scheduler, processing PDF calendars,
checking status, configuring settings, validating data, and more.
It defaults to running the scheduler if no command is specified.
"""
import argparse
import logging
import time
import threading
import sys
import os
import re
import shutil # Retained for potential future file operations, e.g., moving processed files.
import datetime
from queue import Queue, Empty # Used for handling input with timeouts.

# Import project-specific modules
import GscraperEM
import GdbEM             # Supabase database interaction module
import GstatusEM         # Application status reporting module (Supabase version)
import GschedulerEM      # Task scheduling module
import GconfigEM         # Configuration management module (Supabase version)
import GvalidatorEM      # Data validation module (needs Supabase update)
import GsupremetestEM    # Supreme Court docket search testing module (needs Supabase update if saving)
import GcalendarParserEM # Module for parsing calendar PDF files
import GcalendarDbEM     # Module for saving calendar entries to Supabase

log = logging.getLogger(__name__) # Standard way to get a logger instance for this module.

# --- Input Handling Helpers (Unchanged from original, but comments added for clarity) ---
def get_input_with_timeout(prompt, timeout, input_queue):
    """
    Target function for a separate thread to get user input.
    Puts the received input (or None on EOF/error) into a queue.

    Args:
        prompt (str): The text to display to the user.
        timeout (int): The timeout duration (not directly used here, but part of the pattern).
        input_queue (Queue): The queue to put the user's input into.
    """
    try:
        print(prompt, end='', flush=True) # Display the prompt without a newline, flush ensure it's visible.
        user_input = input()              # Blocking call to get input from the user.
        input_queue.put(user_input)       # Place the retrieved input onto the queue.
    except EOFError:
        # Handle Ctrl+D (End Of File) gracefully.
        input_queue.put(None)
    except Exception as e:
        # Log any other exceptions during input and signal with None.
        log.error(f"Input error: {e}", exc_info=True)
        input_queue.put(None)

def prompt_with_timeout(prompt_text, timeout_seconds=300):
    """
    Prompts the user for input with a specified timeout.

    This function creates a separate thread to wait for user input, allowing the main
    thread to continue if the input is not received within the timeout period.

    Args:
        prompt_text (str): The message to display to the user.
        timeout_seconds (int, optional): The number of seconds to wait for input.
                                         Defaults to 300 seconds (5 minutes).

    Returns:
        str or None: The user's stripped input if received within the timeout,
                     otherwise None.
    """
    input_queue = Queue() # Create a queue to communicate between threads.
    # Create a daemon thread that will not block program exit.
    input_thread = threading.Thread(target=get_input_with_timeout,
                                   args=(prompt_text, timeout_seconds, input_queue),
                                   daemon=True)
    input_thread.start()    # Start the input thread.
    input_thread.join(timeout_seconds) # Wait for the thread to complete or timeout.

    if input_thread.is_alive():
        # If thread is still alive, it means input timed out.
        print("\nTimeout waiting for input.")
        return None
    else:
        # Thread finished, try to get data from the queue.
        try:
            data = input_queue.get_nowait() # Non-blocking get from queue.
            return data.strip() if data is not None else None # Strip whitespace if data is not None.
        except Empty:
            # This case should ideally not happen if thread finished and put something.
            log.warning("Input thread finished but queue was empty.")
            return None

# --- Command Handler Functions ---

def handle_run_command(args):
    """
    Handles the 'run' command, which either starts the scheduler or forces an
    immediate opinion scrape operation.

    It first checks Supabase connectivity. If the '--force' flag is not used,
    it starts the scheduler loop. If '--force' is used, it performs a
    detailed opinion scrape, prompts for validation, and saves data to Supabase.

    Args:
        args (argparse.Namespace): Parsed arguments from argparse.
                                   Expected to have a `force` boolean attribute.
    """
    log.info(f"Handling 'run' command. Force flag: {args.force}")

    # --- Crucial Step: Check Supabase Connectivity ---
    try:
        log.info("Checking Supabase connection before proceeding with 'run' command...")
        client = GdbEM.get_supabase_client() # Attempt to get a Supabase client instance.
        if not client:
            # This indicates an issue with URL/key in .env or network problem.
            print("Error: Cannot connect to Supabase. Please check credentials in .env and network connectivity.")
            log.critical("Aborting 'run' command due to Supabase connection failure (client is None).")
            return
        log.info("Supabase connection check passed (client obtained).")
    except Exception as e:
        # Catch any other exceptions during Supabase client initialization.
        print(f"Error: Failed to initialize Supabase client: {e}")
        log.critical(f"Aborting 'run' command due to Supabase initialization error: {e}", exc_info=True)
        return

    if not args.force:
        # --- Default Action: Start Scheduler ---
        log.info("No '--force' flag detected. Starting the GschedulerEM task scheduler loop...")
        print("Starting scheduler mode. Press Ctrl+C to exit if running in foreground.")
        GschedulerEM.start_schedule_loop() # Delegate to the scheduler module.
        return

    # --- Forced Run Logic (Immediate Opinion Scrape) ---
    try:
        log.info("Force flag set. Initiating immediate primary opinion scrape.")
        print("--- Starting Forced Primary Opinion Run ---")
        run_type_force = 'manual-primary-force' # Identifier for this type of run.

        # Step 1: Fetch and Parse Opinions
        print(f"Scraping opinions from {GscraperEM.PAGE_URL}...")
        scraped_opinions, release_date = GscraperEM.fetch_and_parse_opinions()

        if not scraped_opinions:
            print("No opinions found during the scrape.")
            log.info("Forced run: No opinions were found or parsed from the website.")
            return

        # Step 2: Display Scraped Data for User Review (if any)
        print(f"\n--- Scraped Opinion Data ({len(scraped_opinions)} entries) ---")
        # Define fields for clear display, mapping database columns to human-readable labels.
        display_fields = [
            ('ReleaseDate', '[ReleaseDate] Opinion Release Date'),
            ('opinionstatus', '[OpinionStatus] Opinion Status (0=Expected, 1=Released)'),
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
            ('caseconsolidated', '[caseconsolidated] Consolidated Matter (boolean)'),
            ('recordimpounded', '[recordimpounded] Record Impounded (boolean)'),
        ]
        for i, opinion in enumerate(scraped_opinions):
            print(f"\n--- Scraped Entry #{i+1} ---")
            for db_col, label in display_fields:
                value = opinion.get(db_col)
                display_value = "N/A" # Default for missing values.
                if value is not None:
                    if db_col in ['caseconsolidated', 'recordimpounded']: display_value = str(bool(value))
                    elif db_col == 'opinionstatus': display_value = "Released" if int(value) == 1 else "Expected"
                    else: display_value = str(value)
                print(f"  {label}: {display_value}")
            print("-" * 30) # Separator for readability.
        print("--- End of Scraped Opinion Data ---")

        # Step 3: User Validation Prompt
        validation_timeout = 180 # 3 minutes for user to respond.
        prompt_msg = (f"\nValidate {len(scraped_opinions)} entries? "
                      f"(y=Yes, n=No/Discard, s/Enter=Skip validation) [{validation_timeout}s timeout]: ")
        user_response = prompt_with_timeout(prompt_msg, validation_timeout)
        is_validated, proceed_to_save = False, True # Default validation status and save action.

        if user_response is None or user_response.lower() == 's' or user_response == '':
            print("\nSkipping validation. Data will be saved as unvalidated.")
            log.info("Validation skipped by user or timed out during forced run.")
            is_validated = False # Ensure it's marked as not validated by user.
        elif user_response.lower() == 'y':
            print("Marked as validated by user.")
            log.info("User confirmed data is correct (validated) during forced run.")
            is_validated = True
        elif user_response.lower() == 'n':
            print("Discarding scraped data as per user request.")
            log.info("User chose to discard scraped data during forced run.")
            proceed_to_save = False
        else:
            print("Invalid input. Skipping validation. Data will be saved as unvalidated.")
            log.warning(f"Invalid validation input '{user_response}' during forced run. Proceeding as unvalidated.")
            is_validated = False

        # Step 4: Save to Supabase (if not discarded)
        if proceed_to_save:
            print("\nSaving opinions to Supabase tables ('opinions' and 'opinion_history')...")
            # Delegate saving to GdbEM module.
            save_results = GdbEM.save_opinions_to_db(scraped_opinions, is_validated, run_type_force)

            # Display results of the save operation.
            print("\nDatabase processing complete:")
            print(f"  Opinions Processed: {save_results.get('processed', 0)}")
            print(f"  Upserted to 'opinions' table: {save_results.get('upserted', 0)}")
            print(f"  Errors during 'opinions' upsert: {save_results.get('error', 0)}")
            print(f"  Saved to 'opinion_history' table: {save_results.get('history_saved', 0)}")
            print(f"  Errors during 'opinion_history' save: {save_results.get('history_error', 0)}")
        else:
            print("Data discarded. No changes made to the database.")

        print("\nForced primary opinion run finished.")
        log.info("Forced primary opinion run has completed.")

    except Exception as e:
        # Catch-all for any unexpected errors during the forced run.
        log.critical(f"Unexpected error during forced opinion scrape: {e}", exc_info=True)
        print(f"An unexpected error occurred during the forced run: {e}")


def handle_process_calendars_command(args):
    """
    Handles the 'process-calendars' command. It processes PDF calendar files
    from a specified input folder, extracts case information using GcalendarParserEM,
    and saves the extracted entries to Supabase using GcalendarDbEM.
    Processed PDFs are moved to a 'processed_calendars' subfolder.

    Args:
        args (argparse.Namespace): Parsed arguments. Expected to have `folder` attribute
                                   specifying the input directory for PDF files.
    """
    log.info(f"Handling 'process-calendars' command for folder: {args.folder}")
    input_folder = args.folder

    # Validate the input folder.
    if not os.path.isdir(input_folder):
        print(f"Error: Input folder not found or is not a directory: {input_folder}")
        log.error(f"Calendar processing failed: Invalid input folder '{input_folder}'.")
        return

    # --- Crucial Step: Check Supabase Connectivity ---
    try:
        log.info("Checking Supabase connection before processing calendars...")
        client = GdbEM.get_supabase_client()
        if not client:
            print("Error: Cannot connect to Supabase. Check credentials and network.")
            log.critical("Aborting calendar processing due to Supabase connection failure.")
            return
        log.info("Supabase connection check passed for calendar processing.")
    except Exception as e:
        print(f"Error: Failed to initialize Supabase client: {e}")
        log.critical(f"Aborting calendar processing due to Supabase initialization error: {e}", exc_info=True)
        return

    # Define an output folder for successfully processed PDFs.
    output_folder = os.path.join(input_folder, "processed_calendars")
    os.makedirs(output_folder, exist_ok=True) # Create if it doesn't exist.

    # Find all PDF files in the input folder.
    pdf_files = [f for f in os.listdir(input_folder)
                 if f.lower().endswith('.pdf') and os.path.isfile(os.path.join(input_folder, f))]

    if not pdf_files:
        print(f"No PDF files found in folder: {input_folder}")
        log.info(f"No PDF calendars found to process in '{input_folder}'.")
        return

    print(f"Found {len(pdf_files)} PDF files to process in '{input_folder}'.")
    # Initialize counters for summary.
    total_entries_saved = 0
    total_db_errors = 0
    processed_file_count = 0
    failed_file_count = 0

    for pdf_file in pdf_files:
        full_pdf_path = os.path.join(input_folder, pdf_file)
        print(f"\n--- Processing PDF: {pdf_file} ---")
        try:
            # Step 1: Parse the PDF file using GcalendarParserEM.
            # This should return extracted cases and a new filename (e.g., with date).
            parsed_cases, new_filename_suggestion = GcalendarParserEM.parse_calendar_pdf(full_pdf_path)

            if parsed_cases is None: # Indicates a critical parsing failure.
                log.error(f"Failed to parse PDF '{pdf_file}'. It may be corrupted or an unexpected format. Skipping.")
                failed_file_count += 1
                continue # Move to the next file.

            # Determine the filename for the moved file.
            # If parser suggests a new name (e.g. with date), use it. Otherwise, use original name.
            final_moved_filename = new_filename_suggestion if new_filename_suggestion else pdf_file
            # Add a suffix if no entries, to distinguish it.
            if not parsed_cases:
                 final_moved_filename += ".empty_or_unparsed"


            if not parsed_cases:
                log.info(f"No case entries were extracted from '{pdf_file}'. Skipping database save for this file.")
            else:
                # Step 2: Save extracted data to Supabase.
                print(f"  Extracted {len(parsed_cases)} entries from '{pdf_file}'. Saving to Supabase...")
                save_results = GcalendarDbEM.save_calendar_entries(parsed_cases)
                total_entries_saved += save_results.get('inserted', 0)
                total_db_errors += save_results.get('error', 0)
                if save_results.get('error', 0) > 0:
                    print(f"  Encountered {save_results.get('error', 0)} errors during database save for '{pdf_file}'.")

            # Step 3: Move the processed (or attempted to process) file.
            destination_path = os.path.join(output_folder, final_moved_filename)
            try:
                shutil.move(full_pdf_path, destination_path)
                message = f"Moved '{pdf_file}' to '{destination_path}'"
                message += " (no entries extracted)." if not parsed_cases else "."
                print(f"  {message}")
                log.info(message)
                processed_file_count += 1
            except Exception as move_err:
                log.error(f"Error moving file '{pdf_file}' to '{destination_path}': {move_err}")
                # If move fails, it's still considered a failed processing for this file.
                # However, data might have been saved. This logic could be refined.
                failed_file_count +=1 # Increment failed if move fails.

        except Exception as e:
            # Catch-all for errors during processing of a single PDF.
            log.error(f"Critical error processing file '{pdf_file}': {e}", exc_info=True)
            print(f"  Error processing file '{pdf_file}': {e}")
            failed_file_count += 1

    # --- Processing Summary ---
    print("\n--- Calendar Processing Summary ---")
    print(f"  PDF Files Attempted: {len(pdf_files)}")
    print(f"  Successfully Processed & Moved: {processed_file_count}")
    print(f"  Failed to Process or Move:    {failed_file_count}")
    print(f"  Total Calendar Entries Saved to DB: {total_entries_saved}")
    print(f"  Total Database Save Errors:     {total_db_errors}")
    log.info(f"Calendar processing finished. Attempted: {len(pdf_files)}, Succeeded: {processed_file_count}, "
             f"Failed: {failed_file_count}, Entries Saved: {total_entries_saved}, DB Errors: {total_db_errors}")


def handle_status_command(args):
    """
    Handles the 'status' command by calling GstatusEM.display_status()
    to show current application status, database statistics, etc.

    Args:
        args (argparse.Namespace): Parsed arguments (not used by this handler).
    """
    log.info("Handling 'status' command.")
    GstatusEM.display_status() # Delegates to GstatusEM module.

def handle_validate_command(args):
    """
    Handles the 'validate' command for listing or interactively validating
    opinion entries stored in Supabase.

    It requires Supabase connectivity and dispatches to functions in GvalidatorEM
    based on the provided sub-arguments (e.g., --list-unvalidated, --validate-id).

    Args:
        args (argparse.Namespace): Parsed arguments. Expected to have one of:
                                   `list_unvalidated` (bool),
                                   `list_missing_lc` (bool), or
                                   `validate_id` (str).
    """
    log.info(f"Handling 'validate' command with arguments: {args}")
    action_taken = False # Flag to track if any validation action was performed.

    # --- Crucial Step: Check Supabase Connectivity ---
    try:
        log.info("Checking Supabase connection for 'validate' command...")
        client = GdbEM.get_supabase_client()
        if not client:
            print("Error: Cannot connect to Supabase. Check credentials and network.")
            log.critical("Aborting 'validate' command due to Supabase connection failure.")
            return
        log.info("Supabase connection check passed for 'validate' command.")
    except Exception as e:
        print(f"Error: Failed to initialize Supabase client: {e}")
        log.critical(f"Aborting 'validate' command due to Supabase initialization error: {e}", exc_info=True)
        return

    # Dispatch based on the mutually exclusive group arguments.
    if args.list_unvalidated:
        print("Listing unvalidated opinion entries from Supabase...")
        GvalidatorEM.list_entries_supabase(list_type="unvalidated")
        action_taken = True
    elif args.list_missing_lc:
        print("Listing unvalidated opinions potentially missing Lower Court Docket ID from Supabase...")
        GvalidatorEM.list_entries_supabase(list_type="missing_lc_docket")
        action_taken = True
    elif args.validate_id:
         print(f"Starting interactive validation for Opinion UniqueID: {args.validate_id} from Supabase...")
         GvalidatorEM.validate_case_supabase(args.validate_id) # Assumes this function handles interaction.
         action_taken = True

    if not action_taken:
        # This should ideally be caught by argparse if `required=True` is set on the group.
        print("Error: No action specified for 'validate'. Use --list-unvalidated, --list-missing-lc, or --validate-id <ID>.")
        log.error("Validate command handler reached without a required action flag. This might indicate an argparse setup issue.")


def handle_configure_command(args):
    """
    Handles the 'configure' command, allowing modification of non-sensitive
    settings like the logging toggle. Schedule modification is noted as
    not implemented via CLI.

    Args:
        args (argparse.Namespace): Parsed arguments. Expected to have:
                                   `toggle_logging` (str: 'true' or 'false'),
                                   `add_schedule` (bool),
                                   `remove_schedule` (int).
    """
    log.info(f"Handling 'configure' command with arguments: {args}")
    try:
        config = GconfigEM.load_config() # Load current configuration.
        config_updated = False           # Flag to track if changes were made.

        # Handle logging toggle.
        if args.toggle_logging is not None:
            new_logging_state = str(args.toggle_logging).lower() == 'true' # Convert 'true'/'false' string to boolean.
            if config.get('logging') != new_logging_state:
                config['logging'] = new_logging_state
                print(f"File logging has been set to: {config['logging']}.")
                log.info(f"Configuration updated: logging set to {config['logging']}.")
                config_updated = True
            else:
                print(f"File logging is already set to: {new_logging_state}. No change made.")

        # Placeholder for schedule modification (currently not implemented via CLI).
        if args.add_schedule or args.remove_schedule is not None:
             print("Info: Schedule modification via CLI is not currently supported for this version.")
             print("Please edit the 'schedule' section in 'config.json' manually if changes are needed.")
             log.warning("User attempted schedule modification via CLI, which is not implemented.")

        # Save configuration if any changes were made.
        if config_updated:
            GconfigEM.save_config(config)
            print("Configuration saved successfully.")
        else:
            print("No valid configuration changes were applied.")

    except Exception as e:
        log.error(f"Error during 'configure' command execution: {e}", exc_info=True)
        print(f"An error occurred during configuration: {e}")


def handle_reset_counter_command(args):
    """
    Handles the 'reset-counter' command. Prompts the user for confirmation
    before resetting the 'run_counter' in `config.json` to 0.

    Args:
        args (argparse.Namespace): Parsed arguments (not used by this handler).
    """
    log.info("Handling 'reset-counter' command.")
    # Prompt for user confirmation with a timeout.
    confirm = prompt_with_timeout("Are you sure you want to reset the application run counter to 0? (y/n): ", timeout_seconds=60)
    if confirm and confirm.lower() == 'y':
        GconfigEM.reset_run_counter() # Delegate to GconfigEM.
        print("Run counter has been reset to 0.")
        log.info("Run counter was reset by user confirmation.")
    else:
        print("Run counter reset cancelled by user or timed out.")
        log.info("Run counter reset was cancelled or timed out.")


def handle_supreme_command(args):
    """
    Handles the 'supreme' command for testing Supreme Court docket web searches.
    Note: Database saving functionality from GsupremetestEM might be outdated
    (originally for SQLite) and is currently disabled in this handler.

    Args:
        args (argparse.Namespace): Parsed arguments. Expected to have `docket` (str).
    """
    log.warning("Supreme Court scraper test ('supreme' command) initiated. "
                "Note: Database saving from GsupremetestEM is currently disabled here as it might be SQLite-specific.")
    try:
        # Call the search function from GsupremetestEM.
        # Explicitly disable saving until GsupremetestEM is fully adapted for Supabase if needed.
        results = GsupremetestEM.search_supreme_docket(
            args.docket,
            save_results=False # IMPORTANT: Ensure this is False if DB part is not Supabase-ready.
        )

        if results:
            print("\n--- Supreme Court Docket Search Results (from Web Scrape) ---")
            print(f"  Supreme Court Docket: {results.get('sc_docket', 'N/A')}")
            print(f"  Appellate Docket:     {results.get('app_docket', 'N/A')}")
            print(f"  Case Caption:         {results.get('case_name', 'N/A')}")
            # Potentially display other relevant fields returned by the scraper.
        else:
            print(f"\nNo results found via web scrape for Supreme Court docket: {args.docket}")

    except Exception as e:
        log.error(f"Supreme Court docket search failed for docket '{args.docket}': {e}", exc_info=True)
        print(f"An error occurred during the Supreme Court docket search: {e}")
        # Consider not exiting the entire app for a test command failure.
        # sys.exit(1)

def handle_exit_command(args):
    """
    Handles the 'exit' command, terminating the application.

    Args:
        args (argparse.Namespace): Parsed arguments (not used by this handler).
    """
    log.info("Handling 'exit' command. Application will now terminate.")
    print("Exiting application...")
    sys.exit(0) # Successful exit.

# --- Argument Parser Setup ---
def setup_parser():
    """
    Sets up the command-line argument parser using `argparse`.

    This function defines all available commands (as subparsers), their arguments,
    and help messages for the CLI application.

    Returns:
        argparse.ArgumentParser: The configured argument parser instance.
    """
    # Main parser for the application.
    parser = argparse.ArgumentParser(
        description='G-EntryPoint-Manager (GEM) CLI - Manage Court Opinion and Calendar Data with Supabase Backend.',
        formatter_class=argparse.RawTextHelpFormatter # Preserves formatting (like newlines) in help messages.
    )
    # Subparsers for individual commands (e.g., run, status, configure).
    # `required=False` means if no command is given, `args.command` will be None.
    # This allows a default action (like 'run' scheduler) if no command is specified.
    subparsers = parser.add_subparsers(dest='command', help='Available commands:', required=False)

    # --- 'run' Command ---
    # Handles starting the scheduler or forcing an immediate opinion scrape.
    run_parser = subparsers.add_parser('run', help='Run opinion scraping scheduler (default action if no command given) or force an immediate scrape.')
    run_parser.add_argument('--force', action='store_true', help='Force an immediate opinion scrape and save results to Supabase, bypassing the scheduler.')
    # Removed '--test' flag as it was obsolete or Supabase focus makes it less relevant here.

    # --- 'process-calendars' Command ---
    # Handles parsing of PDF calendar files.
    calendar_parser = subparsers.add_parser('process-calendars', help='Parse calendar PDFs from a specified folder and save extracted entries to Supabase.')
    calendar_parser.add_argument('folder', type=str, help='Path to the folder containing the calendar PDF files to be processed.')

    # --- 'status' Command ---
    # Displays application status information.
    status_parser = subparsers.add_parser('status', help='Display current application status, database statistics, and relevant checks.')

    # --- 'validate' Command ---
    # Provides tools for listing or validating opinion entries.
    validate_parser = subparsers.add_parser('validate', help='List or interactively validate opinion entries in the Supabase database.')
    # A mutually exclusive group ensures only one validation action can be chosen at a time.
    validate_group = validate_parser.add_mutually_exclusive_group(required=True) # User must pick one option.
    validate_group.add_argument('--list-unvalidated', action='store_true', help='List opinion entries marked as unvalidated.')
    validate_group.add_argument('--list-missing-lc', action='store_true', help='List unvalidated opinions that might be missing Lower Court Docket IDs.')
    validate_group.add_argument('--validate-id', metavar='UNIQUE_ID', type=str, help='Interactively validate a specific opinion entry by its UniqueID.')
    # Removed '--db' flag as Supabase is now the single, primary data source.

    # --- 'configure' Command ---
    # Allows users to modify certain application settings.
    config_parser = subparsers.add_parser('configure', help='Configure application settings, primarily for toggling file logging.')
    config_parser.add_argument('--toggle-logging', choices=['true', 'false'], help="Enable ('true') or disable ('false') file logging.")
    # Schedule modification via CLI is marked as not implemented for user clarity.
    config_parser.add_argument('--add-schedule', help='(Not Implemented) Add a new schedule entry.', action='store_true')
    config_parser.add_argument('--remove-schedule', type=int, metavar='INDEX', help='(Not Implemented) Remove a schedule entry by its index.')

    # --- 'reset-counter' Command ---
    # Resets a run counter in the configuration file.
    reset_parser = subparsers.add_parser('reset-counter', help='Reset the application run counter in config.json to 0 after confirmation.')

    # --- 'supreme' Command ---
    # For testing Supreme Court docket web searches.
    supreme_parser = subparsers.add_parser('supreme', help='Test Supreme Court docket web search. (Note: Database saving is disabled by default).')
    supreme_parser.add_argument('docket', type=str, help='Supreme Court docket number to search (e.g., "A-XX-YY" format).')
    # '--no-save' is effectively the default behavior now, but flag could be kept for future use or clarity.
    # supreme_parser.add_argument('--no-save', action='store_true', help='(Default behavior) Do not attempt to save search results to the database.')

    # --- 'exit' Command ---
    # Provides a clean way to terminate the application.
    exit_parser = subparsers.add_parser('exit', help='Exit the CLI application.')

    return parser

# --- Main Execution Logic ---
def main():
    """
    Main entry point for the G-EntryPoint-Manager (GEM) CLI application.

    This function performs the following steps:
    1. Sets up logging by calling `GloggerEM.setup_logging()`. It attempts to load
       configuration first to respect the 'logging' flag if available. Includes a
       fallback to basic logging if full setup fails.
    2. Initializes the argument parser using `setup_parser()`.
    3. Parses command-line arguments provided by the user.
    4. If no command is specified, it defaults to the 'run' command in scheduler mode.
    5. Otherwise, it dispatches the parsed arguments to the appropriate `handle_..._command()` function.
    6. Includes a global exception handler to catch unhandled errors and log them before exiting.
    """
    # --- Initial Logging Setup ---
    # Attempt to set up logging as early as possible.
    try:
        # GloggerEM should handle checking config for logging status.
        import GloggerEM # Ensure GloggerEM module is available.
        GloggerEM.setup_logging() # Initialize application-wide logging.
        log.info("--- GEM CLI Application Started ---")
    except Exception as log_e:
        # Fallback to basic logging configuration if GloggerEM.setup_logging() fails.
        # This ensures that critical errors during startup are still logged somewhere.
        logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
        log.error(f"Core logging setup via GloggerEM failed: {log_e}. Basic logging active.", exc_info=True)
        print(f"Warning: Full logging setup failed: {log_e}. Using basic logging.")

    try:
        # --- Argument Parsing ---
        parser = setup_parser()         # Get the configured argparse instance.
        args = parser.parse_args()      # Parse the command-line arguments.

        # --- Default Action: Run Scheduler if No Command is Given ---
        # If `args.command` is None (because `required=False` in `add_subparsers`),
        # default to running the scheduler.
        if args.command is None:
            log.info("No command specified by user, defaulting to 'run' in scheduler mode.")
            # Create a simple namespace object to simulate args for handle_run_command,
            # specifically setting 'force' to False for scheduler mode.
            class DefaultRunArgs:
                force = False
            handle_run_command(DefaultRunArgs()) # Call the run handler with default (scheduler) args.
            return # Exit main function after handling default action.

        # --- Command Dispatching ---
        # Call the appropriate handler function based on the parsed command.
        if args.command == 'run':
            handle_run_command(args)
        elif args.command == 'process-calendars':
            handle_process_calendars_command(args)
        elif args.command == 'status':
            handle_status_command(args)
        elif args.command == 'validate':
            handle_validate_command(args)
        elif args.command == 'configure':
            handle_configure_command(args)
        elif args.command == 'reset-counter':
            handle_reset_counter_command(args)
        elif args.command == 'supreme':
            handle_supreme_command(args)
        elif args.command == 'exit':
            handle_exit_command(args)
        else:
            # This case should not be reached if subparsers are set up correctly
            # and a default action is handled, but it's a good fallback.
            log.warning(f"Unknown command '{args.command}' encountered. Displaying help.")
            parser.print_help()

    except Exception as e:
        # Global exception handler for any unhandled errors in the main CLI logic.
        log.critical(f"An unhandled critical error occurred in CLI main: {e}", exc_info=True)
        print(f"\nAn unexpected critical error occurred: {e}")
        sys.exit(1) # Exit with a non-zero status code to indicate failure.
    finally:
        # This block will execute whether an exception occurred or not.
        log.info("--- GEM CLI Application Finished ---")


if __name__ == "__main__":
    # This ensures main() is called only when the script is executed directly.
    main()

# === End of GcliEM.py ===
