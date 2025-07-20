# GschedulerEM.py
# V3: Implements the new 3-run schedule (Prim1, Prim2, Backup) + Weekly Check
"""
Manages the scheduling and execution of recurring tasks for the G-EntryPoint-Manager (GEM) application.

This module utilizes the `schedule` library (a lightweight job scheduling library)
to run tasks at predefined times and on specific days of the week. Key responsibilities include:

1.  Loading schedule configurations from `GconfigEM.py`, which defines when and
    what type of tasks should run (e.g., "primary-1" scrape, "backup" scrape).
2.  Setting up scheduled jobs based on this configuration, mapping day strings
    (like "Mon-Fri") to specific `schedule` library methods.
3.  Executing defined job functions at their scheduled times. These jobs typically
    involve web scraping (via `GscraperEM`), database operations (via `GdbEM`),
    and maintenance tasks.
4.  Providing a main loop (`start_schedule_loop`) that continuously checks for and
    runs pending scheduled tasks.
5.  Logging scheduler events, including job executions, next scheduled run times,
    and any errors encountered during task execution or scheduler setup.

The primary scheduled tasks include different types of opinion scraping runs and
a weekly maintenance check for data integrity (e.g., missing Lower Court dockets).
"""
import schedule # For scheduling jobs.
import time     # For sleeping in the main scheduler loop.
import datetime # For timestamping and logging.
import logging
import sys      # For exiting the application on KeyboardInterrupt.
import re
import GconfigEM        # For loading schedule configuration and DB filenames.
import GscraperEM       # For performing web scraping tasks.
import GdbEM            # For database interactions (Supabase).
import sqlite3          # Used in legacy comparison logic and weekly check (needs review for Supabase).
import os               # For path checks, e.g., if DB file exists (legacy).

log = logging.getLogger(__name__)

# --- Scheduled Task Functions ---

def run_scrape_job(run_type_tag: str):
    """
    A generic function executed as a scheduled job to perform a web scrape
    and save the retrieved opinion data.

    This function calls `GscraperEM.fetch_and_parse_opinions()` to get the data
    and then `GdbEM.save_opinions_to_db()` (assuming Supabase version of GdbEM)
    to save it. The `run_type_tag` argument is passed to the save function to
    categorize the data.

    For 'scheduled-primary-2' runs, it includes optional logic to compare the
    scraped dockets against those from 'scheduled-primary-1' for the same day,
    logging any discrepancies. (This comparison logic might need updates if
    GdbEM is fully Supabase and `get_opinions_by_date_runtype` is adapted).

    Args:
        run_type_tag (str): A string tag identifying the type of scrape run
                            (e.g., "scheduled-primary-1", "scheduled-backup").
                            This tag is used for logging and data categorization.
    """
    log.info(f"--- Starting Scheduled Run: {run_type_tag} ({datetime.datetime.now()}) ---")
    try:
        # Note: GdbEM.initialize_all_databases() was from SQLite version.
        # With Supabase, client initialization is typically handled by get_supabase_client().
        # Ensure Supabase client is ready (usually done at app start or within GdbEM calls).
        # If GdbEM.initialize_all_databases() had schema checks, those might need a Supabase equivalent if desired.
        log.info("Ensuring Supabase client is accessible (implicitly via GdbEM calls)...")

        log.info(f"Fetching opinion data from {GscraperEM.PAGE_URL} for run_type '{run_type_tag}'")
        # Perform the web scrape.
        opinions, release_date = GscraperEM.fetch_and_parse_opinions()

        if not opinions:
            log.info(f"No opinions found during '{run_type_tag}' scrape. Nothing to save for release date '{release_date}'.")
            # Note: The decision to update timestamps or counters even on no data
            # would depend on application requirements. Current GdbEM logic for Supabase
            # in save_opinions_to_db increments run counter on successful writes.
        else:
            log.info(f"Found {len(opinions)} opinions for release date '{release_date}'. "
                     f"Proceeding to save for run_type '{run_type_tag}'.")
            is_validated_by_schedule = False # Scheduled runs are considered unvalidated by default.
            # Save opinions to the database (Supabase via GdbEM).
            GdbEM.save_opinions_to_db(opinions, is_validated_by_schedule, run_type_tag)

            # --- Legacy Comparison Logic for Primary Run 2 (currently assumes SQLite GdbEM methods) ---
            # This section needs review if GdbEM is fully Supabase, as get_db_filenames and
            # get_opinions_by_date_runtype (if it uses SQLite path) would be incompatible.
            # For Supabase, comparison would involve querying Supabase directly.
            # Example (conceptual for Supabase):
            #   prim1_opinions_dict = GdbEM.get_opinions_by_date_runtype_supabase(release_date, 'scheduled-primary-1')
            # This code is left as is from original, highlighting its potential legacy nature.
            if run_type_tag == 'scheduled-primary-2' and release_date:
                log.info(f"Attempting comparison between Primary Run 2 and Primary Run 1 for date {release_date}.")
                # The following block is SQLite-dependent and needs adaptation for Supabase.
                # For Supabase, you'd call Supabase-specific functions from GdbEM.
                try:
                    db_files = GconfigEM.get_db_filenames() # Assumes this might still return paths for legacy reasons.
                    primary_db_path = db_files.get("primary")
                    if primary_db_path and os.path.exists(primary_db_path): # Check if the SQLite DB file exists.
                        log.info(f"Comparing Primary Run 2 data with Primary Run 1 (from SQLite DB: {primary_db_path}) for date {release_date}")
                        # Fetch what Prim 1 *should have* saved earlier today from the SQLite DB.
                        prim1_opinions_dict_sqlite = GdbEM.get_opinions_by_date_runtype(primary_db_path, release_date, 'scheduled-primary-1')

                        if prim1_opinions_dict_sqlite:
                             current_run_dockets = {o['AppDocketID'] for o in opinions}
                             prim1_run_dockets_sqlite = set(prim1_opinions_dict_sqlite.keys())

                             if current_run_dockets != prim1_run_dockets_sqlite:
                                 new_dockets_in_run2 = current_run_dockets - prim1_run_dockets_sqlite
                                 missing_dockets_in_run2 = prim1_run_dockets_sqlite - current_run_dockets
                                 log.warning("Discrepancy found between Primary Run 2 (current scrape) and Primary Run 1 (SQLite data)!")
                                 if new_dockets_in_run2:
                                     log.warning(f"    New dockets found in Run 2 not in Run 1 (SQLite): {', '.join(sorted(new_dockets_in_run2))}")
                                 if missing_dockets_in_run2:
                                     log.warning(f"    Dockets missing in Run 2 that were in Run 1 (SQLite): {', '.join(sorted(missing_dockets_in_run2))}")
                             else:
                                  log.info("Primary Run 2 dockets match dockets found in Primary Run 1 (SQLite) for this date.")
                        else:
                             log.warning(f"No data from 'scheduled-primary-1' found for date {release_date} in SQLite DB '{primary_db_path}' to compare against.")
                    else:
                         log.warning("Cannot compare Primary Run 2: Primary SQLite DB path not configured or DB file not found.")
                except Exception as comparison_error:
                     log.error(f"Error during Primary Run 2 comparison logic (SQLite): {comparison_error}", exc_info=True)

    except Exception as e:
        # Catch any unhandled errors during the execution of this scheduled job.
        log.error(f"Unhandled error during scheduled run '{run_type_tag}': {e}", exc_info=True)
    finally:
        # This block ensures a log message is always recorded when the job finishes, regardless of success or failure.
        log.info(f"--- Finished Scheduled Run: {run_type_tag} ({datetime.datetime.now()}) ---")


# --- Maintenance Task ---
def check_missing_lc_dockets():
    """
    Scheduled weekly maintenance task to identify and log opinion entries in the
    primary database that might be missing Lower Court (LC) Docket IDs.

    This task queries the 'opinions' table (assumed to be Supabase via GdbEM)
    for unvalidated entries where 'LCdocketID' is null or empty, excluding
    Supreme Court cases (where LC is Appellate Division) and agency cases (where
    LCCounty is 'NJ'). Results are logged for manual review.
    """
    log.info(f"--- Starting Weekly Check for Missing LC Dockets ({datetime.datetime.now()}) ---")
    # This run type tag isn't used for saving data but for identifying the log messages.
    run_type_tag_maintenance = 'maintenance-weekly-lc-check'
    missing_lc_docket_entries = []

    try:
        # Get Supabase client (implicitly via GdbEM calls).
        supabase_client = GdbEM.get_supabase_client()
        if not supabase_client:
            log.error("Weekly Check Failed: Supabase client not available.")
            return

        # Construct the query for Supabase.
        # This query needs to be executed using the Supabase client's methods.
        # Example: response = supabase_client.table('opinions').select(...).execute()
        # The SQL query below is for conceptual understanding; actual implementation uses Supabase client methods.
        # Conceptual SQL:
        #   SELECT UniqueID, AppDocketID, CaseName, ReleaseDate, LowerCourtVenue
        #   FROM opinions
        #   WHERE (LCdocketID IS NULL OR LCdocketID = '')
        #     AND (LowerCourtVenue IS NULL OR LowerCourtVenue != 'Appellate Division')
        #     AND (LCCounty IS NULL OR LCCounty != 'NJ') -- Assuming 'NJ' LCCounty means agency.
        #     AND validated = false
        #   ORDER BY ReleaseDate DESC
        log.debug(f"Querying Supabase for unvalidated entries potentially missing LC Dockets (excluding SC/Agency cases).")

        # Perform the query using Supabase client
        # Using .is_('LCdocketID', None) or .eq('LCdocketID', '') is tricky with PostgREST,
        # often .or_('LCdocketID.is.null,LCdocketID.eq.') is needed.
        # For simplicity, assuming LCdocketID is an empty string if missing, or checking for null.
        query_builder = supabase_client.table('opinions')\
            .select('UniqueID, AppDocketID, CaseName, ReleaseDate, LowerCourtVenue')\
            .or_('LCdocketID.is.null,LCdocketID.eq.')\
            .neq('LowerCourtVenue', 'Appellate Division')\
            .neq('LCCounty', 'NJ') \
            .eq('validated', False)\
            .order('ReleaseDate', desc=True)

        response = query_builder.execute()

        if response.data:
            missing_lc_docket_entries = response.data
        elif response.error:
            log.error(f"Supabase error during weekly check for missing LC dockets: {response.error}")
        else:
            log.info("Weekly Check: No data returned from Supabase for missing LC dockets query.")

    except ConnectionError as conn_err: # Catch specific connection error from get_supabase_client
         log.error(f"Database connection error during weekly check: {conn_err}")
    except Exception as e: # Catch other potential errors (API errors, etc.)
         log.error(f"Unexpected error during weekly check for missing LC dockets: {e}", exc_info=True)

    # Log the findings.
    if missing_lc_docket_entries:
        log.warning(f"Weekly Check Found {len(missing_lc_docket_entries)} Unvalidated Entries Potentially Missing LC Dockets (excluding SC/Agency):")
        for entry in missing_lc_docket_entries:
             log.warning(f"  - UniqueID: {entry.get('UniqueID', 'N/A')[:8]}..., AppDocket: {entry.get('AppDocketID', 'N/A')}, "
                         f"Release: {entry.get('ReleaseDate', 'N/A')}, Case: {entry.get('CaseName', 'N/A')[:50]}...")
        log.warning("Use 'validate --list-missing-lc' and 'validate --validate-id <UniqueID>' CLI commands to review and correct these entries.")
    else:
        log.info("Weekly Check: No unvalidated entries requiring LC Docket review were found (excluding SC/Agency cases).")

    log.info(f"--- Finished Weekly Check for Missing LC Dockets ({datetime.datetime.now()}) ---")


# --- Scheduler Loop ---
def start_schedule_loop():
    """
    Configures and starts the main scheduler loop for the application.

    It loads schedule settings from `GconfigEM.get_schedule()`, which is expected
    to return a list of dictionaries, each specifying a 'time', 'type', and 'days'
    for a scheduled job. It then sets up these jobs using the `schedule` library.
    The 'days' string is parsed (e.g., "Mon-Fri", "Sat") to schedule jobs on the
    correct weekdays. Different job "types" are mapped to `run_type_tag` arguments
    for `run_scrape_job`. A hardcoded weekly maintenance task (`check_missing_lc_dockets`)
    is also scheduled.

    After setup, it enters an infinite loop, continuously checking for and running
    any pending scheduled tasks. It includes error handling for scheduler setup
    and within the loop itself.
    """
    try:
        initial_run_count = GconfigEM.get_run_counter() # Get current run counter for logging.
        log.info(f"Scheduler starting up. Initial Run Count from config: {initial_run_count}")

        # Load schedule configuration from GconfigEM.
        schedule_config_list = GconfigEM.get_schedule()
        log.info(f"Successfully loaded schedule configuration: {schedule_config_list}")

        schedule.clear() # Clear any schedules that might exist from previous runs or states.

        # --- Dynamically Schedule Runs from Configuration ---
        for schedule_entry in schedule_config_list:
            run_time_str = schedule_entry.get("time")    # e.g., "11:00"
            job_type_str = schedule_entry.get("type")    # e.g., "primary-1"
            days_specifier = schedule_entry.get("days")  # e.g., "Mon-Fri", "Sat"

            if not all([run_time_str, job_type_str, days_specifier]):
                log.warning(f"Invalid schedule entry missing time, type, or days: {schedule_entry}. Skipping.")
                continue

            log.info(f"Processing schedule entry: Type='{job_type_str}', Time='{run_time_str}', Days='{days_specifier}'")
            print(f"INFO: Scheduling job Type='{job_type_str}' at Time='{run_time_str}' on Days='{days_specifier}'")

            # Map day strings from config to `schedule` library methods.
            day_mapping = {
                "Mon": schedule.every().monday, "Tue": schedule.every().tuesday,
                "Wed": schedule.every().wednesday, "Thu": schedule.every().thursday,
                "Fri": schedule.every().friday, "Sat": schedule.every().saturday,
                "Sun": schedule.every().sunday
            }

            # Parse the 'days' specifier.
            # Handles ranges like "Mon-Fri" and single days like "Sat".
            days_to_schedule = []
            if "-" in days_specifier: # Range of days
                start_day_str, end_day_str = days_specifier.split("-")
                ordered_days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
                try:
                    start_index = ordered_days.index(start_day_str.capitalize())
                    end_index = ordered_days.index(end_day_str.capitalize())
                    if start_index <= end_index:
                        days_to_schedule.extend(ordered_days[start_index : end_index + 1])
                    else: # Handle wrap-around like Fri-Mon (though less common for this app)
                        days_to_schedule.extend(ordered_days[start_index:])
                        days_to_schedule.extend(ordered_days[:end_index + 1])
                except ValueError:
                    log.error(f"Invalid day range in schedule entry: '{days_specifier}'. Skipping this entry.")
                    continue
            else: # Single day
                days_to_schedule.append(days_specifier.capitalize())

            # Schedule the job for each specified day.
            for day_str_capitalized in days_to_schedule:
                day_schedule_method = day_mapping.get(day_str_capitalized)
                if day_schedule_method:
                    # Construct the run_type_tag for the job.
                    job_run_type_tag = f"scheduled-{job_type_str}"
                    # Schedule the run_scrape_job with the specific run_type_tag.
                    day_schedule_method.at(run_time_str).do(run_scrape_job, run_type_tag=job_run_type_tag).tag(job_type_str, 'scrape')
                    log.info(f"  Successfully scheduled '{job_run_type_tag}' on {day_str_capitalized} at {run_time_str}.")
                else:
                    log.warning(f"Invalid day '{day_str_capitalized}' found in schedule entry '{days_specifier}'. Skipping this day for this entry.")

        # --- Schedule Weekly Maintenance Check (Hardcoded for now) ---
        # This task checks for opinions potentially missing LC Docket IDs.
        weekly_check_run_time = "03:00" # Runs early Sunday morning.
        log.info(f"Scheduling hardcoded Weekly Maintenance Check (check_missing_lc_dockets) on Sunday at {weekly_check_run_time}")
        print(f"INFO: Scheduling Weekly Maintenance Check on Sunday at {weekly_check_run_time}")
        schedule.every().sunday.at(weekly_check_run_time).do(check_missing_lc_dockets).tag('weekly-check', 'maintenance')

        # --- Log Next Scheduled Run Time ---
        if schedule.jobs: # Check if any jobs were actually scheduled.
            next_run_datetime = schedule.next_run()
            if next_run_datetime:
                try:
                    # Attempt to localize the next run time to the system's local timezone for display.
                    local_system_tz = datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo
                    next_run_local_time = next_run_datetime.astimezone(local_system_tz)
                    log.info(f"Scheduler setup complete. Next scheduled job will run at: {next_run_local_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                    print(f"INFO: Scheduler setup complete. Next job at: {next_run_local_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                except Exception as e_timeformat: # Handle potential errors in timezone conversion or formatting.
                    log.warning(f"Could not format next run time for local display: {e_timeformat}. UTC next run: {next_run_datetime}")
                    print(f"INFO: Scheduler setup complete. Next job at (UTC): {next_run_datetime}")
            else:
                # This might happen if all jobs are for past times and won't run again without rescheduling.
                log.warning("Scheduler has jobs, but could not determine the next run time (schedule.next_run() returned None). This may indicate all jobs are in the past.")
        else:
            log.warning("No jobs were scheduled. Check configuration and scheduler setup logic. The scheduler loop will run idly.")

    except FileNotFoundError as e_fnf:
         # Specific handling for missing configuration file, which is critical for scheduler setup.
         log.critical(f"Configuration file ('{GconfigEM.CONFIG_FILE}') not found during scheduler setup: {e_fnf}", exc_info=True)
         # Attempt to get the full expected path for a more user-friendly error message.
         try:
             config_path_expected = GconfigEM._get_config_path() # Accessing a "protected" member for informational purposes.
             print(f"CRITICAL ERROR: Configuration file '{os.path.basename(config_path_expected)}' not found at the expected location: '{config_path_expected}'. Scheduler cannot start.")
         except Exception: # If even getting the path fails for some reason.
             print(f"CRITICAL ERROR: Configuration file '{GconfigEM.CONFIG_FILE}' not found. Scheduler cannot start.")
         return # Exit the function, preventing the scheduler loop from starting.
    except Exception as e_setup:
        # Catch any other unexpected critical errors during the scheduler setup phase.
        log.critical(f"A critical error occurred during scheduler setup: {e_setup}", exc_info=True)
        print(f"CRITICAL ERROR: Scheduler setup failed due to an unexpected error: {e_setup}. Scheduler cannot start.")
        return # Exit the function.

    # --- Main Scheduler Loop ---
    # This loop runs indefinitely, checking for pending jobs and executing them.
    log.info("Entering main scheduler loop. Pending jobs will run as scheduled. Press Ctrl+C to exit.")
    while True:
        try:
            schedule.run_pending() # This is the core call that executes any jobs that are due.

            # Determine a smart sleep interval to avoid busy-waiting and reduce CPU usage.
            idle_seconds_val = schedule.idle_seconds() # Time in seconds until the next job is due.

            # Default sleep duration if no jobs are scheduled or if idle_seconds_val is None/0.
            sleep_duration = 60 # Default to checking every 60 seconds.

            if idle_seconds_val is not None and idle_seconds_val > 0:
                 # If there's a next job, sleep until just after it's due,
                 # but cap the sleep time to a maximum (e.g., 5 minutes).
                 # This ensures the loop remains responsive to external signals (like Ctrl+C)
                 # and can pick up changes if the schedule is modified dynamically (though current code doesn't do that).
                 sleep_duration = min(idle_seconds_val + 1, 300) # Add 1s buffer, max 300s (5 mins).

            log.debug(f"Scheduler sleeping for {sleep_duration:.2f} seconds.")
            time.sleep(sleep_duration)

        except KeyboardInterrupt:
            # Allow the user to gracefully stop the scheduler loop with Ctrl+C.
            log.info("Scheduler loop stopped by user (KeyboardInterrupt).")
            print("\nScheduler stopped by user.")
            sys.exit(0) # Clean exit from the application.

        except Exception as e_loop:
             # Catch any other unexpected errors that occur within the scheduler's main loop.
             log.error(f"An unexpected error occurred within the scheduler loop: {e_loop}", exc_info=True)
             print(f"\nERROR in scheduler loop: {e_loop}. Attempting to continue after a 60-second delay...")
             # Sleep for a longer period after an error to prevent rapid error logging if the issue is persistent.
             time.sleep(60)

# === End of GschedulerEM.py ===