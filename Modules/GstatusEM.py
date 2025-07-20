# GstatusEM.py
# V2: Updated for Supabase and added Judge Name Check.
"""
Provides functions to check and display a comprehensive status of the
G-EntryPoint-Manager (GEM) application.

This module consolidates information from various components:
1.  **Configuration Status**: Retrieves and displays settings from `GconfigEM`,
    such as config file path, logging status, run counter, number of schedule
    entries, and the presence of Supabase URL and key.
2.  **Database Status**: Fetches and shows statistics for the 'opinions' table
    from the Supabase database via `GdbEM.get_db_stats()`, including total,
    validated, and unvalidated record counts.
3.  **Judge Name Check**:
    - Queries the Supabase 'calendar_entries' table (via `get_distinct_judges_from_db()`)
      to get all unique judge names listed in `AssignedJudges` and `PresidingJudgesPart`.
    - Compares these names against a reference list obtained from `GjudgeListEM.get_reference_judge_set()`.
    - Reports any judge names found in the database that are not in the reference list,
      highlighting potential new or unrecognized judge names.
4.  **Scheduler Status**: Currently a placeholder, intended for future integration
    to display information about scheduled tasks (e.g., next run times).

The main function `display_status()` orchestrates these checks and prints the
formatted status report to the console. Includes error handling for each section
to ensure that a failure in one area does not prevent others from being displayed.
"""
import logging
import GdbEM      # For Supabase database interactions (stats, judge names).
import GconfigEM  # For retrieving application configuration values.
import GjudgeListEM # For getting the reference list of known judge names.
import os
from datetime import datetime # Not directly used in V2, but often useful for status reporting.

log = logging.getLogger(__name__)

def get_distinct_judges_from_db() -> set:
    """
    Queries the Supabase 'calendar_entries' table to retrieve all distinct judge names
    found in the 'AssignedJudges' and 'PresidingJudgesPart' columns.

    It handles comma-separated judge names within these fields by splitting them
    and adding each individual name to the resulting set.

    Returns:
        set: A set of unique judge names found in the database. Returns an empty
             set if the Supabase client is unavailable or an error occurs.
    """
    supabase = GdbEM.get_supabase_client() # Obtain Supabase client instance.
    if not supabase:
        log.error("Cannot retrieve distinct judge names: Supabase client is not available.")
        return set() # Return empty set if no client.

    all_judge_names = set() # Use a set to store unique names.
    try:
        # Columns in 'calendar_entries' that might contain judge names.
        fields_to_query = ['AssignedJudges', 'PresidingJudgesPart']
        for field_name in fields_to_query:
            log.debug(f"Querying distinct judge names from field: {field_name}")
            # Supabase query to select the specified field from all rows.
            response = supabase.table('calendar_entries').select(field_name).execute()

            if response.data:
                for row in response.data:
                    judge_string_from_db = row.get(field_name)
                    if judge_string_from_db:
                        # Judge names can be comma-separated; split and strip them.
                        individual_names = [name.strip() for name in judge_string_from_db.split(',') if name.strip()]
                        all_judge_names.update(individual_names) # Add to set for uniqueness.
            elif response.error:
                 # Log error if Supabase reports an issue for a specific field query.
                 log.error(f"Supabase error while fetching distinct judge names from field '{field_name}': {response.error}")

    except Exception as e:
        # Catch any other unexpected errors during the database query.
        log.error(f"Error querying distinct judge names from Supabase 'calendar_entries': {e}", exc_info=True)

    all_judge_names.discard('') # Ensure empty strings are not included if they somehow get added.
    log.info(f"Retrieved {len(all_judge_names)} distinct judge names from 'calendar_entries' table.")
    return all_judge_names


def display_status():
    """
    Checks and displays the current status of various application components.

    This function prints a formatted report to the console, covering:
    - Configuration settings from `GconfigEM`.
    - Database statistics for the 'opinions' table from `GdbEM`.
    - A check of judge names in the 'calendar_entries' table against a reference list.
    - A placeholder for scheduler status.

    It includes error handling for each section to allow partial status display
    if some components are unavailable or report errors.
    """
    log.info("Gathering and displaying application status (Supabase backend)...")
    print("\n--- GEM Application Status Report ---")

    # --- Section 1: Configuration Status ---
    print("\n[Configuration Details]")
    config_load_error = False
    try:
        # Load non-sensitive application configuration.
        app_config = GconfigEM.load_config()
        schedule_entries = app_config.get("schedule", GconfigEM.DEFAULT_SCHEDULE) # Default if not found.
        is_logging_enabled = app_config.get("logging", "N/A") # Default if not found.
        current_run_counter = app_config.get("run_counter", "N/A") # Default if not found.

        # Check presence of Supabase connection details (without displaying actual keys).
        supabase_url_configured = GconfigEM.get_supabase_url()
        supabase_key_is_present = bool(GconfigEM.get_supabase_key()) # True if key string is non-empty.

        # Display configuration information.
        # _get_config_path() is an internal GconfigEM method; using it carefully for info.
        print(f"  Config File Path:      {GconfigEM._get_config_path()}")
        print(f"  File Logging Enabled:  {is_logging_enabled}")
        print(f"  Application Run Counter: {current_run_counter}")
        print(f"  Number of Schedule Entries: {len(schedule_entries)}")
        print(f"  Supabase URL Configured: {'Yes' if supabase_url_configured else 'No, NOT SET!'}")
        print(f"  Supabase Key Configured: {'Yes' if supabase_key_is_present else 'No, NOT SET!'}")

    except Exception as e_config:
        # Handle errors during configuration loading.
        print(f"  ERROR: Could not load or display full configuration: {e_config}")
        log.error(f"Error reading application configuration for status display: {e_config}", exc_info=True)
        config_load_error = True

    # --- Section 2: Database Status (Supabase 'opinions' table) ---
    print("\n[Database Status ('opinions' table via Supabase)]")
    database_stats = {}
    database_query_error = False
    try:
        # Fetch statistics for the 'opinions' table from Supabase.
        database_stats = GdbEM.get_db_stats()
        if database_stats.get("error"): # Check if GdbEM reported an error string.
            print(f"  Status:           ERROR retrieving DB stats ({database_stats['error']})")
            log.error(f"Error reported by GdbEM.get_db_stats: {database_stats['error']}")
            database_query_error = True
        else:
            # Display successfully retrieved stats.
            print(f"  Total Records:    {database_stats.get('total', 'N/A')}")
            print(f"  Validated Records:{database_stats.get('validated', 'N/A')}")
            print(f"  Unvalidated Records:{database_stats.get('unvalidated', 'N/A')}")
    except Exception as e_db:
        # Handle errors connecting to DB or during the GdbEM.get_db_stats() call.
        print(f"  Status:           ERROR - Could not connect or query Supabase for 'opinions' stats: {e_db}")
        log.error(f"Failed to get Supabase 'opinions' table stats: {e_db}", exc_info=True)
        database_query_error = True

    # --- Section 3: Judge Name Consistency Check (from 'calendar_entries') ---
    print("\n[Judge Name Consistency Check (Supabase 'calendar_entries' vs Reference List)]")
    if database_query_error: # Skip if DB connection previously failed.
        print("  Skipping judge name check due to earlier database connection/query errors.")
        log.warning("Judge name check skipped due to database errors.")
    else:
        try:
            # Load the reference set of known judge names.
            reference_judge_names = GjudgeListEM.get_reference_judge_set()
            if not reference_judge_names:
                 print("  WARNING: Reference judge list ('judges_2024_2025.txt') is empty or could not be loaded. Cannot perform check.")
                 log.warning("Reference judge list is empty. Judge name check accuracy is compromised.")
            else:
                 print(f"  Reference judge list loaded: {len(reference_judge_names)} unique names.")
                 # Get distinct judge names currently in the 'calendar_entries' table.
                 judge_names_from_db = get_distinct_judges_from_db()
                 print(f"  Distinct judge names found in 'calendar_entries' table: {len(judge_names_from_db)}.")

                 # Find names in DB that are not in the reference list.
                 unrecognized_judge_names = judge_names_from_db - reference_judge_names
                 if unrecognized_judge_names:
                      print("  >> WARNING: Potential new or unrecognized judge names found in database:")
                      for judge_name in sorted(list(unrecognized_judge_names)):
                           print(f"     - \"{judge_name}\"")
                      log.warning(f"Unrecognized judge names found in 'calendar_entries': {', '.join(sorted(list(unrecognized_judge_names)))}")
                 elif not judge_names_from_db and reference_judge_names : # DB is empty but ref list is not
                      print("  INFO: No judge names currently in 'calendar_entries' table to compare against reference list.")
                 else: # All names in DB are in reference list.
                      print("  >> All judge names found in 'calendar_entries' match the reference list.")

        except Exception as e_judge_check:
             # Handle errors during the judge name checking process.
             print(f"  ERROR during judge name check: {e_judge_check}")
             log.error(f"Failed during judge name consistency check: {e_judge_check}", exc_info=True)

    # --- Section 4: Scheduler Status ---
    print("\n[Scheduler Status]")
    # This section is a placeholder. True integration would require the GschedulerEM module
    # to provide a way to query its status (e.g., next run times for jobs).
    # Accessing `schedule.next_run()` directly here might not work if the scheduler
    # runs in a different context or if its state isn't globally accessible.
    print("  (Scheduler next run time check is not currently implemented in GstatusEM)")
    log.info("Placeholder for scheduler status check; not yet implemented.")

    print("\n--- End of Status Report ---")

# === End of GstatusEM.py ===
