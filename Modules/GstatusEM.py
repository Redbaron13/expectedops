# GstatusEM.py
# V2: Updated for Supabase and added Judge Name Check.
"""
Provides functions to check and display detailed application status,
including configuration, Supabase stats, run counter, and judge name checks.
"""
import logging
import GdbEM      # For getting Supabase stats
import GconfigEM  # For getting config values
import GjudgeListEM # For getting reference judge list
import os
from datetime import datetime

log = logging.getLogger(__name__)

def get_distinct_judges_from_db():
    """Queries Supabase for distinct judge names from calendar entries."""
    supabase = GdbEM.get_supabase_client()
    if not supabase:
        log.error("Cannot get judges: Supabase client not available.")
        return set()

    all_judge_names = set()
    try:
        # Query distinct names from both assigned and presiding fields
        fields_to_query = ['AssignedJudges', 'PresidingJudgesPart']
        for field in fields_to_query:
            response = supabase.table('calendar_entries').select(field).execute()
            if response.data:
                for row in response.data:
                    judge_string = row.get(field)
                    if judge_string:
                        # Split comma-separated names and add to set
                        names = [name.strip() for name in judge_string.split(',') if name.strip()]
                        all_judge_names.update(names)
            elif response.error:
                 log.error(f"Supabase error fetching distinct judges from {field}: {response.error}")

    except Exception as e:
        log.error(f"Error querying distinct judges from Supabase: {e}", exc_info=True)

    # Filter out empty strings just in case
    all_judge_names.discard('')
    return all_judge_names


def display_status():
    """
    Checks and displays the current status of the application using Supabase.
    """
    log.info("Checking application status (Supabase)...")
    print("\n--- Application Status ---")

    # --- Configuration Status ---
    print("\n[Configuration]")
    config_error = False
    try:
        # Load non-sensitive config
        config = GconfigEM.load_config()
        schedule_config = config.get("schedule", GconfigEM.DEFAULT_SCHEDULE)
        logging_enabled = config.get("logging", "N/A")
        run_counter = config.get("run_counter", "N/A")

        # Check Supabase connection details (don't print keys)
        supabase_url = GconfigEM.get_supabase_url()
        supabase_key_present = bool(GconfigEM.get_supabase_key()) # Check if key is set

        print(f"  Config File:      {os.path.basename(GconfigEM._get_config_path())}")
        print(f"  Logging Enabled:  {logging_enabled}")
        print(f"  Run Counter:      {run_counter}")
        print(f"  Schedule Entries: {len(schedule_config)}")
        print(f"  Supabase URL Set: {'Yes' if supabase_url else 'No'}")
        print(f"  Supabase Key Set: {'Yes' if supabase_key_present else 'No'}")

    except Exception as e:
        print(f"  Error loading configuration: {e}")
        log.error(f"Error reading configuration for status: {e}", exc_info=True)
        config_error = True

    # --- Database Status (Supabase 'opinions' table) ---
    print("\n[Database Status ('opinions' table)]")
    db_stats = {}
    any_db_error = False

    try:
        db_stats = GdbEM.get_db_stats()
        if db_stats["error"]:
            print(f"  Status:           ERROR ({db_stats['error']})")
            any_db_error = True
        else:
            print(f"  Total Records:    {db_stats['total']}")
            print(f"  Validated:        {db_stats['validated']}")
            print(f"  Unvalidated:      {db_stats['unvalidated']}")
    except Exception as e:
        print(f"  Status:           ERROR connecting or querying Supabase: {e}")
        log.error(f"Failed to get Supabase stats: {e}", exc_info=True)
        any_db_error = True

    # --- Judge Name Check ---
    print("\n[Judge Name Check (from 'calendar_entries')]")
    if any_db_error:
        print("  Skipping judge check due to database connection errors.")
    else:
        try:
            reference_judges = GjudgeListEM.get_reference_judge_set()
            if not reference_judges:
                 print("  WARNING: Reference judge list is empty or could not be loaded.")
            else:
                 print(f"  Reference list loaded with {len(reference_judges)} names.")
                 db_judges = get_distinct_judges_from_db()
                 print(f"  Found {len(db_judges)} distinct judge names in database.")

                 unknown_judges = db_judges - reference_judges
                 if unknown_judges:
                      print("  >> WARNING: Potential new or unrecognized judge names found:")
                      for judge in sorted(list(unknown_judges)):
                           print(f"     - {judge}")
                      log.warning(f"Unrecognized judge names found: {', '.join(sorted(list(unknown_judges)))}")
                 else:
                      print("  >> All judge names found in DB match the reference list.")

        except Exception as e:
             print(f"  Error during judge name check: {e}")
             log.error(f"Failed during judge name check: {e}", exc_info=True)


    # --- Scheduler Status (Placeholder - Needs Integration) ---
    print("\n[Scheduler]")
    # TODO: Integrate with schedule library if possible to show next run time?
    # This requires accessing the schedule instance from GschedulerEM, which might be complex
    # if GschedulerEM runs as a separate process or isn't easily accessible.
    # For now, keep the placeholder.
    print("  (Scheduler next run time check not implemented in status)")
    log.info("Scheduler status check not yet implemented in GstatusEM.")

    print("\n--- End of Status ---")

# === End of GstatusEM.py ===
