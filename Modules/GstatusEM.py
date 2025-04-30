# GstatusEM.py
"""
Provides functions to check and display detailed application status,
including database statistics, run counter, timestamps, and count comparisons.
"""
import logging
import GdbEM      # For getting DB stats
import GconfigEM  # For getting config values (DB names, counter, timestamps)
import os
from datetime import datetime

log = logging.getLogger(__name__)

def display_status():
    """
    Checks and displays the current status of the application,
    including configuration, database stats, run counter, and comparisons.
    """
    log.info("Checking application status...")
    print("\n--- Application Status ---")

    # --- Configuration Status ---
    print("\n[Configuration]")
    config_error = False
    try:
        config = GconfigEM.load_config()
        db_files = config.get("db_files", GconfigEM.DEFAULT_DB_NAMES)
        schedule_times = config.get("schedule", {})
        primary_time = schedule_times.get("primary", "N/A")
        backup_time = schedule_times.get("backup", "N/A")
        logging_enabled = config.get("logging", "N/A")
        run_counter = config.get("run_counter", "N/A")
        last_timestamps = config.get("last_run_timestamps", GconfigEM.DEFAULT_TIMESTAMPS)

        print(f"  Config File:      {os.path.basename(GconfigEM._get_config_path())}")
        print(f"  Logging Enabled:  {logging_enabled}")
        print(f"  Run Counter:      {run_counter}")
        print(f"  Primary Schedule: {primary_time}")
        print(f"  Backup Schedule:  {backup_time}")

    except Exception as e:
        print(f"  Error loading configuration: {e}")
        log.error(f"Error reading configuration for status: {e}", exc_info=True)
        config_error = True
        # Set defaults to avoid errors below if config failed
        db_files = GconfigEM.DEFAULT_DB_NAMES
        last_timestamps = GconfigEM.DEFAULT_TIMESTAMPS

    # --- Database Status ---
    print("\n[Database Status]")
    db_stats = {} # Store stats for comparison {db_type: stats_dict}
    any_db_error = False

    for db_type, db_filename in db_files.items():
         print(f"  {db_type.capitalize()} DB: {db_filename}")
         stats = GdbEM.get_db_stats(db_filename)
         db_stats[db_type] = stats # Store for later comparison

         if stats["error"]:
             print(f"    Status:           ERROR ({stats['error']})")
             any_db_error = True
         else:
             print(f"    Total Records:    {stats['total']}")
             print(f"    Validated:        {stats['validated']}")
             print(f"    Unvalidated:      {stats['unvalidated']}")

         # Display last run timestamp for this DB type
         ts = last_timestamps.get(db_type)
         ts_display = "Never"
         if ts:
             try:
                 # Attempt to parse ISO format string back to datetime for nicer display
                 ts_dt = datetime.fromisoformat(ts)
                 ts_display = ts_dt.strftime('%Y-%m-%d %H:%M:%S')
             except (ValueError, TypeError):
                 ts_display = str(ts) # Fallback to string if parsing fails
         print(f"    Last Write Time:  {ts_display}")
         print("-" * 25) # Separator


    # --- Count Comparisons ---
    print("\n[Database Count Comparisons]")
    if config_error or any_db_error:
        print("  Skipping comparisons due to configuration or database errors.")
        log.warning("Skipping status comparisons due to errors.")
    else:
        primary_total = db_stats.get("primary", {}).get("total", 0)
        backup_total = db_stats.get("backup", {}).get("total", 0)
        all_runs_total = db_stats.get("all_runs", {}).get("total", 0)

        # 1. Primary vs Backup
        print(f"  Primary ({primary_total}) vs Backup ({backup_total})")
        if primary_total != backup_total:
            diff = abs(primary_total - backup_total)
            print(f"  >> DISCREPANCY: Counts differ by {diff} records.")
            log.warning(f"Status Check: Primary ({primary_total}) and Backup ({backup_total}) counts differ.")
        else:
            print("  >> Counts match.")

        # 2. Primary + Backup vs All Runs
        # Note: This comparison assumes all_runs *should* equal the sum.
        # In reality, all_runs aims for unique entries across primary, backup, test.
        combined_prim_backup = primary_total + backup_total
        print(f"\n  Primary + Backup ({combined_prim_backup}) vs All Runs ({all_runs_total})")
        if combined_prim_backup != all_runs_total:
            diff = abs(combined_prim_backup - all_runs_total)
            print(f"  >> DISCREPANCY: Counts differ by {diff} records.")
            print("     (Note: All Runs aims for unique entries from Primary, Backup, Test - simple sum may not be the expected value)")
            log.warning(f"Status Check: Primary+Backup ({combined_prim_backup}) and All Runs ({all_runs_total}) counts differ.")
        else:
             print("  >> Counts match (Primary+Backup sum equals All Runs total).")


    # --- Scheduler Status (Placeholder) ---
    print("\n[Scheduler]")
    # TODO: Integrate with schedule library if possible to show next run time?
    # next_run = schedule.next_run # Requires access to the scheduler instance
    # print(f"  Next Scheduled Run: {schedule.next_run}") # Example if accessible
    print("  (Scheduler next run time check not yet implemented)")
    log.info("Scheduler status check not yet implemented in GstatusEM.")


    print("\n--- End of Status ---")

# === End of GstatusEM.py ===