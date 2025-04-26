# GstatusEM.py
"""
Provides functions to check and display application status.
Needs updates to reflect multiple databases and new GdbEM stats functions.
"""
import logging
import GdbEM
import GconfigEM
# import GschedulerEM # To check scheduler status later

log = logging.getLogger(__name__)

def display_status():
    """
    Checks and displays the current status of the application,
    including configuration and database info.
    """
    log.info("Checking application status...")
    print("\n--- Application Status ---")

    # Configuration Status
    db_files = {}
    try:
        config = GconfigEM.load_config()
        db_files = config.get("db_files", GconfigEM.DEFAULT_DB_NAMES) # Get configured DB files
        schedule_times = config.get("schedule", {})
        primary_time = schedule_times.get("primary", "N/A")
        backup_time = schedule_times.get("backup", "N/A")
        logging_enabled = config.get("logging", "N/A")

        print("\n[Configuration]")
        print(f"  Config File:      {GconfigEM.CONFIG_FILE}")
        print(f"  Logging Enabled:  {logging_enabled}")
        print(f"  Primary Schedule: {primary_time}")
        print(f"  Backup Schedule:  {backup_time}")
        print("\n  Database Files:")
        for db_type, db_filename in db_files.items():
            print(f"    {db_type.capitalize()}: {db_filename}")

    except Exception as e:
        print("\n[Configuration]")
        print(f"  Error loading configuration: {e}")
        log.error(f"Error reading configuration for status: {e}")

    # Database Status (Needs GdbEM.get_db_stats implemented)
    print("\n[Database Status]")
    if db_files:
        for db_type, db_filename in db_files.items():
             print(f"  Checking '{db_type}' DB: {db_filename}")
             try:
                 # TODO: Implement GdbEM.get_db_stats(db_filename)
                 # stats = GdbEM.get_db_stats(db_filename)
                 # print(f"    Total Records:     {stats.get('total', 'N/I')}")
                 # print(f"    Unvalidated:       {stats.get('unvalidated', 'N/I')}")
                 print("    (DB statistics functions need to be implemented in GdbEM.py)") # Placeholder reminder
                 log.info(f"DB status check for {db_filename} needs GdbEM.get_db_stats function.")
             except ConnectionError as e:
                  print(f"    Error connecting to database: {e}")
                  log.error(f"Database connection error during status check for {db_filename}: {e}")
             except Exception as e:
                 print(f"    Error accessing database: {e}")
                 log.error(f"Error accessing database '{db_filename}' for status: {e}", exc_info=True)
    else:
        print("  Skipping database status check as DB files could not be determined from config.")

    # Scheduler Status (Placeholder)
    print("\n[Scheduler]")
    print("  (Scheduler status check not yet implemented)")
    log.info("Scheduler status check not yet implemented.")

    print("\n--- End of Status ---")

# === End of GstatusEM.py ===