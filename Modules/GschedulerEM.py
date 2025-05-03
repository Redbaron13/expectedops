# GschedulerEM.py
# V3: Implements the new 3-run schedule (Prim1, Prim2, Backup) + Weekly Check
"""
Handles scheduled execution of scraping tasks with a new multi-run schedule.
- Primary Run 1: Mon-Fri 11:00 (Type: scheduled-primary-1 -> primary, all_runs)
- Primary Run 2: Mon-Fri 17:30 (Type: scheduled-primary-2 -> primary, all_runs)
- Backup Run:    Tue-Sat 08:00 (Type: scheduled-backup    -> backup, all_runs)
- Weekly Check:  Sun 03:00     (Type: maintenance)
"""
import schedule
import time
import datetime
import logging
import sys
import re
import GconfigEM
import GscraperEM
import GdbEM
import sqlite3
import os # Added for path check

log = logging.getLogger(__name__)

# --- Scheduled Task Functions ---

def run_scrape_job(run_type_tag):
    """Generic function to run a scrape and save job."""
    log.info(f"--- Starting Scheduled Run: {run_type_tag} ({datetime.datetime.now()}) ---")
    try:
        # Ensure databases are initialized before scraping
        GdbEM.initialize_all_databases() # Check schema on each run

        log.info(f"Fetching data from {GscraperEM.PAGE_URL} for run '{run_type_tag}'")
        opinions, release_date = GscraperEM.fetch_and_parse_opinions()

        if not opinions:
            log.info(f"No opinions found during {run_type_tag} scrape. Nothing to save.")
            # Consider if timestamps/counter should update even on no data?
            # Current logic in save_opinions_to_dbs only updates on successful writes.
        else:
            log.info(f"Found {len(opinions)} opinions for release date {release_date}. Saving for run type '{run_type_tag}'.")
            is_validated = False # Scheduled runs are not auto-validated
            # save_opinions_to_dbs determines target DBs based on run_type_tag
            GdbEM.save_opinions_to_dbs(opinions, is_validated, run_type_tag)

            # --- Comparison Logic for Primary Run 2 (Optional) ---
            # Compare current scrape (Prim 2) with data saved by Prim 1 *for the same day*
            if run_type_tag == 'scheduled-primary-2' and release_date:
                db_files = GconfigEM.get_db_filenames()
                primary_db = db_files.get("primary")
                if primary_db and os.path.exists(primary_db):
                    log.info(f"Comparing Primary Run 2 data with Primary Run 1 for date {release_date}")
                    try:
                        # Fetch what Prim 1 *should have* saved earlier today
                        prim1_opinions_dict = GdbEM.get_opinions_by_date_runtype(primary_db, release_date, 'scheduled-primary-1')

                        if prim1_opinions_dict:
                             current_dockets = {o['AppDocketID'] for o in opinions}
                             prim1_dockets = set(prim1_opinions_dict.keys())

                             if current_dockets != prim1_dockets:
                                 new_since_prim1 = current_dockets - prim1_dockets
                                 missing_since_prim1 = prim1_dockets - current_dockets
                                 log.warning("Discrepancy found between Primary Run 1 and Primary Run 2!")
                                 if new_since_prim1: log.warning(f"    New dockets found in Run 2: {', '.join(sorted(new_since_prim1))}")
                                 if missing_since_prim1: log.warning(f"    Dockets missing in Run 2 (were in Run 1): {', '.join(sorted(missing_since_prim1))}")
                             else:
                                  log.info("Primary Run 2 data matches dockets found in Primary Run 1 for this date.")
                        else:
                             log.warning(f"No data from 'scheduled-primary-1' found for {release_date} in {primary_db} to compare against.")
                    except Exception as comp_err:
                         log.error(f"Error during Primary Run 2 comparison logic: {comp_err}", exc_info=True)
                else:
                     log.warning("Cannot compare Primary Run 2: Primary DB not configured or not found.")

    except Exception as e:
        log.error(f"Error during scheduled run '{run_type_tag}': {e}", exc_info=True)
    finally:
        log.info(f"--- Finished Scheduled Run: {run_type_tag} ({datetime.datetime.now()}) ---")


# --- Maintenance Task ---
def check_missing_lc_dockets():
    """
    Scheduled task (weekly) to identify records possibly missing LC Docket IDs
    in the primary database and log them for manual review.
    """
    log.info(f"--- Starting Weekly Check for Missing LC Dockets ({datetime.datetime.now()}) ---")
    run_type_tag = 'maintenance' # Identify this run
    db_key = "primary"
    db_files = GconfigEM.get_db_filenames()
    db_filename = db_files.get(db_key)

    if not db_filename:
        log.error(f"Weekly Check Failed: Primary DB ('{db_key}') not configured.")
        return
    if not os.path.exists(db_filename):
        log.error(f"Weekly Check Failed: Primary DB file '{db_filename}' not found.")
        return

    conn = None
    missing_lc_dockets = []
    try:
        conn = GdbEM.get_db_connection(db_filename)
        cursor = conn.cursor()
        query = """
            SELECT UniqueID, AppDocketID, CaseName, ReleaseDate, LowerCourtVenue
            FROM opinions
            WHERE (LCdocketID IS NULL OR LCdocketID = '') -- Missing LC Docket ID
              AND (LowerCourtVenue IS NULL OR LowerCourtVenue != 'Appellate Division') -- Exclude SC cases where LC is App Div
              AND (LCCounty IS NULL OR LCCounty != 'NJ') -- Exclude NJ Agency cases
              AND validated = 0 -- Only check unvalidated entries
            ORDER BY ReleaseDate DESC
        """
        log.debug(f"Executing missing LC docket check query: {query}")
        cursor.execute(query) # No parameters needed for this version
        rows = cursor.fetchall()
        missing_lc_dockets = [dict(row) for row in rows]

    except sqlite3.Error as e:
        log.error(f"Database error during weekly check in '{db_filename}': {e}", exc_info=True)
    except ConnectionError as e:
         log.error(f"Database connection error during weekly check: {e}")
    except Exception as e:
         log.error(f"Unexpected error during weekly check: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

    if missing_lc_dockets:
        log.warning(f"Weekly Check Found {len(missing_lc_dockets)} Unvalidated Entries Potentially Missing LC Dockets (excluding SC/Agency):")
        for entry in missing_lc_dockets:
             log.warning(f"  - UniqueID: {entry['UniqueID'][:8]}..., AppDocket: {entry['AppDocketID']}, Release: {entry['ReleaseDate']}, Case: {entry['CaseName'][:50]}...")
        log.warning("Use 'validate --list-missing-lc' and 'validate --validate-id <UniqueID>' to review and correct.")
    else:
        log.info("Weekly Check: No unvalidated entries requiring LC Docket review were found (excluding SC/Agency cases).")

    log.info(f"--- Finished Weekly Check for Missing LC Dockets ({datetime.datetime.now()}) ---")


# --- Scheduler Loop ---
def start_schedule_loop():
    """Configures and runs the main scheduler loop with the new schedule."""
    initial_run_count = GconfigEM.get_run_counter()
    log.info(f"Scheduler starting. Initial Run Count: {initial_run_count}")

    # Define Schedule Times (Make easily configurable if needed, but hardcoded for now)
    primary_run_1_time = "11:00"
    primary_run_2_time = "17:30"
    backup_run_time = "08:00"
    weekly_check_time = "03:00"

    try:
        # Validate time formats (basic check)
        time_pattern = r"^\d{2}:\d{2}$"
        if not all(re.match(time_pattern, t) for t in [primary_run_1_time, primary_run_2_time, backup_run_time, weekly_check_time]):
            log.critical("Invalid time format detected in scheduler setup. Use HH:MM.")
            print("Error: Invalid time format in scheduler configuration.")
            return

        log.info("Configuring scheduler with new multi-run schedule:")
        log.info(f"  Primary Run 1 (Mon-Fri): {primary_run_1_time} (Type: scheduled-primary-1)")
        log.info(f"  Primary Run 2 (Mon-Fri): {primary_run_2_time} (Type: scheduled-primary-2)")
        log.info(f"  Backup Run    (Tue-Sat): {backup_run_time} (Type: scheduled-backup)")
        log.info(f"  Weekly Check  (Sun)    : {weekly_check_time} (Type: maintenance)")

        print(f"--- Starting Scheduler ---")
        print(f"Primary Run 1 (Mon-Fri) scheduled at {primary_run_1_time}")
        print(f"Primary Run 2 (Mon-Fri) scheduled at {primary_run_2_time}")
        print(f"Backup Run    (Tue-Sat) scheduled at {backup_run_time}")
        print(f"Weekly Check  (Sun)     scheduled at {weekly_check_time}")
        print("Press Ctrl+C to exit.")

        schedule.clear() # Clear any previous schedules

        # Schedule Primary Runs (Mon-Fri)
        schedule.every().monday.at(primary_run_1_time).do(run_scrape_job, run_type_tag='scheduled-primary-1').tag('primary', 'weekday')
        schedule.every().tuesday.at(primary_run_1_time).do(run_scrape_job, run_type_tag='scheduled-primary-1').tag('primary', 'weekday')
        schedule.every().wednesday.at(primary_run_1_time).do(run_scrape_job, run_type_tag='scheduled-primary-1').tag('primary', 'weekday')
        schedule.every().thursday.at(primary_run_1_time).do(run_scrape_job, run_type_tag='scheduled-primary-1').tag('primary', 'weekday')
        schedule.every().friday.at(primary_run_1_time).do(run_scrape_job, run_type_tag='scheduled-primary-1').tag('primary', 'weekday')

        schedule.every().monday.at(primary_run_2_time).do(run_scrape_job, run_type_tag='scheduled-primary-2').tag('primary', 'weekday')
        schedule.every().tuesday.at(primary_run_2_time).do(run_scrape_job, run_type_tag='scheduled-primary-2').tag('primary', 'weekday')
        schedule.every().wednesday.at(primary_run_2_time).do(run_scrape_job, run_type_tag='scheduled-primary-2').tag('primary', 'weekday')
        schedule.every().thursday.at(primary_run_2_time).do(run_scrape_job, run_type_tag='scheduled-primary-2').tag('primary', 'weekday')
        schedule.every().friday.at(primary_run_2_time).do(run_scrape_job, run_type_tag='scheduled-primary-2').tag('primary', 'weekday')

        # Schedule Backup Run (Tue-Sat)
        schedule.every().tuesday.at(backup_run_time).do(run_scrape_job, run_type_tag='scheduled-backup').tag('backup', 'tue-sat')
        schedule.every().wednesday.at(backup_run_time).do(run_scrape_job, run_type_tag='scheduled-backup').tag('backup', 'tue-sat')
        schedule.every().thursday.at(backup_run_time).do(run_scrape_job, run_type_tag='scheduled-backup').tag('backup', 'tue-sat')
        schedule.every().friday.at(backup_run_time).do(run_scrape_job, run_type_tag='scheduled-backup').tag('backup', 'tue-sat')
        schedule.every().saturday.at(backup_run_time).do(run_scrape_job, run_type_tag='scheduled-backup').tag('backup', 'tue-sat')

        # Schedule Weekly Check (Sun)
        schedule.every().sunday.at(weekly_check_time).do(check_missing_lc_dockets).tag('weekly', 'maintenance')


        # Log next run time
        jobs = schedule.get_jobs()
        if jobs:
            next_run_times = [job.next_run for job in jobs if job.next_run is not None]
            if next_run_times:
                next_run_time = min(next_run_times)
                # Ensure timezone information is included if possible
                local_tz = datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo
                next_run_local = next_run_time.astimezone(local_tz)
                log.info(f"Next scheduled run at: {next_run_local.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                print(f"Next scheduled run at: {next_run_local.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            else:
                 log.warning("Scheduled jobs exist, but next run time could not be determined.")
        else:
            log.warning("No scheduled jobs found after setup. Check schedule times and logic.")

    except FileNotFoundError as e:
         log.critical(f"Configuration file not found during scheduler setup: {e}", exc_info=True)
         config_path = GconfigEM._get_config_path()
         print(f"Error: Configuration file '{os.path.basename(config_path)}' not found at expected location '{config_path}'.")
         return
    except Exception as e:
        log.critical(f"Critical error setting up scheduler: {e}", exc_info=True)
        print(f"Critical Error setting up scheduler: {e}")
        return

    # --- Run the scheduler loop ---
    while True:
        try:
            schedule.run_pending()
            # Sleep for a longer interval if no jobs are due soon to save CPU
            idle_seconds = schedule.idle_seconds()
            sleep_interval = 60 # Default check every minute
            if idle_seconds is not None and idle_seconds > 1:
                 # Sleep up to the next job, but max 5 minutes
                 sleep_interval = min(idle_seconds + 1, 300)
            time.sleep(sleep_interval)
        except KeyboardInterrupt:
            log.info("Scheduler stopped by user (Ctrl+C).")
            print("\nScheduler stopped.")
            sys.exit(0)
        except Exception as e:
             log.error(f"An error occurred within the scheduler loop: {e}", exc_info=True)
             print(f"\nError in scheduler loop: {e}. Attempting to continue...")
             time.sleep(60) # Longer sleep after an error

# === End of GschedulerEM.py ===