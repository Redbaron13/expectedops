# GschedulerEM.py
"""
Handles scheduled execution of scraping and verification tasks.
Corrected error when getting next run time.
Ensures backup runs save to backup and all_runs DBs.
Updates run counter and timestamps.
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

log = logging.getLogger(__name__)

# --- Scheduled Task Functions ---

def run_primary_scrape():
    """Scheduled task for the primary data collection run."""
    run_type = 'scheduled-primary'
    log.info(f"--- Starting Scheduled Primary Run ({datetime.datetime.now()}) ---")
    try:
        log.info(f"Fetching data from {GscraperEM.PAGE_URL}")
        opinions, release_date = GscraperEM.fetch_and_parse_opinions()

        if not opinions:
            log.info("No opinions found during primary scrape. Nothing to save.")
            # Optionally update timestamp for 'primary' and 'all_runs' to show an attempt was made?
            # GconfigEM.update_last_run_timestamp('primary')
            # GconfigEM.update_last_run_timestamp('all_runs')
            # GconfigEM.increment_run_counter() # Increment even if no data? Debateable. Let's only increment on successful writes via save_opinions_to_dbs
        else:
            log.info(f"Found {len(opinions)} opinions for release date {release_date}. Saving to primary and all_runs DBs.")
            is_validated = False # Scheduled runs are not auto-validated
            # save_opinions_to_dbs handles counter increment and timestamps
            GdbEM.save_opinions_to_dbs(opinions, is_validated, run_type)

    except Exception as e:
        log.error(f"Error during scheduled primary scrape: {e}", exc_info=True)
    finally:
        log.info(f"--- Finished Scheduled Primary Run ({datetime.datetime.now()}) ---")

def run_backup_verification_scrape():
    """
    Scheduled task for backup/verification run.
    Fetches current data, compares with primary (optional),
    and saves current data to backup and all_runs DBs.
    """
    run_type = 'scheduled-backup'
    log.info(f"--- Starting Scheduled Backup/Verification Run ({datetime.datetime.now()}) ---")
    successful_write = False
    try:
        db_files = GconfigEM.get_db_filenames()
        primary_db = db_files.get("primary") # Needed for comparison
        backup_db = db_files.get("backup")
        all_runs_db = db_files.get("all_runs")

        # Ensure target DBs exist
        if not backup_db:
            log.error("Backup DB not configured. Skipping backup scheduled run.")
            return
        if not all_runs_db:
            log.error("AllRuns DB not configured. Skipping backup scheduled run.")
            return

        log.info(f"Fetching current data from {GscraperEM.PAGE_URL}")
        current_opinions, current_release_date = GscraperEM.fetch_and_parse_opinions()

        if not current_opinions:
            log.info("No opinions found during backup scrape. Nothing to save.")
            # Update timestamps to show run occurred?
            # GconfigEM.update_last_run_timestamp('backup')
            # GconfigEM.update_last_run_timestamp('all_runs')
            # GconfigEM.increment_run_counter() # Increment even if no data?
            return

        # --- Comparison Logic (Optional but recommended) ---
        discrepancies_found = False
        if primary_db and current_release_date:
            log.info(f"Comparing with previous primary run data for date {current_release_date} from {primary_db}")
            # Fetch the primary run closest *before* this backup run for the *same release date*
            # This requires more complex logic than get_opinions_by_date_runtype provides.
            # For simplicity, let's just compare against *any* primary run for that date.
            previous_primary_opinions = GdbEM.get_opinions_by_date_runtype(primary_db, current_release_date, 'scheduled-primary')

            if previous_primary_opinions:
                current_dockets = {o['AppDocketID'] for o in current_opinions}
                previous_dockets = set(previous_primary_opinions.keys())
                new_since_primary = current_dockets - previous_dockets
                missing_from_backup = previous_dockets - current_dockets
                discrepancies_found = bool(new_since_primary or missing_from_backup)

                log.info(f"Comparison Results for {current_release_date}:")
                if not discrepancies_found: log.info("  No discrepancies found compared to primary run.")
                else:
                    log.warning("  Discrepancies found!")
                    if new_since_primary: log.warning(f"    New dockets found now: {', '.join(sorted(new_since_primary))}")
                    if missing_from_backup: log.warning(f"    Dockets missing now: {', '.join(sorted(missing_from_backup))}")
            else:
                log.warning(f"No previous 'scheduled-primary' run data found in {primary_db} for date {current_release_date}. Cannot perform comparison.")
        else:
             log.warning("Cannot perform comparison: Primary DB not configured or release date undetermined.")


        # --- Save current data to Backup and AllRuns DBs ---
        log.info(f"Saving {len(current_opinions)} current opinions to backup and all_runs DBs.")
        is_validated = False # Scheduled runs save unvalidated
        dbs_to_target = [('backup', backup_db), ('all_runs', all_runs_db)]
        results_map = {key: {"total": 0, "inserted": 0, "updated": 0, "skipped": 0, "error": 0} for key, _ in dbs_to_target}
        db_write_success = {key: False for key, _ in dbs_to_target}

        for opinion in current_opinions:
            for db_key, db_filename in dbs_to_target:
                results_map[db_key]["total"] += 1
                status = GdbEM.add_or_update_opinion_to_db(db_filename, opinion, is_validated, run_type)
                if status == "inserted" or status.startswith("updated"):
                    successful_write = True # Mark overall success
                    db_write_success[db_key] = True # Mark success for this DB
                    if status == "inserted": results_map[db_key]["inserted"] += 1
                    else: results_map[db_key]["updated"] += 1
                elif status.startswith("skipped"): results_map[db_key]["skipped"] += 1
                else: results_map[db_key]["error"] += 1

        # Log final save results for backup/all_runs
        for db_key, db_filename in dbs_to_target:
             if results_map[db_key]["total"] > 0:
                 log.info(f"DB '{db_key}' ({db_filename}): Processed {results_map[db_key]['total']} entries -> "
                          f"I:{results_map[db_key]['inserted']}, U:{results_map[db_key]['updated']}, "
                          f"S:{results_map[db_key]['skipped']}, E:{results_map[db_key]['error']}")

        # Log if discrepancies require review
        if discrepancies_found:
            log.warning(f"Discrepancies found for {current_release_date}. Manual review recommended.")

        # --- Update Config State ---
        if successful_write:
            GconfigEM.increment_run_counter() # Increment counter once for the backup run
            # Update timestamps for the DBs that had writes
            for db_key, success in db_write_success.items():
                if success:
                    GconfigEM.update_last_run_timestamp(db_key)


    except Exception as e:
        log.error(f"Error during scheduled backup/verification scrape: {e}", exc_info=True)
    finally:
        log.info(f"--- Finished Scheduled Backup/Verification Run ({datetime.datetime.now()}) ---")


# --- Scheduler Loop ---
def start_schedule_loop():
    """Configures and runs the main scheduler loop."""
    initial_run_count = GconfigEM.get_run_counter()
    log.info(f"Scheduler starting. Initial Run Count: {initial_run_count}")

    try:
        schedule_times = GconfigEM.get_schedule_times()
        primary_time = schedule_times.get("primary")
        backup_time = schedule_times.get("backup")

        time_format_valid = True
        if not primary_time or not re.match(r"^\d{2}:\d{2}$", primary_time):
            log.error(f"Invalid primary schedule time format: '{primary_time}'. Use HH:MM.")
            time_format_valid = False
        if not backup_time or not re.match(r"^\d{2}:\d{2}$", backup_time):
            log.error(f"Invalid backup schedule time format: '{backup_time}'. Use HH:MM.")
            time_format_valid = False

        if not time_format_valid:
            print("Error: Invalid schedule time format in config. Use 'configure --schedule HH:MM,HH:MM'.")
            return

        log.info(f"Configuring scheduler. Primary Run: {primary_time}, Backup/Verify Run: {backup_time}")
        print(f"--- Starting Scheduler ---")
        print(f"Primary run scheduled daily at {primary_time}")
        print(f"Backup/Verify run scheduled daily at {backup_time}")
        print("Press Ctrl+C to exit.")

        schedule.clear()
        schedule.every().day.at(primary_time).do(run_primary_scrape)
        schedule.every().day.at(backup_time).do(run_backup_verification_scrape)

        jobs = schedule.get_jobs()
        if jobs:
            valid_next_runs = [job.next_run for job in jobs if job.next_run is not None]
            if valid_next_runs:
                next_run_time = min(valid_next_runs)
                log.info(f"Next scheduled run at: {next_run_time.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"Next scheduled run at: {next_run_time.strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                log.warning("Scheduled jobs found, but next run time could not be determined.")
        else:
            log.warning("No scheduled jobs found after setup. Check schedule times.")

    except Exception as e:
        log.error(f"Error setting up scheduler: {e}", exc_info=True)
        print(f"Error setting up scheduler: {e}")
        return

    # --- Run the scheduler loop ---
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            log.info("Scheduler stopped by user (Ctrl+C).")
            print("\nScheduler stopped.")
            sys.exit(0)
        except Exception as e:
             log.error(f"An error occurred within the scheduler loop: {e}", exc_info=True)
             print(f"\nError in scheduler loop: {e}. Continuing...")
             time.sleep(5)

# === End of GschedulerEM.py ===