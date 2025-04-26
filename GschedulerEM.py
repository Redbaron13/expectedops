# GschedulerEM.py
"""
Handles scheduled execution of scraping and verification tasks.
Corrected error when getting next run time.
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
# Need prompt_with_timeout for conditional validation prompt (if ever implemented for scheduled)
# from GcliEM import prompt_with_timeout

log = logging.getLogger(__name__)

# --- Scheduled Task Functions (Unchanged) ---

def run_primary_scrape():
    """Scheduled task for the primary data collection run."""
    run_type = 'scheduled-primary'
    log.info(f"--- Starting Scheduled Primary Run ({datetime.datetime.now()}) ---")
    try:
        db_files = GconfigEM.get_db_filenames()
        primary_db = db_files.get("primary")
        all_runs_db = db_files.get("all_runs")
        if not primary_db or not all_runs_db:
            log.error("Primary and/or AllRuns DB not configured. Skipping primary scheduled run.")
            return
        log.info(f"Fetching data from {GscraperEM.PAGE_URL}")
        opinions, release_date = GscraperEM.fetch_and_parse_opinions()
        if not opinions:
            log.info("No opinions found during primary scrape.")
        else:
            log.info(f"Found {len(opinions)} opinions for release date {release_date}. Saving to DBs.")
            is_validated = False
            GdbEM.save_opinions_to_dbs(opinions, is_validated, run_type) # Saves to primary & all_runs
    except Exception as e:
        log.error(f"Error during scheduled primary scrape: {e}", exc_info=True)
    finally:
        log.info(f"--- Finished Scheduled Primary Run ({datetime.datetime.now()}) ---")

def run_backup_verification_scrape():
    """Scheduled task for backup/verification run with comparison."""
    run_type = 'scheduled-backup'
    log.info(f"--- Starting Scheduled Backup/Verification Run ({datetime.datetime.now()}) ---")
    try:
        db_files = GconfigEM.get_db_filenames()
        primary_db = db_files.get("primary")
        backup_db = db_files.get("backup")
        all_runs_db = db_files.get("all_runs")
        if not backup_db or not all_runs_db or not primary_db:
            log.error("Primary, Backup, and/or AllRuns DB not configured. Skipping backup/verification run.")
            return

        log.info(f"Fetching current data from {GscraperEM.PAGE_URL}")
        current_opinions, current_release_date = GscraperEM.fetch_and_parse_opinions()

        dbs_to_save_to = []
        if backup_db: dbs_to_save_to.append(("backup", backup_db))
        if all_runs_db: dbs_to_save_to.append(("all_runs", all_runs_db))
        results_map = {key: {"total": 0, "inserted": 0, "updated": 0, "skipped": 0, "error": 0} for key, _ in dbs_to_save_to}

        if not current_opinions:
            log.info("No opinions found during backup scrape. No data saved.")
            return

        is_validated = False # Scheduled runs save unvalidated
        discrepancies_found = False # Default to no discrepancies

        if not current_release_date:
            log.warning("Could not determine release date from current scrape. Cannot perform comparison.")
            # Save without comparison
            log.info(f"Saving {len(current_opinions)} current opinions found (without comparison) to backup/all_runs DBs.")
            for opinion in current_opinions:
                 for db_key, db_filename in dbs_to_save_to:
                     results_map[db_key]["total"] += 1
                     status = GdbEM.add_or_update_opinion_to_db(db_filename, opinion, is_validated, run_type)
                     if status == "inserted": results_map[db_key]["inserted"] += 1
                     elif status.startswith("updated"): results_map[db_key]["updated"] += 1
                     elif status.startswith("skipped"): results_map[db_key]["skipped"] += 1
                     else: results_map[db_key]["error"] += 1

        else:
            # --- Comparison Logic ---
            log.info(f"Fetching previous primary run data for date {current_release_date} from {primary_db}")
            previous_primary_opinions = GdbEM.get_opinions_by_date_runtype(primary_db, current_release_date, 'scheduled-primary')

            if not previous_primary_opinions:
                 log.warning(f"No previous 'scheduled-primary' run data found in {primary_db} for date {current_release_date}. Cannot perform comparison.")
                 # Save current data without comparison
                 log.info(f"Saving {len(current_opinions)} current opinions found (without comparison) to backup/all_runs DBs.")
                 for opinion in current_opinions:
                      for db_key, db_filename in dbs_to_save_to:
                          results_map[db_key]["total"] += 1
                          status = GdbEM.add_or_update_opinion_to_db(db_filename, opinion, is_validated, run_type)
                          if status == "inserted": results_map[db_key]["inserted"] += 1
                          elif status.startswith("updated"): results_map[db_key]["updated"] += 1
                          elif status.startswith("skipped"): results_map[db_key]["skipped"] += 1
                          else: results_map[db_key]["error"] += 1
            else:
                # --- Proceed with Comparison ---
                current_dockets = {o['AppDocketID'] for o in current_opinions}
                previous_dockets = set(previous_primary_opinions.keys())
                new_since_primary = current_dockets - previous_dockets
                missing_from_backup = previous_dockets - current_dockets
                discrepancies_found = bool(new_since_primary or missing_from_backup)

                log.info(f"Comparison Results for {current_release_date}:")
                if not discrepancies_found: log.info("  No discrepancies found.")
                else:
                    log.warning("  Discrepancies found!")
                    if new_since_primary: log.warning(f"    New dockets found: {', '.join(sorted(new_since_primary))}")
                    if missing_from_backup: log.warning(f"    Dockets missing now: {', '.join(sorted(missing_from_backup))}")

                # --- Save current data to Backup and AllRuns DBs ---
                log.info(f"Saving {len(current_opinions)} current opinions found to backup/all_runs DBs.")
                for opinion in current_opinions:
                     for db_key, db_filename in dbs_to_save_to:
                         results_map[db_key]["total"] += 1
                         status = GdbEM.add_or_update_opinion_to_db(db_filename, opinion, is_validated, run_type)
                         if status == "inserted": results_map[db_key]["inserted"] += 1
                         elif status.startswith("updated"): results_map[db_key]["updated"] += 1
                         elif status.startswith("skipped"): results_map[db_key]["skipped"] += 1
                         else: results_map[db_key]["error"] += 1

        # Log final save results
        for db_key, db_filename in dbs_to_save_to:
             if results_map[db_key]["total"] > 0:
                 log.info(f"DB '{db_key}' ({db_filename}): Processed {results_map[db_key]['total']} entries -> "
                          f"I:{results_map[db_key]['inserted']}, U:{results_map[db_key]['updated']}, "
                          f"S:{results_map[db_key]['skipped']}, E:{results_map[db_key]['error']}")

        # Log if discrepancies require review
        if discrepancies_found:
            log.warning(f"Discrepancies found for {current_release_date}. Manual review recommended.")

    except Exception as e:
        log.error(f"Error during scheduled backup/verification scrape: {e}", exc_info=True)
    finally:
        log.info(f"--- Finished Scheduled Backup/Verification Run ({datetime.datetime.now()}) ---")


# --- Scheduler Loop ---

def start_schedule_loop():
    """Configures and runs the main scheduler loop."""

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

        log.info(f"Scheduler starting. Primary Run: {primary_time}, Backup/Verify Run: {backup_time}")
        print(f"--- Starting Scheduler ---")
        print(f"Primary run scheduled daily at {primary_time}")
        print(f"Backup/Verify run scheduled daily at {backup_time}")
        print("Press Ctrl+C to exit.")

        schedule.clear()
        schedule.every().day.at(primary_time).do(run_primary_scrape)
        schedule.every().day.at(backup_time).do(run_backup_verification_scrape)

        # --- CORRECTED Next Run Logic ---
        jobs = schedule.get_jobs()
        if jobs:
            # Find the minimum next_run time among all jobs that have one
            valid_next_runs = [job.next_run for job in jobs if job.next_run is not None]
            if valid_next_runs:
                next_run_time = min(valid_next_runs)
                log.info(f"Next scheduled run at: {next_run_time.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"Next scheduled run at: {next_run_time.strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                log.warning("Scheduled jobs found, but next run time could not be determined (might be immediate).")
        else:
            log.warning("No scheduled jobs found after setup. Check schedule times.")
        # --- END CORRECTION ---

    except Exception as e:
        log.error(f"Error setting up scheduler: {e}", exc_info=True)
        print(f"Error setting up scheduler: {e}")
        return

    # --- Run the scheduler loop ---
    while True:
        try:
            schedule.run_pending()
            time.sleep(1) # Check every second
        except KeyboardInterrupt:
            log.info("Scheduler stopped by user (Ctrl+C).")
            print("\nScheduler stopped.")
            sys.exit(0)
        except Exception as e:
             log.error(f"An error occurred within the scheduler loop: {e}", exc_info=True)
             print(f"\nError in scheduler loop: {e}. Continuing...")
             time.sleep(5) # Pause briefly after an error in the loop

# === End of GschedulerEM.py ===