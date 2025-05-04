# GmergerEM.py
# V2: Enhanced logging for debugging merge issues.
"""
Utility to merge data from an older version database file into a current target database.
Handles schema differences by mapping known columns and providing defaults for new ones.
Ensures duplicate content (based on UniqueID hash) is not added to the target DB (primary/backup/test),
but logs ALL merged records to the AllRuns history DB.
Includes detailed logging for debugging merge operations.
"""
import sqlite3
import os
import logging
import json
import datetime
import GdbEM # Import for DB connection, hashing, ID generation, init, history saving
import GconfigEM # To get target DB filename

# Setup logger for this module specifically if not relying on root logger config
# If GmainEM setup is sufficient, this line can be removed.
# logging.basicConfig(level=logging.INFO) # Example basic config if run standalone
log = logging.getLogger(__name__) # Get logger instance

# Define columns expected in the LATEST schema (V3 based on last update)
LATEST_SCHEMA_COLS = [
    "UniqueID", "AppDocketID", "ReleaseDate", "DataHash", "DuplicateFlag", "LinkedDocketIDs",
    "CaseName", "LCdocketID", "LCCounty", "Venue", "LowerCourtVenue", "LowerCourtSubCaseType",
    "OPJURISAPP", "DecisionTypeCode", "DecisionTypeText", "StateAgency1", "StateAgency2",
    "CaseNotes", "RunType", "entry_method", "validated", "caseconsolidated", "recordimpounded",
    "opinionstatus", "migration_source_version", "first_scraped_ts", "last_updated_ts",
    "last_validated_run_ts"
]

def merge_old_database(source_db_path, target_db_key, source_schema_version):
    """
    Merges data from an older source DB into the target DB (primary, backup, or test).

    - Skips inserting records into the target if a record with the same UniqueID (content hash) already exists.
    - Saves ALL processed records from the source into the AllRuns history DB.

    Args:
        source_db_path (str): Path to the old database file.
        target_db_key (str): Key ('primary', 'backup', 'test') of the target DB from config.
        source_schema_version (int): The schema version number of the source DB.

    Returns:
        bool: True if the process completed (potentially with row errors), False on critical setup/connection failure.
    """
    log.info(f"Starting merge from '{source_db_path}' (Schema V{source_schema_version}) into target DB '{target_db_key}'.")

    # --- Validate Inputs & Get Paths ---
    if not os.path.exists(source_db_path):
        log.error(f"Source database file not found: {source_db_path}")
        print(f"Error: Source database file not found: {source_db_path}")
        return False

    db_files = GconfigEM.get_db_filenames()
    target_db_path = db_files.get(target_db_key)
    all_runs_db_path = db_files.get("all_runs")

    if not target_db_path:
        log.error(f"Target database key '{target_db_key}' not found in configuration.")
        print(f"Error: Target database '{target_db_key}' not configured.")
        return False
    if not all_runs_db_path:
        log.warning("AllRuns DB not configured. History of merged items will not be saved.")

    # --- Initialize Databases ---
    try:
        log.info(f"Ensuring target '{target_db_path}' is initialized...")
        GdbEM.initialize_database(target_db_path)
        if all_runs_db_path and os.path.exists(all_runs_db_path): # Check existence before initializing history
             log.info(f"Ensuring all_runs '{all_runs_db_path}' is initialized...")
             GdbEM.initialize_database(all_runs_db_path)
        elif all_runs_db_path and not os.path.exists(all_runs_db_path):
             log.info(f"AllRuns DB '{all_runs_db_path}' not found, initializing...")
             GdbEM.initialize_database(all_runs_db_path) # Initialize if missing
        log.info("Target/AllRuns database schema check/update complete.")
    except Exception as e:
        log.error(f"Failed to initialize target/all_runs DBs: {e}", exc_info=True)
        print(f"Error: Could not initialize databases: {e}")
        return False

    conn_source, conn_target, conn_all_runs = None, None, None
    processed_count, inserted_target_count, skipped_target_count = 0, 0, 0
    inserted_history_count, error_row_count = 0, 0
    success = False

    try:
        # --- Connect to Databases ---
        log.debug("Connecting to databases...")
        conn_source = GdbEM.get_db_connection(source_db_path); conn_source.row_factory = sqlite3.Row; cursor_source = conn_source.cursor()
        conn_target = GdbEM.get_db_connection(target_db_path); cursor_target = conn_target.cursor()
        conn_all_runs = GdbEM.get_db_connection(all_runs_db_path) if all_runs_db_path else None

        # --- Introspect Source Schema ---
        log.debug(f"Fetching source schema info from {source_db_path}")
        cursor_source.execute("PRAGMA table_info(opinions);")
        source_columns = [row['name'] for row in cursor_source.fetchall()]
        if not source_columns: raise ValueError("Source 'opinions' table missing/empty.")
        log.info(f"Source DB V{source_schema_version} columns: {source_columns}")

        # --- Read Source Data ---
        log.info("Reading data from source database...")
        cursor_source.execute("SELECT * FROM opinions;")

        # --- Begin Target Transaction ---
        conn_target.execute("BEGIN;")

        # --- Iterate, Transform, Insert/Skip ---
        log.info("Processing and merging records...")
        while True:
            rows = cursor_source.fetchmany(100)
            if not rows: break

            for old_row_obj in rows:
                processed_count += 1
                old_row = None # Initialize for error logging
                new_row = {}
                app_docket = "UNKNOWN" # For logging context
                unique_id = "UNKNOWN"
                try:
                    old_row = dict(old_row_obj)
                    app_docket = old_row.get('AppDocketID','UNKNOWN') # Get for logging

                    # Map old data to new schema, provide defaults
                    for col in LATEST_SCHEMA_COLS:
                        if col in old_row: new_row[col] = old_row[col]
                        else: # Assign defaults for missing columns
                            default_value = None
                            if col == 'entry_method': default_value = f'migrated_v{source_schema_version}'
                            elif col == 'opinionstatus': default_value = 0
                            elif col == 'migration_source_version': default_value = source_schema_version
                            elif col in ['validated','caseconsolidated','recordimpounded','DuplicateFlag']: default_value = 0
                            new_row[col] = default_value
                            # Log defaults assignment only at DEBUG level
                            if col not in ['UniqueID','DataHash','first_scraped_ts','last_updated_ts','last_validated_run_ts']:
                                log.debug(f"Default '{default_value}' for missing col '{col}' (AppDocket: {app_docket})")

                    # Regenerate Hash/UniqueID
                    case_name = new_row.get('CaseName')
                    if not app_docket or app_docket == "UNKNOWN" or not case_name:
                        log.warning(f"Skipping row transform: Missing AppDocket/CaseName. Source data: {old_row}")
                        error_row_count += 1; continue
                    new_row['DataHash'] = GdbEM.generate_data_hash(new_row)
                    new_row['UniqueID'] = GdbEM.generate_unique_id(new_row['DataHash'], app_docket)
                    unique_id = new_row['UniqueID'] # Update for logging
                    if not unique_id or not new_row['DataHash']:
                        log.warning(f"Skipping row transform: Missing UniqueID/DataHash. AppDocket: {app_docket}")
                        error_row_count += 1; continue

                    # Log the transformed data at debug level before insertion attempt
                    log.debug(f"Transformed data for {app_docket} (UniqueID: {unique_id[:8]}...): { {k:v for k,v in new_row.items() if k not in ['first_scraped_ts', 'last_updated_ts', 'last_validated_run_ts']} }") # Log subset

                    # A. Check duplicates & Insert into Target DB
                    cursor_target.execute("SELECT 1 FROM opinions WHERE UniqueID = ? LIMIT 1", (unique_id,))
                    exists_in_target = cursor_target.fetchone()

                    if exists_in_target:
                        # Change from DEBUG to INFO to make skips more visible if desired
                        log.info(f"Skipping target insert (duplicate UniqueID): {unique_id[:8]}... (AppDocket: {app_docket})")
                        skipped_target_count += 1
                    else:
                        # Prepare INSERT for target
                        cols_str = ", ".join(f'"{c}"' for c in LATEST_SCHEMA_COLS)
                        placeholders = ", ".join(["?"] * len(LATEST_SCHEMA_COLS))
                        insert_sql = f"INSERT INTO opinions ({cols_str}) VALUES ({placeholders})"
                        insert_values = [new_row.get(col) for col in LATEST_SCHEMA_COLS]

                        cursor_target.execute(insert_sql, tuple(insert_values))
                        inserted_target_count += 1
                        log.debug(f"Inserted into target ({target_db_key}): {unique_id[:8]}... ({app_docket})")

                    # B. Save snapshot to AllRuns History DB (always attempt)
                    if conn_all_runs:
                        history_run_type = f"merged_from_v{source_schema_version}"
                        # Pass the full transformed row (new_row) to history save
                        hist_status = GdbEM._save_to_all_runs_history(all_runs_db_path, new_row, history_run_type)
                        if hist_status == "inserted_history":
                            inserted_history_count += 1
                        else:
                            # Log history save failure with more context
                            log.warning(f"Failed history save for UniqueID {unique_id[:8]}... (AppDocket: {app_docket}). Status: {hist_status}")
                            # Should this count as a row error? Maybe separate history error count? Keep in main error count for now.
                            error_row_count += 1
                    else:
                         # Log if history saving is skipped due to config only once maybe?
                         # For now, covered by initial warning.
                         pass


                except Exception as row_err:
                    # Enhanced error logging for individual row processing failure.
                    log.error(f"Error processing row (AppDocket: {app_docket}, UniqueID: {unique_id[:8]}...): {row_err}", exc_info=True) # Add traceback
                    error_row_count += 1
                    # Optionally log the problematic old_row data (be mindful of size/sensitivity)
                    log.debug(f"Problematic source row data: {old_row}")


            if processed_count % 500 == 0:
                log.info(f"Processed {processed_count} source records...")


        # --- Commit Target Transaction ---
        log.info(f"Committing {inserted_target_count} inserts to target database '{target_db_key}'...")
        conn_target.commit()
        log.info("Merge process finished.")
        success = True

    except (sqlite3.Error, ConnectionError, ValueError) as e: # Catch expected critical errors
        log.error(f"Critical error during merge: {e}", exc_info=True)
        print(f"Error: {e}")
        if conn_target: conn_target.rollback() # Rollback target on critical error
    except Exception as e:
        log.error(f"Unexpected critical error during merge: {e}", exc_info=True)
        print(f"Error: An unexpected error occurred: {e}")
        if conn_target: conn_target.rollback()
    finally:
        # Close all connections
        if conn_source: conn_source.close()
        if conn_target: conn_target.close()
        if conn_all_runs: conn_all_runs.close()

    # Final Summary Log (More detailed)
    log.info(f"Merge Complete. Summary: Processed={processed_count}, TargetInserted={inserted_target_count}, TargetSkipped(Dup)={skipped_target_count}, HistoryInserted={inserted_history_count}, RowErrors={error_row_count}")
    print("-" * 30)
    print("Merge Summary:")
    print(f"  Records Processed from Source : {processed_count}")
    print(f"  Records Inserted into Target ('{target_db_key}') : {inserted_target_count}")
    print(f"  Records Skipped (Duplicate in Target) : {skipped_target_count}")
    print(f"  Records Saved to AllRuns History : {inserted_history_count}")
    print(f"  Rows with Processing Errors : {error_row_count}")
    print("-" * 30)
    if error_row_count > 0:
         print("WARNING: Some rows encountered errors during processing. Check application log file for details.")
         log.warning(f"{error_row_count} rows encountered processing errors during merge.")

    return success

# === End of GmergerEM.py ===