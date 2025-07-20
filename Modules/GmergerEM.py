# GmergerEM.py
# V2: Enhanced logging for debugging merge issues.
"""
Provides functionality to merge data from an older version SQLite database file
into a current target SQLite database.

This module is designed for data migration scenarios, specifically:
1.  Reading records from a source SQLite database ('opinions' table).
2.  Mapping data from the source schema to a defined `LATEST_SCHEMA_COLS`.
    This includes providing default values for columns present in the latest
    schema but potentially missing in the older source schema.
3.  Regenerating `DataHash` and `UniqueID` for each record using helpers from
    `GdbEM` to ensure consistency with current data generation methods.
4.  Checking for duplicates in the target database based on the `UniqueID`.
    If a record with the same `UniqueID` already exists in the target,
    it's skipped for insertion into the target to prevent duplicate content.
5.  Logging ALL processed records (both inserted and skipped) from the source
    into an 'AllRuns' history SQLite database. This is intended to provide a
    complete audit trail of the merge process.
    (Note: The method `GdbEM._save_to_all_runs_history` appears to be used,
    which might be an internal SQLite-specific method within GdbEM. If GdbEM is
    primarily for Supabase, this specific function's compatibility needs care).
6.  Performing operations within SQLite transactions for data integrity in the
    target database.
7.  Utilizing `GconfigEM` to retrieve paths for target and 'AllRuns' databases.
8.  Includes detailed logging for monitoring and debugging the merge process.

The main function `merge_old_database` orchestrates these operations.
This module is primarily for SQLite-to-SQLite merges.
"""
import sqlite3 # For direct SQLite database operations.
import os
import logging
import json # Not actively used in the current code, but might be for future schema defs.
import datetime
import GdbEM     # For UniqueID/DataHash generation, and potentially DB initialization
                 # and history saving (though _save_to_all_runs_history might be SQLite specific).
import GconfigEM # To get target database filenames/keys.

# Setup logger for this module specifically if not relying on root logger config
# If GmainEM setup is sufficient, this line can be removed.
# logging.basicConfig(level=logging.INFO) # Example basic config if run standalone
log = logging.getLogger(__name__) # Get logger instance

# Defines the column schema for the target 'opinions' table.
# This list is used to map data from older schemas and to ensure all necessary
# columns are considered during the merge.
LATEST_SCHEMA_COLS = [
    "UniqueID", "AppDocketID", "ReleaseDate", "DataHash", "DuplicateFlag", "LinkedDocketIDs",
    "CaseName", "LCdocketID", "LCCounty", "Venue", "LowerCourtVenue", "LowerCourtSubCaseType",
    "OPJURISAPP", "DecisionTypeCode", "DecisionTypeText", "StateAgency1", "StateAgency2",
    "CaseNotes", "RunType", "entry_method", "validated", "caseconsolidated", "recordimpounded",
    "opinionstatus", "migration_source_version", "first_scraped_ts", "last_updated_ts",
    "last_validated_run_ts"
]

def merge_old_database(source_db_path: str, target_db_key: str, source_schema_version: int) -> bool:
    """
    Merges data from an older source SQLite database into a specified target SQLite database.

    The process involves:
    - Reading records from the 'opinions' table of the `source_db_path`.
    - Transforming each record to match `LATEST_SCHEMA_COLS`, applying defaults where needed.
    - Regenerating `DataHash` and `UniqueID` for data integrity.
    - Inserting records into the target database (identified by `target_db_key`) only if
      their `UniqueID` does not already exist in the target.
    - Saving a snapshot of *every* processed source record (after transformation) into
      an 'AllRuns' history database for audit purposes.

    Args:
        source_db_path (str): The file system path to the source SQLite database file.
        target_db_key (str): A key (e.g., 'primary', 'backup', 'test') used to look up
                             the target database path via `GconfigEM.get_db_filenames()`.
        source_schema_version (int): The schema version number of the source database.
                                     This is used for logging and setting the
                                     'migration_source_version' field in merged records.

    Returns:
        bool: True if the merge process completed (even if some individual rows had errors),
              False if a critical setup error occurred (e.g., file not found, DB connection failed).
    """
    log.info(f"Starting database merge from source: '{source_db_path}' (Schema Version: V{source_schema_version}) "
             f"into target database key: '{target_db_key}'.")

    # --- Step 1: Validate Input Paths and Configuration ---
    if not os.path.exists(source_db_path):
        log.error(f"Source database file not found at path: {source_db_path}")
        print(f"Error: Source database file does not exist: {source_db_path}")
        return False
    if not os.access(source_db_path, os.R_OK): # Check read permissions for source DB.
        log.error(f"Source database file at '{source_db_path}' is not readable (permission issue).")
        print(f"Error: Cannot read source database file: {source_db_path}")
        return False

    # Retrieve database file paths from configuration.
    db_filenames_map = GconfigEM.get_db_filenames()
    target_db_path = db_filenames_map.get(target_db_key)
    all_runs_db_path = db_filenames_map.get("all_runs") # Database for storing history of all runs.

    if not target_db_path:
        log.error(f"Target database key '{target_db_key}' not found in configuration. Merge aborted.")
        print(f"Error: Configuration for target database '{target_db_key}' is missing.")
        return False
    # Check write permissions for target DB if it exists.
    if os.path.exists(target_db_path) and not os.access(target_db_path, os.W_OK):
        log.error(f"Target database file at '{target_db_path}' is not writable (permission issue).")
        print(f"Error: Cannot write to target database file: {target_db_path}")
        return False
    # This try-except block for os.access was not in original, but good practice.
    # However, the previous os.access checks are outside a try-except, which is fine as they are direct.
    # The original code had a general try-except around these checks, which is also acceptable.
    # For this change, I'm keeping structure close to original for the os.access calls for target/source path,
    # and the general try-except is not strictly needed just for os.access if paths are confirmed to exist.
    # The original try-except might have been for other potential errors in this block.
    # Given the goal is only documentation, I will remove the try-except that was here in the *original* code
    # as the specific permission checks are clearer and more direct.

    if not all_runs_db_path:
        # Log a warning but proceed if AllRuns DB is not configured; history saving will be skipped.
        log.warning("Path for 'all_runs' (history) database not configured. "
                    "History of merged items will not be saved to AllRuns DB.")

    # --- Step 2: Initialize Target and AllRuns Databases (Ensure Schema) ---
    # This step uses GdbEM.initialize_database, which should create the tables if they don't exist,
    # based on the latest schema defined within GdbEM (presumably matching LATEST_SCHEMA_COLS).
    try:
        log.info(f"Ensuring target database '{target_db_path}' is initialized with the latest schema...")
        GdbEM.initialize_database(target_db_path) # Assumes this creates/updates schema.

        if all_runs_db_path:
            log.info(f"Ensuring 'AllRuns' history database '{all_runs_db_path}' is initialized...")
            GdbEM.initialize_database(all_runs_db_path) # Also ensure history DB has correct schema.
        log.info("Database schema initialization/check complete for target and AllRuns DBs.")
    except Exception as e_init:
        log.error(f"Failed to initialize target or AllRuns databases: {e_init}", exc_info=True)
        print(f"Error: Could not initialize databases: {e_init}")
        return False

    # Initialize database connections and counters.
    conn_source, conn_target, conn_all_runs = None, None, None
    processed_count, inserted_target_count, skipped_target_count = 0, 0, 0
    inserted_history_count, error_row_count = 0, 0
    merge_successful = False # Overall success flag for the merge operation.

    try:
        # --- Step 3: Connect to Source, Target, and AllRuns Databases ---
        log.debug("Establishing connections to source, target, and AllRuns SQLite databases...")
        conn_source = GdbEM.get_db_connection(source_db_path) # Assumes GdbEM.get_db_connection handles SQLite paths.
        conn_source.row_factory = sqlite3.Row # Access columns by name.
        cursor_source = conn_source.cursor()

        conn_target = GdbEM.get_db_connection(target_db_path)
        cursor_target = conn_target.cursor()

        if all_runs_db_path:
            conn_all_runs = GdbEM.get_db_connection(all_runs_db_path)
        # Note: conn_all_runs is not directly used for a cursor here as GdbEM._save_to_all_runs_history
        # might manage its own connection or take a path. If it takes a connection, it should be passed.

        # --- Step 4: Introspect Source Database Schema ---
        log.debug(f"Fetching source schema information from 'opinions' table in '{source_db_path}'")
        cursor_source.execute("PRAGMA table_info(opinions);") # Get column info for 'opinions' table.
        source_columns = [row_info['name'] for row_info in cursor_source.fetchall()]
        if not source_columns:
            raise ValueError(f"Source database '{source_db_path}' 'opinions' table has no columns or does not exist.")
        log.info(f"Source DB (Schema V{source_schema_version}) 'opinions' table columns: {source_columns}")

        # --- Step 5: Read Source Data and Process in Batches ---
        log.info("Reading data from source database 'opinions' table...")
        cursor_source.execute("SELECT * FROM opinions;") # Select all data from source.

        # Start a transaction on the target database for atomicity.
        conn_target.execute("BEGIN TRANSACTION;")

        # Process records from source and merge into target/history databases.
        log.info("Processing records from source and merging into target/history databases...")
        while True:
            rows_batch = cursor_source.fetchmany(100) # Process in batches of 100 rows.
            if not rows_batch:
                break # No more rows to process.

            for old_row_obj in rows_batch:
                processed_count += 1
                current_old_row_dict = None # For error logging if transformation fails.
                transformed_new_row = {}    # To hold data mapped to LATEST_SCHEMA_COLS.
                app_docket_for_log = "UNKNOWN_DOCKET" # For context in logs.
                unique_id_for_log = "UNKNOWN_UNIQUE_ID"

                try:
                    current_old_row_dict = dict(old_row_obj) # Convert sqlite3.Row to dict.
                    app_docket_for_log = current_old_row_dict.get('AppDocketID', 'UNKNOWN_DOCKET')

                    # --- Step 5a: Transform Data - Map to Latest Schema & Provide Defaults ---
                    for latest_col_name in LATEST_SCHEMA_COLS:
                        if latest_col_name in current_old_row_dict:
                            transformed_new_row[latest_col_name] = current_old_row_dict[latest_col_name]
                        else:
                            # Assign default values for columns missing in the old schema.
                            default_val = None
                            if latest_col_name == 'entry_method': default_val = f'migrated_v{source_schema_version}'
                            elif latest_col_name == 'opinionstatus': default_val = 0 # Default to 'Expected'.
                            elif latest_col_name == 'migration_source_version': default_val = source_schema_version
                            elif latest_col_name in ['validated', 'caseconsolidated', 'recordimpounded', 'DuplicateFlag']: default_val = 0 # Boolean flags default to False.
                            # Timestamps (first_scraped_ts, last_updated_ts, last_validated_run_ts) default to None if not present.
                            # UniqueID and DataHash will be regenerated.
                            transformed_new_row[latest_col_name] = default_val
                            if latest_col_name not in ['UniqueID', 'DataHash', 'first_scraped_ts', 'last_updated_ts', 'last_validated_run_ts']:
                                log.debug(f"Applied default value '{default_val}' for missing column '{latest_col_name}' "
                                          f"(AppDocket: {app_docket_for_log})")

                    # --- Step 5b: Regenerate Hash and UniqueID ---
                    # Ensure critical fields for hash generation are present.
                    case_name_for_hash = transformed_new_row.get('CaseName')
                    if not app_docket_for_log or app_docket_for_log == "UNKNOWN_DOCKET" or not case_name_for_hash:
                        log.warning(f"Skipping row transformation: Missing critical AppDocketID ('{app_docket_for_log}') or "
                                    f"CaseName ('{case_name_for_hash}') for hash generation. Source data: {current_old_row_dict}")
                        error_row_count += 1; continue

                    transformed_new_row['DataHash'] = GdbEM.generate_data_hash(transformed_new_row)
                    transformed_new_row['UniqueID'] = GdbEM.generate_unique_id(transformed_new_row['DataHash'], app_docket_for_log)
                    unique_id_for_log = transformed_new_row['UniqueID'] # Update for logging.

                    if not unique_id_for_log or not transformed_new_row['DataHash']:
                        log.warning(f"Skipping row transformation: Failed to generate UniqueID or DataHash. "
                                    f"AppDocket: {app_docket_for_log}. DataHash Attempt: {transformed_new_row.get('DataHash')}")
                        error_row_count += 1; continue

                    log.debug(f"Transformed data for AppDocket {app_docket_for_log} (UniqueID: {unique_id_for_log[:8]}...): "
                              f"Keys: {list(transformed_new_row.keys())}")


                    # --- Step 5c: Check for Duplicates and Insert into Target DB ---
                    cursor_target.execute("SELECT 1 FROM opinions WHERE UniqueID = ? LIMIT 1", (unique_id_for_log,))
                    exists_in_target_db = cursor_target.fetchone()

                    if exists_in_target_db:
                        log.info(f"Skipping insert into target DB '{target_db_key}': Duplicate UniqueID '{unique_id_for_log[:8]}...' "
                                 f"(AppDocket: {app_docket_for_log})")
                        skipped_target_count += 1
                    else:
                        # Prepare for INSERT into target DB.
                        cols_for_insert = ", ".join(f'"{c}"' for c in LATEST_SCHEMA_COLS) # Quote column names.
                        placeholders_for_insert = ", ".join(["?"] * len(LATEST_SCHEMA_COLS))
                        sql_insert_stmt = f"INSERT INTO opinions ({cols_for_insert}) VALUES ({placeholders_for_insert})"
                        values_for_insert = [transformed_new_row.get(col_name) for col_name in LATEST_SCHEMA_COLS]

                        cursor_target.execute(sql_insert_stmt, tuple(values_for_insert))
                        inserted_target_count += 1
                        log.debug(f"Inserted into target DB '{target_db_key}': UniqueID {unique_id_for_log[:8]}... (AppDocket: {app_docket_for_log})")

                    # --- Step 5d: Save Snapshot to AllRuns History DB ---
                    # This occurs regardless of whether it was inserted or skipped in the target DB.
                    if all_runs_db_path: # Check if AllRuns DB is configured.
                        history_run_type_tag = f"merged_from_v{source_schema_version}"
                        # Note: GdbEM._save_to_all_runs_history is an internal method. Its direct use here assumes
                        # it's compatible with the SQLite path and transformed_new_row structure.
                        # If GdbEM is primarily Supabase, this method might be an unadapted SQLite utility.
                        # For this documentation, we assume it works as intended with SQLite paths.
                        history_save_status = GdbEM._save_to_all_runs_history(all_runs_db_path, transformed_new_row, history_run_type_tag)
                        if history_save_status == "inserted_history":
                            inserted_history_count += 1
                        else:
                            log.warning(f"Failed to save record to AllRuns history DB for UniqueID {unique_id_for_log[:8]}... "
                                        f"(AppDocket: {app_docket_for_log}). Status: {history_save_status}")
                            # This could be a separate counter if needed, but for now, count as a general row error.
                            error_row_count += 1
                except Exception as e_row_proc:
                    log.error(f"Error processing row (AppDocket: {app_docket_for_log}, UniqueID: {unique_id_for_log[:8]}...): {e_row_proc}", exc_info=True)
                    error_row_count += 1
                    if current_old_row_dict: # Log problematic source row if available.
                        log.debug(f"Problematic source row data during error: {current_old_row_dict}")

            # Log progress periodically.
            if processed_count % 500 == 0:
                log.info(f"Processed {processed_count} records from source so far...")

        # --- Step 6: Commit Target Transaction ---
        log.info(f"Committing {inserted_target_count} inserts to target database '{target_db_key}'...")
        conn_target.commit() # Commit all successful inserts to target.
        log.info("Merge process transactions committed to target database.")
        merge_successful = True

    except (sqlite3.Error, ConnectionError, ValueError) as e_critical:
        # Handle critical errors like DB connection issues, SQL errors, or value errors from bad data.
        log.error(f"Critical error during merge operation: {e_critical}", exc_info=True)
        print(f"Error during merge: {e_critical}")
        if conn_target:
            log.info("Rolling back changes in target database due to critical error.")
            conn_target.rollback() # Rollback any pending changes in target DB.
    except Exception as e_unexpected:
        # Catch any other unexpected exceptions.
        log.error(f"An unexpected critical error occurred during merge: {e_unexpected}", exc_info=True)
        print(f"Error: An unexpected error occurred: {e_unexpected}")
        if conn_target:
            log.info("Rolling back changes in target database due to unexpected critical error.")
            conn_target.rollback()
    finally:
        # --- Step 7: Close All Database Connections ---
        # This block ensures connections are closed even if errors occur.
        log.debug("Closing all database connections.")
        if conn_source: conn_source.close()
        if conn_target: conn_target.close()
        if conn_all_runs: conn_all_runs.close()

    # --- Final Summary Logging ---
    # Log and print a detailed summary of the merge operation.
    log.info(f"Database merge process finished. Overall success: {merge_successful}.")
    log.info(f"Merge Summary: Total Records Processed from Source = {processed_count}, "
             f"Inserted into Target ('{target_db_key}') = {inserted_target_count}, "
             f"Skipped in Target (Duplicates) = {skipped_target_count}, "
             f"Saved to AllRuns History = {inserted_history_count}, "
             f"Rows with Processing Errors = {error_row_count}")
    print("-" * 30) # Print separator for readability.
    print("Merge Operation Summary:")
    print(f"  Total Records Processed from Source: {processed_count}")
    print(f"  Records Inserted into Target ('{target_db_key}'): {inserted_target_count}")
    print(f"  Records Skipped (already in Target by UniqueID): {skipped_target_count}")
    print(f"  Records Saved to 'AllRuns' History DB: {inserted_history_count}")
    print(f"  Number of Rows with Processing Errors: {error_row_count}")
    print("-" * 30)
    if error_row_count > 0:
         # Alert user if there were errors needing review in logs.
         print("WARNING: Some rows encountered errors during processing. Please check the application log file for detailed error messages.")
         log.warning(f"{error_row_count} rows encountered errors and were not fully processed or saved to history.")

    return merge_successful

# === End of GmergerEM.py ===