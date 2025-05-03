# GmergerEM.py
"""
Utility to merge data from an older version database file into a current target database.
Handles schema differences by mapping known columns and providing defaults for new ones.
"""
import sqlite3
import os
import logging
import json
import datetime
import GdbEM # Import for DB connection, hashing, ID generation, init
import GconfigEM # To get target DB filename

log = logging.getLogger(__name__)

# Define columns expected in the LATEST schema (V3 based on last update)
# Must match the order and names used in GdbEM build_combo_db's 'cols' list
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

    Args:
        source_db_path (str): Path to the old database file.
        target_db_key (str): Key ('primary', 'backup', 'test') of the target DB from config.
        source_schema_version (int): The schema version number of the source DB.
    """
    log.info(f"Starting merge from '{source_db_path}' (Schema V{source_schema_version}) into target DB '{target_db_key}'.")

    # --- Validate Inputs ---
    if not os.path.exists(source_db_path):
        log.error(f"Source database file not found: {source_db_path}")
        print(f"Error: Source database file not found: {source_db_path}")
        return False

    db_files = GconfigEM.get_db_filenames()
    target_db_path = db_files.get(target_db_key)
    if not target_db_path:
        log.error(f"Target database key '{target_db_key}' not found in configuration.")
        print(f"Error: Target database '{target_db_key}' not configured.")
        return False

    # Ensure target DB is initialized to latest schema
    try:
        log.info(f"Ensuring target database '{target_db_path}' is initialized to latest schema...")
        GdbEM.initialize_database(target_db_path)
        log.info("Target database schema check/update complete.")
    except Exception as e:
        log.error(f"Failed to initialize/migrate target database '{target_db_path}': {e}", exc_info=True)
        print(f"Error: Could not initialize target database '{target_db_path}': {e}")
        return False

    conn_source = None
    conn_target = None
    processed_count = 0
    inserted_count = 0
    skipped_count = 0
    error_count = 0

    try:
        # --- Connect to Databases ---
        log.debug("Connecting to source and target databases...")
        conn_source = GdbEM.get_db_connection(source_db_path)
        conn_target = GdbEM.get_db_connection(target_db_path)
        # Use dictionary cursor for source for easier column mapping
        conn_source.row_factory = sqlite3.Row # Fetch rows as dict-like objects
        cursor_source = conn_source.cursor()
        cursor_target = conn_target.cursor()

        # --- Introspect Source Schema ---
        log.debug(f"Fetching schema info for source table 'opinions' in {source_db_path}")
        try:
            cursor_source.execute("PRAGMA table_info(opinions);")
            source_columns_info = cursor_source.fetchall()
            source_columns = [row['name'] for row in source_columns_info]
            log.info(f"Source DB contains columns: {source_columns}")
            if not source_columns:
                 raise ValueError("Source database 'opinions' table has no columns or does not exist.")
        except Exception as e:
             log.error(f"Could not read schema from source database '{source_db_path}': {e}", exc_info=True)
             print(f"Error: Could not read table structure from source database.")
             return False


        # --- Read Source Data ---
        log.info("Reading data from source database...")
        # Select only columns that are expected based on source version (or SELECT *)
        # Simpler to SELECT * and handle missing keys during mapping
        cursor_source.execute("SELECT * FROM opinions;")

        # --- Begin Transaction on Target ---
        conn_target.execute("BEGIN;")

        # --- Iterate, Transform, Insert ---
        log.info("Processing and merging records...")
        while True:
            rows = cursor_source.fetchmany(100) # Process in batches
            if not rows:
                break

            for old_row_obj in rows:
                processed_count += 1
                try:
                    old_row = dict(old_row_obj) # Convert row object to dictionary
                    new_row = {}

                    # Map old data to new schema, provide defaults for missing
                    for col in LATEST_SCHEMA_COLS:
                        if col in old_row:
                            new_row[col] = old_row[col]
                        else:
                            # Provide defaults based on column name and source version
                            default_value = None
                            if col == 'entry_method': default_value = f'migrated_v{source_schema_version}'
                            elif col == 'opinionstatus': default_value = 0 # Default to Expected if missing
                            elif col == 'migration_source_version': default_value = source_schema_version
                            elif col in ['validated', 'caseconsolidated', 'recordimpounded', 'DuplicateFlag']: default_value = 0 # Default flags to 0
                            # Add other defaults as needed for future columns
                            new_row[col] = default_value
                            if col not in ['UniqueID', 'DataHash', 'first_scraped_ts', 'last_updated_ts', 'last_validated_run_ts']: # Don't log for generated/timestamp cols
                                log.debug(f"Assigning default '{default_value}' for missing column '{col}' in source record (AppDocket: {old_row.get('AppDocketID','N/A')})")

                    # Regenerate Hash and UniqueID based on potentially modified/defaulted data
                    # Ensure essential fields for hash exist
                    if new_row.get('AppDocketID') and new_row.get('CaseName'):
                        new_row['DataHash'] = GdbEM.generate_data_hash(new_row)
                        new_row['UniqueID'] = GdbEM.generate_unique_id(new_row['DataHash'], new_row['AppDocketID'])
                    else:
                        log.warning(f"Skipping record due to missing AppDocketID/CaseName after mapping: {old_row}")
                        skipped_count += 1
                        continue

                    # Ensure required target fields are not None (adjust as needed)
                    if new_row['UniqueID'] is None or new_row['AppDocketID'] is None or new_row['DataHash'] is None:
                        log.warning(f"Skipping record due to missing UniqueID/AppDocketID/DataHash after mapping: {new_row}")
                        skipped_count += 1
                        continue

                    # Check for duplicate UniqueID in target DB
                    cursor_target.execute("SELECT 1 FROM opinions WHERE UniqueID = ? LIMIT 1", (new_row['UniqueID'],))
                    exists = cursor_target.fetchone()

                    if exists:
                        log.debug(f"Skipping duplicate UniqueID: {new_row['UniqueID']} (AppDocket: {new_row['AppDocketID']})")
                        skipped_count += 1
                    else:
                        # Prepare for Insert (match order of LATEST_SCHEMA_COLS)
                        insert_values = []
                        for col in LATEST_SCHEMA_COLS:
                            # Handle potential missing keys after transformation (shouldn't happen with defaults)
                            insert_values.append(new_row.get(col))

                        cols_str = ", ".join(f'"{c}"' for c in LATEST_SCHEMA_COLS)
                        placeholders = ", ".join(["?"] * len(LATEST_SCHEMA_COLS))
                        insert_sql = f"INSERT INTO opinions ({cols_str}) VALUES ({placeholders})"

                        cursor_target.execute(insert_sql, tuple(insert_values))
                        inserted_count += 1
                        log.debug(f"Inserted record with UniqueID: {new_row['UniqueID']}")

                except Exception as row_err:
                    log.error(f"Error processing row: {dict(old_row) if old_row else 'N/A'} - {row_err}", exc_info=True)
                    error_count += 1

            if processed_count % 1000 == 0: # Log progress
                log.info(f"Processed {processed_count} records so far...")


        # --- Commit Transaction ---
        log.info("Committing merged data to target database...")
        conn_target.commit()
        log.info("Merge complete.")
        success = True

    except sqlite3.Error as e:
        log.error(f"Database error during merge: {e}", exc_info=True)
        print(f"Error: Database error during merge: {e}")
        if conn_target: conn_target.rollback()
    except Exception as e:
        log.error(f"Unexpected error during merge: {e}", exc_info=True)
        print(f"Error: An unexpected error occurred during merge: {e}")
        if conn_target: conn_target.rollback()
    finally:
        if conn_source: conn_source.close()
        if conn_target: conn_target.close()

    log.info(f"Merge Summary: Processed={processed_count}, Inserted={inserted_count}, Skipped(Duplicates)={skipped_count}, Errors={error_count}")
    print(f"Merge Summary: Processed={processed_count}, Inserted={inserted_count}, Skipped(Duplicates)={skipped_count}, Errors={error_count}")
    return success

# === End of GmergerEM.py ===