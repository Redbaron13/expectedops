# GdbEM.py
"""
Handles database interactions. Schema updated with StateAgency columns.
Corrected INSERT statement to include OPJURISAPP.
Added function to retrieve opinions by date and run type.
"""
import sqlite3
import os
import logging
import datetime
import hashlib
import uuid
import GconfigEM # Use config to get DB names

log = logging.getLogger(__name__)

# --- Database Connection (Unchanged) ---
def get_db_connection(db_filename):
    # ... (code remains the same) ...
    if not db_filename:
        log.error("DB filename is missing. Cannot connect.")
        raise ConnectionError("Database filename not provided.")
    db_dir = os.path.dirname(db_filename)
    if db_dir and not os.path.exists(db_dir):
        try:
            os.makedirs(db_dir, exist_ok=True)
            log.info(f"Created directory for database: {db_dir}")
        except OSError as e:
             log.error(f"Failed to create directory {db_dir} for database: {e}", exc_info=True)
             raise ConnectionError(f"Failed to create database directory: {e}") from e
    log.debug(f"Connecting to database: {db_filename}")
    try:
        conn = sqlite3.connect(db_filename, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e:
        log.error(f"Failed to connect to database {db_filename}: {e}", exc_info=True)
        raise ConnectionError(f"Could not connect to database {db_filename}: {e}") from e


# --- Database Initialization (Unchanged) ---
# Updated Schema with LowerCourtVenue, LowerCourtSubCaseType
DB_SCHEMA = '''
CREATE TABLE IF NOT EXISTS opinions (
    UniqueID TEXT PRIMARY KEY,
    AppDocketID TEXT NOT NULL,
    ReleaseDate TEXT,
    DataHash TEXT NOT NULL,
    DuplicateFlag INTEGER DEFAULT 0,
    LinkedDocketIDs TEXT,
    CaseName TEXT,
    LCdocketID TEXT,
    LCCounty TEXT,
    Venue TEXT,
    LowerCourtVenue TEXT,
    LowerCourtSubCaseType TEXT,
    OPJURISAPP TEXT,
    DecisionTypeCode TEXT,
    DecisionTypeText TEXT,
    StateAgency1 TEXT,
    StateAgency2 TEXT,
    CaseNotes TEXT,
    RunType TEXT NOT NULL,
    validated BOOLEAN NOT NULL DEFAULT 0,
    first_scraped_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_validated_run_ts TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_appdocketid_releasedate ON opinions (AppDocketID, ReleaseDate);
CREATE INDEX IF NOT EXISTS idx_datahash ON opinions (DataHash);
CREATE INDEX IF NOT EXISTS idx_runtype ON opinions (RunType);
CREATE INDEX IF NOT EXISTS idx_stateagency1 ON opinions (StateAgency1);
CREATE INDEX IF NOT EXISTS idx_lowercourtvenue ON opinions (LowerCourtVenue);

CREATE TRIGGER IF NOT EXISTS opinions_update_trigger
AFTER UPDATE ON opinions FOR EACH ROW
WHEN OLD.UniqueID = NEW.UniqueID
BEGIN
    UPDATE opinions SET last_updated_ts = CURRENT_TIMESTAMP WHERE UniqueID = OLD.UniqueID;
END;
'''

def initialize_database(db_filename):
    # ... (code remains the same) ...
    log.info(f"Initializing database schema in: {db_filename}")
    conn = None
    try:
        conn = get_db_connection(db_filename)
        cursor = conn.cursor()
        cursor.executescript(DB_SCHEMA)
        conn.commit()
        log.info(f"Database '{db_filename}' schema initialized/verified successfully.")
    except sqlite3.Error as e:
        log.error(f"Database initialization error in '{db_filename}': {e}", exc_info=True)
        print(f"Database initialization error: {e}")
        raise
    except ConnectionError as e:
        log.error(f"Database connection failed during initialization for {db_filename}: {e}")
        print(f"Database connection failed during initialization: {e}")
        raise
    finally:
        if conn:
            conn.close()


def initialize_all_databases():
    # ... (code remains the same) ...
    log.info("Initializing schemas for all configured databases...")
    db_files = GconfigEM.get_db_filenames()
    initialized_dbs = []
    failed_dbs = []
    for db_type, db_filename in db_files.items():
        if not db_filename:
            log.warning(f"Skipping initialization for '{db_type}' as filename is missing in config.")
            continue
        try:
            initialize_database(db_filename)
            initialized_dbs.append(db_filename)
        except Exception as e:
            log.error(f"Failed to initialize database '{db_filename}' ({db_type}): {e}")
            failed_dbs.append(db_filename)
    if failed_dbs:
         log.error(f"Failed to initialize the following databases: {', '.join(failed_dbs)}")
    if initialized_dbs:
         log.info(f"Successfully initialized/verified schemas for: {', '.join(initialized_dbs)}")


# --- Data Handling Helpers (Unchanged) ---
def generate_data_hash(opinion_data):
    # ... (code remains the same) ...
    core_data_str = (
        f"{opinion_data.get('AppDocketID', '')}|"
        f"{opinion_data.get('ReleaseDate', '')}|"
        f"{opinion_data.get('CaseName', '')}|"
        f"{opinion_data.get('DecisionTypeCode', '')}|"
        f"{opinion_data.get('Venue', '')}"
    )
    return hashlib.sha256(core_data_str.encode('utf-8')).hexdigest()

def generate_unique_id(data_hash, run_type):
    # ... (code remains the same) ...
    namespace = uuid.NAMESPACE_DNS
    base_uuid = uuid.uuid5(namespace, data_hash)
    unique_id_str = str(base_uuid)
    return unique_id_str

def check_duplicate_by_hash(cursor, data_hash):
    # ... (code remains the same) ...
    cursor.execute("SELECT 1 FROM opinions WHERE DataHash = ?", (data_hash,))
    return cursor.fetchone() is not None

# --- Main Data Insertion/Update Logic (Unchanged) ---
def add_or_update_opinion_to_db(db_filename, opinion_data, is_validated, run_type):
    # ... (code remains the same) ...
    required_keys = ["AppDocketID", "CaseName"]
    if not all(key in opinion_data and opinion_data.get(key) is not None for key in required_keys):
        log.warning(f"Skipping entry for DB '{db_filename}' due to missing required keys: {opinion_data.get('AppDocketID')}")
        return "error_missing_keys"

    status = "error_unknown"
    conn = None
    app_docket_id = opinion_data["AppDocketID"]

    try:
        conn = get_db_connection(db_filename)
        cursor = conn.cursor()

        data_hash = generate_data_hash(opinion_data)
        unique_id = generate_unique_id(data_hash, run_type)
        hash_exists = check_duplicate_by_hash(cursor, data_hash)
        duplicate_flag = 1 if hash_exists else 0

        cursor.execute("SELECT * FROM opinions WHERE UniqueID = ?", (unique_id,))
        existing_opinion = cursor.fetchone()
        now_ts = datetime.datetime.now()

        if existing_opinion is None:
            log.info(f"Inserting new opinion (UniqueID: {unique_id[:8]}..., AppDocket: {app_docket_id}) into {db_filename}.")
            sql = '''
                INSERT INTO opinions (
                    UniqueID, AppDocketID, ReleaseDate, DataHash, DuplicateFlag, LinkedDocketIDs,
                    CaseName, LCdocketID, LCCounty, Venue, LowerCourtVenue, LowerCourtSubCaseType,
                    OPJURISAPP, DecisionTypeCode, DecisionTypeText, StateAgency1, StateAgency2,
                    CaseNotes, RunType, validated, last_validated_run_ts
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            values = (
                unique_id, opinion_data.get("AppDocketID"), opinion_data.get("ReleaseDate"), data_hash, duplicate_flag,
                opinion_data.get("LinkedDocketIDs", ""), opinion_data.get("CaseName"), opinion_data.get("LCdocketID", ""),
                opinion_data.get("LCCounty", ""), opinion_data.get("Venue"),
                opinion_data.get("LowerCourtVenue"), opinion_data.get("LowerCourtSubCaseType"),
                opinion_data.get("OPJURISAPP", ""), opinion_data.get("DecisionTypeCode"),
                opinion_data.get("DecisionTypeText"), opinion_data.get("StateAgency1"),
                opinion_data.get("StateAgency2"), opinion_data.get("CaseNotes", ""),
                run_type, is_validated, now_ts if is_validated else None
            )
            try:
                 cursor.execute(sql, values)
                 status = "inserted"
            except sqlite3.IntegrityError:
                 log.warning(f"Integrity error (likely UniqueID {unique_id[:8]}... collision) on insert for {app_docket_id} in {db_filename}. Skipping.")
                 status = "skipped_integrity_error_on_insert"
            except sqlite3.OperationalError as oe:
                 log.error(f"Operational error during INSERT for {app_docket_id} in {db_filename}: {oe}", exc_info=True)
                 status = f"error_operational_insert_{db_filename}"
                 raise oe

        else:
            log.info(f"Found existing opinion (UniqueID: {unique_id[:8]}...) for {app_docket_id} in {db_filename}. Comparing metadata.")
            update_fields = {}
            existing_validated = bool(existing_opinion['validated'])
            if is_validated and not existing_validated:
                update_fields['validated'] = True
                update_fields['last_validated_run_ts'] = now_ts
                log.debug(f"Updating validated status to True for {unique_id[:8]}...")
            if existing_opinion['RunType'] != run_type:
                update_fields['RunType'] = run_type
                log.debug(f"Updating RunType to '{run_type}' for {unique_id[:8]}...")
            if existing_opinion['DuplicateFlag'] != duplicate_flag:
                 update_fields['DuplicateFlag'] = duplicate_flag
                 log.debug(f"Updating DuplicateFlag to '{duplicate_flag}' for {unique_id[:8]}...")
            # Compare other fields...
            if opinion_data.get("StateAgency1") != existing_opinion.get("StateAgency1"): update_fields['StateAgency1'] = opinion_data.get("StateAgency1")
            if opinion_data.get("StateAgency2") != existing_opinion.get("StateAgency2"): update_fields['StateAgency2'] = opinion_data.get("StateAgency2")
            if opinion_data.get("LowerCourtVenue") != existing_opinion.get("LowerCourtVenue"): update_fields['LowerCourtVenue'] = opinion_data.get("LowerCourtVenue")
            if opinion_data.get("LowerCourtSubCaseType") != existing_opinion.get("LowerCourtSubCaseType"): update_fields['LowerCourtSubCaseType'] = opinion_data.get("LowerCourtSubCaseType")
            if opinion_data.get("OPJURISAPP") != existing_opinion.get("OPJURISAPP"): update_fields['OPJURISAPP'] = opinion_data.get("OPJURISAPP")

            if update_fields:
                set_clauses = ", ".join([f"{col} = ?" for col in update_fields.keys()])
                sql_values = list(update_fields.values()) + [unique_id]
                sql = f"UPDATE opinions SET {set_clauses}, last_updated_ts = CURRENT_TIMESTAMP WHERE UniqueID = ?"
                log.debug(f"Updating metadata for {unique_id[:8]}... SQL: {sql} Values: {sql_values}")
                cursor.execute(sql, tuple(sql_values))
                status = "updated_metadata"
            else:
                status = "skipped_no_changes"
                log.info(f"Skipping update for {unique_id[:8]}... in {db_filename}, no metadata changes needed.")

        conn.commit()

    # ... (Error handling and finally block remain the same) ...
    except sqlite3.Error as e:
        log.error(f"Database error processing {app_docket_id} in {db_filename}: {e}", exc_info=True)
        if conn: conn.rollback()
        status = f"error_sqlite_{db_filename}"
    except ConnectionError as e:
        log.error(f"Database connection failed processing {app_docket_id} in {db_filename}: {e}")
        status = f"error_connection_{db_filename}"
    except Exception as e:
        log.error(f"Unexpected error processing {app_docket_id} in {db_filename}: {e}", exc_info=True)
        if conn: conn.rollback()
        status = f"error_unexpected_{db_filename}"
    finally:
        if conn:
            conn.close()
            log.debug(f"Database connection closed for {app_docket_id} processing in {db_filename}.")

    return status

# --- Save Opinions to DBs (Unchanged) ---
def save_opinions_to_dbs(opinion_list, is_validated, run_type):
    # ... (code remains the same) ...
    db_files = GconfigEM.get_db_filenames()
    primary_db = db_files.get("primary")
    all_runs_db = db_files.get("all_runs")
    results = {"primary": {"total": 0, "inserted": 0, "updated": 0, "skipped": 0, "error": 0},
               "all_runs": {"total": 0, "inserted": 0, "updated": 0, "skipped": 0, "error": 0}}
    dbs_to_update = []
    if primary_db: dbs_to_update.append(("primary", primary_db))
    else: log.error("Primary DB filename not configured.")
    if all_runs_db: dbs_to_update.append(("all_runs", all_runs_db))
    else: log.error("All_Runs DB filename not configured.")
    if not dbs_to_update: return results
    for opinion in opinion_list:
         for db_key, db_filename in dbs_to_update:
             results[db_key]["total"] += 1
             status = add_or_update_opinion_to_db(db_filename, opinion, is_validated, run_type)
             if status == "inserted": results[db_key]["inserted"] += 1
             elif status.startswith("updated"): results[db_key]["updated"] += 1
             elif status.startswith("skipped"): results[db_key]["skipped"] += 1
             else: results[db_key]["error"] += 1
    for db_key, result_counts in results.items():
         if result_counts["total"] > 0:
              log.info(f"DB '{db_key}' ({db_files.get(db_key)}): Processed {result_counts['total']} entries -> "
                       f"Inserted: {result_counts['inserted']}, Updated: {result_counts['updated']}, "
                       f"Skipped: {result_counts['skipped']}, Errors: {result_counts['error']}")
    return results

# --- New Function to Get Previous Run Data ---
def get_opinions_by_date_runtype(db_filename, release_date, run_type):
    """Fetches opinions matching a specific release date and run type."""
    log.debug(f"Fetching opinions from {db_filename} for ReleaseDate={release_date}, RunType={run_type}")
    opinions = {} # Use AppDocketID as key for easy lookup
    conn = None
    try:
        conn = get_db_connection(db_filename)
        cursor = conn.cursor()
        # Fetch all relevant columns needed for comparison or context
        cursor.execute("""
            SELECT *
            FROM opinions
            WHERE ReleaseDate = ? AND RunType = ?
        """, (release_date, run_type))
        rows = cursor.fetchall()
        for row in rows:
            # Convert sqlite3.Row to a dictionary
            row_dict = dict(row)
            app_docket_id = row_dict.get("AppDocketID")
            if app_docket_id:
                # Store the most recent record if multiple exist for same AppDocketID/Date/RunType (shouldn't happen with UniqueID)
                 opinions[app_docket_id] = row_dict
            else:
                 log.warning(f"Record found with missing AppDocketID in {db_filename} for date {release_date}, run {run_type}. Skipping.")
        log.debug(f"Found {len(opinions)} records for {release_date} / {run_type} in {db_filename}")
    except sqlite3.Error as e:
        log.error(f"Error fetching opinions for {release_date}/{run_type} from {db_filename}: {e}", exc_info=True)
    except ConnectionError as e:
         log.error(f"Connection error fetching opinions from {db_filename}: {e}")
    finally:
        if conn:
            conn.close()
    return opinions


# --- Status Functions (Placeholder) ---
def get_db_stats(db_filename):
    log.warning(f"get_db_stats not implemented yet for {db_filename}.")
    return {"total": "N/I", "unvalidated": "N/I"}

# === End of GdbEM.py ===