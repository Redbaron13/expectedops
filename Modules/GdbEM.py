# GdbEM.py
# V4: Fixed initialization order error for triggers/columns
"""
Handles database interactions.
- Main schema (opinions table) updated with 'entry_method'.
- Implements append-only history logic for GAllRunsOpinionsEM.db using 'opinion_history' table.
- Adds function to build GComboEM.db from primary and backup.
- Handles different DB schemas during initialization.
- Updates insertion/update logic for entry_method and validated record protection.
- V4 Fix: Separated table creation from index/trigger creation in initialization.
"""
import sqlite3
import os
import logging
import datetime
import hashlib
import uuid
import json # For storing opinion data in history
import GconfigEM

log = logging.getLogger(__name__)

# --- Database Schemas ---

# Schema parts for Primary/Backup/Test DBs
OPINIONS_TABLE_DEF = '''
CREATE TABLE IF NOT EXISTS opinions (
    UniqueID TEXT PRIMARY KEY, -- Based on core data hash
    AppDocketID TEXT NOT NULL,
    ReleaseDate TEXT,
    DataHash TEXT NOT NULL,    -- Hash of core data fields for quick comparison
    DuplicateFlag INTEGER DEFAULT 0, -- Flag if DataHash seen before (in this DB)
    LinkedDocketIDs TEXT,
    CaseName TEXT,
    LCdocketID TEXT,
    LCCounty TEXT,
    Venue TEXT,                 -- Venue of the opinion (Supreme, Appellate, Trial, Tax)
    LowerCourtVenue TEXT,       -- Venue of the lower court/agency being appealed
    LowerCourtSubCaseType TEXT, -- Subtype of the lower court/agency
    OPJURISAPP TEXT,
    DecisionTypeCode TEXT,
    DecisionTypeText TEXT,
    StateAgency1 TEXT,
    StateAgency2 TEXT,
    CaseNotes TEXT,
    RunType TEXT NOT NULL,      -- Identifier for the run (e.g., scheduled-primary-1, manual-test)
    entry_method TEXT,          -- How the record was added/validated (e.g., scheduled, user_validated)
    validated BOOLEAN NOT NULL DEFAULT 0,
    caseconsolidated INTEGER DEFAULT 0,
    recordimpounded INTEGER DEFAULT 0,
    first_scraped_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_validated_run_ts TIMESTAMP
);
'''

OPINIONS_INDEXES_TRIGGER = '''
CREATE INDEX IF NOT EXISTS idx_opinions_appdocketid_releasedate ON opinions (AppDocketID, ReleaseDate);
CREATE INDEX IF NOT EXISTS idx_opinions_datahash ON opinions (DataHash);
CREATE INDEX IF NOT EXISTS idx_opinions_runtype ON opinions (RunType);
CREATE INDEX IF NOT EXISTS idx_opinions_validated ON opinions (validated);
CREATE INDEX IF NOT EXISTS idx_opinions_entrymethod ON opinions (entry_method);

-- Update last_updated_ts whenever a row is updated
CREATE TRIGGER IF NOT EXISTS trg_opinions_update_timestamp
AFTER UPDATE ON opinions FOR EACH ROW
WHEN OLD.UniqueID = NEW.UniqueID -- Trigger references columns, must exist
BEGIN
    UPDATE opinions SET last_updated_ts = CURRENT_TIMESTAMP WHERE UniqueID = OLD.UniqueID;
END;
'''

# Schema for GAllRunsOpinionsEM.db (History Table)
# (Unchanged from previous version)
ALL_RUNS_SCHEMA = '''
CREATE TABLE IF NOT EXISTS opinion_history (
    HistoryID INTEGER PRIMARY KEY AUTOINCREMENT,
    UniqueID TEXT NOT NULL,
    AppDocketID TEXT NOT NULL,
    ReleaseDate TEXT,
    RunType TEXT NOT NULL,
    ScrapeTimestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    OpinionDataJSON TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_history_uniqueid ON opinion_history (UniqueID);
CREATE INDEX IF NOT EXISTS idx_history_appdocketid ON opinion_history (AppDocketID);
CREATE INDEX IF NOT EXISTS idx_history_releasedate ON opinion_history (ReleaseDate);
CREATE INDEX IF NOT EXISTS idx_history_runtype ON opinion_history (RunType);
CREATE INDEX IF NOT EXISTS idx_history_scrapetimestamp ON opinion_history (ScrapeTimestamp);
'''

# Schema for GComboEM.db (Combined View)
# (Unchanged from previous version)
COMBO_SCHEMA = '''
CREATE TABLE IF NOT EXISTS combined_opinions (
    SourceDB TEXT NOT NULL, -- 'primary' or 'backup'
    UniqueID TEXT, AppDocketID TEXT, ReleaseDate TEXT, DataHash TEXT, DuplicateFlag INTEGER,
    LinkedDocketIDs TEXT, CaseName TEXT, LCdocketID TEXT, LCCounty TEXT, Venue TEXT,
    LowerCourtVenue TEXT, LowerCourtSubCaseType TEXT, OPJURISAPP TEXT, DecisionTypeCode TEXT,
    DecisionTypeText TEXT, StateAgency1 TEXT, StateAgency2 TEXT, CaseNotes TEXT, RunType TEXT,
    entry_method TEXT, validated BOOLEAN, caseconsolidated INTEGER, recordimpounded INTEGER,
    first_scraped_ts TIMESTAMP, last_updated_ts TIMESTAMP, last_validated_run_ts TIMESTAMP,
    PRIMARY KEY (SourceDB, UniqueID)
);
CREATE INDEX IF NOT EXISTS idx_combo_uniqueid ON combined_opinions (UniqueID);
CREATE INDEX IF NOT EXISTS idx_combo_appdocketid ON combined_opinions (AppDocketID);
CREATE INDEX IF NOT EXISTS idx_combo_sourcedb ON combined_opinions (SourceDB);
'''


# --- Database Connection (Unchanged) ---
def get_db_connection(db_filename):
    """Gets a connection to the specified SQLite database file."""
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


# --- Database Initialization (Updated) ---
def initialize_database(db_filename):
    """Initializes the database schema based on the filename convention."""
    log.info(f"Initializing database schema in: {db_filename}")

    is_opinions_schema = False
    schema_to_use = None
    post_create_sql = None # SQL to run after table creation
    db_basename = os.path.basename(db_filename)

    # Determine schema based on filename
    db_config_files = GconfigEM.DEFAULT_DB_NAMES # Get default names for comparison
    if db_basename == db_config_files.get("all_runs"):
        schema_to_use = ALL_RUNS_SCHEMA
        log.info(f"Applying 'opinion_history' schema to {db_filename}.")
    elif db_basename == db_config_files.get("combo"):
        schema_to_use = COMBO_SCHEMA
        log.info(f"Applying 'combined_opinions' schema to {db_filename}.")
    elif db_basename in [db_config_files.get("primary"),
                         db_config_files.get("backup"),
                         db_config_files.get("test")]:
        schema_to_use = OPINIONS_TABLE_DEF # Only table definition
        post_create_sql = OPINIONS_INDEXES_TRIGGER # Indexes/Trigger run after
        is_opinions_schema = True
        log.info(f"Applying 'opinions' table schema to {db_filename}.")
    else:
        log.warning(f"Unknown database filename convention: {db_basename}. Applying standard 'opinions' schema as default.")
        schema_to_use = OPINIONS_TABLE_DEF # Default schema (table only)
        post_create_sql = OPINIONS_INDEXES_TRIGGER
        is_opinions_schema = True # Assume it's opinions table

    conn = None
    try:
        conn = get_db_connection(db_filename)
        cursor = conn.cursor()

        # Execute main table creation/schema script
        log.debug(f"Executing main schema script for {db_filename}...")
        cursor.executescript(schema_to_use)
        conn.commit() # Commit table creation/changes first
        log.debug("Main schema script committed.")

        # If there's post-creation SQL (indexes/triggers for opinions table), run it now
        if post_create_sql:
            log.debug(f"Executing post-creation SQL (indexes/triggers) for {db_filename}...")
            cursor.executescript(post_create_sql)
            conn.commit() # Commit indexes/triggers
            log.debug("Post-creation SQL committed.")

        log.info(f"Database '{db_filename}' schema initialized/verified successfully.")

    except sqlite3.Error as e:
        # Log the specific SQL part that failed if possible (less easy with executescript)
        log.error(f"Database initialization error in '{db_filename}': {e}", exc_info=True)
        print(f"Database initialization error for {db_filename}: {e}")
        raise # Re-raise the exception
    except ConnectionError as e:
        print(f"Database connection failed during initialization for {db_filename}: {e}")
        raise
    finally:
        if conn:
            conn.close()

# --- initialize_all_databases (Unchanged - relies on corrected initialize_database) ---
def initialize_all_databases():
    """Initializes the schema for all databases defined in the configuration."""
    # ... (code remains the same) ...
    log.info("Initializing schemas for all configured databases...")
    db_files = GconfigEM.get_db_filenames() # Gets validated names including 'combo'
    initialized_dbs = []
    failed_dbs = []

    for db_type, db_filename in db_files.items():
        if not db_filename:
            log.warning(f"Skipping initialization for '{db_type}' as filename is missing in config.")
            continue
        try:
            log.debug(f"Initializing DB type '{db_type}' using file '{db_filename}'")
            initialize_database(db_filename) # Pass full path
            initialized_dbs.append(db_filename)
        except Exception as e:
            log.error(f"Failed to initialize database '{db_filename}' (Type: {db_type}): {e}", exc_info=True)
            failed_dbs.append(f"{db_filename} ({db_type})")

    if failed_dbs:
         log.error(f"Failed to initialize the following databases: {', '.join(failed_dbs)}")
    if initialized_dbs:
         log.info(f"Successfully initialized/verified schemas for: {', '.join(initialized_dbs)}")

# --- Data Handling Helpers (generate_data_hash, generate_unique_id, check_duplicate_by_hash - Unchanged) ---
# ... (code remains the same) ...
def generate_data_hash(opinion_data):
    """Generates a SHA-256 hash based on core opinion data fields."""
    core_data_str = (
        f"{opinion_data.get('AppDocketID', '')}|"
        f"{opinion_data.get('ReleaseDate', '')}|"
        f"{opinion_data.get('CaseName', '')}|"
        f"{opinion_data.get('DecisionTypeCode', '')}|"
        f"{opinion_data.get('Venue', '')}|"
        f"{opinion_data.get('LCdocketID', '')}|"
        f"{opinion_data.get('LowerCourtVenue', '')}"
    )
    return hashlib.sha256(core_data_str.encode('utf-8')).hexdigest()

def generate_unique_id(data_hash, app_docket_id):
    """Generates a consistent UUID based on the data hash."""
    namespace = uuid.NAMESPACE_DNS
    name_string = data_hash
    base_uuid = uuid.uuid5(namespace, name_string)
    unique_id_str = str(base_uuid)
    return unique_id_str

def check_duplicate_by_hash(cursor, data_hash):
    """Checks if an opinion with the same DataHash already exists in the opinions table."""
    try:
        cursor.execute("SELECT 1 FROM opinions WHERE DataHash = ? LIMIT 1", (data_hash,))
        return cursor.fetchone() is not None
    except sqlite3.OperationalError as e:
        log.warning(f"Could not check for duplicate hash (table might not exist or wrong table?): {e}")
        return False


# --- Main Data Insertion/Update Logic for opinions table (Updated for entry_method and locking) ---
def add_or_update_opinion_to_db(db_filename, opinion_data, is_validated, run_type):
    """
    Adds/updates opinion in 'opinions' table (for Primary/Backup/Test DBs).
    Handles entry_method population and prevents unvalidated overwrites of validated data.
    """
    required_keys = ["AppDocketID", "CaseName", "UniqueID", "DataHash"] # UniqueID/Hash now added before call
    if not all(key in opinion_data for key in required_keys):
        log.warning(f"Skipping entry in {db_filename} due to missing required keys (AppDocketID, CaseName, UniqueID, DataHash): {opinion_data.get('AppDocketID')}")
        return "error_missing_keys"

    status = "error_unknown"
    conn = None
    app_docket_id = opinion_data["AppDocketID"]
    unique_id = opinion_data["UniqueID"] # Use pre-generated UniqueID
    data_hash = opinion_data["DataHash"] # Use pre-generated Hash

    try:
        conn = get_db_connection(db_filename)
        cursor = conn.cursor()

        # Check if record with this UniqueID exists
        cursor.execute("SELECT * FROM opinions WHERE UniqueID = ?", (unique_id,))
        existing_opinion = cursor.fetchone()
        now_ts = datetime.datetime.now()

        # Determine Entry Method based on how this function is called
        entry_method = "unknown"
        if run_type == 'manual-immediate' or run_type == 'manual-primary-force':
            entry_method = 'user_forced_validated' if is_validated else 'user_forced_unvalidated'
        elif run_type == 'manual-test':
             entry_method = 'test_run' # Treat test run distinctly
        elif run_type.startswith('scheduled-'):
            entry_method = 'scheduled_unvalidated' # Scheduled are initially unvalidated
        elif run_type == 'user_validated': # Set explicitly by validator call
             entry_method = 'user_validated'
        else:
             entry_method = 'other' # Fallback


        if existing_opinion is None:
            # --- Insert New Record ---
            log.info(f"Inserting new opinion (UniqueID: {unique_id[:8]}..., AppDocket: {app_docket_id}) into {db_filename} with method '{entry_method}'.")
            # Check for hash duplication *before* insert if needed (or rely on UniqueID collision)
            hash_exists = check_duplicate_by_hash(cursor, data_hash) # Check if content hash seen before
            duplicate_flag = 1 if hash_exists else 0

            sql = '''
                INSERT INTO opinions (
                    UniqueID, AppDocketID, ReleaseDate, DataHash, DuplicateFlag, LinkedDocketIDs,
                    CaseName, LCdocketID, LCCounty, Venue, LowerCourtVenue, LowerCourtSubCaseType,
                    OPJURISAPP, DecisionTypeCode, DecisionTypeText, StateAgency1, StateAgency2,
                    CaseNotes, RunType, validated, entry_method, caseconsolidated, recordimpounded,
                    last_validated_run_ts
                    -- first_scraped_ts, last_updated_ts have defaults
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            values = (
                unique_id, opinion_data.get("AppDocketID"), opinion_data.get("ReleaseDate"), data_hash, duplicate_flag,
                opinion_data.get("LinkedDocketIDs"), opinion_data.get("CaseName"), opinion_data.get("LCdocketID"),
                opinion_data.get("LCCounty"), opinion_data.get("Venue"),
                opinion_data.get("LowerCourtVenue"), opinion_data.get("LowerCourtSubCaseType"),
                opinion_data.get("OPJURISAPP", "Statewide"),
                opinion_data.get("DecisionTypeCode"), opinion_data.get("DecisionTypeText"),
                opinion_data.get("StateAgency1"), opinion_data.get("StateAgency2"),
                opinion_data.get("CaseNotes"), run_type,
                is_validated, entry_method, # Add entry method
                opinion_data.get("caseconsolidated", 0), opinion_data.get("recordimpounded", 0),
                now_ts if is_validated else None
            )
            try:
                 cursor.execute(sql, values)
                 status = "inserted"
            except sqlite3.IntegrityError:
                 # This means the UniqueID (based on hash) already exists. Update instead?
                 log.warning(f"Integrity error on insert for UniqueID {unique_id[:8]}... in {db_filename}. Record likely already exists. Attempting update logic.")
                 # Fall through to update logic below
                 existing_opinion = True # Mark as existing to trigger update block
                 status = "skipped_insert_exists" # Temporary status
            except sqlite3.Error as oe:
                 log.error(f"Database error during INSERT for {app_docket_id} in {db_filename}: {oe}", exc_info=True)
                 status = f"error_operational_insert_{db_filename}"
                 raise oe

        # --- Update Logic (Handles existing records or fall-through from insert collision) ---
        if existing_opinion:
            if status != "skipped_insert_exists": # Only re-fetch if we didn't just try inserting
                 cursor.execute("SELECT * FROM opinions WHERE UniqueID = ?", (unique_id,))
                 existing_opinion = cursor.fetchone()
                 if not existing_opinion: # Should not happen if it existed before, but safety check
                      log.error(f"Failed to re-fetch existing opinion for UniqueID {unique_id[:8]}... Aborting update.")
                      return "error_fetch_failed_update"

            log.info(f"Processing existing opinion (UniqueID: {unique_id[:8]}...) for {app_docket_id} in {db_filename}.")
            existing_validated = bool(existing_opinion['validated'])
            existing_entry_method = existing_opinion['entry_method']

            # Determine if update should proceed based on validation status ("Locking")
            # Allow update ONLY IF:
            # 1. Incoming data is validated (is_validated=True OR entry_method='user_validated')
            # OR
            # 2. Existing record is NOT validated.
            is_incoming_validated = is_validated or (entry_method == 'user_validated')
            allow_update = is_incoming_validated or not existing_validated

            if not allow_update:
                 # Also update RunType even if locked? Maybe not. Keep record as is.
                 log.info(f"Skipping update for VALIDATED UniqueID {unique_id[:8]}... by unvalidated run_type '{run_type}'. Record is locked.")
                 status = "skipped_validated_locked"
            else:
                 # Proceed with update comparison
                 update_fields = {}
                 changed = False

                 # Compare all fields except timestamps managed by defaults/triggers
                 fields_to_compare = [
                     "AppDocketID", "ReleaseDate", "DataHash", "DuplicateFlag", "LinkedDocketIDs", "CaseName",
                     "LCdocketID", "LCCounty", "Venue", "LowerCourtVenue", "LowerCourtSubCaseType", "OPJURISAPP",
                     "DecisionTypeCode", "DecisionTypeText", "StateAgency1", "StateAgency2", "CaseNotes",
                     "RunType", "validated", "entry_method", "caseconsolidated", "recordimpounded",
                     "last_validated_run_ts" # Include this for explicit updates
                 ]

                 # Prepare new values, ensuring flags are int
                 new_values = opinion_data.copy()
                 new_values['RunType'] = run_type
                 new_values['validated'] = is_validated
                 new_values['entry_method'] = entry_method
                 new_values['caseconsolidated'] = 1 if new_values.get('caseconsolidated') else 0
                 new_values['recordimpounded'] = 1 if new_values.get('recordimpounded') else 0
                 new_values['last_validated_run_ts'] = now_ts if is_validated else None
                 # Need to re-check hash duplication based on current DB state if not done before insert
                 if status == "skipped_insert_exists":
                      new_values['DuplicateFlag'] = 1 if check_duplicate_by_hash(cursor, data_hash) else 0
                 else:
                      new_values['DuplicateFlag'] = duplicate_flag

                 # Compare existing with new values
                 for field in fields_to_compare:
                     new_value = new_values.get(field)
                     existing_value = existing_opinion[field]

                     # Careful comparison (treat None and '' as same? Yes for text fields)
                     is_different = False
                     if isinstance(existing_value, (str, type(None))) and isinstance(new_value, (str, type(None))):
                          if (existing_value or '') != (new_value or ''): is_different = True
                     elif existing_value != new_value: # Handles numbers, booleans correctly
                          is_different = True

                     if is_different:
                          update_fields[field] = new_value
                          changed = True
                          log.debug(f"Field '{field}' changed: '{existing_value}' -> '{new_value}'")

                 if changed:
                     set_clauses = ", ".join([f"{col} = ?" for col in update_fields.keys()])
                     # Ensure last_updated_ts is implicitly updated by trigger, no need to set manually
                     sql_values = list(update_fields.values()) + [unique_id]
                     sql = f"UPDATE opinions SET {set_clauses} WHERE UniqueID = ?"
                     log.debug(f"Executing update for {unique_id[:8]}... SQL: {sql} Values: {sql_values}")
                     try:
                         cursor.execute(sql, tuple(sql_values))
                         status = "updated"
                     except sqlite3.Error as ue:
                         log.error(f"Error during UPDATE for {unique_id[:8]} in {db_filename}: {ue}", exc_info=True)
                         status = f"error_operational_update_{db_filename}"
                         raise ue
                 else:
                     status = "skipped_no_changes"
                     log.info(f"Skipping update for {unique_id[:8]}... in {db_filename}, no changes detected.")

        conn.commit() # Commit transaction

    except sqlite3.Error as e:
        log.error(f"Database error processing {app_docket_id} in {db_filename}: {e}", exc_info=True)
        if conn: conn.rollback()
        status = f"error_sqlite_{db_filename}"
    except ConnectionError as e:
        status = f"error_connection_{db_filename}"
    except Exception as e:
        log.error(f"Unexpected error processing {app_docket_id} in {db_filename}: {e}", exc_info=True)
        if conn: conn.rollback()
        status = f"error_unexpected_{db_filename}"
    finally:
        if conn:
            conn.close()

    return status

# --- Helper function to save to AllRuns History Table (Unchanged) ---
def _save_to_all_runs_history(db_filename, opinion_data, run_type):
    """Saves a snapshot of the opinion data to the opinion_history table."""
    # ... (code remains the same - make sure UniqueID is in opinion_data) ...
    unique_id = opinion_data.get("UniqueID")
    app_docket_id = opinion_data.get("AppDocketID")
    release_date = opinion_data.get("ReleaseDate")

    if not unique_id or not app_docket_id:
        log.warning(f"Skipping history save for run '{run_type}' due to missing UniqueID or AppDocketID.")
        return "error_missing_keys_history"

    conn = None
    status = "error_unknown_history"
    try:
        conn = get_db_connection(db_filename)
        cursor = conn.cursor()
        opinion_json = json.dumps(opinion_data)

        sql = '''
            INSERT INTO opinion_history (
                UniqueID, AppDocketID, ReleaseDate, RunType, OpinionDataJSON
            ) VALUES (?, ?, ?, ?, ?)
        '''
        values = (unique_id, app_docket_id, release_date, run_type, opinion_json)

        cursor.execute(sql, values)
        conn.commit()
        status = "inserted_history"
        log.debug(f"Inserted history record for UniqueID {unique_id[:8]}... (Run: {run_type})")

    except sqlite3.Error as e:
        log.error(f"History DB error saving {unique_id[:8]} in {db_filename}: {e}", exc_info=True)
        if conn: conn.rollback()
        status = f"error_sqlite_history_{db_filename}"
    except ConnectionError as e:
        status = f"error_connection_history_{db_filename}"
    except Exception as e:
        log.error(f"Unexpected error saving history {unique_id[:8]} in {db_filename}: {e}", exc_info=True)
        if conn: conn.rollback()
        status = f"error_unexpected_history_{db_filename}"
    finally:
        if conn:
            conn.close()
    return status

# --- Save Opinions to DBs (Updated for Run Type Targeting) ---
def save_opinions_to_dbs(opinion_list, is_validated, run_type):
    """
    Saves opinions to appropriate databases based on run_type.
    Uses standard add/update for primary/backup/test.
    Uses append-only history save for all_runs.
    """
    db_files = GconfigEM.get_db_filenames()
    results = {} # Store results per DB key
    standard_dbs_to_target = [] # Tuples of (db_key, db_filename) for opinions table
    all_runs_target = None # Tuple (db_key, db_filename) for history

    log.info(f"Saving opinions for run_type: '{run_type}'")

    # --- Determine Target DBs ---
    target_keys_standard = [] # Keys for primary, backup, test
    target_all_runs = False # Flag to target all_runs

    if run_type == 'manual-test':
        log.info("Manual Test run: Targeting primary, backup, test (standard) AND all_runs (history).")
        target_keys_standard = ["primary", "backup", "test"]
        target_all_runs = True
    elif run_type == 'manual-primary-force':
        log.info("Manual Primary Force run: Targeting primary (standard) AND all_runs (history).")
        target_keys_standard = ["primary"]
        target_all_runs = True
    elif run_type in ['scheduled-primary-1', 'scheduled-primary-2']:
        log.info(f"Scheduled Primary run ({run_type}): Targeting primary (standard) AND all_runs (history).")
        target_keys_standard = ["primary"]
        target_all_runs = True
    elif run_type == 'scheduled-backup':
        log.info("Scheduled Backup run: Targeting backup (standard) AND all_runs (history).")
        target_keys_standard = ["backup"]
        target_all_runs = True
    elif run_type == 'maintenance':
         log.info("Maintenance run type: No data saving actions required.")
         return {}
    elif run_type == 'user_validated': # If called directly after validation
         log.info("User Validated run: Targeting primary, backup (standard) AND all_runs (history).")
         target_keys_standard = ["primary", "backup"] # Update primary and backup
         target_all_runs = True # Also log validated state to history
    else:
         log.warning(f"Unrecognized run_type '{run_type}'. Saving ONLY to all_runs history (if configured).")
         target_keys_standard = []
         target_all_runs = True # Save history even for unknown types? Yes.

    # --- Prepare DB Targets and Results Dict ---
    # Standard DBs
    for db_key in target_keys_standard:
        db_filename = db_files.get(db_key)
        if db_filename:
            try:
                # Use specific schema for init check if needed, but initialize_database handles it
                initialize_database(db_filename)
                standard_dbs_to_target.append((db_key, db_filename))
                results[db_key] = {"total": 0, "inserted": 0, "updated": 0, "skipped": 0, "error": 0}
            except Exception as init_e:
                 log.error(f"Failed to initialize/verify database '{db_filename}' for key '{db_key}': {init_e}")
                 results[db_key] = {"total": 0, "inserted": 0, "updated": 0, "skipped": 0, "error": -1}
        else:
            log.error(f"Database filename for '{db_key}' not configured. Skipping target.")

    # AllRuns History DB
    if target_all_runs:
        all_runs_db_file = db_files.get("all_runs")
        if all_runs_db_file:
            try:
                initialize_database(all_runs_db_file) # Ensures history table exists
                all_runs_target = ("all_runs", all_runs_db_file)
                results["all_runs"] = {"total": 0, "inserted_history": 0, "error_history": 0}
            except Exception as init_e:
                log.error(f"Failed to initialize/verify all_runs history database '{all_runs_db_file}': {init_e}")
                results["all_runs"] = {"total": 0, "inserted_history": 0, "error_history": -1} # Init error
        else:
             log.error("Database filename for 'all_runs' not configured. Skipping history saving.")

    # Check if any targets are available
    if not standard_dbs_to_target and not all_runs_target:
        log.error(f"No target databases available or initialized for run type '{run_type}'. Cannot save opinions.")
        return results

    # --- Process Opinions ---
    if not opinion_list:
         log.info("No opinions provided in the list to save.")
         return results

    successful_writes_standard = {db_key: False for db_key, _ in standard_dbs_to_target}
    successful_writes_history = False
    overall_success = False # Track if any write happens anywhere

    for opinion_orig in opinion_list:
        if not isinstance(opinion_orig, dict):
            log.warning(f"Skipping non-dictionary item in opinion list: {type(opinion_orig)}")
            continue
        opinion = opinion_orig.copy() # Work on a copy

        # Generate IDs and Hashes FIRST (needed for both standard and history)
        app_docket = opinion.get("AppDocketID")
        case_name = opinion.get("CaseName")
        if not app_docket or not case_name:
             log.warning(f"Skipping opinion due to missing AppDocketID or CaseName: {opinion}")
             continue

        data_hash = generate_data_hash(opinion)
        unique_id = generate_unique_id(data_hash, app_docket)
        opinion["DataHash"] = data_hash
        opinion["UniqueID"] = unique_id
        opinion['caseconsolidated'] = 1 if opinion.get('caseconsolidated') else 0
        opinion['recordimpounded'] = 1 if opinion.get('recordimpounded') else 0

        # Save to Standard DBs
        for db_key, db_filename in standard_dbs_to_target:
             results[db_key]["total"] += 1
             status = add_or_update_opinion_to_db(db_filename, opinion, is_validated, run_type)
             if status == "inserted": results[db_key]["inserted"] += 1; successful_writes_standard[db_key] = True; overall_success = True
             elif status == "updated": results[db_key]["updated"] += 1; successful_writes_standard[db_key] = True; overall_success = True
             elif status.startswith("skipped"): results[db_key]["skipped"] += 1
             else: results[db_key]["error"] += 1 # Error

        # Save to AllRuns History DB
        if all_runs_target:
             db_key_hist, db_filename_hist = all_runs_target
             results[db_key_hist]["total"] += 1
             # Pass the opinion dict (which now includes UniqueID/Hash)
             hist_status = _save_to_all_runs_history(db_filename_hist, opinion, run_type)
             if hist_status == "inserted_history": results[db_key_hist]["inserted_history"] += 1; successful_writes_history = True; overall_success = True
             else: results[db_key_hist]["error_history"] += 1 # Error

    # --- Log Summary & Update Config ---
    # (Log summary logic remains the same as previous version)
    # ...
    # Log standard DB results
    for db_key, result_counts in results.items():
         if db_key == "all_runs": continue # Skip history log here
         if result_counts["total"] > 0 or result_counts["error"] == -1:
              db_path = db_files.get(db_key, "N/A")
              if result_counts["error"] == -1:
                   log.error(f"DB '{db_key}' ({db_path}): Skipped due to initialization error.")
              else:
                   log.info(f"DB '{db_key}' ({db_path}): Processed {result_counts['total']} -> "
                            f"Inserted: {result_counts['inserted']}, Updated: {result_counts['updated']}, "
                            f"Skipped: {result_counts['skipped']}, Errors: {result_counts['error']}")
    # Log history DB results
    if "all_runs" in results:
        hist_results = results["all_runs"]
        hist_path = db_files.get("all_runs") or "N/A"
        if hist_results["total"] > 0 or hist_results["error_history"] == -1:
            if hist_results["error_history"] == -1:
                log.error(f"DB 'all_runs' ({hist_path}): Skipped due to initialization error.")
            else:
                log.info(f"DB 'all_runs' ({hist_path}): Processed {hist_results['total']} -> "
                         f"History Inserted: {hist_results['inserted_history']}, History Errors: {hist_results['error_history']}")

    if overall_success:
        try:
            log.info("Successful writes detected, updating run counter and timestamps.")
            current_counter = GconfigEM.increment_run_counter()
            for db_key, success in successful_writes_standard.items():
                if success: GconfigEM.update_last_run_timestamp(db_key)
            if successful_writes_history: GconfigEM.update_last_run_timestamp("all_runs")
        except Exception as e:
            log.error(f"Failed to update config (counter/timestamps) after successful writes: {e}", exc_info=True)
    elif any(r['total'] > 0 for r in results.values()):
        log.warning("Opinions processed but no changes resulted in successful database writes.")

    return results


# --- Function to Build Combo DB (Unchanged) ---
def build_combo_db(combo_db_file, primary_db_file, backup_db_file):
    """Clears and rebuilds the Combo DB from Primary and Backup."""
    # ... (code remains the same) ...
    log.info(f"Starting build of Combo DB: {combo_db_file}")
    if not os.path.exists(primary_db_file): log.error(f"Cannot build Combo DB: Primary DB file not found at {primary_db_file}"); return False, "Primary DB not found"
    if not os.path.exists(backup_db_file): log.error(f"Cannot build Combo DB: Backup DB file not found at {backup_db_file}"); return False, "Backup DB not found"

    conn_combo, conn_primary, conn_backup = None, None, None
    success, error_msg = False, None
    try:
        initialize_database(combo_db_file)
        conn_combo = get_db_connection(combo_db_file); cursor_combo = conn_combo.cursor()
        log.debug("Clearing existing data from Combo DB."); cursor_combo.execute("DELETE FROM combined_opinions;"); conn_combo.commit()

        conn_primary = get_db_connection(primary_db_file); cursor_primary = conn_primary.cursor()
        conn_backup = get_db_connection(backup_db_file); cursor_backup = conn_backup.cursor()

        cols = ["UniqueID", "AppDocketID", "ReleaseDate", "DataHash", "DuplicateFlag", "LinkedDocketIDs", "CaseName", "LCdocketID", "LCCounty", "Venue", "LowerCourtVenue", "LowerCourtSubCaseType", "OPJURISAPP", "DecisionTypeCode", "DecisionTypeText", "StateAgency1", "StateAgency2", "CaseNotes", "RunType", "entry_method", "validated", "caseconsolidated", "recordimpounded", "first_scraped_ts", "last_updated_ts", "last_validated_run_ts"]
        cols_str = ", ".join(cols); placeholders = ", ".join(["?"] * (len(cols) + 1))
        insert_sql = f"INSERT INTO combined_opinions (SourceDB, {cols_str}) VALUES ({placeholders})"

        log.info(f"Copying data from Primary DB: {primary_db_file}"); cursor_primary.execute(f"SELECT {cols_str} FROM opinions"); primary_rows = cursor_primary.fetchall()
        primary_data = [('primary',) + tuple(row) for row in primary_rows]
        if primary_data: cursor_combo.executemany(insert_sql, primary_data); log.info(f"Inserted {len(primary_data)} records from Primary DB.")
        else: log.info("No records found in Primary DB.")

        log.info(f"Copying data from Backup DB: {backup_db_file}"); cursor_backup.execute(f"SELECT {cols_str} FROM opinions"); backup_rows = cursor_backup.fetchall()
        backup_data = [('backup',) + tuple(row) for row in backup_rows]
        if backup_data: cursor_combo.executemany(insert_sql, backup_data); log.info(f"Inserted {len(backup_data)} records from Backup DB.")
        else: log.info("No records found in Backup DB.")

        conn_combo.commit(); success = True; log.info(f"Successfully rebuilt Combo DB: {combo_db_file}")
    except sqlite3.Error as e: log.error(f"Database error building Combo DB: {e}", exc_info=True); error_msg = f"Database error: {e}"; conn_combo.rollback()
    except ConnectionError as e: log.error(f"Connection error building Combo DB: {e}"); error_msg = f"Connection error: {e}"
    except Exception as e: log.error(f"Unexpected error building Combo DB: {e}", exc_info=True); error_msg = f"Unexpected error: {e}"; conn_combo.rollback()
    finally:
        if conn_combo: conn_combo.close()
        if conn_primary: conn_primary.close()
        if conn_backup: conn_backup.close()
    return success, error_msg

# --- get_opinions_by_date_runtype, get_db_stats (Unchanged from V3) ---
# ... (code remains the same) ...
def get_opinions_by_date_runtype(db_filename, release_date, run_type):
    """Fetches opinions matching a specific release date and run type from 'opinions' table."""
    log.debug(f"Fetching opinions from {db_filename} for ReleaseDate={release_date}, RunType={run_type}")
    opinions = {}
    conn = None
    try:
        db_basename = os.path.basename(db_filename)
        db_configs = GconfigEM.DEFAULT_DB_NAMES
        if db_basename == db_configs.get("all_runs") or db_basename == db_configs.get("combo"):
            log.warning(f"get_opinions_by_date_runtype called on DB '{db_filename}' which doesn't use 'opinions' table structure. Returning empty.")
            return opinions

        conn = get_db_connection(db_filename)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM opinions WHERE ReleaseDate = ? AND RunType = ?", (release_date, run_type))
        rows = cursor.fetchall()
        for row in rows:
            row_dict = dict(row)
            app_docket_id = row_dict.get("AppDocketID")
            if app_docket_id: opinions[app_docket_id] = row_dict
            else: log.warning(f"Record found with missing AppDocketID in {db_filename} for date {release_date}, run {run_type}. Skipping.")
        log.debug(f"Found {len(opinions)} records for {release_date} / {run_type} in {db_filename}")
    except sqlite3.Error as e: log.error(f"Error fetching opinions for {release_date}/{run_type} from {db_filename}: {e}", exc_info=True)
    except ConnectionError as e: log.warning(f"Connection error while fetching opinions for {release_date}/{run_type} from {db_filename}: {e}")
    finally:
        if conn: conn.close()
    return opinions

def get_db_stats(db_filename):
    """Provides basic statistics for DBs based on their schema."""
    stats = {"total": 0, "validated": "N/A", "unvalidated": "N/A", "schema_type": "unknown", "error": None}
    conn = None
    if not os.path.exists(db_filename): stats["error"] = "File not found"; return stats

    db_basename = os.path.basename(db_filename); db_configs = GconfigEM.DEFAULT_DB_NAMES
    schema_type, table_name = "unknown", None
    if db_basename == db_configs.get("all_runs"): schema_type, table_name = "history", "opinion_history"
    elif db_basename == db_configs.get("combo"): schema_type, table_name = "combo", "combined_opinions"
    elif db_basename in [db_configs.get("primary"), db_configs.get("backup"), db_configs.get("test")]: schema_type, table_name = "opinions", "opinions"
    else: stats["error"] = "Unrecognized DB file for stats"; return stats
    stats["schema_type"] = schema_type

    try:
        conn = get_db_connection(db_filename); cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}"); total = cursor.fetchone()
        stats["total"] = total[0] if total else 0

        if schema_type == "opinions":
            cursor.execute("SELECT COUNT(*) FROM opinions WHERE validated = 1"); validated = cursor.fetchone()
            stats["validated"] = validated[0] if validated else 0
            stats["unvalidated"] = stats["total"] - stats["validated"]
        elif schema_type == "combo":
             cursor.execute("SELECT COUNT(*) FROM combined_opinions WHERE validated = 1 AND SourceDB = 'primary'"); val_prim = cursor.fetchone()[0]
             cursor.execute("SELECT COUNT(*) FROM combined_opinions WHERE validated = 1 AND SourceDB = 'backup'"); val_back = cursor.fetchone()[0]
             stats["validated"] = f"P:{val_prim}, B:{val_back}"
    except sqlite3.Error as e: stats["error"] = str(e); log.error(f"DB error getting stats from '{db_filename}' (Schema: {schema_type}): {e}", exc_info=True)
    except ConnectionError as e: stats["error"] = str(e)
    except Exception as e: stats["error"] = f"Unexpected: {e}"; log.error(f"Unexpected error getting stats from '{db_filename}': {e}", exc_info=True)
    finally:
        if conn: conn.close()
    return stats


# === End of GdbEM.py ===