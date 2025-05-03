# GdbEM.py
# V3: Added entry_method, AllRuns history table, Combo DB logic
"""
Handles database interactions.
- Main schema (opinions table) updated with 'entry_method'.
- Implements append-only history logic for GAllRunsOpinionsEM.db using 'opinion_history' table.
- Adds function to build GComboEM.db from primary and backup.
- Handles different DB schemas during initialization.
- Updates insertion/update logic for entry_method and validated record protection.
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

# Schema for Primary, Backup, Test DBs
OPINIONS_TABLE_SCHEMA = '''
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

CREATE INDEX IF NOT EXISTS idx_opinions_appdocketid_releasedate ON opinions (AppDocketID, ReleaseDate);
CREATE INDEX IF NOT EXISTS idx_opinions_datahash ON opinions (DataHash);
CREATE INDEX IF NOT EXISTS idx_opinions_runtype ON opinions (RunType);
CREATE INDEX IF NOT EXISTS idx_opinions_validated ON opinions (validated);
CREATE INDEX IF NOT EXISTS idx_opinions_entrymethod ON opinions (entry_method);

-- Update last_updated_ts whenever a row is updated
CREATE TRIGGER IF NOT EXISTS trg_opinions_update_timestamp
AFTER UPDATE ON opinions FOR EACH ROW
WHEN OLD.UniqueID = NEW.UniqueID
BEGIN
    UPDATE opinions SET last_updated_ts = CURRENT_TIMESTAMP WHERE UniqueID = OLD.UniqueID;
END;
'''

# Schema for GAllRunsOpinionsEM.db (History Table)
ALL_RUNS_SCHEMA = '''
CREATE TABLE IF NOT EXISTS opinion_history (
    HistoryID INTEGER PRIMARY KEY AUTOINCREMENT, -- Simple auto-incrementing ID for this table
    UniqueID TEXT NOT NULL,         -- UniqueID from the opinion data (links versions)
    AppDocketID TEXT NOT NULL,      -- Store for easier querying
    ReleaseDate TEXT,               -- Store for easier querying
    RunType TEXT NOT NULL,          -- Run type that generated this snapshot
    ScrapeTimestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- When this specific snapshot was saved
    OpinionDataJSON TEXT NOT NULL   -- Store the full opinion dict as JSON
);

CREATE INDEX IF NOT EXISTS idx_history_uniqueid ON opinion_history (UniqueID);
CREATE INDEX IF NOT EXISTS idx_history_appdocketid ON opinion_history (AppDocketID);
CREATE INDEX IF NOT EXISTS idx_history_releasedate ON opinion_history (ReleaseDate);
CREATE INDEX IF NOT EXISTS idx_history_runtype ON opinion_history (RunType);
CREATE INDEX IF NOT EXISTS idx_history_scrapetimestamp ON opinion_history (ScrapeTimestamp);
'''

# Schema for GComboEM.db (Combined View)
COMBO_SCHEMA = '''
CREATE TABLE IF NOT EXISTS combined_opinions (
    SourceDB TEXT NOT NULL, -- 'primary' or 'backup'
    -- Include all columns from the main 'opinions' table
    UniqueID TEXT,
    AppDocketID TEXT,
    ReleaseDate TEXT,
    DataHash TEXT,
    DuplicateFlag INTEGER,
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
    RunType TEXT,
    entry_method TEXT,
    validated BOOLEAN,
    caseconsolidated INTEGER,
    recordimpounded INTEGER,
    first_scraped_ts TIMESTAMP,
    last_updated_ts TIMESTAMP,
    last_validated_run_ts TIMESTAMP,
    -- Maybe add a combined primary key?
    PRIMARY KEY (SourceDB, UniqueID)
);

CREATE INDEX IF NOT EXISTS idx_combo_uniqueid ON combined_opinions (UniqueID);
CREATE INDEX IF NOT EXISTS idx_combo_appdocketid ON combined_opinions (AppDocketID);
CREATE INDEX IF NOT EXISTS idx_combo_sourcedb ON combined_opinions (SourceDB);
'''


# --- Database Connection (Unchanged) ---
def get_db_connection(db_filename):
    """Gets a connection to the specified SQLite database file."""
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
        # Enable foreign key support if we ever use relationships
        # conn.execute("PRAGMA foreign_keys = ON;")
        return conn
    except sqlite3.Error as e:
        log.error(f"Failed to connect to database {db_filename}: {e}", exc_info=True)
        raise ConnectionError(f"Could not connect to database {db_filename}: {e}") from e


# --- Database Initialization (Updated) ---
def initialize_database(db_filename):
    """Initializes the database schema based on the filename convention."""
    log.info(f"Initializing database schema in: {db_filename}")

    schema_to_use = None
    db_basename = os.path.basename(db_filename)

    if db_basename == GconfigEM.DEFAULT_DB_NAMES.get("all_runs"):
        schema_to_use = ALL_RUNS_SCHEMA
        log.info(f"Applying 'opinion_history' schema to {db_filename}.")
    elif db_basename == GconfigEM.DEFAULT_DB_NAMES.get("combo"):
        schema_to_use = COMBO_SCHEMA
        log.info(f"Applying 'combined_opinions' schema to {db_filename}.")
    elif db_basename in [GconfigEM.DEFAULT_DB_NAMES.get("primary"),
                         GconfigEM.DEFAULT_DB_NAMES.get("backup"),
                         GconfigEM.DEFAULT_DB_NAMES.get("test")]:
        schema_to_use = OPINIONS_TABLE_SCHEMA
        log.info(f"Applying 'opinions' table schema to {db_filename}.")
    else:
        log.warning(f"Unknown database filename convention: {db_basename}. Applying standard 'opinions' schema as default.")
        schema_to_use = OPINIONS_TABLE_SCHEMA # Default schema

    conn = None
    try:
        conn = get_db_connection(db_filename)
        cursor = conn.cursor()
        cursor.executescript(schema_to_use)
        conn.commit()
        log.info(f"Database '{db_filename}' schema initialized/verified successfully.")
    except sqlite3.Error as e:
        log.error(f"Database initialization error in '{db_filename}': {e}", exc_info=True)
        print(f"Database initialization error for {db_filename}: {e}")
        raise
    except ConnectionError as e:
        print(f"Database connection failed during initialization for {db_filename}: {e}")
        raise
    finally:
        if conn:
            conn.close()

def initialize_all_databases():
    """Initializes the schema for all databases defined in the configuration."""
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
            # Log error with db_type for context
            log.error(f"Failed to initialize database '{db_filename}' (Type: {db_type}): {e}", exc_info=True)
            failed_dbs.append(f"{db_filename} ({db_type})") # Include type in failed list

    if failed_dbs:
         log.error(f"Failed to initialize the following databases: {', '.join(failed_dbs)}")
    if initialized_dbs:
         log.info(f"Successfully initialized/verified schemas for: {', '.join(initialized_dbs)}")


# --- Data Handling Helpers (Unchanged) ---
def generate_data_hash(opinion_data):
    """Generates a SHA-256 hash based on core opinion data fields."""
    core_data_str = (
        f"{opinion_data.get('AppDocketID', '')}|"
        f"{opinion_data.get('ReleaseDate', '')}|"
        f"{opinion_data.get('CaseName', '')}|"
        f"{opinion_data.get('DecisionTypeCode', '')}|"
        f"{opinion_data.get('Venue', '')}|"
        # Include more fields if needed to define uniqueness for primary/backup
        f"{opinion_data.get('LCdocketID', '')}|"
        f"{opinion_data.get('LowerCourtVenue', '')}"
    )
    return hashlib.sha256(core_data_str.encode('utf-8')).hexdigest()

def generate_unique_id(data_hash, app_docket_id):
    """Generates a consistent UUID based on the data hash."""
    # Using DataHash ensures same content = same UniqueID
    namespace = uuid.NAMESPACE_DNS # Use DNS namespace for consistency
    name_string = data_hash # Base the UUID on the content hash
    base_uuid = uuid.uuid5(namespace, name_string)
    unique_id_str = str(base_uuid)
    return unique_id_str

def check_duplicate_by_hash(cursor, data_hash):
    """Checks if an opinion with the same DataHash already exists in the opinions table."""
    try:
        cursor.execute("SELECT 1 FROM opinions WHERE DataHash = ? LIMIT 1", (data_hash,))
        return cursor.fetchone() is not None
    except sqlite3.OperationalError as e:
        # Handle case where the table might not be the 'opinions' table (e.g., during init)
        log.warning(f"Could not check for duplicate hash (table might not exist or wrong table?): {e}")
        return False

# --- Main Data Insertion/Update Logic for opinions table (Primary/Backup/Test) ---
def add_or_update_opinion_to_db(db_filename, opinion_data, is_validated, run_type):
    """
    Adds/updates opinion in 'opinions' table (for Primary/Backup/Test DBs).
    Handles entry_method population and prevents unvalidated overwrites of validated data.
    """
    required_keys = ["AppDocketID", "CaseName"] # Minimum required fields
    if not all(key in opinion_data and opinion_data.get(key) is not None for key in required_keys):
        log.warning(f"Skipping entry in {db_filename} due to missing required keys: {opinion_data.get('AppDocketID')}")
        return "error_missing_keys"

    status = "error_unknown"
    conn = None
    app_docket_id = opinion_data["AppDocketID"]

    try:
        conn = get_db_connection(db_filename)
        cursor = conn.cursor()

        data_hash = generate_data_hash(opinion_data)
        unique_id = generate_unique_id(data_hash, app_docket_id) # Generate ID based on hash
        hash_exists = check_duplicate_by_hash(cursor, data_hash)
        duplicate_flag = 1 if hash_exists else 0

        cursor.execute("SELECT * FROM opinions WHERE UniqueID = ?", (unique_id,))
        existing_opinion = cursor.fetchone()
        now_ts = datetime.datetime.now()

        # Determine Entry Method
        entry_method = "unknown"
        if run_type == 'manual-immediate':
            entry_method = 'user_forced_validated' if is_validated else 'user_forced_unvalidated'
        elif run_type == 'manual-primary-force':
             entry_method = 'user_forced_validated' if is_validated else 'user_forced_unvalidated' # Treat same as immediate?
        elif run_type == 'manual-test':
            entry_method = 'test_run'
        elif run_type.startswith('scheduled-'):
            entry_method = 'scheduled_unvalidated' # Scheduled are initially unvalidated
        elif run_type == 'user_validated': # Set explicitly by validator
             entry_method = 'user_validated'
        else:
             entry_method = 'other'


        if existing_opinion is None:
            # --- Insert New Record ---
            log.info(f"Inserting new opinion (UniqueID: {unique_id[:8]}..., AppDocket: {app_docket_id}) into {db_filename} with method '{entry_method}'.")
            sql = '''
                INSERT INTO opinions (
                    UniqueID, AppDocketID, ReleaseDate, DataHash, DuplicateFlag, LinkedDocketIDs,
                    CaseName, LCdocketID, LCCounty, Venue, LowerCourtVenue, LowerCourtSubCaseType,
                    OPJURISAPP, DecisionTypeCode, DecisionTypeText, StateAgency1, StateAgency2,
                    CaseNotes, RunType, validated, entry_method, caseconsolidated, recordimpounded,
                    last_validated_run_ts
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            values = (
                unique_id, opinion_data.get("AppDocketID"), opinion_data.get("ReleaseDate"), data_hash, duplicate_flag,
                opinion_data.get("LinkedDocketIDs"), opinion_data.get("CaseName"), opinion_data.get("LCdocketID"),
                opinion_data.get("LCCounty"), opinion_data.get("Venue"),
                opinion_data.get("LowerCourtVenue"), opinion_data.get("LowerCourtSubCaseType"),
                opinion_data.get("OPJURISAPP", "Statewide"), # Default OPJURISAPP
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
                 log.warning(f"Integrity error (likely UniqueID {unique_id[:8]}... collision) on insert for {app_docket_id} in {db_filename}. Skipping insert.")
                 # If it exists, maybe try an update? Or just skip? Let's skip for simplicity.
                 status = "skipped_integrity_error_on_insert"
            except sqlite3.Error as oe: # Catch any SQLite error during insert
                 log.error(f"Operational error during INSERT for {app_docket_id} in {db_filename}: {oe}", exc_info=True)
                 status = f"error_operational_insert_{db_filename}"
                 raise oe # Re-raise to signal failure

        else:
            # --- Update Existing Record ---
            log.info(f"Found existing opinion (UniqueID: {unique_id[:8]}...) for {app_docket_id} in {db_filename}. Comparing for update.")
            existing_validated = bool(existing_opinion['validated'])

            # --- Locking Logic: Prevent unvalidated runs from overwriting validated data ---
            # Allow updates if:
            # 1. The incoming run is validated (e.g., user force validated, validator)
            # 2. The existing record is NOT validated.
            # Skip updates if incoming run is NOT validated AND existing record IS validated.
            if not is_validated and existing_validated and entry_method != 'user_validated':
                 log.info(f"Skipping update for VALIDATED UniqueID {unique_id[:8]}... by unvalidated run_type '{run_type}'.")
                 status = "skipped_validated_locked"
            else:
                 # Proceed with update comparison
                 update_fields = {}
                 force_overwrite = (run_type in ['manual-immediate', 'manual-primary-force'] and is_validated) or (entry_method == 'user_validated')

                 # Fields to potentially update
                 fields_to_compare = [
                     "AppDocketID", "ReleaseDate", "LinkedDocketIDs", "CaseName", "LCdocketID", "LCCounty", "Venue",
                     "LowerCourtVenue", "LowerCourtSubCaseType", "OPJURISAPP", "DecisionTypeCode", "DecisionTypeText",
                     "StateAgency1", "StateAgency2", "CaseNotes", "caseconsolidated", "recordimpounded"
                 ]
                 metadata_fields = ["RunType", "entry_method", "validated", "last_validated_run_ts", "DuplicateFlag"]

                 changed = False
                 # Compare core data fields
                 for field in fields_to_compare:
                     new_value = opinion_data.get(field, None) # Use None if missing in new data
                     existing_value = existing_opinion[field]
                     # Normalize binary flags for comparison
                     if field in ["caseconsolidated", "recordimpounded"]:
                          new_value = 1 if new_value else 0
                          existing_value = 1 if existing_value else 0
                     # Treat None and empty string as potentially different if needed, or same? Let's treat as same for now.
                     if (new_value or '') != (existing_value or ''):
                          update_fields[field] = new_value
                          changed = True
                          log.debug(f"Field '{field}' changed: '{existing_value}' -> '{new_value}'")

                 # Check metadata fields
                 if existing_opinion['RunType'] != run_type: update_fields['RunType'] = run_type; changed=True
                 # Update entry_method if current run suggests a different status (e.g., user validated)
                 if entry_method != 'unknown' and existing_opinion['entry_method'] != entry_method: update_fields['entry_method'] = entry_method; changed=True
                 # Handle validation status change
                 if is_validated != existing_validated: update_fields['validated'] = is_validated; changed=True
                 if is_validated and not existing_opinion['last_validated_run_ts']: update_fields['last_validated_run_ts'] = now_ts; changed=True
                 if not is_validated and existing_opinion['last_validated_run_ts']: update_fields['last_validated_run_ts'] = None; changed=True # Clear if marked unvalidated
                 if existing_opinion['DuplicateFlag'] != duplicate_flag: update_fields['DuplicateFlag'] = duplicate_flag; changed=True


                 if changed:
                     set_clauses = ", ".join([f"{col} = ?" for col in update_fields.keys()])
                     sql_values = list(update_fields.values()) + [unique_id]
                     sql = f"UPDATE opinions SET {set_clauses} WHERE UniqueID = ?"
                     log.debug(f"Executing update for {unique_id[:8]}... SQL: {sql} Values: {sql_values}")
                     try:
                         cursor.execute(sql, tuple(sql_values))
                         status = "updated" # Generic update status
                     except sqlite3.Error as ue:
                         log.error(f"Error during UPDATE for {unique_id[:8]} in {db_filename}: {ue}", exc_info=True)
                         status = f"error_operational_update_{db_filename}"
                         raise ue # Re-raise to signal failure
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

# --- Helper function to save to AllRuns History Table ---
def _save_to_all_runs_history(db_filename, opinion_data, run_type):
    """Saves a snapshot of the opinion data to the opinion_history table."""
    unique_id = opinion_data.get("UniqueID") # Get pre-generated UniqueID
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
        # Convert full opinion data to JSON string
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


# --- Save Opinions to DBs (Updated for AllRuns History & Run Types) ---
def save_opinions_to_dbs(opinion_list, is_validated, run_type):
    """
    Saves opinions to appropriate databases based on run_type.
    Uses standard add/update for primary/backup/test.
    Uses append-only history save for all_runs.
    """
    db_files = GconfigEM.get_db_filenames()
    results = {} # Store results per DB key
    dbs_to_target = [] # Tuples of (db_key, db_filename)

    all_runs_db_file = db_files.get("all_runs") # Get all_runs filename separately

    # Determine target DBs based on run_type
    if run_type == 'manual-test':
        log.info("Manual Test run: Targeting primary, backup, all_runs, test.")
        db_keys_targeted = ["primary", "backup", "test"] # Exclude all_runs from standard processing
    elif run_type == 'manual-primary-force':
        log.info("Manual Primary Force run: Targeting primary, all_runs.")
        db_keys_targeted = ["primary"] # Exclude all_runs from standard processing
    elif run_type in ['scheduled-primary-1', 'scheduled-primary-2']:
        log.info(f"Scheduled Primary run ({run_type}): Targeting primary, all_runs.")
        db_keys_targeted = ["primary"] # Exclude all_runs from standard processing
    elif run_type == 'scheduled-backup':
        log.info("Scheduled Backup run: Targeting backup, all_runs.")
        db_keys_targeted = ["backup"] # Exclude all_runs from standard processing
    else:
        log.warning(f"Unrecognized run_type '{run_type}' for standard DB saving. Only saving history to all_runs (if configured).")
        db_keys_targeted = [] # No standard DBs targeted

    # Prepare standard DB targets
    for db_key in db_keys_targeted:
        db_filename = db_files.get(db_key)
        if db_filename:
            try:
                initialize_database(db_filename) # Ensure schema exists
                dbs_to_target.append((db_key, db_filename))
                results[db_key] = {"total": 0, "inserted": 0, "updated": 0, "skipped": 0, "error": 0}
            except Exception as init_e:
                 log.error(f"Failed to initialize/verify database '{db_filename}' for key '{db_key}' before saving: {init_e}")
                 results[db_key] = {"total": 0, "inserted": 0, "updated": 0, "skipped": 0, "error": -1} # Mark init error
        else:
            log.error(f"Database filename for '{db_key}' not configured. Skipping target.")

    # Prepare all_runs target if configured
    all_runs_target = None
    if all_runs_db_file:
        try:
            initialize_database(all_runs_db_file) # Ensure history table exists
            all_runs_target = ("all_runs", all_runs_db_file)
            results["all_runs"] = {"total": 0, "inserted_history": 0, "error_history": 0} # Specific keys for history
        except Exception as init_e:
            log.error(f"Failed to initialize/verify all_runs history database '{all_runs_db_file}': {init_e}")
            results["all_runs"] = {"total": 0, "inserted_history": 0, "error_history": -1}

    # Check if any targets are available
    if not dbs_to_target and not all_runs_target:
        log.error(f"No target databases available or initialized for run type '{run_type}'. Cannot save opinions.")
        return results

    # --- Process Opinions ---
    if not opinion_list:
         log.info("No opinions provided in the list to save.")
         return results

    successful_writes_standard = {db_key: False for db_key, _ in dbs_to_target}
    successful_writes_history = False
    overall_success = False # Track if any write happens anywhere

    for opinion in opinion_list:
        if not isinstance(opinion, dict):
            log.warning(f"Skipping non-dictionary item in opinion list: {type(opinion)}")
            continue

        # --- Generate IDs and Hashes (needed for both standard and history) ---
        # Ensure required fields exist before hashing/ID generation
        app_docket = opinion.get("AppDocketID")
        case_name = opinion.get("CaseName")
        if not app_docket or not case_name:
             log.warning(f"Skipping opinion due to missing AppDocketID or CaseName: {opinion}")
             continue

        data_hash = generate_data_hash(opinion)
        unique_id = generate_unique_id(data_hash, app_docket)
        # Add these to the opinion dict so they are saved in history JSON
        opinion["DataHash"] = data_hash
        opinion["UniqueID"] = unique_id
        opinion['caseconsolidated'] = 1 if opinion.get('caseconsolidated') else 0 # Ensure flags are int
        opinion['recordimpounded'] = 1 if opinion.get('recordimpounded') else 0

        # --- Save to Standard DBs (Primary/Backup/Test) ---
        for db_key, db_filename in dbs_to_target:
             results[db_key]["total"] += 1
             status = add_or_update_opinion_to_db(db_filename, opinion, is_validated, run_type)
             # Tally results
             if status == "inserted":
                 results[db_key]["inserted"] += 1; successful_writes_standard[db_key] = True; overall_success = True
             elif status == "updated":
                 results[db_key]["updated"] += 1; successful_writes_standard[db_key] = True; overall_success = True
             elif status.startswith("skipped"):
                 results[db_key]["skipped"] += 1
             else: # Error
                 results[db_key]["error"] += 1

        # --- Save to AllRuns History DB ---
        if all_runs_target:
             db_key_hist, db_filename_hist = all_runs_target
             results[db_key_hist]["total"] += 1
             hist_status = _save_to_all_runs_history(db_filename_hist, opinion, run_type)
             if hist_status == "inserted_history":
                  results[db_key_hist]["inserted_history"] += 1
                  successful_writes_history = True
                  overall_success = True
             elif hist_status.startswith("error"):
                  results[db_key_hist]["error_history"] += 1


    # --- Log Summary ---
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
        hist_path = all_runs_db_file or "N/A"
        if hist_results["total"] > 0 or hist_results["error_history"] == -1:
            if hist_results["error_history"] == -1:
                log.error(f"DB 'all_runs' ({hist_path}): Skipped due to initialization error.")
            else:
                log.info(f"DB 'all_runs' ({hist_path}): Processed {hist_results['total']} -> "
                         f"History Inserted: {hist_results['inserted_history']}, History Errors: {hist_results['error_history']}")


    # --- Update Config State ---
    if overall_success:
        try:
            log.info("Successful writes detected, updating run counter and timestamps.")
            current_counter = GconfigEM.increment_run_counter() # Increment once per successful call
            # Update timestamps only for DBs that had successful writes
            for db_key, success in successful_writes_standard.items():
                if success: GconfigEM.update_last_run_timestamp(db_key)
            if successful_writes_history: GconfigEM.update_last_run_timestamp("all_runs")
        except Exception as e:
            log.error(f"Failed to update config (counter/timestamps) after successful writes: {e}", exc_info=True)
    elif any(r['total'] > 0 for r in results.values()):
        log.warning("Opinions processed but no changes resulted in successful database writes.")

    return results


# --- Function to Build Combo DB ---
def build_combo_db(combo_db_file, primary_db_file, backup_db_file):
    """Clears and rebuilds the Combo DB from Primary and Backup."""
    log.info(f"Starting build of Combo DB: {combo_db_file}")

    # Ensure source DBs exist
    if not os.path.exists(primary_db_file):
        log.error(f"Cannot build Combo DB: Primary DB file not found at {primary_db_file}")
        return False, "Primary DB not found"
    if not os.path.exists(backup_db_file):
        log.error(f"Cannot build Combo DB: Backup DB file not found at {backup_db_file}")
        return False, "Backup DB not found"

    conn_combo = None
    conn_primary = None
    conn_backup = None
    success = False
    error_msg = None

    try:
        # Ensure Combo DB is initialized
        initialize_database(combo_db_file)
        conn_combo = get_db_connection(combo_db_file)
        cursor_combo = conn_combo.cursor()

        # Clear existing data from Combo DB
        log.debug("Clearing existing data from Combo DB.")
        cursor_combo.execute("DELETE FROM combined_opinions;")
        conn_combo.commit() # Commit delete before inserting

        # Connect to source DBs
        conn_primary = get_db_connection(primary_db_file)
        conn_backup = get_db_connection(backup_db_file)
        cursor_primary = conn_primary.cursor()
        cursor_backup = conn_backup.cursor()

        # Get all columns from the standard 'opinions' table schema
        # (Assume OPINIONS_TABLE_SCHEMA is accessible or hardcode columns)
        # Manually list columns to ensure order and exclude potential metadata issues
        cols = [
            "UniqueID", "AppDocketID", "ReleaseDate", "DataHash", "DuplicateFlag", "LinkedDocketIDs",
            "CaseName", "LCdocketID", "LCCounty", "Venue", "LowerCourtVenue", "LowerCourtSubCaseType",
            "OPJURISAPP", "DecisionTypeCode", "DecisionTypeText", "StateAgency1", "StateAgency2",
            "CaseNotes", "RunType", "entry_method", "validated", "caseconsolidated", "recordimpounded",
            "first_scraped_ts", "last_updated_ts", "last_validated_run_ts"
        ]
        cols_str = ", ".join(cols)
        placeholders = ", ".join(["?"] * (len(cols) + 1)) # +1 for SourceDB

        # Copy from Primary
        log.info(f"Copying data from Primary DB: {primary_db_file}")
        cursor_primary.execute(f"SELECT {cols_str} FROM opinions")
        primary_rows = cursor_primary.fetchall()
        insert_sql = f"INSERT INTO combined_opinions (SourceDB, {cols_str}) VALUES ({placeholders})"
        primary_data_to_insert = [('primary',) + tuple(row) for row in primary_rows]
        if primary_data_to_insert:
            cursor_combo.executemany(insert_sql, primary_data_to_insert)
            log.info(f"Inserted {len(primary_data_to_insert)} records from Primary DB.")
        else:
             log.info("No records found in Primary DB to copy.")

        # Copy from Backup
        log.info(f"Copying data from Backup DB: {backup_db_file}")
        cursor_backup.execute(f"SELECT {cols_str} FROM opinions")
        backup_rows = cursor_backup.fetchall()
        backup_data_to_insert = [('backup',) + tuple(row) for row in backup_rows]
        if backup_data_to_insert:
            cursor_combo.executemany(insert_sql, backup_data_to_insert)
            log.info(f"Inserted {len(backup_data_to_insert)} records from Backup DB.")
        else:
             log.info("No records found in Backup DB to copy.")

        conn_combo.commit() # Commit inserts
        success = True
        log.info(f"Successfully rebuilt Combo DB: {combo_db_file}")

    except sqlite3.Error as e:
        log.error(f"Database error building Combo DB: {e}", exc_info=True)
        if conn_combo: conn_combo.rollback()
        error_msg = f"Database error: {e}"
    except ConnectionError as e:
         log.error(f"Connection error building Combo DB: {e}")
         error_msg = f"Connection error: {e}"
    except Exception as e:
        log.error(f"Unexpected error building Combo DB: {e}", exc_info=True)
        if conn_combo: conn_combo.rollback()
        error_msg = f"Unexpected error: {e}"
    finally:
        if conn_combo: conn_combo.close()
        if conn_primary: conn_primary.close()
        if conn_backup: conn_backup.close()

    return success, error_msg


# --- Other functions like get_opinions_by_date_runtype, get_db_stats (largely unchanged) ---
# Ensure get_db_stats can handle the different table names based on DB type if needed,
# but currently it only queries 'opinions' table. Might need adjustment if stats
# are desired for all_runs or combo. For now, keep as is.

def get_opinions_by_date_runtype(db_filename, release_date, run_type):
    """Fetches opinions matching a specific release date and run type from 'opinions' table."""
    # This function remains targeted at the 'opinions' table structure
    log.debug(f"Fetching opinions from {db_filename} for ReleaseDate={release_date}, RunType={run_type}")
    opinions = {}
    conn = None
    try:
        # Check if the target DB uses the opinions schema
        db_basename = os.path.basename(db_filename)
        if db_basename == GconfigEM.DEFAULT_DB_NAMES.get("all_runs") or \
           db_basename == GconfigEM.DEFAULT_DB_NAMES.get("combo"):
            log.warning(f"get_opinions_by_date_runtype called on DB '{db_filename}' which doesn't use 'opinions' table structure. Returning empty.")
            return opinions # Return empty as this query structure doesn't apply

        conn = get_db_connection(db_filename)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT *
            FROM opinions
            WHERE ReleaseDate = ? AND RunType = ?
        """, (release_date, run_type))
        rows = cursor.fetchall()
        for row in rows:
            row_dict = dict(row)
            app_docket_id = row_dict.get("AppDocketID")
            if app_docket_id:
                 opinions[app_docket_id] = row_dict
            else:
                 log.warning(f"Record found with missing AppDocketID in {db_filename} for date {release_date}, run {run_type}. Skipping.")
        log.debug(f"Found {len(opinions)} records for {release_date} / {run_type} in {db_filename}")
    except sqlite3.Error as e:
        log.error(f"Error fetching opinions for {release_date}/{run_type} from {db_filename}: {e}", exc_info=True)
    except ConnectionError as e:
         log.warning(f"Connection error while fetching opinions for {release_date}/{run_type} from {db_filename}: {e}")
    finally:
        if conn:
            conn.close()
    return opinions

def get_db_stats(db_filename):
    """Provides basic statistics (total, validated) for DBs using the 'opinions' table."""
    stats = {"total": 0, "validated": 0, "unvalidated": 0, "schema_type": "unknown", "error": None}
    conn = None
    if not os.path.exists(db_filename):
        stats["error"] = "File not found"
        log.warning(f"Database file not found for stats: {db_filename}")
        return stats

    # Determine expected schema to adjust query if needed
    db_basename = os.path.basename(db_filename)
    schema_type = "opinions" # Default assumption
    table_name = "opinions"
    if db_basename == GconfigEM.DEFAULT_DB_NAMES.get("all_runs"):
        schema_type = "history"
        table_name = "opinion_history"
    elif db_basename == GconfigEM.DEFAULT_DB_NAMES.get("combo"):
         schema_type = "combo"
         table_name = "combined_opinions"
    stats["schema_type"] = schema_type

    try:
        conn = get_db_connection(db_filename)
        cursor = conn.cursor()

        # Get total count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total = cursor.fetchone()
        stats["total"] = total[0] if total else 0

        # Get validated count only if it's the opinions schema
        if schema_type == "opinions":
            cursor.execute("SELECT COUNT(*) FROM opinions WHERE validated = 1")
            validated = cursor.fetchone()
            stats["validated"] = validated[0] if validated else 0
            stats["unvalidated"] = stats["total"] - stats["validated"]
        elif schema_type == "combo":
             # Could count validated from primary/backup sources if needed
             cursor.execute("SELECT COUNT(*) FROM combined_opinions WHERE validated = 1 AND SourceDB = 'primary'")
             val_prim = cursor.fetchone()[0]
             cursor.execute("SELECT COUNT(*) FROM combined_opinions WHERE validated = 1 AND SourceDB = 'backup'")
             val_back = cursor.fetchone()[0]
             # This double counts if validated in both, maybe just report total?
             stats["validated"] = f"Primary: {val_prim}, Backup: {val_back}" # Example reporting
             stats["unvalidated"] = "N/A for Combo"


    except sqlite3.Error as e:
        log.error(f"Database error getting stats from '{db_filename}' (Schema: {schema_type}): {e}", exc_info=True)
        stats["error"] = str(e)
    except ConnectionError as e:
        stats["error"] = str(e)
    except Exception as e:
         log.error(f"Unexpected error getting stats from '{db_filename}': {e}", exc_info=True)
         stats["error"] = f"Unexpected error: {e}"
    finally:
        if conn:
            conn.close()
    return stats


# === End of GdbEM.py ===