# GdbEM.py
# V8: Add migration_source_version column and V3 migration logic
"""
Handles database interactions.
- V8: Added migration_source_version column and migration step V2->V3.
- V7: Implement schema versioning using PRAGMA user_version and migration logic.
- V6: Added 'opinionstatus' column.
- V5 Fix: Schema init order; ensure init in build_combo_db.
"""
import sqlite3
import os
import logging
import datetime
import hashlib
import uuid
import json
import GconfigEM

log = logging.getLogger(__name__)

# --- Schema Version ---
# Increment this number whenever the OPINIONS_TABLE_SCHEMA changes
# V1: Original schema before entry_method, opinionstatus
# V2: Added entry_method, opinionstatus
# V3: Added migration_source_version
LATEST_SCHEMA_VERSION = 3

# --- Database Schemas ---
# Schema parts for Primary, Backup, Test DBs ('opinions' table)
OPINIONS_TABLE_DEF = '''
CREATE TABLE IF NOT EXISTS opinions (
    UniqueID TEXT PRIMARY KEY, AppDocketID TEXT NOT NULL, ReleaseDate TEXT, DataHash TEXT NOT NULL,
    DuplicateFlag INTEGER DEFAULT 0, LinkedDocketIDs TEXT, CaseName TEXT, LCdocketID TEXT, LCCounty TEXT,
    Venue TEXT, LowerCourtVenue TEXT, LowerCourtSubCaseType TEXT, OPJURISAPP TEXT, DecisionTypeCode TEXT,
    DecisionTypeText TEXT, StateAgency1 TEXT, StateAgency2 TEXT, CaseNotes TEXT, RunType TEXT NOT NULL,
    entry_method TEXT,          -- Added V2
    validated BOOLEAN NOT NULL DEFAULT 0,
    caseconsolidated INTEGER DEFAULT 0,
    recordimpounded INTEGER DEFAULT 0,
    opinionstatus INTEGER DEFAULT 0, -- Added V2
    migration_source_version INTEGER, -- Added V3 (NULL if not migrated)
    first_scraped_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_validated_run_ts TIMESTAMP
);
'''

OPINIONS_INDEXES = '''
CREATE INDEX IF NOT EXISTS idx_opinions_appdocketid_releasedate ON opinions (AppDocketID, ReleaseDate);
CREATE INDEX IF NOT EXISTS idx_opinions_datahash ON opinions (DataHash);
CREATE INDEX IF NOT EXISTS idx_opinions_runtype ON opinions (RunType);
CREATE INDEX IF NOT EXISTS idx_opinions_validated ON opinions (validated);
CREATE INDEX IF NOT EXISTS idx_opinions_entrymethod ON opinions (entry_method);
CREATE INDEX IF NOT EXISTS idx_opinions_opinionstatus ON opinions (opinionstatus);
CREATE INDEX IF NOT EXISTS idx_opinions_migration_source ON opinions (migration_source_version); -- NEW Index V3
'''

OPINIONS_TRIGGER = '''
DROP TRIGGER IF EXISTS trg_opinions_update_timestamp; -- Drop first before creating
CREATE TRIGGER trg_opinions_update_timestamp
AFTER UPDATE ON opinions FOR EACH ROW
WHEN OLD.UniqueID = NEW.UniqueID
BEGIN
    UPDATE opinions SET last_updated_ts = CURRENT_TIMESTAMP WHERE UniqueID = OLD.UniqueID;
END;
'''

# --- Schemas for ALL_RUNS, COMBO (Unchanged from V6 - simple structures, no complex migration needed for them yet) ---
# Schema for GAllRunsOpinionsEM.db ('opinion_history' table)
ALL_RUNS_SCHEMA = ''' /* ... remains same ... */ '''
# Schema for GComboEM.db ('combined_opinions' table) - Updated manually if needed by build_combo_db
COMBO_SCHEMA = ''' /* ... remains same ... */ '''

# --- get_db_connection (Unchanged) ---
def get_db_connection(db_filename):
    # ... (code remains the same) ...
    if not db_filename:
        log.error("DB missing")
        raise ConnectionError("DB missing")
    db_dir = os.path.dirname(db_filename)
    if db_dir and not os.path.exists(db_dir):
        try:
            os.makedirs(db_dir, exist_ok=True)
            log.info(f"Created DB dir: {db_dir}")
        except OSError as e:
            log.error(f"Failed dir create {db_dir}: {e}")
            raise ConnectionError("Failed dir create") from e
    log.debug(f"Connecting to DB: {db_filename}")
    try:
        conn = sqlite3.connect(db_filename, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES, timeout=10.0)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e:
        log.error(f"Failed connect {db_filename}: {e}")
        raise ConnectionError("Could not connect") from e


# --- Migration Logic ---
def _check_column_exists(cursor, table_name, column_name):
    """Checks if a column exists in a table using PRAGMA table_info."""
    try:
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = [row['name'] for row in cursor.fetchall()]
        exists = column_name in columns
        log.debug(f"Column check: {table_name}.{column_name} exists? {exists}")
        return exists
    except sqlite3.Error as e:
        log.warning(f"Could not check column {table_name}.{column_name}: {e}")
        return False

def _run_migration(conn, current_version):
    """Applies migration steps sequentially based on current_version."""
    cursor = conn.cursor()
    log.info(f"DB schema version {current_version}. Applying migrations up to V{LATEST_SCHEMA_VERSION}.")

    try:
        conn.execute("BEGIN;") # Start transaction for migration

        # ----- Migration from Version 1 to Version 2 -----
        if current_version < 2:
            log.info("Applying migration V1 -> V2: Add entry_method, opinionstatus.")
            if not _check_column_exists(cursor, "opinions", "entry_method"):
                log.debug("Adding column: entry_method TEXT")
                cursor.execute("ALTER TABLE opinions ADD COLUMN entry_method TEXT;")
            if not _check_column_exists(cursor, "opinions", "opinionstatus"):
                log.debug("Adding column: opinionstatus INTEGER DEFAULT 0")
                cursor.execute("ALTER TABLE opinions ADD COLUMN opinionstatus INTEGER DEFAULT 0;")
            # No need to recreate indexes/trigger here, do it after all version bumps

        # ----- Migration from Version 2 to Version 3 -----
        if current_version < 3:
            log.info("Applying migration V2 -> V3: Add migration_source_version.")
            if not _check_column_exists(cursor, "opinions", "migration_source_version"):
                log.debug("Adding column: migration_source_version INTEGER")
                cursor.execute("ALTER TABLE opinions ADD COLUMN migration_source_version INTEGER;")
            # No need to recreate indexes/trigger here yet

        # ----- Add future migration steps here -----
        # if current_version < 4:
        #    log.info("Applying migration V3 -> V4: ...")
        #    # Add ALTER TABLE, UPDATE, etc. commands for V4 schema

        # ----- Finalization: Recreate Indexes/Trigger and Update Version -----
        if current_version < LATEST_SCHEMA_VERSION:
            log.info("Recreating indexes and trigger after schema modifications...")
            # Drop indexes (ignore errors) - Use specific index names
            indexes_to_drop = ["idx_opinions_appdocketid_releasedate", "idx_opinions_datahash", "idx_opinions_runtype", "idx_opinions_validated", "idx_opinions_entrymethod", "idx_opinions_opinionstatus", "idx_opinions_migration_source"]
            for idx in indexes_to_drop:
                try:
                    cursor.execute(f"DROP INDEX IF EXISTS {idx};")
                except sqlite3.Error as e:
                    log.warning(f"Ignoring error dropping index {idx}: {e}")
            # Drop trigger (ignore errors)
            try:
                cursor.execute("DROP TRIGGER IF EXISTS trg_opinions_update_timestamp;")
            except sqlite3.Error as e:
                log.warning(f"Ignoring error dropping trigger: {e}")

            # Recreate using latest definitions
            log.debug("Recreating indexes...")
            cursor.executescript(OPINIONS_INDEXES)
            log.debug("Recreating trigger...")
            cursor.executescript(OPINIONS_TRIGGER) # Executes DROP IF EXISTS then CREATE

            # Update version pragma to the latest version
            log.info(f"Updating database user_version to {LATEST_SCHEMA_VERSION}.")
            cursor.execute(f"PRAGMA user_version = {LATEST_SCHEMA_VERSION};")

        conn.commit() # Commit migration transaction
        log.info("Schema migration successful.")
        return True

    except sqlite3.Error as e:
        log.error(f"Database migration error during V{current_version} -> V{LATEST_SCHEMA_VERSION}: {e}", exc_info=True)
        log.warning("Rolling back migration changes.")
        conn.rollback()
        return False

# --- initialize_database (Updated for Migration Flow) ---
def initialize_database(db_filename):
    """Initializes and migrates the database schema."""
    log.info(f"Initializing/Migrating database schema: {db_filename}")
    is_opinions_db = False
    schema_to_use = None
    post_create_sql = None # Now only used for non-opinion tables if needed
    db_basename = os.path.basename(db_filename)
    db_configs = GconfigEM.DEFAULT_DB_NAMES

    # Determine schema type
    if db_basename == db_configs.get("all_runs"):
        schema_to_use = ALL_RUNS_SCHEMA
        log.info("Using 'opinion_history' schema.")
    elif db_basename == db_configs.get("combo"):
        schema_to_use = COMBO_SCHEMA
        log.info("Using 'combined_opinions' schema.")
    elif db_basename in [db_configs.get(k) for k in ["primary", "backup", "test"]]:
        schema_to_use = OPINIONS_TABLE_DEF
        is_opinions_db = True
        log.info("Using 'opinions' schema (with migration check).")
    else:
        log.warning(f"Unknown DB: {db_basename}. Using default 'opinions' schema.")
        schema_to_use = OPINIONS_TABLE_DEF
        is_opinions_db = True

    conn = None
    try:
        conn = get_db_connection(db_filename)
        cursor = conn.cursor()

        if is_opinions_db:
            log.debug("Checking schema version for opinions DB...")
            cursor.execute("PRAGMA user_version;")
            current_version = cursor.fetchone()[0]
            log.info(f"DB current user_version: {current_version}, Code expects: {LATEST_SCHEMA_VERSION}")
            # Ensure base table exists *before* migration attempt
            cursor.execute(OPINIONS_TABLE_DEF)
            conn.commit() # Commit base table check/creation
            if current_version < LATEST_SCHEMA_VERSION:
                if not _run_migration(conn, current_version):
                    raise sqlite3.Error("Schema migration failed.")
            else: # Version is current or newer, just ensure indexes/trigger exist
                log.debug("Schema version OK. Verifying indexes/trigger...")
                cursor.executescript(OPINIONS_INDEXES)
                cursor.executescript(OPINIONS_TRIGGER)
                conn.commit()
                log.debug("Indexes/trigger verified.")
        else: # For AllRuns, Combo - just create if not exists
            log.debug(f"Applying schema directly for {db_basename}...")
            cursor.executescript(schema_to_use)
            conn.commit()

        log.info(f"Database '{db_filename}' schema init/migration OK.")
    except sqlite3.Error as e:
        log.error(f"DB init/migration error '{db_filename}': {e}", exc_info=True)
        print(f"DB error {db_filename}: {e}")
        raise
    except ConnectionError as e:
        print(f"DB connection failed {db_filename}: {e}")
        raise
    finally:
        if conn:
            conn.close()


# --- initialize_all_databases (Updated) ---
def initialize_all_databases():
    """Initializes/Migrates schemas for all configured databases."""
    log.info("Initializing/Migrating schemas for all configured databases...")
    db_files = GconfigEM.get_db_filenames()
    init_ok = []
    init_fail = []
    
    for db_t, db_f in db_files.items():
        if not db_f:
            log.warning(f"Skip init '{db_t}': no filename.")
            continue
            
        try:
            log.debug(f"Initializing '{db_t}' ('{db_f}')")
            initialize_database(db_f)
            init_ok.append(db_f)
        except Exception as e:
            log.error(f"Failed init '{db_f}' ({db_t}): {e}", exc_info=True)
            init_fail.append(f"{db_f} ({db_t})")

    if init_fail:
        log.error(f"Failed init DBs: {', '.join(init_fail)}")
    if init_ok:
        log.info(f"Successfully init/migrated schemas for: {', '.join(init_ok)}")


# --- Data Handling Helpers (Unchanged) ---
def generate_data_hash(opinion_data): # ...
    core_data_str = ( f"{opinion_data.get('AppDocketID', '')}|{opinion_data.get('ReleaseDate', '')}|{opinion_data.get('CaseName', '')}|"
                      f"{opinion_data.get('DecisionTypeCode', '')}|{opinion_data.get('Venue', '')}|{opinion_data.get('LCdocketID', '')}|"
                      f"{opinion_data.get('LowerCourtVenue', '')}|{opinion_data.get('LowerCourtSubCaseType', '')}|"
                      f"{opinion_data.get('CaseNotes', '')}|{opinion_data.get('LinkedDocketIDs', '')}" )
    return hashlib.sha256(core_data_str.encode('utf-8')).hexdigest()
def generate_unique_id(data_hash, app_docket_id): # ...
    namespace = uuid.NAMESPACE_DNS; name_string = data_hash; base_uuid = uuid.uuid5(namespace, name_string); return str(base_uuid)
def check_duplicate_by_hash(cursor, data_hash): # ...
    try: cursor.execute("SELECT 1 FROM opinions WHERE DataHash = ? LIMIT 1", (data_hash,)); return cursor.fetchone() is not None
    except sqlite3.OperationalError: return False

# --- save_opinions_to_dbs (Updated) ---
def save_opinions_to_dbs(opinion_list, is_validated, run_type):
    """Saves opinions to appropriate databases based on run type."""
    if not opinion_list:
        log.warning("No opinions provided to save.")
        return {}  # Return empty dict instead of None
    
    results = {}  # Initialize results dict
    
    db_files = GconfigEM.get_db_filenames()
    std_targets = []
    ar_target = None
    log.info(f"Save: type '{run_type}'")
    
    target_keys_std = []
    target_ar = False
    
    # Determine target DBs based on run type
    if run_type.startswith('manual-test'):
        target_keys_std = ["primary", "backup", "test"]
        target_ar = True
        log.info("Test run: Target P/B/T + AR.")
    elif run_type == 'manual-primary-force':
        target_keys_std = ["primary"]
        target_ar = True
        log.info("Force run: Target P + AR.")
    elif run_type in ['scheduled-primary-1', 'scheduled-primary-2']:
        target_keys_std = ["primary"]
        target_ar = True
        log.info(f"{run_type}: Target P + AR.")
    elif run_type == 'scheduled-backup':
        target_keys_std = ["backup"]
        target_ar = True
        log.info("Backup run: Target B + AR.")
    elif run_type == 'user_validated':
        target_keys_std = ["primary", "backup"]
        target_ar = True
        log.info("Validated run: Target P/B + AR.")
    else:
        log.warning(f"Unrecognized run '{run_type}'. ONLY history save.")
        target_keys_std = []
        target_ar = True

    # Prepare targets
    for db_k in target_keys_std:
        fn = db_files.get(db_k)
        if fn:
            try:
                initialize_database(fn)
                std_targets.append((db_k, fn))
                results[db_k] = {
                    "total": 0,
                    "inserted": 0,
                    "updated": 0,
                    "skipped": 0,
                    "error": 0
                }
            except Exception as e:
                log.error(f"Failed init DB '{fn}' ({db_k}): {e}")
                results[db_k] = {"error": -1}
        else:
            log.error(f"Filename missing '{db_k}'.")

    try:
        # ...rest of the existing code...
        return results
    except Exception as e:
        log.error(f"Error during save operation: {e}", exc_info=True)
        return {"error": str(e)}  # Return error dict instead of None