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
    if not db_filename: log.error("DB missing"); raise ConnectionError("DB missing")
    db_dir = os.path.dirname(db_filename)
    if db_dir and not os.path.exists(db_dir): try: os.makedirs(db_dir, exist_ok=True); log.info(f"Created DB dir: {db_dir}")
    except OSError as e: log.error(f"Failed dir create {db_dir}: {e}"); raise ConnectionError("Failed dir create") from e
    log.debug(f"Connecting to DB: {db_filename}")
    try: conn = sqlite3.connect(db_filename, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES, timeout=10.0); conn.row_factory = sqlite3.Row; return conn
    except sqlite3.Error as e: log.error(f"Failed connect {db_filename}: {e}"); raise ConnectionError("Could not connect") from e


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
                 log.debug("Adding column: entry_method TEXT"); cursor.execute("ALTER TABLE opinions ADD COLUMN entry_method TEXT;")
            if not _check_column_exists(cursor, "opinions", "opinionstatus"):
                 log.debug("Adding column: opinionstatus INTEGER DEFAULT 0"); cursor.execute("ALTER TABLE opinions ADD COLUMN opinionstatus INTEGER DEFAULT 0;")
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
                  try: cursor.execute(f"DROP INDEX IF EXISTS {idx};")
                  except sqlite3.Error as e: log.warning(f"Ignoring error dropping index {idx}: {e}")
             # Drop trigger (ignore errors)
             try: cursor.execute("DROP TRIGGER IF EXISTS trg_opinions_update_timestamp;")
             except sqlite3.Error as e: log.warning(f"Ignoring error dropping trigger: {e}")

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
    db_basename = os.path.basename(db_filename); db_configs = GconfigEM.DEFAULT_DB_NAMES

    # Determine schema type
    if db_basename == db_configs.get("all_runs"): schema_to_use = ALL_RUNS_SCHEMA; log.info("Using 'opinion_history' schema.")
    elif db_basename == db_configs.get("combo"): schema_to_use = COMBO_SCHEMA; log.info("Using 'combined_opinions' schema.")
    elif db_basename in [db_configs.get(k) for k in ["primary", "backup", "test"]]:
        schema_to_use = OPINIONS_TABLE_DEF; is_opinions_db = True; log.info("Using 'opinions' schema (with migration check).")
    else: log.warning(f"Unknown DB: {db_basename}. Using default 'opinions' schema."); schema_to_use = OPINIONS_TABLE_DEF; is_opinions_db = True

    conn = None
    try:
        conn = get_db_connection(db_filename); cursor = conn.cursor()

        if is_opinions_db:
            log.debug("Checking schema version for opinions DB..."); cursor.execute("PRAGMA user_version;"); current_version = cursor.fetchone()[0]
            log.info(f"DB current user_version: {current_version}, Code expects: {LATEST_SCHEMA_VERSION}")
            # Ensure base table exists *before* migration attempt
            cursor.execute(OPINIONS_TABLE_DEF); conn.commit() # Commit base table check/creation
            if current_version < LATEST_SCHEMA_VERSION:
                if not _run_migration(conn, current_version): raise sqlite3.Error("Schema migration failed.")
            else: # Version is current or newer, just ensure indexes/trigger exist
                 log.debug("Schema version OK. Verifying indexes/trigger..."); cursor.executescript(OPINIONS_INDEXES); cursor.executescript(OPINIONS_TRIGGER); conn.commit(); log.debug("Indexes/trigger verified.")
        else: # For AllRuns, Combo - just create if not exists
            log.debug(f"Applying schema directly for {db_basename}..."); cursor.executescript(schema_to_use); conn.commit()

        log.info(f"Database '{db_filename}' schema init/migration OK.")
    except sqlite3.Error as e: log.error(f"DB init/migration error '{db_filename}': {e}", exc_info=True); print(f"DB error {db_filename}: {e}"); raise
    except ConnectionError as e: print(f"DB connection failed {db_filename}: {e}"); raise
    finally:
        if conn: conn.close()


# --- initialize_all_databases (Unchanged) ---
def initialize_all_databases():
    # ... (code remains the same) ...
    log.info("Initializing/Migrating schemas for all configured databases...")
    db_files=GconfigEM.get_db_filenames(); init_ok, init_fail = [], []
    for db_t, db_f in db_files.items():
        if not db_f: log.warning(f"Skip init '{db_t}': no filename."); continue
        try: log.debug(f"Initializing '{db_t}' ('{db_f}')"); initialize_database(db_f); init_ok.append(db_f)
        except Exception as e: log.error(f"Failed init '{db_f}' ({db_t}): {e}", exc_info=True); init_fail.append(f"{db_f} ({db_t})")
    if init_fail: log.error(f"Failed init DBs: {', '.join(init_fail)}")
    if init_ok: log.info(f"Successfully init/migrated schemas for: {', '.join(init_ok)}")


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

# --- add_or_update_opinion_to_db (Unchanged from V6 - already includes opinionstatus) ---
def add_or_update_opinion_to_db(db_filename, opinion_data, is_validated, run_type):
    """Adds/updates opinion in 'opinions' table. Includes opinionstatus."""
    # ... (code remains the same as V6) ...
    required_keys = ["AppDocketID", "CaseName", "UniqueID", "DataHash"]
    if not all(key in opinion_data for key in required_keys): return "error_missing_keys"
    status, conn = "error_unknown", None
    app_docket_id, unique_id, data_hash = opinion_data["AppDocketID"], opinion_data["UniqueID"], opinion_data["DataHash"]
    try:
        conn = get_db_connection(db_filename); cursor = conn.cursor()
        cursor.execute("SELECT * FROM opinions WHERE UniqueID = ?", (unique_id,))
        existing_opinion_row = cursor.fetchone(); now_ts = datetime.datetime.now()
        entry_method = "unknown"
        if run_type == 'manual-immediate' or run_type == 'manual-primary-force': entry_method = 'user_forced_validated' if is_validated else 'user_forced_unvalidated'
        elif run_type.startswith('manual-test'): entry_method = 'test_run'
        elif run_type.startswith('scheduled-'): entry_method = 'scheduled_unvalidated'
        elif run_type == 'user_validated': entry_method = 'user_validated'
        opinion_status_val = opinion_data.get("opinionstatus", 0)
        if existing_opinion_row is None:
            log.info(f"Inserting {unique_id[:8]} ({app_docket_id}) method '{entry_method}' -> {db_filename}.")
            duplicate_flag = 1 if check_duplicate_by_hash(cursor, data_hash) else 0
            sql = '''INSERT INTO opinions (UniqueID, AppDocketID, ReleaseDate, DataHash, DuplicateFlag, LinkedDocketIDs, CaseName, LCdocketID, LCCounty, Venue, LowerCourtVenue, LowerCourtSubCaseType, OPJURISAPP, DecisionTypeCode, DecisionTypeText, StateAgency1, StateAgency2, CaseNotes, RunType, validated, entry_method, caseconsolidated, recordimpounded, opinionstatus, last_validated_run_ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
            values = ( unique_id, app_docket_id, opinion_data.get("ReleaseDate"), data_hash, duplicate_flag, opinion_data.get("LinkedDocketIDs"), opinion_data.get("CaseName"), opinion_data.get("LCdocketID"), opinion_data.get("LCCounty"), opinion_data.get("Venue"), opinion_data.get("LowerCourtVenue"), opinion_data.get("LowerCourtSubCaseType"), opinion_data.get("OPJURISAPP", "Statewide"), opinion_data.get("DecisionTypeCode"), opinion_data.get("DecisionTypeText"), opinion_data.get("StateAgency1"), opinion_data.get("StateAgency2"), opinion_data.get("CaseNotes"), run_type, is_validated, entry_method, opinion_data.get("caseconsolidated", 0), opinion_data.get("recordimpounded", 0), opinion_status_val, now_ts if is_validated else None )
            try: cursor.execute(sql, values); status = "inserted"
            except sqlite3.IntegrityError: log.warning(f"Insert fail (Integrity) {unique_id[:8]}. Try update."); existing_opinion_row = True; status = "skipped_insert_exists"
            except sqlite3.Error as oe: log.error(f"Insert DB error {app_docket_id}: {oe}", exc_info=True); status = f"error_insert"; raise oe
        if existing_opinion_row:
            if status != "skipped_insert_exists": cursor.execute("SELECT * FROM opinions WHERE UniqueID = ?", (unique_id,)); existing_opinion_row = cursor.fetchone()
            if not existing_opinion_row: log.error(f"Failed re-fetch {unique_id[:8]}"); return "error_fetch_update"
            log.info(f"Processing existing {unique_id[:8]} ({app_docket_id}) -> {db_filename}.")
            existing_opinion_dict = dict(existing_opinion_row); existing_validated = bool(existing_opinion_dict['validated'])
            is_incoming_validated = is_validated or (entry_method == 'user_validated')
            allow_update = is_incoming_validated or not existing_validated
            if not allow_update: log.info(f"Skipping update: VALIDATED {unique_id[:8]} locked by unvalidated '{run_type}'."); status = "skipped_validated_locked"
            else:
                 update_fields, changed = {}, False; fields_to_compare = ["AppDocketID", "ReleaseDate", "DataHash", "DuplicateFlag", "LinkedDocketIDs", "CaseName", "LCdocketID", "LCCounty", "Venue", "LowerCourtVenue", "LowerCourtSubCaseType", "OPJURISAPP", "DecisionTypeCode", "DecisionTypeText", "StateAgency1", "StateAgency2", "CaseNotes", "RunType", "validated", "entry_method", "caseconsolidated", "recordimpounded", "opinionstatus", "last_validated_run_ts"]
                 new_values = opinion_data.copy(); new_values['RunType'] = run_type; new_values['validated'] = is_validated; new_values['entry_method'] = entry_method; new_values['caseconsolidated'] = 1 if new_values.get('caseconsolidated') else 0; new_values['recordimpounded'] = 1 if new_values.get('recordimpounded') else 0; new_values['opinionstatus'] = opinion_status_val; new_values['last_validated_run_ts'] = now_ts if is_validated else None; new_values['DuplicateFlag'] = 1 if check_duplicate_by_hash(cursor, data_hash) else 0
                 for field in fields_to_compare:
                     new_val = new_values.get(field); existing_val = existing_opinion_dict.get(field); is_diff = False
                     if isinstance(existing_val,(str,type(None))) and isinstance(new_val,(str,type(None))): is_diff=(existing_val or '')!=(new_val or '')
                     elif existing_val != new_val: is_diff = True
                     if is_diff: update_fields[field] = new_val; changed = True; log.debug(f"Field '{field}' changed")
                 if changed:
                     set_clauses = ", ".join([f'"{col}" = ?' for col in update_fields.keys()]); sql_values = list(update_fields.values()) + [unique_id]
                     sql = f'UPDATE opinions SET {set_clauses} WHERE "UniqueID" = ?'; log.debug(f"Executing update {unique_id[:8]}..."); cursor.execute(sql, tuple(sql_values)); status = "updated"
                 else: status = "skipped_no_changes"; log.info(f"Skipping update {unique_id[:8]}: no changes.")
        conn.commit()
    except sqlite3.Error as e: log.error(f"DB error {app_docket_id}: {e}", exc_info=True); status = f"error_sqlite"; conn.rollback()
    except ConnectionError as e: status = f"error_connection"
    except Exception as e: log.error(f"Unexpected error {app_docket_id}: {e}", exc_info=True); status = f"error_unexpected"; conn.rollback()
    finally:
        if conn: conn.close()
    return status

# --- _save_to_all_runs_history (Unchanged) ---
def _save_to_all_runs_history(db_filename, opinion_data, run_type):
    # ... (code remains the same) ...
    unique_id=opinion_data.get("UniqueID"); app_id=opinion_data.get("AppDocketID"); rel_dt=opinion_data.get("ReleaseDate")
    if not unique_id or not app_id: log.warning("Skip history: no ID."); return "error_keys"
    conn, status = None, "error_hist"
    try:
        conn=get_db_connection(db_filename); cursor=conn.cursor(); opinion_json=json.dumps(opinion_data, default=str)
        sql='INSERT INTO opinion_history (UniqueID, AppDocketID, ReleaseDate, RunType, OpinionDataJSON) VALUES (?, ?, ?, ?, ?)'
        cursor.execute(sql, (unique_id, app_id, rel_dt, run_type, opinion_json)); conn.commit(); status="inserted_history"
        log.debug(f"History insert {unique_id[:8]} ({run_type})")
    except Exception as e: log.error(f"History save error {unique_id[:8]}: {e}", exc_info=True); status=f"error_hist_save"; conn.rollback()
    finally:
        if conn: conn.close()
    return status


# --- save_opinions_to_dbs (Unchanged) ---
def save_opinions_to_dbs(opinion_list, is_validated, run_type):
    # ... (code remains the same - logic handles different run types and targets) ...
    db_files=GconfigEM.get_db_filenames(); results={}; std_targets=[]; ar_target=None; log.info(f"Save: type '{run_type}'")
    target_keys_std, target_ar = [], False
    if run_type.startswith('manual-test'): target_keys_std, target_ar = ["primary", "backup", "test"], True; log.info("Test run: Target P/B/T + AR.")
    elif run_type=='manual-primary-force': target_keys_std, target_ar = ["primary"], True; log.info("Force run: Target P + AR.")
    elif run_type in ['scheduled-primary-1','scheduled-primary-2']: target_keys_std, target_ar = ["primary"], True; log.info(f"{run_type}: Target P + AR.")
    elif run_type=='scheduled-backup': target_keys_std, target_ar = ["backup"], True; log.info("Backup run: Target B + AR.")
    elif run_type=='user_validated': target_keys_std, target_ar = ["primary", "backup"], True; log.info("Validated run: Target P/B + AR.")
    else: log.warning(f"Unrecognized run '{run_type}'. ONLY history save."); target_keys_std, target_ar = [], True
    # Prepare targets... (code unchanged)
    for db_k in target_keys_std: fn=db_files.get(db_k);
                              if fn: try: initialize_database(fn); std_targets.append((db_k, fn)); results[db_k] = {"total": 0, "inserted": 0, "updated": 0, "skipped": 0, "error": 0}
                                     except Exception as e: log.error(f"Failed init DB '{fn}' ({db_k}): {e}"); results[db_k]={"error": -1}
                              else: log.error(f"Filename missing '{db_k}'.")
    if target_ar: fn_ar=db_files.get("all_runs");
                  if fn_ar: try: initialize_database(fn_ar); ar_target=("all_runs", fn_ar); results["all_runs"]={"total": 0, "inserted_history": 0, "error_history": 0}
                            except Exception as e: log.error(f"Failed init AR '{fn_ar}': {e}"); results["all_runs"]={"error_history": -1}
                  else: log.error("Filename missing 'all_runs'.")
    if not std_targets and not ar_target: log.error(f"No targets for '{run_type}'."); return results
    # Process opinions... (code unchanged)
    if not opinion_list: log.info("No opinions to save."); return results
    s_writes_std={k:False for k,_ in std_targets}; s_writes_hist, overall_ok = False, False
    for op_orig in opinion_list:
        if not isinstance(op_orig,dict): continue; op=op_orig.copy()
        app_id, cname = op.get("AppDocketID"), op.get("CaseName");
        if not app_id or not cname: continue
        d_hash=generate_data_hash(op); u_id=generate_unique_id(d_hash, app_id); op["DataHash"], op["UniqueID"] = d_hash, u_id
        op['caseconsolidated']=1 if op.get('caseconsolidated') else 0; op['recordimpounded']=1 if op.get('recordimpounded') else 0
        # Save standard
        for db_k, db_f in std_targets: results[db_k]["total"]+=1; status=add_or_update_opinion_to_db(db_f, op, is_validated, run_type);
                                      if status=="inserted": results[db_k]["inserted"]+=1; s_writes_std[db_k]=True; overall_ok=True
                                      elif status=="updated": results[db_k]["updated"]+=1; s_writes_std[db_k]=True; overall_ok=True
                                      elif status.startswith("skipped"): results[db_k]["skipped"]+=1
                                      else: results[db_k]["error"]+=1
        # Save history
        if ar_target: db_k_h, db_f_h = ar_target; results[db_k_h]["total"]+=1; hist_status=_save_to_all_runs_history(db_f_h, op, run_type)
                      if hist_status=="inserted_history": results[db_k_h]["inserted_history"]+=1; s_writes_hist=True; overall_ok=True
                      else: results[db_k_h]["error_history"]+=1
    # Log summary... (code unchanged)
    for db_k, res in results.items(): db_p=db_files.get(db_k,"N/A");
                                   if res.get("error",0)==-1 or res.get("error_history",0)==-1: log.error(f"DB '{db_k}' Skipped: init error.")
                                   elif res.get("total",0)>0:
                                       if db_k=="all_runs": log.info(f"DB AR: Proc {res['total']}->HistIns:{res['inserted_history']}, HistErr:{res['error_history']}")
                                       else: log.info(f"DB '{db_k}': Proc {res['total']}->Ins:{res['inserted']}, Upd:{res['updated']}, Skp:{res['skipped']}, Err:{res['error']}")
    # Update config... (code unchanged)
    if overall_ok: try: log.info("Updating run counter/timestamps."); GconfigEM.increment_run_counter();
                         for db_k, success in s_writes_std.items():
                             if success: GconfigEM.update_last_run_timestamp(db_k)
                         if s_writes_hist: GconfigEM.update_last_run_timestamp("all_runs")
                   except Exception as e: log.error(f"Failed config update: {e}", exc_info=True)
    elif any(r.get('total',0)>0 for r in results.values()): log.warning("Processed but no successful DB writes.")
    return results


# --- build_combo_db (Add opinionstatus and migration_source_version) ---
def build_combo_db(combo_db_file, primary_db_file, backup_db_file):
    """ Clears and rebuilds the Combo DB from Primary and Backup. Includes new columns."""
    log.info(f"Building Combo DB: {combo_db_file}")
    if not os.path.exists(primary_db_file): return False, "Primary DB missing"
    if not os.path.exists(backup_db_file): return False, "Backup DB missing"
    conn_c, conn_p, conn_b = None, None, None; success, msg = False, None
    try:
        log.info("Initializing/Migrating schemas...");
        try: initialize_database(combo_db_file)
        except Exception as e: raise ConnectionError(f"Failed init Combo DB: {e}")
        try: initialize_database(primary_db_file)
        except Exception as e: raise ConnectionError(f"Failed init Primary DB: {e}")
        try: initialize_database(backup_db_file)
        except Exception as e: raise ConnectionError(f"Failed init Backup DB: {e}")
        log.info("Schemas OK.")

        conn_c=get_db_connection(combo_db_file); cur_c=conn_c.cursor()
        log.debug("Clearing Combo DB."); cur_c.execute("BEGIN;"); cur_c.execute("DELETE FROM combined_opinions;");

        conn_p=get_db_connection(primary_db_file); cur_p=conn_p.cursor()
        conn_b=get_db_connection(backup_db_file); cur_b=conn_b.cursor()

        # Updated column list for V3 schema
        cols = ["UniqueID", "AppDocketID", "ReleaseDate", "DataHash", "DuplicateFlag", "LinkedDocketIDs", "CaseName", "LCdocketID", "LCCounty", "Venue", "LowerCourtVenue", "LowerCourtSubCaseType", "OPJURISAPP", "DecisionTypeCode", "DecisionTypeText", "StateAgency1", "StateAgency2", "CaseNotes", "RunType", "entry_method", "validated", "caseconsolidated", "recordimpounded", "opinionstatus", "migration_source_version", "first_scraped_ts", "last_updated_ts", "last_validated_run_ts"]
        cols_str = ", ".join(f'"{c}"' for c in cols); placeholders = ", ".join(["?"] * (len(cols) + 1))
        insert_sql = f'INSERT INTO combined_opinions (SourceDB, {cols_str}) VALUES ({placeholders})'

        log.info(f"Copying from Primary: {primary_db_file}"); cur_p.execute(f"SELECT {cols_str} FROM opinions"); rows_p=cur_p.fetchall()
        data_p = [('primary',) + tuple(r) for r in rows_p];
        if data_p: cur_c.executemany(insert_sql, data_p); log.info(f"Prepared {len(data_p)} from Primary.")
        else: log.info("No records in Primary.")

        log.info(f"Copying from Backup: {backup_db_file}"); cur_b.execute(f"SELECT {cols_str} FROM opinions"); rows_b=cur_b.fetchall()
        data_b = [('backup',) + tuple(r) for r in rows_b];
        if data_b: cur_c.executemany(insert_sql, data_b); log.info(f"Prepared {len(data_b)} from Backup.")
        else: log.info("No records in Backup.")

        conn_c.commit(); success=True; log.info(f"Rebuilt Combo DB: {combo_db_file}")
    except (sqlite3.Error, ConnectionError) as e: log.error(f"Combo build error: {e}", exc_info=True); msg=f"DB/Connection error: {e}"; conn_c.rollback()
    except Exception as e: log.error(f"Unexpected combo build error: {e}", exc_info=True); msg=f"Unexpected error: {e}"; conn_c.rollback()
    finally:
        if conn_c: conn_c.close();
        if conn_p: conn_p.close();
        if conn_b: conn_b.close()
    return success, msg

# --- get_opinions_by_date_runtype (Unchanged) ---
def get_opinions_by_date_runtype(db_filename, release_date, run_type):
    # ... (code remains the same) ...
    log.debug(f"Fetching opinions {db_filename} {release_date}/{run_type}")
    opinions, conn = {}, None
    try:
        db_basename=os.path.basename(db_filename); db_configs=GconfigEM.DEFAULT_DB_NAMES
        if db_basename == db_configs.get("all_runs") or db_basename == db_configs.get("combo"): return opinions
        conn = get_db_connection(db_filename); cursor = conn.cursor()
        cursor.execute("SELECT * FROM opinions WHERE ReleaseDate = ? AND RunType = ?", (release_date, run_type))
        rows = cursor.fetchall(); opinions = {dict(r)['AppDocketID']: dict(r) for r in rows if dict(r).get('AppDocketID')}
        log.debug(f"Found {len(opinions)} for {release_date}/{run_type} in {db_filename}")
    except Exception as e: log.error(f"DB error fetch {release_date}/{run_type} from {db_filename}: {e}", exc_info=True)
    finally:
        if conn: conn.close()
    return opinions

# --- get_db_stats (Add opinionstatus/migration to output maybe?) ---
def get_db_stats(db_filename):
    """Provides basic statistics for DBs based on their schema."""
    # Add migration_source_version count? Keep simple for now.
    stats = {"total": 0, "validated": "N/A", "unvalidated": "N/A", "schema_type": "unknown", "schema_version": "N/A", "error": None}
    # ... (rest of the function remains the same as V6) ...
    conn = None
    if not os.path.exists(db_filename): stats["error"] = "File not found"; return stats
    db_basename = os.path.basename(db_filename); db_configs = GconfigEM.DEFAULT_DB_NAMES
    schema_type, table_name = "unknown", None
    is_opinions_schema = False
    if db_basename == db_configs.get("all_runs"): schema_type, table_name = "history", "opinion_history"
    elif db_basename == db_configs.get("combo"): schema_type, table_name = "combo", "combined_opinions"
    elif db_basename in [db_configs.get(k) for k in ["primary", "backup", "test"]]: schema_type, table_name = "opinions", "opinions"; is_opinions_schema = True
    else: stats["error"] = "Unrecognized DB"; return stats
    stats["schema_type"] = schema_type
    try:
        conn = get_db_connection(db_filename); cursor = conn.cursor()
        if is_opinions_schema: # Get schema version for opinions tables
             cursor.execute("PRAGMA user_version;"); ver = cursor.fetchone(); stats["schema_version"] = ver[0] if ver else 0
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}"); total = cursor.fetchone()
        stats["total"] = total[0] if total else 0
        if schema_type == "opinions":
            cursor.execute("SELECT COUNT(*) FROM opinions WHERE validated = 1"); validated = cursor.fetchone()
            stats["validated"] = validated[0] if validated else 0; stats["unvalidated"] = stats["total"] - stats["validated"]
        elif schema_type == "combo":
             cursor.execute("SELECT COUNT(*) FROM combined_opinions WHERE validated = 1 AND SourceDB = 'primary'"); vp = cursor.fetchone()[0]
             cursor.execute("SELECT COUNT(*) FROM combined_opinions WHERE validated = 1 AND SourceDB = 'backup'"); vb = cursor.fetchone()[0]
             stats["validated"] = f"P:{vp}, B:{vb}"
    except Exception as e: stats["error"] = str(e); log.error(f"DB stats error '{db_filename}' ({schema_type}): {e}", exc_info=True)
    finally:
        if conn: conn.close()
    return stats

# === End of GdbEM.py ===