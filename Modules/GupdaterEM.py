# GupdaterEM.py
"""
Handles retroactive updating of existing database records based on current parsing logic.
Cannot re-scrape, relies on existing data in the database.
"""
import sqlite3
import logging
import re
import GconfigEM
import GdbEM

log = logging.getLogger(__name__)

# --- Constants (copied from GscraperEM for reuse) ---

# County Code Map (ensure this is kept in sync with GscraperEM)
COUNTY_CODE_MAP = {
    "Atlantic County": "ATL", "Bergen County": "BER", "Burlington County": "BUR",
    "Camden County": "CAM", "Cape May County": "CPM", "Cumberland County": "CUM",
    "Essex County": "ESX", "Gloucester County": "GLO", "Hudson County": "HUD",
    "Hunterdon County": "HNT", "Mercer County": "MER", "Middlesex County": "MID",
    "Monmouth County": "MON", "Morris County": "MRS", "Ocean County": "OCN",
    "Passaic County": "PAS", "Salem County": "SLM", "Somerset County": "SOM",
    "Sussex County": "SSX", "Union County": "UNN", "Warren County": "WRN"
}

# Regex patterns for Special Civil Part dockets (ensure these match GscraperEM)
SPECIAL_CIVIL_PATTERNS = [
    re.compile(r'\b([A-Z]{3})-(DC|LT|SC)-(\d+)-(\d{2})\b', re.IGNORECASE),
    re.compile(r'\b(DC|LT|SC)-(\d+)-(\d{2})\b', re.IGNORECASE)
]
SPECIAL_CIVIL_VENUE = "Law Division - Special Civil Part"


# --- Updater Function ---

def run_retroactive_update(db_key, update_all=False):
    """
    Scans a database and updates records to conform to current formatting rules,
    specifically for Special Civil Part venue and CaseNotes.

    Args:
        db_key (str): The key for the database in config (e.g., 'primary', 'all_runs').
        update_all (bool): If True, update all records. If False, update only unvalidated.
    """
    log.info(f"Starting retroactive update for database key '{db_key}'. Update All: {update_all}")
    db_files = GconfigEM.get_db_filenames()
    db_filename = db_files.get(db_key)

    if not db_filename:
        log.error(f"No database filename configured for key '{db_key}'. Aborting update.")
        print(f"Error: Database for '{db_key}' not found in configuration.")
        return

    conn = None
    updated_count = 0
    scanned_count = 0
    fetch_limit = 500 # Process in batches to avoid memory issues

    try:
        log.info(f"Connecting to database: {db_filename}")
        conn = GdbEM.get_db_connection(db_filename) # Use existing connection func
        conn.row_factory = sqlite3.Row # Ensure row factory is set
        cursor = conn.cursor()
        update_cursor = conn.cursor() # Separate cursor for updates

        # Base query
        sql_select = """
            SELECT UniqueID, LCdocketID, LCCounty, LowerCourtVenue, LowerCourtSubCaseType, CaseNotes, validated
            FROM opinions
        """
        if not update_all:
            sql_select += " WHERE validated = 0"

        log.debug(f"Executing SELECT query: {sql_select} (batched)")
        cursor.execute(sql_select)

        while True:
            rows = cursor.fetchmany(fetch_limit)
            if not rows:
                break # No more rows

            updates_to_commit = [] # List of tuples (updates_dict, unique_id)

            for row in rows:
                scanned_count += 1
                row_dict = dict(row) # Work with a dictionary copy
                unique_id = row_dict['UniqueID']
                lc_docket_str = row_dict.get('LCdocketID') or ''
                lc_county = row_dict.get('LCCounty')
                current_venue = row_dict.get('LowerCourtVenue')
                current_subtype = row_dict.get('LowerCourtSubCaseType')
                current_notes = row_dict.get('CaseNotes') or ''

                updates = {}
                is_special_civil = False
                primary_sc_docket = None
                derived_subtype = None

                # Check if any docket in LCdocketID matches Special Civil patterns
                # This simplified check assumes LCdocketID might contain multiple comma-sep dockets
                dockets = [d.strip() for d in lc_docket_str.split(',') if d.strip()]
                for docket in dockets:
                    for pattern in SPECIAL_CIVIL_PATTERNS:
                        match = pattern.search(docket)
                        if match:
                            is_special_civil = True
                            primary_sc_docket = match.group(0) # Use the full matched docket
                            # Derive subtype based on matched group (DC/LT/SC)
                            type_code_match = re.search(r'-(DC|LT|SC)-', primary_sc_docket, re.IGNORECASE)
                            if type_code_match:
                                derived_subtype = f"Special Civil ({type_code_match.group(1).upper()})"
                            else: # Handle case where prefix might be missing but type code is first group
                                type_code_match_no_prefix = re.match(r'(DC|LT|SC)-', primary_sc_docket, re.IGNORECASE)
                                if type_code_match_no_prefix:
                                     derived_subtype = f"Special Civil ({type_code_match_no_prefix.group(1).upper()})"

                            log.debug(f"Record {unique_id[:8]}: Found Special Civil docket '{primary_sc_docket}'. Derived Subtype: {derived_subtype}")
                            break # Found a special civil docket for this record
                    if is_special_civil:
                        break

                # 1. Update Venue if necessary
                if is_special_civil and current_venue != SPECIAL_CIVIL_VENUE:
                    updates['LowerCourtVenue'] = SPECIAL_CIVIL_VENUE
                    log.info(f"Record {unique_id[:8]}: Updating venue to '{SPECIAL_CIVIL_VENUE}' (was '{current_venue}').")
                    # Also update subtype if derived and different
                    if derived_subtype and derived_subtype != current_subtype:
                         updates['LowerCourtSubCaseType'] = derived_subtype
                         log.info(f"Record {unique_id[:8]}: Updating subtype to '{derived_subtype}' (was '{current_subtype}').")

                # 2. Update CaseNotes if necessary for Special Civil
                if is_special_civil:
                    special_civil_note_to_add = None
                    # Construct the note [CCC-Docket] or [Docket]
                    ccc_match = re.match(r'([A-Z]{3})-', primary_sc_docket, re.IGNORECASE)
                    if ccc_match:
                        special_civil_note_to_add = f"[{primary_sc_docket}]" # CCC included in docket
                    else:
                        ccc = COUNTY_CODE_MAP.get(lc_county) if lc_county else None
                        if ccc:
                            special_civil_note_to_add = f"[{ccc}-{primary_sc_docket}]" # Prepend CCC
                        else:
                            special_civil_note_to_add = f"[{primary_sc_docket}]" # Fallback, no CCC

                    # Check if note already exists (exact match within brackets)
                    note_exists = special_civil_note_to_add in current_notes if special_civil_note_to_add else False

                    if special_civil_note_to_add and not note_exists:
                        # Prepend the note
                        new_notes = f"{special_civil_note_to_add}, {current_notes}" if current_notes else special_civil_note_to_add
                        updates['CaseNotes'] = new_notes.strip(', ')
                        log.info(f"Record {unique_id[:8]}: Prepending Special Civil note '{special_civil_note_to_add}'.")


                # If any updates were identified, add to batch
                if updates:
                    updates_to_commit.append((updates, unique_id))

            # --- Execute Batch Update ---
            if updates_to_commit:
                log.info(f"Committing batch of {len(updates_to_commit)} updates...")
                for update_data, uid in updates_to_commit:
                    set_clauses = ", ".join([f"{col} = ?" for col in update_data.keys()])
                    sql_values = list(update_data.values()) + [uid]
                    sql_update = f"UPDATE opinions SET {set_clauses} WHERE UniqueID = ?"
                    try:
                        update_cursor.execute(sql_update, tuple(sql_values))
                        updated_count += 1
                    except sqlite3.Error as e:
                         log.error(f"Failed to update record {uid[:8]} in {db_filename}: {e}", exc_info=True)

                conn.commit() # Commit after each batch
                log.info("Batch committed.")
            else:
                 log.debug("No updates needed in this batch.")


        log.info(f"Retroactive update scan complete for {db_filename}.")
        log.info(f"Scanned: {scanned_count} records. Updated: {updated_count} records.")
        print(f"Update complete for {db_filename}. Scanned: {scanned_count}, Updated: {updated_count}.")

    except sqlite3.Error as e:
        log.error(f"Database error during retroactive update for {db_filename}: {e}", exc_info=True)
        print(f"Database error during update: {e}")
    except ConnectionError as e:
        print(f"Database connection error during update: {e}")
    except Exception as e:
        log.error(f"Unexpected error during retroactive update for {db_filename}: {e}", exc_info=True)
        print(f"An unexpected error occurred during update: {e}")
    finally:
        if conn:
            conn.close()
            log.debug(f"Closed connection to {db_filename} after update.")


# === End of GupdaterEM.py ===