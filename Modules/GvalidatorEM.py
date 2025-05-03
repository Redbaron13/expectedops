# GvalidatorEM.py
# V3: Added entry_method update on validation
"""
Handles manual validation of scraped opinion entries.
Includes display of potential decision PDF URL.
Allows listing of entries needing LC Docket ID review.
Updates 'entry_method' upon successful validation.
"""
import sqlite3
import shutil
import os
import logging
import re
import datetime
import GconfigEM
import GdbEM # For DB connection and URL construction helper if moved there

# Setup logger for this module
log = logging.getLogger(__name__)

# --- Helper to Construct URL (Keep here or move to GdbEM/utils?) ---
def construct_decision_url(app_docket_id, release_date_str):
    """Constructs the potential URL for a decision PDF."""
    # ... (implementation is the same as previous version) ...
    if not app_docket_id or not release_date_str:
        return None
    cleaned_docket = re.sub(r'[^a-z0-9-]', '', app_docket_id.lower())
    try:
        # Ensure date is in YYYY-MM-DD format for parsing
        release_date_dt = datetime.datetime.strptime(release_date_str, '%Y-%m-%d')
        release_year = release_date_dt.year
    except (ValueError, TypeError):
        log.warning(f"Could not parse release date '{release_date_str}' to get year for URL construction.")
        return None
    url = f"https://www.njcourts.gov/system/files/court-opinions/{release_year}/{cleaned_docket}.pdf"
    return url


# --- Main Validation Function (Updated) ---
def validate_case(unique_id_to_validate, db_key="primary"): # Add db_key parameter
    """
    Allows interactive review and validation of a specific case entry by its UniqueID
    in the specified database (defaulting to primary). Updates entry_method.
    """
    log.info(f"Starting validation process for UniqueID: {unique_id_to_validate} in DB: '{db_key}'")
    db_files = GconfigEM.get_db_filenames()
    db_filename = db_files.get(db_key)

    # Check if the target DB is appropriate for validation (uses 'opinions' schema)
    db_basename = os.path.basename(db_filename) if db_filename else None
    allowed_db_keys = ["primary", "backup", "test"] # DBs with the 'opinions' table
    allowed_db_files = [GconfigEM.DEFAULT_DB_NAMES.get(k) for k in allowed_db_keys]

    if not db_filename:
        print(f"Error: Database file for '{db_key}' not found in configuration.")
        log.error(f"Validation failed: DB filename for '{db_key}' missing.")
        return
    if db_basename not in allowed_db_files:
         print(f"Error: Validation can only be performed on Primary, Backup, or Test databases. '{db_key}' ({db_basename}) is not suitable.")
         log.error(f"Validation failed: Attempted validation on unsuitable DB type '{db_key}' ({db_basename}).")
         return
    if not os.path.exists(db_filename):
        print(f"Error: Database file '{db_filename}' not found.")
        log.error(f"Validation failed: Database file '{db_filename}' does not exist.")
        return

    conn = None
    try:
        conn = GdbEM.get_db_connection(db_filename)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM opinions WHERE UniqueID = ?", (unique_id_to_validate,))
        row = cursor.fetchone()

        if not row:
            print(f"No entry found with UniqueID {unique_id_to_validate} in '{db_filename}'.")
            log.warning(f"validate_case called for non-existent UniqueID: {unique_id_to_validate} in DB {db_key}")
            return

        entry = dict(row)
        original_entry = entry.copy()

        print(f"\n--- Reviewing Entry UniqueID: {entry['UniqueID'][:8]}... (DB: {db_key}) ---")
        print(f"  Appellate Docket: {entry.get('AppDocketID', 'N/A')}")
        print(f"  Release Date:     {entry.get('ReleaseDate', 'N/A')}")
        print(f"  Current Validated Status: {bool(entry.get('validated', 0))}")
        print(f"  Current Entry Method:   {entry.get('entry_method', 'N/A')}")

        # --- Display Potential PDF URL ---
        pdf_url = construct_decision_url(entry.get('AppDocketID'), entry.get('ReleaseDate'))
        if pdf_url: print(f"  Potential PDF URL: {pdf_url}")
        else: print("  (Could not construct potential PDF URL)")

        print("\n--- Current Data (Editable Fields) ---")
        editable_fields = list(entry.keys())
        non_editable = ['UniqueID', 'DataHash', 'DuplicateFlag', 'first_scraped_ts', 'last_updated_ts', 'last_validated_run_ts', 'RunType', 'entry_method'] # Add entry_method here
        for key in non_editable:
            if key in editable_fields: editable_fields.remove(key)
        # Also don't directly edit validated here, use the prompt below
        if 'validated' in editable_fields: editable_fields.remove('validated')

        updated_values = {}
        for key in sorted(editable_fields): # Sort for consistent order
            current_value = entry.get(key)
            display_value = current_value if current_value is not None else "[empty]"
            if key in ['caseconsolidated', 'recordimpounded']: display_value = bool(current_value)

            user_input = input(f"  {key:<22}: {display_value} | Edit? (Enter new value or press Enter): ").strip()

            if user_input:
                new_value = user_input
                if key in ['caseconsolidated', 'recordimpounded']:
                    if user_input.lower() in ['true', '1', 'yes', 'y']: new_value = 1
                    elif user_input.lower() in ['false', '0', 'no', 'n']: new_value = 0
                    else:
                        print(f"    Invalid input for {key}. Keeping original."); new_value = current_value
                updated_values[key] = new_value

        # --- Validation Status ---
        entry.update(updated_values) # Apply field edits before asking validation
        print("\n--- Validation Status ---")
        confirm_validate_input = input(f"Mark this entry as validated? (Current: {bool(original_entry['validated'])}) (y/n/Enter=no change): ").strip().lower()
        validation_changed = False
        new_validated_status = original_entry['validated']

        if confirm_validate_input == 'y':
            new_validated_status = 1; validation_changed = True; print("Entry will be marked as validated.")
        elif confirm_validate_input == 'n':
            new_validated_status = 0; validation_changed = True; print("Entry will be marked as NOT validated.")
        else:
            print("Validation status remains unchanged.")

        if validation_changed:
            entry['validated'] = new_validated_status
            entry['last_validated_run_ts'] = datetime.datetime.now().isoformat() if new_validated_status == 1 else None
            # Set entry_method if validated
            if new_validated_status == 1:
                 entry['entry_method'] = 'user_validated'
            # If marked unvalidated, maybe revert entry_method? Or keep historical? Keep for now.

        # --- Final confirmation ---
        made_changes = bool(updated_values or validation_changed)
        if not made_changes:
            print("\nNo changes were made to the entry.")
            log.info(f"No changes detected for Entry UniqueID {entry['UniqueID']} during validation.")
            return

        print("\n--- Summary of Changes ---")
        # ... (display changes - same as before) ...
        for key, value in updated_values.items():
             print(f"  {key}: '{original_entry.get(key)}' -> '{value}'")
        if validation_changed:
            print(f"  validated: {bool(original_entry['validated'])} -> {bool(entry['validated'])}")
            if new_validated_status == 1: print(f"  entry_method will be set to: 'user_validated'")


        confirm_save = input("\nSave these changes to the database? (y/n): ").strip().lower()
        if confirm_save == 'y':
            # Prepare update query
            fields_to_update = list(updated_values.keys())
            if validation_changed:
                fields_to_update.extend(['validated', 'last_validated_run_ts'])
                # Always update entry_method if validation happened
                if 'entry_method' not in fields_to_update: fields_to_update.append('entry_method')

            fields_to_update = list(set(fields_to_update)) # Unique fields

            if not fields_to_update:
                 print("Internal check: No fields marked for update. Aborting save.")
                 return

            set_clauses = ", ".join([f"{key} = ?" for key in fields_to_update])
            sql_values = [entry.get(key) for key in fields_to_update] + [entry['UniqueID']]
            sql = f"UPDATE opinions SET {set_clauses} WHERE UniqueID = ?"

            log.debug(f"Executing SQL: {sql} with values: {sql_values}")
            cursor.execute(sql, tuple(sql_values))
            conn.commit()
            log.info(f"Entry UniqueID {entry['UniqueID']} updated in database '{db_filename}'. Validated: {bool(entry['validated'])}, Method: {entry.get('entry_method')}")
            print(f"Entry UniqueID {entry['UniqueID']} updated successfully.")

        else:
            print("Changes discarded.")
            log.info(f"User discarded changes for Entry UniqueID {entry['UniqueID']}.")

    # ... (error handling remains the same) ...
    except sqlite3.Error as e:
        log.error(f"Database error during validation of UniqueID {unique_id_to_validate} in '{db_filename}': {e}", exc_info=True)
        print(f"Database error: {e}")
    except ConnectionError as e:
         print(f"Database connection error: {e}")
    except Exception as e:
         log.error(f"Unexpected error during validation of UniqueID {unique_id_to_validate}: {e}", exc_info=True)
         print(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()


# --- list_entries function (Updated to handle db_key) ---
def list_entries(db_key="primary", list_type="unvalidated", limit=50):
    """
    Lists entries from the specified DB based on criteria:
    'unvalidated' or 'missing_lc_docket'.
    """
    log.info(f"Listing entries for DB '{db_key}', type '{list_type}'")
    db_files = GconfigEM.get_db_filenames()
    db_filename = db_files.get(db_key)

    # Check if DB is suitable for this listing (uses 'opinions' schema)
    db_basename = os.path.basename(db_filename) if db_filename else None
    allowed_db_keys = ["primary", "backup", "test"]
    allowed_db_files = [GconfigEM.DEFAULT_DB_NAMES.get(k) for k in allowed_db_keys]

    if not db_filename:
        print(f"Error: Database file for '{db_key}' not found in configuration.")
        log.error(f"Listing failed: DB filename for '{db_key}' missing.")
        return
    if db_basename not in allowed_db_files:
         print(f"Error: Listing '{list_type}' can only be performed on Primary, Backup, or Test databases. '{db_key}' is not suitable.")
         log.error(f"Listing failed: Attempted list on unsuitable DB type '{db_key}'.")
         return
    if not os.path.exists(db_filename):
        print(f"Error: Database file '{db_filename}' not found.")
        log.error(f"Listing failed: Database file '{db_filename}' does not exist.")
        return

    conn = None
    try:
        conn = GdbEM.get_db_connection(db_filename)
        cursor = conn.cursor()

        where_clause = ""
        params = []
        description = ""

        if list_type == "unvalidated":
            where_clause = "WHERE validated = 0"
            description = "Unvalidated Entries"
        elif list_type == "missing_lc_docket":
            # Updated check: NULL, empty, OR contains '[LC Docket Missing]' note, AND is unvalidated, AND not SC/Agency
            where_clause = """
                WHERE validated = 0
                  AND (LCdocketID IS NULL OR LCdocketID = '' OR CaseNotes LIKE ?)
                  AND (LowerCourtVenue IS NULL OR LowerCourtVenue != 'Appellate Division') -- Exclude SC cases
                  AND (LCCounty IS NULL OR LCCounty != 'NJ') -- Exclude NJ Agency cases
            """
            params.append('%[LC Docket Missing]%') # Parameter for LIKE
            description = "Unvalidated Entries Missing LC Docket ID (Non-SC/Agency)"
        else:
            print(f"Error: Unknown list type '{list_type}'. Use 'unvalidated' or 'missing_lc_docket'.")
            log.error(f"Invalid list_type provided: {list_type}")
            return

        # Select key fields
        query = f"""
            SELECT UniqueID, AppDocketID, CaseName, ReleaseDate, LowerCourtVenue, LCdocketID, CaseNotes, entry_method
            FROM opinions
            {where_clause}
            ORDER BY ReleaseDate DESC, AppDocketID ASC
            LIMIT ?
        """
        params.append(limit) # Add limit parameter

        log.debug(f"Executing list query on {db_key}: {query} with params: {params}")
        cursor.execute(query, tuple(params))
        rows = cursor.fetchall()

        if not rows:
            print(f"No {description.lower()} found in '{db_filename}'.")
            log.info(f"list_entries found no matching entries for type '{list_type}' in DB '{db_key}'.")
            return

        print(f"\n--- {description} (DB: {db_key}, Max {limit}) ---")
        print(" UniqueID (Start) | AppDocketID | CaseName                         | ReleaseDate  | LC Venue         | LC Docket(s)     | Entry Method     | Notes (Snippet)")
        print("------------------|-------------|----------------------------------|--------------|------------------|------------------|------------------|--------------------")
        for row in rows:
            entry = dict(row)
            uid_s = (entry['UniqueID'] or '')[:8]
            cn_s = (entry['CaseName'] or '')[:32]
            lcv_s = (entry['LowerCourtVenue'] or 'N/A')[:16]
            lcd_s = (entry['LCdocketID'] or 'N/A')[:16]
            em_s = (entry['entry_method'] or 'N/A')[:16]
            notes_s = (entry['CaseNotes'] or '')[:18]
            if len(entry.get('CaseNotes', '')) > 18: notes_s += "..."

            print(f" {uid_s:<16} | {entry['AppDocketID']:<11} | {cn_s:<32} | {entry['ReleaseDate']:<12} | {lcv_s:<16} | {lcd_s:<16} | {em_s:<16} | {notes_s}")
        print("-" * 170) # Adjust width
        print(f"Found {len(rows)} entries. Use 'validate --validate-id <UniqueID> --db {db_key}' to review and edit.")

    # ... (error handling remains the same) ...
    except sqlite3.Error as e:
        log.error(f"Database error listing entries ({list_type}) in '{db_filename}': {e}", exc_info=True)
        print(f"Database error: {e}")
    except ConnectionError as e:
        print(f"Database connection error: {e}")
    except Exception as e:
         log.error(f"Unexpected error listing entries ({list_type}): {e}", exc_info=True)
         print(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()


# === End of GvalidatorEM.py ===