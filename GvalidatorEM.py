# GvalidatorEM.py
"""
Handles manual validation of scraped opinion entries.
"""
import sqlite3
import shutil
import os
import logging # Import standard logging
import GconfigEM # Correct config import

# Setup logger for this module
log = logging.getLogger(__name__)

def validate_case(case_id):
    """
    Allows interactive review and validation of a specific case entry by its database ID.
    Optionally uses a temporary copy of the database for safety.
    """
    try:
        db_name = GconfigEM.get_db_name()
    except Exception as e:
        log.error(f"Failed to get DB name from config for validation: {e}")
        print(f"Error: Could not load database name from configuration: {e}")
        return

    if not os.path.exists(db_name):
        print(f"Error: Database file '{db_name}' not found.")
        log.error(f"Validation failed: Database file '{db_name}' does not exist.")
        return

    # Optional temporary DB logic (consider if truly needed)
    use_temp_db = False
    temp_db = f"temp_validation_{db_name}"
    # Simplified: Don't force temp DB for now, operate on main DB carefully
    # user_input = input(f"Validate directly in '{db_name}' or use temporary copy? (d=direct/t=temp): ").strip().lower()
    # if user_input == 't':
    #     try:
    #         shutil.copyfile(db_name, temp_db)
    #         log.info(f"Temporary validation DB created at {temp_db}")
    #         db_to_use = temp_db
    #         use_temp_db = True
    #     except Exception as e:
    #         log.error(f"Failed to create temporary DB '{temp_db}': {e}")
    #         print(f"Error creating temporary database: {e}. Aborting.")
    #         return
    # else:
    #     db_to_use = db_name
    db_to_use = db_name # Operate directly for now

    conn = None
    try:
        conn = sqlite3.connect(db_to_use)
        conn.row_factory = sqlite3.Row # Ensure row factory is set
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM opinions WHERE id = ?", (case_id,))
        row = cursor.fetchone()

        if not row:
            print(f"No entry found with ID {case_id}.")
            log.warning(f"validate_case called for non-existent ID: {case_id}")
            return

        entry = dict(row) # Convert row object to dictionary
        original_entry = entry.copy() # Keep a copy for comparison/logging

        print(f"\n--- Reviewing Entry ID: {case_id} (Current Validated Status: {bool(entry.get('validated', 0))}) ---")
        updated_values = {}
        for key, value in entry.items():
            if key in ['id', 'first_scraped_ts', 'last_updated_ts', 'last_validated_run_ts']: # Non-editable fields
                print(f"  {key}: {value} (read-only)")
                continue
            if key == 'validated': # Special handling for validation status
                print(f"  {key}: {value}")
                continue

            # Handle None values appropriately for display
            display_value = value if value is not None else "[empty]"
            user_val = input(f"  {key}: {display_value} | Edit or press Enter: ").strip()
            if user_val:
                updated_values[key] = user_val # Store user's edit

        # Apply edits to the entry dictionary
        entry.update(updated_values)

        # Explicitly ask to validate
        print("\n--- Validation ---")
        confirm_validate = input("Mark this entry as validated? (y/n): ").strip().lower()
        if confirm_validate == 'y':
            entry['validated'] = 1 # Mark as validated
            entry['last_validated_run_ts'] = datetime.datetime.now() # Update timestamp
            print("Entry marked as validated.")
        else:
            # Keep existing validation status unless explicitly changed above? No, prompt resets it.
            entry['validated'] = 0 # Mark as unvalidated if not confirming 'y'
            entry['last_validated_run_ts'] = None # Clear validation timestamp
            print("Entry marked as NOT validated.")


        # Final confirmation to save changes
        confirm_save = input("\nSave changes to database? (y/n): ").strip().lower()
        if confirm_save == 'y':
            # Prepare update query
            set_clauses = []
            sql_values = []
            # Iterate through original keys to ensure order/completeness, apply updates
            for key in original_entry.keys():
                 if key != 'id': # Don't update ID
                      set_clauses.append(f"{key} = ?")
                      sql_values.append(entry.get(key)) # Use potentially updated value

            if not set_clauses:
                print("No changes detected to save.")
                log.info(f"No changes detected for Entry ID {case_id} during validation.")
                return

            sql_values.append(case_id) # For the WHERE clause
            sql = f"UPDATE opinions SET {', '.join(set_clauses)} WHERE id = ?"

            log.debug(f"Executing SQL: {sql} with values: {sql_values}")
            cursor.execute(sql, tuple(sql_values))
            conn.commit()
            log.info(f"Entry ID {case_id} updated in database '{db_to_use}'. Validated status: {bool(entry['validated'])}")
            print(f"Entry ID {case_id} updated successfully.")

            # Optional: Ask to merge temp changes back to original DB if use_temp_db was True
            # if use_temp_db: ... handle merging ...

        else:
            print("Changes discarded.")
            log.info(f"User discarded changes for Entry ID {case_id}.")


    except sqlite3.Error as e:
        log.error(f"Database error during validation of ID {case_id} in '{db_to_use}': {e}", exc_info=True)
        print(f"Database error: {e}")
    except Exception as e:
         log.error(f"Unexpected error during validation of ID {case_id}: {e}", exc_info=True)
         print(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()
        # Optional: Clean up temp DB if used
        # if use_temp_db and os.path.exists(temp_db): os.remove(temp_db)


def list_unvalidated():
    """Lists entries from the database that are marked as not validated."""
    try:
        db_name = GconfigEM.get_db_name()
    except Exception as e:
        log.error(f"Failed to get DB name from config for listing unvalidated: {e}")
        print(f"Error: Could not load database name from configuration: {e}")
        return

    if not os.path.exists(db_name):
        print(f"Error: Database file '{db_name}' not found.")
        log.error(f"List unvalidated failed: Database file '{db_name}' does not exist.")
        return

    conn = None
    try:
        conn = sqlite3.connect(db_name)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        # Select key fields for identification
        cursor.execute("""
            SELECT id, AppDocketID, CaseName, ReleaseDate, first_scraped_ts
            FROM opinions
            WHERE validated = 0
            ORDER BY first_scraped_ts DESC
            LIMIT 50
        """) # Limit output for brevity
        rows = cursor.fetchall()

        if not rows:
            print("No unvalidated entries found.")
            log.info("list_unvalidated found no entries.")
            return

        print("\n--- Unvalidated Entries (Showing recent first, max 50) ---")
        print(" ID  | AppDocketID | CaseName                         | ReleaseDate   | Scraped On")
        print("-----|-------------|----------------------------------|---------------|--------------------")
        for row in rows:
            case_name_short = (row['CaseName'] or '')[:32] # Truncate long names
            print(f" {row['id']:<4}| {row['AppDocketID']:<11} | {case_name_short:<32} | {row['ReleaseDate']:<13} | {row['first_scraped_ts']}")
        print("--------------------------------------------------------------------------------------")
        print(f"Found {len(rows)} unvalidated entries (display limited). Use 'validate --id <ID>' to review.")

    except sqlite3.Error as e:
        log.error(f"Database error listing unvalidated entries in '{db_name}': {e}", exc_info=True)
        print(f"Database error: {e}")
    except Exception as e:
         log.error(f"Unexpected error listing unvalidated entries: {e}", exc_info=True)
         print(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()

# === End of GvalidatorEM.py ===