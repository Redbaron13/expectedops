# GvalidatorEM.py
# V4: Adapted for Supabase backend.
"""
Handles manual validation of scraped opinion entries stored in Supabase.
Includes display of potential decision PDF URL.
Allows listing of entries needing LC Docket ID review.
Updates 'entry_method' and timestamps upon successful validation.
"""
import logging
import re
import datetime
import GconfigEM # Not strictly needed now, but keep for potential future use
import GdbEM # Supabase version
from GcliEM import prompt_with_timeout # Use CLI input helper

log = logging.getLogger(__name__)

# --- Helper to Construct URL (Unchanged) ---
def construct_decision_url(app_docket_id, release_date_str):
    """Constructs the potential URL for a decision PDF."""
    if not app_docket_id or not release_date_str:
        return None
    # Basic cleaning, might need refinement based on actual docket formats
    cleaned_docket = re.sub(r'[^a-z0-9-]', '', app_docket_id.lower())
    try:
        # Ensure date is in YYYY-MM-DD format for parsing
        release_date_dt = datetime.datetime.strptime(release_date_str, '%Y-%m-%d')
        release_year = release_date_dt.year
    except (ValueError, TypeError):
        log.warning(f"Could not parse release date '{release_date_str}' to get year for URL construction.")
        return None
    # Construct the URL based on observed patterns
    url = f"https://www.njcourts.gov/system/files/court-opinions/{release_year}/{cleaned_docket}.pdf"
    return url

# --- Main Validation Function (Supabase Version) ---
def validate_case_supabase(unique_id_to_validate):
    """
    Allows interactive review and validation of a specific opinion entry
    by its UniqueID from the Supabase 'opinions' table. Updates entry_method.
    """
    log.info(f"Starting validation process for Opinion UniqueID: {unique_id_to_validate}")

    # Fetch the entry from Supabase
    entry = GdbEM.get_opinion_by_id(unique_id_to_validate)

    if not entry:
        print(f"No opinion entry found with UniqueID {unique_id_to_validate} in Supabase.")
        log.warning(f"validate_case_supabase called for non-existent UniqueID: {unique_id_to_validate}")
        return

    original_entry = entry.copy() # Keep original for comparison

    print(f"\n--- Reviewing Opinion Entry UniqueID: {entry['UniqueID'][:8]}... ---")
    print(f"  Appellate Docket: {entry.get('AppDocketID', 'N/A')}")
    print(f"  Release Date:     {entry.get('ReleaseDate', 'N/A')}")
    print(f"  Current Validated Status: {bool(entry.get('validated', False))}") # Default to False if missing
    print(f"  Current Entry Method:   {entry.get('entry_method', 'N/A')}")
    print(f"  Last Updated TS:  {entry.get('last_updated_ts', 'N/A')}")
    print(f"  Validated TS:     {entry.get('last_validated_run_ts', 'N/A')}")

    # --- Display Potential PDF URL ---
    pdf_url = construct_decision_url(entry.get('AppDocketID'), entry.get('ReleaseDate'))
    if pdf_url: print(f"  Potential PDF URL: {pdf_url}")
    else: print("  (Could not construct potential PDF URL)")

    print("\n--- Current Data (Editable Fields) ---")
    # Define fields that are generally editable by the user during validation
    editable_fields = [
        'AppDocketID', 'ReleaseDate', 'LinkedDocketIDs', 'CaseName', 'LCdocketID',
        'LCCounty', 'Venue', 'LowerCourtVenue', 'LowerCourtSubCaseType', 'OPJURISAPP',
        'DecisionTypeCode', 'DecisionTypeText', 'StateAgency1', 'StateAgency2', 'CaseNotes',
        'caseconsolidated', 'recordimpounded', 'opinionstatus'
        # Exclude: UniqueID, DataHash, RunType, entry_method, validated, timestamps (handled separately)
    ]

    updated_values = {}
    for key in sorted(editable_fields):
        current_value = entry.get(key)
        display_value = current_value if current_value is not None else "[empty]"
        # Format booleans/integers for display
        if key in ['caseconsolidated', 'recordimpounded']: display_value = str(bool(current_value))
        if key == 'opinionstatus': display_value = "Released" if int(current_value) == 1 else "Expected"

        # Use simple input() here, timeout handled by GcliEM if needed
        user_input = input(f"  {key:<22}: {display_value} | Edit? (Enter new value or press Enter): ").strip()

        if user_input:
            new_value = user_input
            # Handle boolean/integer conversion
            if key in ['caseconsolidated', 'recordimpounded']:
                if user_input.lower() in ['true', '1', 'yes', 'y']: new_value = True
                elif user_input.lower() in ['false', '0', 'no', 'n']: new_value = False
                else:
                    print(f"    Invalid input for {key} (boolean). Keeping original."); new_value = current_value
            elif key == 'opinionstatus':
                 if user_input.lower() in ['released', '1', 'yes', 'y']: new_value = 1
                 elif user_input.lower() in ['expected', '0', 'no', 'n']: new_value = 0
                 else:
                      print(f"    Invalid input for {key} (0 or 1). Keeping original."); new_value = current_value
            # Add other type conversions if necessary (e.g., dates)

            # Only record if the value actually changed
            if new_value != current_value:
                 updated_values[key] = new_value
                 entry[key] = new_value # Update the working copy

    # --- Validation Status ---
    print("\n--- Validation Status ---")
    current_validated_status = bool(original_entry.get('validated', False))
    confirm_validate_input = input(f"Mark this entry as validated? (Current: {current_validated_status}) (y/n/Enter=no change): ").strip().lower()
    validation_changed = False
    new_validated_status = current_validated_status # Start with current status

    if confirm_validate_input == 'y':
        new_validated_status = True
        if not current_validated_status: validation_changed = True # Changed only if it wasn't already true
        print("Entry will be marked as validated.")
    elif confirm_validate_input == 'n':
        new_validated_status = False
        if current_validated_status: validation_changed = True # Changed only if it wasn't already false
        print("Entry will be marked as NOT validated.")
    else:
        print("Validation status remains unchanged.")

    if validation_changed:
        updated_values['validated'] = new_validated_status
        updated_values['last_validated_run_ts'] = datetime.datetime.now(datetime.timezone.utc).isoformat() if new_validated_status else None
        # Set entry_method if validated
        if new_validated_status:
             updated_values['entry_method'] = 'user_validated'
        # Optional: Revert entry_method if un-validated? For now, keep historical method.


    # --- Final confirmation ---
    if not updated_values: # Check if any changes were actually made
        print("\nNo changes were made to the entry.")
        log.info(f"No changes detected for Opinion UniqueID {entry['UniqueID']} during validation.")
        return

    print("\n--- Summary of Changes ---")
    for key, value in updated_values.items():
         original_value = original_entry.get(key)
         # Format booleans for comparison display
         if key in ['validated', 'caseconsolidated', 'recordimpounded']:
              original_display = str(bool(original_value))
              new_display = str(bool(value))
         elif key == 'opinionstatus':
              original_display = "Released" if int(original_value or 0) == 1 else "Expected"
              new_display = "Released" if int(value) == 1 else "Expected"
         else:
              original_display = f"'{original_value}'" if original_value is not None else "[empty]"
              new_display = f"'{value}'" if value is not None else "[empty]"
         print(f"  {key}: {original_display} -> {new_display}")
    if 'validated' in updated_values and updated_values['validated']:
        print(f"  (entry_method will be set to: 'user_validated')")


    confirm_save = input("\nSave these changes to the Supabase database? (y/n): ").strip().lower()
    if confirm_save == 'y':
        # Use the GdbEM update function
        success = GdbEM.update_opinion(entry['UniqueID'], updated_values)

        if success:
            log.info(f"Opinion UniqueID {entry['UniqueID']} updated successfully in Supabase.")
            print(f"Opinion UniqueID {entry['UniqueID']} updated successfully.")
        else:
            log.error(f"Failed to update Opinion UniqueID {entry['UniqueID']} in Supabase.")
            print("Error: Failed to save changes to the database.")

    else:
        print("Changes discarded.")
        log.info(f"User discarded validation changes for Opinion UniqueID {entry['UniqueID']}.")


# --- list_entries function (Supabase Version) ---
def list_entries_supabase(list_type="unvalidated", limit=50):
    """
    Lists opinion entries from Supabase based on criteria:
    'unvalidated' or 'missing_lc_docket'.
    """
    log.info(f"Listing opinions from Supabase, type '{list_type}'")
    supabase = GdbEM.get_supabase_client()
    if not supabase:
        print("Error: Cannot connect to Supabase.")
        return

    query = supabase.table('opinions')
    description = ""

    # Define base fields to select
    select_fields = "UniqueID, AppDocketID, CaseName, ReleaseDate, LowerCourtVenue, LCdocketID, CaseNotes, entry_method, validated"

    if list_type == "unvalidated":
        query = query.select(select_fields).eq('validated', False)
        description = "Unvalidated Opinion Entries"
    elif list_type == "missing_lc_docket":
        # Logic: Unvalidated AND (LCdocketID is null OR LCdocketID is empty OR CaseNotes contains marker)
        # AND not a Supreme Court case (where LC Venue is App Div) AND not an Agency case (where County is NJ)
        query = query.select(select_fields)\
                     .eq('validated', False)\
                     .or_('LCdocketID.is.null,LCdocketID.eq.,CaseNotes.like.%[LC Docket Missing]%')\
                     .neq('LowerCourtVenue', 'Appellate Division')\
                     .neq('LCCounty', 'NJ') # Simple exclusion for Agency
        description = "Unvalidated Opinions Potentially Missing LC Docket ID (Non-SC/Agency)"
    else:
        print(f"Error: Unknown list type '{list_type}'. Use 'unvalidated' or 'missing_lc_docket'.")
        log.error(f"Invalid list_type provided for listing: {list_type}")
        return

    try:
        # Add ordering and limit
        response = query.order('ReleaseDate', desc=True).order('AppDocketID').limit(limit).execute()

        if response.data:
            rows = response.data
            print(f"\n--- {description} (Supabase, Max {limit}) ---")
            # Adjust formatting as needed
            print(" UniqueID (Start) | Valid | AppDocketID | CaseName (Snippet)               | Release    | LC Venue         | LC Docket        | Entry Method     | Notes (Snippet)")
            print("------------------|-------|-------------|----------------------------------|------------|------------------|------------------|------------------|--------------------")
            for entry in rows:
                uid_s = (entry.get('UniqueID') or '')[:8]
                val_s = "Y" if entry.get('validated') else "N"
                app_s = (entry.get('AppDocketID') or 'N/A')[:11]
                cn_s = (entry.get('CaseName') or '')[:32]
                rel_s = (entry.get('ReleaseDate') or 'N/A')[:10]
                lcv_s = (entry.get('LowerCourtVenue') or 'N/A')[:16]
                lcd_s = (entry.get('LCdocketID') or 'N/A')[:16]
                em_s = (entry.get('entry_method') or 'N/A')[:16]
                notes_s = (entry.get('CaseNotes') or '')[:18]
                if len(entry.get('CaseNotes', '')) > 18: notes_s += "..."

                print(f" {uid_s:<16} | {val_s:<5} | {app_s:<11} | {cn_s:<32} | {rel_s:<10} | {lcv_s:<16} | {lcd_s:<16} | {em_s:<16} | {notes_s}")
            print("-" * 170) # Adjust width
            print(f"Found {len(rows)} entries. Use 'validate --validate-id <UniqueID>' to review and edit.")

        elif response.error:
            print(f"Error querying Supabase: {response.error}")
            log.error(f"Supabase error listing entries ({list_type}): {response.error}")
        else:
            print(f"No {description.lower()} found matching criteria in Supabase.")
            log.info(f"list_entries_supabase found no matching entries for type '{list_type}'.")

    except Exception as e:
         log.error(f"Unexpected error listing entries ({list_type}): {e}", exc_info=True)
         print(f"An unexpected error occurred during listing: {e}")


# === End of GvalidatorEM.py ===
