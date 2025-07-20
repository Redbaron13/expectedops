# GvalidatorEM.py
# V4: Adapted for Supabase backend.
"""
Handles manual validation and listing of scraped opinion entries stored in the Supabase database.

This module provides functionalities to:
1.  Interactively review and edit individual opinion entries fetched from Supabase
    via `GdbEM.get_opinion_by_id()`.
2.  Allow users to modify various fields of an opinion and mark it as 'validated'.
3.  Update the 'entry_method' to 'user_validated' and set a validation timestamp
    (`last_validated_run_ts`) upon successful validation.
4.  Construct potential URLs to decision PDFs on the NJ Courts website based on
    docket ID and release date.
5.  List entries from Supabase that meet specific criteria, such as:
    - All unvalidated entries.
    - Unvalidated entries that are potentially missing Lower Court (LC) Docket IDs
      (excluding Supreme Court and direct agency cases).
6.  Uses `GcliEM.prompt_with_timeout` for user input during interactive validation,
    though the current implementation within `validate_case_supabase` uses `input()` directly.
    (Note: `prompt_with_timeout` is imported but not explicitly used in `validate_case_supabase` itself).

The primary interactions are with `GdbEM` for database operations (fetching and updating opinions)
and potentially `GconfigEM` (though not strictly used in the current version, it's imported).
"""
import logging
import re
import datetime
import GconfigEM # Imported, but not actively used in current functions. Retained for potential future config needs.
import GdbEM     # For all Supabase database interactions (fetching/updating opinions).
from GcliEM import prompt_with_timeout # For user input with timeout (currently not used in validate_case_supabase).

log = logging.getLogger(__name__)

# --- Helper to Construct URL ---
def construct_decision_url(app_docket_id: str | None, release_date_str: str | None) -> str | None:
    """
    Constructs a potential URL for a decision PDF on the NJ Courts website.

    The URL pattern is based on observed structures, typically involving the
    release year and a cleaned version of the appellate docket number.

    Args:
        app_docket_id (str | None): The Appellate Docket ID of the opinion.
        release_date_str (str | None): The release date of the opinion in
                                      "YYYY-MM-DD" format.

    Returns:
        str | None: The constructed URL string if both inputs are valid,
                    otherwise None.
    """
    if not app_docket_id or not release_date_str:
        log.debug("Cannot construct PDF URL: app_docket_id or release_date_str is missing.")
        return None

    # Clean the docket ID: lowercase and remove characters not typically in URL filenames.
    # This is a basic cleaning; specific court URL patterns might require more nuanced cleaning.
    cleaned_docket = re.sub(r'[^a-z0-9-]', '', app_docket_id.lower())

    try:
        # Parse the release date string to extract the year.
        release_date_dt = datetime.datetime.strptime(release_date_str, '%Y-%m-%d')
        release_year = release_date_dt.year
    except (ValueError, TypeError) as e:
        # Log a warning if the date string cannot be parsed.
        log.warning(f"Could not parse release date '{release_date_str}' to get year for URL construction: {e}")
        return None

    # Construct the URL using the common pattern found on the NJ Courts website.
    # Example: https://www.njcourts.gov/system/files/court-opinions/2023/a-1234-22.pdf
    url = f"https://www.njcourts.gov/system/files/court-opinions/{release_year}/{cleaned_docket}.pdf"
    log.debug(f"Constructed potential PDF URL: {url}")
    return url

# --- Main Validation Function (Supabase Version) ---
def validate_case_supabase(unique_id_to_validate: str):
    """
    Allows interactive review and validation of a specific opinion entry
    identified by its UniqueID from the Supabase 'opinions' table.

    The process involves:
    1. Fetching the opinion entry using `GdbEM.get_opinion_by_id()`.
    2. Displaying current data for review, including a potential PDF URL.
    3. Prompting the user to edit predefined `editable_fields`.
    4. Handling type conversions for edited boolean and integer fields.
    5. Prompting the user to change the 'validated' status of the entry.
    6. If changes are made (either to data fields or validation status),
       it confirms with the user before saving.
    7. If confirmed, it updates the opinion in Supabase via `GdbEM.update_opinion()`,
       setting 'entry_method' to 'user_validated' and updating timestamps if
       the entry is marked as validated.

    Args:
        unique_id_to_validate (str): The UniqueID of the opinion entry to validate.
    """
    log.info(f"Starting interactive validation process for Opinion UniqueID: {unique_id_to_validate}")

    # Step 1: Fetch the opinion entry from Supabase.
    entry = GdbEM.get_opinion_by_id(unique_id_to_validate)

    if not entry:
        print(f"No opinion entry found with UniqueID '{unique_id_to_validate}' in Supabase.")
        log.warning(f"validate_case_supabase called for a non-existent UniqueID: {unique_id_to_validate}")
        return

    original_entry = entry.copy() # Keep a copy of the original data for comparison later.

    # Step 2: Display current data for review.
    print(f"\n--- Reviewing Opinion Entry UniqueID: {entry.get('UniqueID', 'N/A')[:8]}... ---") # Show partial UniqueID for brevity.
    print(f"  Appellate Docket: {entry.get('AppDocketID', '[N/A]')}")
    print(f"  Release Date:     {entry.get('ReleaseDate', '[N/A]')}")
    print(f"  Current Validated Status: {bool(entry.get('validated', False))}") # Default to False if 'validated' key is missing.
    print(f"  Current Entry Method:   {entry.get('entry_method', '[N/A]')}")
    print(f"  Last Updated TS:  {entry.get('last_updated_ts', '[N/A]')}")
    print(f"  Validated TS:     {entry.get('last_validated_run_ts', '[N/A]')}") # Timestamp of last validation.

    # Construct and display a potential URL to the decision PDF.
    pdf_url = construct_decision_url(entry.get('AppDocketID'), entry.get('ReleaseDate'))
    if pdf_url:
        print(f"  Potential PDF URL: {pdf_url}")
    else:
        print("  (Could not construct potential PDF URL for this entry)")

    print("\n--- Current Data (Editable Fields) ---")
    # Define which fields the user can edit during this interactive session.
    # Fields like UniqueID, DataHash, RunType are typically system-managed and not user-editable here.
    editable_fields = [
        'AppDocketID', 'ReleaseDate', 'LinkedDocketIDs', 'CaseName', 'LCdocketID',
        'LCCounty', 'Venue', 'LowerCourtVenue', 'LowerCourtSubCaseType', 'OPJURISAPP',
        'DecisionTypeCode', 'DecisionTypeText', 'StateAgency1', 'StateAgency2', 'CaseNotes',
        'caseconsolidated', 'recordimpounded', 'opinionstatus'
    ]

    updated_values = {} # Dictionary to store any changes made by the user.

    # Step 3: Prompt user for edits on each editable field.
    for key in sorted(editable_fields): # Iterate in alphabetical order for consistent prompting.
        current_value = entry.get(key)
        # Prepare a display-friendly version of the current value.
        display_value = current_value if current_value is not None else "[empty]"
        if key in ['caseconsolidated', 'recordimpounded']: # Boolean fields.
            display_value = str(bool(current_value))
        elif key == 'opinionstatus': # Integer field representing a status.
            display_value = "Released" if int(current_value or 0) == 1 else "Expected"

        # Prompt user for new input or to skip (press Enter).
        user_input = input(f"  {key:<22}: {display_value} | Edit? (Enter new value or press Enter to keep): ").strip()

        if user_input: # If user provided any input (didn't just press Enter).
            new_value = user_input
            # Step 4: Handle type conversions for specific fields.
            if key in ['caseconsolidated', 'recordimpounded']:
                if user_input.lower() in ['true', '1', 'yes', 'y', 't']: new_value = True
                elif user_input.lower() in ['false', '0', 'no', 'n', 'f']: new_value = False
                else:
                    print(f"    Invalid input '{user_input}' for boolean field '{key}'. Keeping original value: {current_value}")
                    new_value = current_value # Revert to original if input is invalid.
            elif key == 'opinionstatus':
                 if user_input.lower() in ['released', '1', 'r']: new_value = 1
                 elif user_input.lower() in ['expected', '0', 'e']: new_value = 0
                 else:
                      print(f"    Invalid input '{user_input}' for opinionstatus (0 or 1, E or R). Keeping original value: {current_value}")
                      new_value = current_value
            # Add other type conversions here if necessary (e.g., for date fields if not already ISO).

            # Record the change only if the new value is different from the current one.
            if new_value != current_value:
                 updated_values[key] = new_value
                 entry[key] = new_value # Update the working copy of the entry for immediate feedback/display.
                 log.debug(f"Field '{key}' staged for update. Old: '{current_value}', New: '{new_value}'.")

    # --- Step 5: Prompt for Validation Status Change ---
    print("\n--- Validation Status ---")
    current_validated_status = bool(original_entry.get('validated', False))
    prompt_message = (f"Mark this entry as validated? (Currently: {current_validated_status}) "
                      f"(y=Yes, n=No, Enter=no change): ")
    confirm_validate_input = input(prompt_message).strip().lower()

    validation_status_changed_by_user = False
    new_overall_validated_status = current_validated_status # Default to no change.

    if confirm_validate_input == 'y':
        new_overall_validated_status = True
        if not current_validated_status: # If it was False and now True
            validation_status_changed_by_user = True
        print("Entry will be marked as VALIDATED.")
    elif confirm_validate_input == 'n':
        new_overall_validated_status = False
        if current_validated_status: # If it was True and now False
            validation_status_changed_by_user = True
        print("Entry will be marked as NOT VALIDATED.")
    else:
        print(f"Validation status remains: {current_validated_status}.")

    # If validation status was changed by user interaction, add to updated_values.
    if validation_status_changed_by_user:
        updated_values['validated'] = new_overall_validated_status
        # If marked as validated, set validation timestamp and update entry method.
        if new_overall_validated_status:
            updated_values['last_validated_run_ts'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            updated_values['entry_method'] = 'user_validated'
        else: # If marked as not validated
            updated_values['last_validated_run_ts'] = None # Clear validation timestamp.
            # Optional: Revert entry_method if un-validating?
            # Current logic: if un-validated, entry_method is not changed back from 'user_validated' automatically.
            # This preserves the history that it was once user-validated.
            # If it was 'scraper' and marked 'N' for validated, it remains 'scraper'.
            if original_entry.get('entry_method') == 'user_validated':
                 log.info(f"Entry {entry['UniqueID']} was un-validated by user, but entry_method remains 'user_validated'.")


    # --- Step 6: Final Confirmation and Save ---
    if not updated_values: # Check if any actual changes (data or validation status) were made.
        print("\nNo changes were made to the entry.")
        log.info(f"No changes detected for Opinion UniqueID {entry['UniqueID']} during interactive validation session.")
        return

    # Display a summary of all proposed changes.
    print("\n--- Summary of Proposed Changes ---")
    for key, new_val in updated_values.items():
         original_val_for_summary = original_entry.get(key)
         # Format boolean/status fields for clearer comparison in summary.
         if key in ['validated', 'caseconsolidated', 'recordimpounded']:
              original_display_val = str(bool(original_val_for_summary))
              new_display_val = str(bool(new_val))
         elif key == 'opinionstatus':
              original_display_val = "Released" if int(original_val_for_summary or 0) == 1 else "Expected"
              new_display_val = "Released" if int(new_val) == 1 else "Expected"
         else:
              original_display_val = f"'{original_val_for_summary}'" if original_val_for_summary is not None else "[empty]"
              new_display_val = f"'{new_val}'" if new_val is not None else "[empty]"
         print(f"  {key}: {original_display_val}  ==>  {new_display_val}")

    # Final confirmation from user to save changes to database.
    confirm_save_input = input("\nSave these changes to the Supabase database? (yes/no): ").strip().lower()
    if confirm_save_input in ['y', 'yes']:
        # Step 7: Update the opinion in Supabase via GdbEM.
        success = GdbEM.update_opinion(entry['UniqueID'], updated_values)
        if success:
            log.info(f"Opinion UniqueID {entry['UniqueID']} updated successfully in Supabase with changes: {updated_values}")
            print(f"Opinion UniqueID {entry['UniqueID']} updated successfully.")
        else:
            log.error(f"Failed to update Opinion UniqueID {entry['UniqueID']} in Supabase. Changes: {updated_values}")
            print("Error: Failed to save changes to the database. Check logs.")
    else:
        print("Changes were discarded by the user.")
        log.info(f"User discarded validation changes for Opinion UniqueID {entry['UniqueID']}. Staged changes: {updated_values}")


def list_entries_supabase(list_type: str = "unvalidated", limit: int = 50):
    """
    Lists opinion entries from the Supabase 'opinions' table based on specified criteria.

    Supported `list_type` values:
    - "unvalidated": Lists all entries where the 'validated' flag is False.
    - "missing_lc_docket": Lists unvalidated entries that are potentially missing
      a Lower Court (LC) Docket ID. This excludes Supreme Court cases (where the
      LC is the Appellate Division) and presumed agency cases (where LCCounty is 'NJ').

    Args:
        list_type (str, optional): The type of listing to perform.
                                   Defaults to "unvalidated".
        limit (int, optional): The maximum number of entries to fetch and display.
                               Defaults to 50.
    """
    log.info(f"Listing opinions from Supabase, type: '{list_type}', limit: {limit}")
    supabase = GdbEM.get_supabase_client() # Get Supabase client instance.
    if not supabase:
        print("Error: Cannot connect to Supabase. Please check configuration and network.")
        return

    # Base query builder for the 'opinions' table.
    query_builder = supabase.table('opinions')
    description_for_print = "" # For user-friendly output header.

    # Define base fields to select for the listing.
    select_fields = "UniqueID, AppDocketID, CaseName, ReleaseDate, LowerCourtVenue, LCdocketID, CaseNotes, entry_method, validated"

    # --- Construct Query Based on list_type ---
    if list_type == "unvalidated":
        query_builder = query_builder.select(select_fields).eq('validated', False)
        description_for_print = "All Unvalidated Opinion Entries"
    elif list_type == "missing_lc_docket":
        # This query identifies unvalidated entries that:
        # 1. Have LCdocketID as null or empty string OR CaseNotes contains "[LC Docket Missing]".
        # 2. Are NOT Appellate Division cases (LowerCourtVenue != 'Appellate Division', typical for SC source).
        # 3. Are NOT agency cases (assumed if LCCounty is 'NJ').
        query_builder = query_builder.select(select_fields)\
                     .eq('validated', False)\
                     .or_('LCdocketID.is.null,LCdocketID.eq.,CaseNotes.ilike.%[LC Docket Missing]%') # Case-insensitive like for notes
                     .neq('LowerCourtVenue', 'Appellate Division')\
                     .neq('LCCounty', 'NJ') # Simple exclusion for potential Agency cases.
        description_for_print = "Unvalidated Opinions Potentially Missing LC Docket ID (Excluding SC/Agency Cases)"
    else:
        print(f"Error: Unknown list type '{list_type}'. Supported types are 'unvalidated' or 'missing_lc_docket'.")
        log.error(f"Invalid list_type ('{list_type}') provided for listing entries.")
        return

    try:
        # Apply ordering (most recent first) and limit to the query.
        response = query_builder.order('ReleaseDate', desc=True).order('AppDocketID').limit(limit).execute()

        if response.data:
            retrieved_entries = response.data
            print(f"\n--- {description_for_print} (Supabase, Max {limit} Entries) ---")
            # Define table headers for formatted output.
            headers = ["UniqueID (Start)", "Valid", "AppDocketID", "CaseName (Snippet)", "Release", "LC Venue", "LC Docket", "Entry Method", "Notes (Snippet)"]
            # Define column widths for alignment.
            col_widths = [18, 7, 13, 34, 12, 18, 18, 18, 20]
            header_str = " | ".join([f"{h:<{col_widths[i]}}" for i, h in enumerate(headers)])
            print(header_str)
            print("-" * len(header_str)) # Separator line.

            for entry in retrieved_entries:
                # Extract and format data for each column, handling None values.
                uid_s = (entry.get('UniqueID') or '')[:8] # Show first 8 chars of UniqueID.
                val_s = "Yes" if entry.get('validated') else "No"
                app_s = (entry.get('AppDocketID') or '[N/A]')[:11]
                cn_s = (entry.get('CaseName') or '[N/A]')[:32] # Truncate long case names.
                rel_s = (entry.get('ReleaseDate') or '[N/A]')[:10]
                lcv_s = (entry.get('LowerCourtVenue') or '[N/A]')[:16]
                lcd_s = (entry.get('LCdocketID') or '[N/A]')[:16]
                em_s = (entry.get('entry_method') or '[N/A]')[:16]
                notes_s = (entry.get('CaseNotes') or '')[:18] # Truncate long notes.
                if len(entry.get('CaseNotes', '')) > 18: notes_s += "..."

                # Print the formatted row.
                row_data = [uid_s, val_s, app_s, cn_s, rel_s, lcv_s, lcd_s, em_s, notes_s]
                row_str = " | ".join([f"{str(d):<{col_widths[i]}}" for i, d in enumerate(row_data)])
                print(row_str)

            print("-" * len(header_str)) # Footer separator.
            print(f"Found {len(retrieved_entries)} entries matching criteria.")
            if len(retrieved_entries) > 0:
                 print("Use 'validate --validate-id <UniqueID>' CLI command to review and edit specific entries.")

        elif response.error:
            # Handle errors reported by Supabase during query execution.
            print(f"Error querying Supabase for '{list_type}': {response.error}")
            log.error(f"Supabase error listing entries (type: '{list_type}'): {response.error}")
        else:
            # No data and no error means no entries matched the criteria.
            print(f"No entries found for '{description_for_print.lower()}' matching the criteria in Supabase.")
            log.info(f"list_entries_supabase found no matching entries for list_type '{list_type}'.")

    except Exception as e:
         # Catch any other unexpected errors during the listing process.
         log.error(f"Unexpected error while listing entries (type: '{list_type}'): {e}", exc_info=True)
         print(f"An unexpected error occurred during listing: {e}")


# === End of GvalidatorEM.py ===
[end of Modules/GvalidatorEM.py]
