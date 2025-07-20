# GcalendarDbEM.py
"""
Manages the storage of parsed calendar event data into the Supabase database.

This module is specifically responsible for interacting with the `calendar_entries`
table in the Supabase backend. Its primary function is to take structured data
representing calendar events (typically parsed from PDF files by `GcalendarParserEM.py`)
and save this data to the database.

Key operations include:
- Connecting to Supabase using the client provided by `GdbEM.get_supabase_client()`.
- Performing bulk `insert` operations into the `calendar_entries` table.
- Performing necessary data type transformations (e.g., boolean conversions,
  converting lists to comma-separated strings) to ensure compatibility with
  the database schema before insertion.
- Reporting success and error counts for the save operations.

Note: This module currently focuses on inserting new calendar entries.
It does not implement upsert logic or explicit checks for pre-existing
calendar entries based on a unique identifier within this module itself.
Duplicate prevention would rely on database constraints (e.g., Primary Keys)
or pre-filtering of data before calling `save_calendar_entries`.
"""
import logging
from GdbEM import get_supabase_client # Import the Supabase client getter from GdbEM.
from supabase import PostgrestAPIResponse # For type hinting Supabase responses.

log = logging.getLogger(__name__)

def save_calendar_entries(calendar_data_list: list[dict]) -> dict:
    """
    Saves a list of parsed calendar entry dictionaries to the Supabase
    `calendar_entries` table using a bulk insert operation.

    Before insertion, it performs some data transformations:
    - Ensures boolean fields ('OralArgument', 'IsConsolidated') are explicitly boolean.
    - Converts the 'CalendarArgumentDays' list into a comma-separated string if it's a list.

    Args:
        calendar_data_list (list[dict]): A list of dictionaries, where each
                                         dictionary represents a parsed calendar
                                         event (case entry) from `GcalendarParserEM.py`.
                                         Each dictionary should conform to the expected
                                         schema of the `calendar_entries` table.

    Returns:
        dict: A summary dictionary of the insertion results, containing:
              {'inserted': count_of_successfully_inserted_records,
               'error': count_of_records_that_failed_or_were_not_attempted_due_to_initial_error}.
    """
    if not calendar_data_list:
        log.warning("No calendar entries provided to save_calendar_entries. Nothing to do.")
        return {'inserted': 0, 'error': 0}

    # Obtain the Supabase client instance.
    supabase = get_supabase_client()
    if not supabase:
        log.error("Cannot save calendar entries: Supabase client is not available. "
                  "Ensure Supabase URL and Key are correctly configured.")
        # If client is not available, all entries in the list are considered errored.
        return {'inserted': 0, 'error': len(calendar_data_list)}

    log.info(f"Attempting to insert {len(calendar_data_list)} calendar entries "
             f"into Supabase table 'calendar_entries'...")

    try:
        # --- Data Preparation ---
        # Iterate through each entry to ensure data types are compatible with Supabase/JSON.
        for entry_dict in calendar_data_list:
             # Explicitly convert boolean fields to bool.
             entry_dict['OralArgument'] = bool(entry_dict.get('OralArgument', False))
             entry_dict['IsConsolidated'] = bool(entry_dict.get('IsConsolidated', False))

             # Convert 'CalendarArgumentDays' list to a comma-separated string if it's a list.
             # This is necessary if the database column is of type TEXT or VARCHAR.
             if 'CalendarArgumentDays' in entry_dict and isinstance(entry_dict['CalendarArgumentDays'], list):
                  entry_dict['CalendarArgumentDays'] = ",".join(sorted(list(set(entry_dict['CalendarArgumentDays'])))) # Sort and unique for consistency

        # --- Database Operation: Bulk Insert ---
        # Perform a bulk insert of all prepared calendar entries.
        # Note: Supabase Python client's insert method can handle a list of dicts.
        # Consider chunking `calendar_data_list` into smaller batches if inserting
        # a very large number of records at once causes issues (e.g., payload size limits).
        response: PostgrestAPIResponse = supabase.table('calendar_entries').insert(calendar_data_list).execute()

        # --- Handle Response ---
        if hasattr(response, 'data') and response.data is not None:
            # Supabase `insert` operation typically returns a list of the inserted records.
            inserted_count = len(response.data)
            log.info(f"Successfully inserted {inserted_count} calendar entries into 'calendar_entries'.")

            # Verify if the number of inserted records matches the number of records attempted.
            # Discrepancies might indicate partial success or issues not caught as an explicit error
            # (e.g., if database has row-level security or unique constraints that silently prevent some inserts in a batch).
            if inserted_count != len(calendar_data_list):
                 log.warning(f"Inserted count ({inserted_count}) differs from input count ({len(calendar_data_list)}). "
                             "This might indicate partial success or that some records were not inserted due to "
                             "database constraints (e.g., primary key conflicts if table has them and this isn't an upsert).")
            # Calculate errors based on discrepancy, assuming records not in response.data failed.
            num_errors = len(calendar_data_list) - inserted_count
            return {'inserted': inserted_count, 'error': num_errors}
        elif hasattr(response, 'error') and response.error:
            # An explicit error was returned by Supabase.
            log.error(f"Supabase API error while inserting calendar entries: {response.error}")
            return {'inserted': 0, 'error': len(calendar_data_list)} # Assume all failed if API reports error.
        else:
            # The response format is unexpected (neither data nor error clearly present).
            log.error(f"Unexpected response format from Supabase during calendar entries insert. Response: {response}")
            return {'inserted': 0, 'error': len(calendar_data_list)} # Assume all failed.

    except Exception as e:
        # Catch any other exceptions during the process (e.g., network issues, unexpected errors in client library).
        log.critical(f"A critical error occurred during Supabase calendar entries insert: {e}", exc_info=True)
        return {'inserted': 0, 'error': len(calendar_data_list)} # Assume all failed on critical exception.

# === End of GcalendarDbEM.py ===
