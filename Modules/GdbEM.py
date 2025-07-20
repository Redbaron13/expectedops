# GdbEM.py
# V9: Rewritten for Supabase backend.
"""
Manages all database interactions with the Supabase backend for the G-EntryPoint-Manager (GEM) application.

This module is responsible for:
1.  Initializing and providing a singleton Supabase client instance using credentials
    loaded via `GconfigEM`.
2.  Handling CRUD (Create, Read, Update, Delete) operations for various database tables,
    primarily:
    - `opinions`: Stores detailed information about court opinions.
    - `opinion_history`: Keeps a historical log of changes or additions to opinions.
    - (Implicitly supports other tables if functions are added, e.g., `calendar_entries`
      which is primarily managed by `GcalendarDbEM.py` but might use the client from here).
3.  Generating unique identifiers and data hashes for opinion records to ensure data integrity
    and facilitate efficient updates.
4.  Providing helper functions to query database statistics and retrieve specific records.
5.  Encapsulating Supabase-specific API calls and error handling.
"""
import os
import logging
import datetime
import hashlib # For generating SHA256 data hashes.
import uuid    # For generating UUIDv5 unique identifiers.
import GconfigEM # To get Supabase credentials from environment variables.
from supabase import create_client, Client, PostgrestAPIResponse # Supabase Python client library.

log = logging.getLogger(__name__)

# --- Supabase Client Initialization ---
supabase_client: Client | None = None # Global singleton for the Supabase client.

def get_supabase_client():
    """
    Initializes and returns the Supabase client as a singleton.

    This function ensures that only one instance of the Supabase client is created
    during the application's lifecycle. It retrieves Supabase URL and Key using
    `GconfigEM.get_supabase_url()` and `GconfigEM.get_supabase_key()`.

    Returns:
        Client or None: The initialized Supabase client instance if successful.
                        None if initialization fails (e.g., missing credentials).

    Raises:
        ConnectionError: If Supabase URL or Key is not set, or if the client
                         fails to initialize for other reasons.
    """
    global supabase_client
    if supabase_client is None:
        # Retrieve Supabase credentials from configuration (environment variables).
        supabase_url = GconfigEM.get_supabase_url()
        supabase_key = GconfigEM.get_supabase_key()

        if not supabase_url or not supabase_key:
            log.critical("Supabase URL or Key is missing in environment configuration. Cannot connect to the database.")
            # Raising an error here is important as the application cannot function without DB access.
            raise ConnectionError("Supabase URL or Key environment variables not set. Please check .env or system environment.")

        try:
            log.info("Initializing Supabase client with provided URL...")
            # Create the Supabase client instance.
            supabase_client = create_client(supabase_url, supabase_key)
            # Optional: A lightweight test to confirm client is working.
            # For example, trying to fetch table definitions or a single row.
            # This was commented out in original, can be enabled for stricter checks.
            # Example test:
            #   response = supabase_client.table('opinions').select('UniqueID', count='exact').limit(1).execute()
            #   if response.count is None: # Check if count is None, indicating potential issue
            #       log.warning("Supabase connection test failed: 'opinions' table inaccessible or other issue.")
            #       # Depending on strictness, could raise ConnectionError here.
            #   else:
            #       log.info("Supabase client initialized and basic connection test to 'opinions' table OK.")
            log.info("Supabase client initialized successfully.")
        except Exception as e:
            log.critical(f"Failed to create or test Supabase client: {e}", exc_info=True)
            supabase_client = None # Ensure client remains None on failure.
            # Propagate the error to alert the calling function.
            raise ConnectionError(f"Failed to initialize Supabase client: {e}") from e
    return supabase_client

# --- Data Handling Helpers (Hashing/ID Generation) ---
def generate_data_hash(opinion_data: dict) -> str:
    """
    Generates a SHA256 hash for a defined set of core opinion data fields.

    This hash is used to detect changes in opinion content. The selection of fields
    is crucial for determining what constitutes a "change".

    Args:
        opinion_data (dict): A dictionary containing opinion data. Missing keys
                             for hashed fields will default to empty strings.

    Returns:
        str: A SHA256 hex digest representing the core data of the opinion.
    """
    # Define the core fields that contribute to the hash.
    # Order matters here. Using .get() provides default empty strings for missing fields.
    core_data_str = (
        f"{opinion_data.get('AppDocketID', '')}|{opinion_data.get('ReleaseDate', '')}|{opinion_data.get('CaseName', '')}|"
        f"{opinion_data.get('DecisionTypeCode', '')}|{opinion_data.get('Venue', '')}|{opinion_data.get('LCdocketID', '')}|"
        f"{opinion_data.get('LowerCourtVenue', '')}|{opinion_data.get('LowerCourtSubCaseType', '')}|"
        f"{opinion_data.get('CaseNotes', '')}|{opinion_data.get('LinkedDocketIDs', '')}"
    )
    # Encode the string to UTF-8 before hashing.
    return hashlib.sha256(core_data_str.encode('utf-8')).hexdigest()

def generate_unique_id(data_hash: str, app_docket_id: str | None) -> str | None:
    """
    Generates a UUIDv5 based on the provided data hash.

    UUIDv5 is deterministic, meaning the same namespace and name will always
    produce the same UUID. Here, `data_hash` serves as the unique "name"
    within a standard DNS namespace.

    Args:
        data_hash (str): The SHA256 hash generated by `generate_data_hash()`.
        app_docket_id (str | None): The appellate docket ID, used for logging if
                                    `data_hash` is missing.

    Returns:
        str or None: The generated UUIDv5 as a string, or None if `data_hash` is empty.
    """
    if not data_hash:
        log.warning(f"Cannot generate UniqueID: data_hash is empty. (AppDocket: {app_docket_id or 'N/A'})")
        return None
    # uuid.NAMESPACE_DNS is a standard predefined UUID namespace.
    # The name string used for UUIDv5 generation is the data_hash itself.
    base_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, data_hash)
    return str(base_uuid)

# --- Database Operations for 'opinions' Table ---

def save_opinions_to_db(opinion_list: list, is_validated: bool, run_type: str) -> dict:
    """
    Saves or updates a list of opinion records in the Supabase 'opinions' table
    using an 'upsert' operation. Also logs each processed opinion to the
    'opinion_history' table.

    The function prepares each opinion record by:
    - Generating a data hash and a unique ID.
    - Setting run type, validation status, and entry method.
    - Converting boolean and integer fields to appropriate types.
    - Adding timestamps.

    Args:
        opinion_list (list): A list of dictionaries, where each dictionary
                             represents an opinion's data.
        is_validated (bool): A boolean flag indicating if the opinions in this batch
                             are from a user-validated run. This influences the
                             'validated' and 'entry_method' fields.
        run_type (str): An identifier for the type of run that produced these opinions
                        (e.g., 'scheduled-primary-1', 'manual-force').

    Returns:
        dict: A summary of the database operations, including counts for
              opinions processed, upserted, skipped, errors, history records saved,
              and history save errors.
              Example: {"processed": 10, "upserted": 8, "skipped": 0, "error": 2,
                        "history_saved": 8, "history_error": 0}
    """
    if not opinion_list:
        log.warning("No opinions provided to save_opinions_to_db. Aborting save.")
        return {"processed": 0, "upserted": 0, "skipped": 0, "error": 0, "history_saved": 0, "history_error": 0}

    supabase = get_supabase_client() # Get the Supabase client.
    if not supabase:
        log.error("Supabase client not available. Cannot save opinions.")
        # Return a summary indicating all opinions failed due to DB connection issue.
        return {"processed": len(opinion_list), "upserted": 0, "skipped": 0, "error": len(opinion_list),
                "history_saved": 0, "history_error": 0}

    records_to_upsert = []    # List to hold records for the 'opinions' table.
    records_for_history = []  # List to hold records for the 'opinion_history' table.
    processed_count = 0       # Counter for opinions processed in this function call.
    initial_error_count = 0   # Counter for errors during data preparation.

    log.info(f"Preparing {len(opinion_list)} opinions for Supabase upsert (run_type: {run_type}, is_validated: {is_validated})...")

    for opinion_data in opinion_list:
        processed_count += 1
        try:
            # --- Data Preparation ---
            current_app_docket_id = opinion_data.get('AppDocketID', 'UNKNOWN_DOCKET')
            data_hash = generate_data_hash(opinion_data)
            unique_id = generate_unique_id(data_hash, current_app_docket_id)

            if not unique_id:
                log.warning(f"Skipping opinion due to missing UniqueID (AppDocket: {current_app_docket_id}). Likely data_hash was empty.")
                initial_error_count += 1
                continue # Skip this opinion if it can't get a UniqueID.

            # Create a working copy of the opinion data.
            record = opinion_data.copy()
            record['UniqueID'] = unique_id
            record['DataHash'] = data_hash
            record['RunType'] = run_type # Tag record with the type of run.

            # Handle validation status and entry method.
            # If 'validated' field already exists in scraped data, respect it unless current run is_validated.
            # The `is_validated` flag for the run takes precedence.
            if is_validated:
                record['validated'] = True
                record['entry_method'] = 'user_validated' # Mark as validated by user.
                record['last_validated_run_ts'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            elif 'validated' not in record: # If not a validating run and 'validated' field absent
                record['validated'] = False # Default to False.
            else: # If not a validating run but 'validated' field is present, keep its value.
                record['validated'] = bool(record.get('validated', False))

            # Set entry_method if not already set by validation logic.
            record.setdefault('entry_method', 'scraper')

            # Timestamps: last_updated_ts is set for every upsert.
            # 'first_scraped_ts' should ideally be handled by a DB trigger or default value on insert.
            # If not, upsert logic needs care to avoid overwriting it on subsequent updates.
            record['last_updated_ts'] = datetime.datetime.now(datetime.timezone.utc).isoformat()

            # Ensure correct data types for Supabase (booleans, integers).
            for bool_field in ['validated', 'caseconsolidated', 'recordimpounded']:
                 record[bool_field] = bool(record.get(bool_field, False)) # Default to False if missing.
            for int_field in ['opinionstatus']: # Add other integer fields if necessary.
                 record[int_field] = int(record.get(int_field, 0)) # Default to 0 if missing.

            # Fields like 'first_scraped_ts' are often best handled by database defaults on insert
            # to prevent them from being overwritten on every update.
            # If not handled by DB, logic here would need to check if it's an insert or update.
            # record.pop('first_scraped_ts', None) # Example: remove if DB handles it.

            records_to_upsert.append(record)

            # --- Prepare History Record ---
            # Create a snapshot of the current state for the history table.
            history_record = record.copy()
            history_record['run_timestamp'] = record['last_updated_ts'] # Use the same timestamp for consistency.
            # Potentially remove very large fields or fields not relevant for history.
            # e.g., history_record.pop('some_very_large_text_field', None)
            records_for_history.append(history_record)

        except Exception as e:
            # Catch errors during preparation of a single opinion.
            log.error(f"Error preparing opinion (AppDocket: {opinion_data.get('AppDocketID', 'N/A')}) for save: {e}", exc_info=True)
            initial_error_count += 1

    # --- Perform Supabase Upsert for 'opinions' table ---
    upserted_count = 0
    db_upsert_error_count = 0
    if records_to_upsert:
        log.info(f"Upserting {len(records_to_upsert)} records into 'opinions' table...")
        try:
            # Upsert operation: inserts new records or updates existing ones based on 'UniqueID' conflict.
            response: PostgrestAPIResponse = supabase.table('opinions').upsert(
                records_to_upsert,
                on_conflict='UniqueID' # Specify the column(s) for conflict detection.
            ).execute()

            if hasattr(response, 'data') and response.data is not None:
                 upserted_count = len(response.data) # Supabase returns the processed records.
                 log.info(f"Supabase upsert response indicates {upserted_count} records processed in 'opinions'.")
                 # Note: Upsert in Supabase typically doesn't distinguish between inserted and updated rows in the response count.
            elif hasattr(response, 'error') and response.error:
                 log.error(f"Supabase upsert failed for 'opinions': {response.error}")
                 db_upsert_error_count = len(records_to_upsert) # Assume all failed in this batch on error.
            else:
                 # Handle unexpected response format.
                 log.warning(f"Supabase 'opinions' upsert executed, but response format unexpected or no data returned. Count may be inaccurate. Response: {response}")
                 # Cautiously assume all were processed if no explicit error.
                 upserted_count = len(records_to_upsert)
        except Exception as e:
            log.critical(f"Critical error during Supabase upsert to 'opinions': {e}", exc_info=True)
            db_upsert_error_count = len(records_to_upsert) # Assume all in batch failed.

    # --- Perform Supabase Insert for 'opinion_history' table ---
    history_saved_count = 0
    history_error_count = 0
    if records_for_history:
        log.info(f"Inserting {len(records_for_history)} records into 'opinion_history' table...")
        try:
            # History records are always new inserts.
            response: PostgrestAPIResponse = supabase.table('opinion_history').insert(records_for_history).execute()
            if hasattr(response, 'data') and response.data is not None:
                 history_saved_count = len(response.data)
                 log.info(f"Successfully inserted {history_saved_count} records into 'opinion_history'.")
            elif hasattr(response, 'error') and response.error:
                 log.error(f"Supabase insert failed for 'opinion_history': {response.error}")
                 history_error_count = len(records_for_history) # Assume all failed.
            else:
                 log.warning(f"Supabase 'opinion_history' insert executed, but response format unexpected or no data returned. Response: {response}")
                 # Assume failure for safety if response is unclear.
                 history_error_count = len(records_for_history)
        except Exception as e:
            log.critical(f"Critical error during Supabase insert to 'opinion_history': {e}", exc_info=True)
            history_error_count = len(records_for_history) # Assume all failed.

    # --- Update Run Counter (if configured) ---
    # Increment run counter if any data was successfully written to the database.
    if upserted_count > 0 or history_saved_count > 0:
        try:
            GconfigEM.increment_run_counter()
            log.info("Run counter incremented due to successful database write.")
        except Exception as e:
            log.error(f"Failed to increment run counter after DB save: {e}", exc_info=True)

    # --- Compile and Return Summary ---
    total_errors = initial_error_count + db_upsert_error_count # Sum of preparation and DB errors for 'opinions'.
    # 'skipped' is hard to determine accurately with upsert without pre-selecting.
    # This is an estimate: total processed minus those successfully upserted and those that errored out before upsert.
    estimated_skipped = max(0, len(opinion_list) - upserted_count - total_errors)

    summary = {
        "processed_in_function": processed_count, # Total opinions this function attempted to process.
        "upserted_to_opinions": upserted_count,    # Successfully upserted to 'opinions'.
        "skipped_opinions": estimated_skipped, # Estimated skips (e.g. due to no change on upsert, or pre-upsert errors).
        "errors_opinions": total_errors,         # Errors related to 'opinions' table (prep or DB).
        "history_saved": history_saved_count,    # Successfully inserted into 'opinion_history'.
        "history_error": history_error_count     # Errors related to 'opinion_history' table.
    }
    log.info(f"Opinion save_opinions_to_db process summary: {summary}")
    return summary


def get_opinions_by_date_runtype(release_date: str, run_type_tag: str) -> dict:
    """
    Fetches opinions from the Supabase 'opinions' table that match a specific
    release date and run type tag.

    Args:
        release_date (str): The release date to filter by (e.g., "YYYY-MM-DD").
        run_type_tag (str): The run type tag to filter by.

    Returns:
        dict: A dictionary of matching opinions, keyed by 'AppDocketID', with values
              being the opinion record dictionaries (selected fields only).
              Returns an empty dictionary if no matches are found or an error occurs.
    """
    supabase = get_supabase_client()
    if not supabase:
        log.error("Supabase client not available for get_opinions_by_date_runtype.")
        return {}

    opinions_dict = {}
    try:
        # Perform the select query on the 'opinions' table.
        response: PostgrestAPIResponse = supabase.table('opinions')\
                           .select('AppDocketID, UniqueID, DataHash, validated, CaseName')\
                           .eq('ReleaseDate', release_date)\
                           .eq('RunType', run_type_tag)\
                           .execute()

        if response.data:
            for row in response.data:
                opinions_dict[row['AppDocketID']] = row # Use AppDocketID as key for easy lookup.
            log.info(f"Found {len(opinions_dict)} opinions for ReleaseDate '{release_date}' and RunType '{run_type_tag}'.")
        else:
            # No data found, check for errors in response.
            if hasattr(response, 'error') and response.error:
                 log.error(f"Supabase error fetching opinions by date/runtype (Date: {release_date}, RunType: {run_type_tag}): {response.error}")
            else:
                 log.info(f"No opinions found for ReleaseDate '{release_date}' and RunType '{run_type_tag}'.")
    except Exception as e:
        log.error(f"Error fetching opinions by date/runtype (Date: {release_date}, RunType: {run_type_tag}): {e}", exc_info=True)

    return opinions_dict

def get_opinion_by_id(unique_id: str) -> dict | None:
    """
    Fetches a single opinion record from the Supabase 'opinions' table
    by its 'UniqueID'.

    Args:
        unique_id (str): The UniqueID of the opinion to retrieve.

    Returns:
        dict or None: The opinion record as a dictionary if found, otherwise None.
    """
    supabase = get_supabase_client()
    if not supabase:
        log.error(f"Supabase client not available for get_opinion_by_id (ID: {unique_id}).")
        return None
    try:
        # Select all columns (*) for the opinion matching the UniqueID.
        response: PostgrestAPIResponse = supabase.table('opinions').select('*').eq('UniqueID', unique_id).limit(1).execute()
        if response.data:
            log.info(f"Successfully fetched opinion with UniqueID: {unique_id}.")
            return response.data[0] # Return the first (and should be only) record.
        else:
            if hasattr(response, 'error') and response.error:
                 log.error(f"Supabase error fetching opinion by UniqueID '{unique_id}': {response.error}")
            else:
                 log.info(f"No opinion found with UniqueID: {unique_id}")
            return None
    except Exception as e:
        log.error(f"Error fetching opinion by UniqueID '{unique_id}': {e}", exc_info=True)
        return None

def update_opinion(unique_id: str, update_data: dict) -> bool:
     """
     Updates specific fields for an existing opinion in the 'opinions' table,
     identified by its 'UniqueID'.

     Automatically adds 'last_updated_ts' to the update payload.

     Args:
         unique_id (str): The UniqueID of the opinion to update.
         update_data (dict): A dictionary containing the fields and their new values
                             to be updated.

     Returns:
         bool: True if the update was successful, False otherwise.
     """
     supabase = get_supabase_client()
     if not supabase:
         log.error(f"Supabase client not available for update_opinion (ID: {unique_id}).")
         return False
     if not update_data:
         log.warning(f"No update_data provided for opinion UniqueID: {unique_id}. Update aborted.")
         return False

     try:
         # Automatically set the 'last_updated_ts' to the current time.
         update_data['last_updated_ts'] = datetime.datetime.now(datetime.timezone.utc).isoformat()

         # Perform the update operation.
         response: PostgrestAPIResponse = supabase.table('opinions').update(update_data).eq('UniqueID', unique_id).execute()

         if response.data: # Supabase returns the updated records in response.data
             log.info(f"Successfully updated opinion with UniqueID: {unique_id}. Data updated: {len(response.data)} record(s).")
             return True
         else:
             # Update failed or no records matched the UniqueID.
             error_msg = response.error if hasattr(response, 'error') and response.error else "No data returned or record not found."
             log.error(f"Failed to update opinion UniqueID '{unique_id}'. Error: {error_msg}")
             return False
     except Exception as e:
         log.error(f"Error updating opinion UniqueID '{unique_id}': {e}", exc_info=True)
         return False


def get_db_stats() -> dict:
    """
    Retrieves basic statistics from the Supabase 'opinions' table, including
    total number of opinions, count of validated opinions, and count of
    unvalidated opinions.

    Returns:
        dict: A dictionary containing the statistics:
              {"total": int, "validated": int, "unvalidated": int, "error": str|None}.
              The "error" key will contain an error message string if any part of
              fetching stats failed, otherwise it's None.
    """
    supabase = get_supabase_client()
    stats = {"total": 0, "validated": 0, "unvalidated": 0, "error": None} # Initialize stats.
    if not supabase:
        stats["error"] = "Supabase client not initialized. Cannot fetch DB stats."
        log.error(stats["error"])
        return stats

    try:
        # --- Get total count of opinions ---
        response_total: PostgrestAPIResponse = supabase.table('opinions').select('UniqueID', count='exact').execute()
        if response_total.count is not None:
            stats["total"] = response_total.count
        else:
            # If total count fails, it's a significant issue.
            error_msg = f"Failed to get total opinion count: {response_total.error if hasattr(response_total, 'error') else 'Unknown error'}"
            log.error(f"Supabase error: {error_msg}")
            stats["error"] = error_msg
            return stats # Return early as other stats depend on this.

        # --- Get count of validated opinions ---
        response_validated: PostgrestAPIResponse = supabase.table('opinions').select('UniqueID', count='exact').eq('validated', True).execute()
        if response_validated.count is not None:
            stats["validated"] = response_validated.count
        else:
            error_msg = f"Failed to get validated opinion count: {response_validated.error if hasattr(response_validated, 'error') else 'Unknown error'}"
            log.error(f"Supabase error: {error_msg}")
            # Append to existing error or set if first error.
            stats["error"] = f"{stats['error']}; {error_msg}" if stats["error"] else error_msg
            # Don't return early; attempt to get unvalidated or calculate.

        # --- Calculate or Query Unvalidated Opinions ---
        # Prefer calculation if total and validated counts are reliable.
        if stats["total"] >= stats["validated"]:
             stats["unvalidated"] = stats["total"] - stats["validated"]
        else:
             # This case indicates an inconsistency (e.g., total was 0 but validated > 0).
             # Log a warning and attempt to query unvalidated count directly.
             log.warning(f"Total opinion count ({stats['total']}) is less than validated count ({stats['validated']}). "
                         "Querying unvalidated count separately for accuracy.")
             response_unvalidated: PostgrestAPIResponse = supabase.table('opinions').select('UniqueID', count='exact').eq('validated', False).execute()
             if response_unvalidated.count is not None:
                  stats["unvalidated"] = response_unvalidated.count
             else:
                  error_msg = f"Failed to get unvalidated opinion count: {response_unvalidated.error if hasattr(response_unvalidated, 'error') else 'Unknown error'}"
                  log.error(f"Supabase error: {error_msg}")
                  stats["error"] = f"{stats['error']}; {error_msg}" if stats["error"] else error_msg

    except Exception as e:
        # Catch any other unexpected errors during stats fetching.
        error_msg = f"Unexpected error while getting database stats from Supabase: {e}"
        log.error(error_msg, exc_info=True)
        stats["error"] = f"{stats['error']}; {error_msg}" if stats["error"] else error_msg

    if stats["error"]:
        log.warning(f"DB stats collection encountered errors: {stats['error']}")
    else:
        log.info(f"DB stats successfully retrieved: Total={stats['total']}, Validated={stats['validated']}, Unvalidated={stats['unvalidated']}")
    return stats

# Note: Calendar-specific database operations (save_calendar_entries, etc.)
# are expected to be in GcalendarDbEM.py. GcalendarDbEM.py can import and use
# get_supabase_client() from this module (GdbEM.py) to interact with Supabase.

# Obsolete SQLite functions (e.g., build_combo_db, initialize_database, check_duplicate_by_hash)
# have been removed or their logic has been adapted/integrated into the Supabase-specific functions above.

# === End of GdbEM.py ===
