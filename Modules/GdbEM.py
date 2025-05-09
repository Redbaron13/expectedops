# GdbEM.py
# V9: Rewritten for Supabase backend.
"""
Handles database interactions with Supabase.
Manages connections and data operations for 'opinions' and 'calendar_entries' tables.
"""
import os
import logging
import datetime
import hashlib
import uuid
import GconfigEM # To get Supabase credentials
from supabase import create_client, Client, PostgrestAPIResponse # Added PostgrestAPIResponse

log = logging.getLogger(__name__)

# --- Supabase Client Initialization ---
supabase_client: Client | None = None

def get_supabase_client():
    """Initializes and returns the Supabase client singleton."""
    global supabase_client
    if supabase_client is None:
        supabase_url = GconfigEM.get_supabase_url()
        supabase_key = GconfigEM.get_supabase_key()
        if not supabase_url or not supabase_key:
            log.critical("Supabase URL or Key is missing. Cannot connect to database.")
            raise ConnectionError("Supabase URL or Key environment variables not set.")
        try:
            log.info("Initializing Supabase client...")
            supabase_client = create_client(supabase_url, supabase_key)
            # Optional: Test connection (e.g., fetch schema or a dummy record)
            # response = supabase_client.table('opinions').select('UniqueID', count='exact').limit(1).execute()
            # if response.count is None: # Check if count is None, indicating potential issue
            #     log.warning("Supabase connection test failed or 'opinions' table inaccessible.")
            #     # Decide if this should be a critical error or just a warning
            # else:
            #     log.info("Supabase client initialized and connection seems OK.")
            log.info("Supabase client initialized.") # Keep it simple for now
        except Exception as e:
            log.critical(f"Failed to create Supabase client: {e}", exc_info=True)
            supabase_client = None # Ensure it stays None on failure
            raise ConnectionError("Failed to initialize Supabase client") from e
    return supabase_client

# --- Data Handling Helpers (Hashing/ID Generation - Unchanged Python Logic) ---
def generate_data_hash(opinion_data):
    """Generates a SHA256 hash for core opinion data fields."""
    # Ensure keys exist, default to empty string if missing
    core_data_str = (
        f"{opinion_data.get('AppDocketID', '')}|{opinion_data.get('ReleaseDate', '')}|{opinion_data.get('CaseName', '')}|"
        f"{opinion_data.get('DecisionTypeCode', '')}|{opinion_data.get('Venue', '')}|{opinion_data.get('LCdocketID', '')}|"
        f"{opinion_data.get('LowerCourtVenue', '')}|{opinion_data.get('LowerCourtSubCaseType', '')}|"
        f"{opinion_data.get('CaseNotes', '')}|{opinion_data.get('LinkedDocketIDs', '')}"
    )
    return hashlib.sha256(core_data_str.encode('utf-8')).hexdigest()

def generate_unique_id(data_hash, app_docket_id):
    """Generates a UUIDv5 based on the data hash."""
    if not data_hash:
        log.warning(f"Cannot generate UniqueID without data_hash (AppDocket: {app_docket_id})")
        return None
    namespace = uuid.NAMESPACE_DNS # Using DNS namespace as a standard base
    name_string = data_hash # Use the content hash as the name
    base_uuid = uuid.uuid5(namespace, name_string)
    return str(base_uuid)

# --- Database Operations for 'opinions' Table ---

def save_opinions_to_db(opinion_list, is_validated, run_type):
    """
    Saves or updates opinions in the Supabase 'opinions' table.
    Uses 'upsert' for efficiency. Also logs to 'opinion_history'.

    Args:
        opinion_list (list): List of opinion data dictionaries.
        is_validated (bool): Whether the data comes from a validated run.
        run_type (str): Identifier for the type of run (e.g., 'scheduled-primary-1').

    Returns:
        dict: Summary of operations (inserted/updated count, errors).
    """
    if not opinion_list:
        log.warning("No opinions provided to save.")
        return {"inserted": 0, "updated": 0, "skipped": 0, "error": 0, "history_saved": 0, "history_error": 0} # Added history tracking

    supabase = get_supabase_client()
    if not supabase:
        return {"inserted": 0, "updated": 0, "skipped": 0, "error": len(opinion_list), "history_saved": 0, "history_error": 0}

    records_to_upsert = []
    records_for_history = []
    processed_count = 0
    error_count = 0

    log.info(f"Preparing {len(opinion_list)} opinions for Supabase upsert (run_type: {run_type})...")

    for opinion in opinion_list:
        try:
            processed_count += 1
            # Generate hash and ID
            data_hash = generate_data_hash(opinion)
            unique_id = generate_unique_id(data_hash, opinion.get('AppDocketID', 'UNKNOWN'))
            if not unique_id:
                log.warning(f"Skipping opinion due to missing UniqueID (AppDocket: {opinion.get('AppDocketID')}).")
                error_count += 1
                continue

            # Prepare record for upsert
            record = opinion.copy() # Work with a copy
            record['UniqueID'] = unique_id
            record['DataHash'] = data_hash
            record['RunType'] = run_type # Record the run type that led to this state
            record['validated'] = bool(record.get('validated', is_validated)) # Use existing 'validated' if present, else use run status
            record['entry_method'] = record.get('entry_method', 'scraper') # Keep existing method unless validated

            # Special handling for validation status
            if is_validated:
                record['validated'] = True
                record['entry_method'] = 'user_validated'
                record['last_validated_run_ts'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            elif 'validated' not in record: # If not explicitly validated and field missing, default to False
                 record['validated'] = False

            # Add/Update timestamps - Supabase handles CURRENT_TIMESTAMP via defaults/triggers if set up
            # We only need to set last_updated_ts explicitly if the trigger isn't used
            record['last_updated_ts'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            # first_scraped_ts should ideally be set only on insert. Upsert might overwrite.
            # A Supabase function/trigger is better for managing 'first_scraped_ts'.
            # For simplicity here, we'll omit setting it explicitly on upsert.

            # Ensure boolean/integer fields are correct type for Supabase/JSON
            for bool_field in ['validated', 'caseconsolidated', 'recordimpounded']:
                 record[bool_field] = bool(record.get(bool_field, 0))
            for int_field in ['opinionstatus']: # Add other int fields if any
                 record[int_field] = int(record.get(int_field, 0))

            # Remove fields potentially not in DB or handled by DB (like first_scraped_ts if trigger exists)
            # record.pop('first_scraped_ts', None)

            records_to_upsert.append(record)

            # Prepare history record (snapshot of current data)
            history_record = record.copy()
            history_record['run_timestamp'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            # Remove fields not relevant to history or potentially large/problematic if needed
            # history_record.pop('some_large_field', None)
            records_for_history.append(history_record)

        except Exception as e:
            log.error(f"Error preparing opinion {opinion.get('AppDocketID', 'N/A')} for save: {e}", exc_info=True)
            error_count += 1

    # --- Perform Supabase Upsert for 'opinions' ---
    upserted_count = 0
    if records_to_upsert:
        log.info(f"Upserting {len(records_to_upsert)} records into 'opinions' table...")
        try:
            # Assuming 'UniqueID' is the primary key for conflict resolution
            response: PostgrestAPIResponse = supabase.table('opinions').upsert(records_to_upsert, on_conflict='UniqueID').execute()
            # Check response structure - Supabase Python client V1 vs V2 might differ
            # V2 typically returns data in response.data
            if hasattr(response, 'data') and response.data:
                 upserted_count = len(response.data)
                 log.info(f"Supabase upsert response indicates {upserted_count} records processed.")
                 # Note: Upsert doesn't easily distinguish between insert/update count in the response.
                 # We report the total processed by upsert. More granular counts would require selects first.
            elif hasattr(response, 'error') and response.error:
                 log.error(f"Supabase upsert failed: {response.error}")
                 error_count += len(records_to_upsert) # Assume all failed if error reported
            else:
                 # Handle cases where response might not have data or error (e.g., empty list upserted?)
                 log.warning(f"Supabase upsert executed but response format unexpected or no data returned. Response: {response}")
                 # Assume success but log warning, count might be inaccurate
                 upserted_count = len(records_to_upsert)


        except Exception as e:
            log.error(f"Critical error during Supabase upsert: {e}", exc_info=True)
            error_count += len(records_to_upsert) # Assume all failed

    # --- Perform Supabase Insert for 'opinion_history' ---
    history_saved_count = 0
    history_error_count = 0
    if records_for_history:
        log.info(f"Inserting {len(records_for_history)} records into 'opinion_history' table...")
        try:
            # History should always be inserts
            response: PostgrestAPIResponse = supabase.table('opinion_history').insert(records_for_history).execute()
            if hasattr(response, 'data') and response.data:
                 history_saved_count = len(response.data)
                 log.info(f"Successfully inserted {history_saved_count} history records.")
            elif hasattr(response, 'error') and response.error:
                 log.error(f"Supabase history insert failed: {response.error}")
                 history_error_count += len(records_for_history)
            else:
                 log.warning(f"Supabase history insert executed but response format unexpected or no data returned. Response: {response}")
                 # Assume success? Or failure? Assume failure for safety.
                 history_error_count += len(records_for_history)

        except Exception as e:
            log.error(f"Critical error during Supabase history insert: {e}", exc_info=True)
            history_error_count += len(records_for_history)

    # --- Update Run Counter ---
    if upserted_count > 0 or history_saved_count > 0: # Increment if any DB write occurred
        try:
            GconfigEM.increment_run_counter()
        except Exception as e:
            log.error(f"Failed to increment run counter: {e}")

    # --- Return Summary ---
    # Since upsert doesn't distinguish insert/update, we report 'processed' by upsert
    # and 'skipped' is implicitly handled by the upsert logic (no change if data matches)
    summary = {
        "processed": processed_count,
        "upserted": upserted_count, # Count processed by upsert operation
        "skipped": processed_count - upserted_count - error_count, # Estimate skips
        "error": error_count,
        "history_saved": history_saved_count,
        "history_error": history_error_count
    }
    log.info(f"Opinion save summary: {summary}")
    return summary


def get_opinions_by_date_runtype(release_date, run_type_tag):
    """Fetches opinions from Supabase matching a specific release date and run type."""
    supabase = get_supabase_client()
    if not supabase: return {}

    opinions_dict = {}
    try:
        response = supabase.table('opinions')\
                           .select('AppDocketID, UniqueID, DataHash, validated, CaseName')\
                           .eq('ReleaseDate', release_date)\
                           .eq('RunType', run_type_tag)\
                           .execute()

        if response.data:
            for row in response.data:
                opinions_dict[row['AppDocketID']] = row # Store full row data keyed by docket
            log.info(f"Found {len(opinions_dict)} opinions for date {release_date}, run_type {run_type_tag}")
        else:
            log.info(f"No opinions found for date {release_date}, run_type {run_type_tag}")
            if response.error:
                 log.error(f"Supabase error fetching opinions by date/runtype: {response.error}")

    except Exception as e:
        log.error(f"Error fetching opinions by date/runtype: {e}", exc_info=True)

    return opinions_dict

def get_opinion_by_id(unique_id):
    """Fetches a single opinion by its UniqueID."""
    supabase = get_supabase_client()
    if not supabase: return None
    try:
        response = supabase.table('opinions').select('*').eq('UniqueID', unique_id).limit(1).execute()
        if response.data:
            return response.data[0]
        else:
            if response.error:
                 log.error(f"Supabase error fetching opinion by ID {unique_id}: {response.error}")
            return None
    except Exception as e:
        log.error(f"Error fetching opinion by ID {unique_id}: {e}", exc_info=True)
        return None

def update_opinion(unique_id, update_data):
     """Updates specific fields for an opinion by UniqueID."""
     supabase = get_supabase_client()
     if not supabase or not update_data: return False
     try:
         # Add last_updated_ts automatically
         update_data['last_updated_ts'] = datetime.datetime.now(datetime.timezone.utc).isoformat()

         response = supabase.table('opinions').update(update_data).eq('UniqueID', unique_id).execute()
         if response.data:
             log.info(f"Successfully updated opinion {unique_id}")
             return True
         else:
             log.error(f"Failed to update opinion {unique_id}. Error: {response.error}")
             return False
     except Exception as e:
         log.error(f"Error updating opinion {unique_id}: {e}", exc_info=True)
         return False


def get_db_stats():
    """Gets basic statistics (total, validated, unvalidated) from the Supabase 'opinions' table."""
    supabase = get_supabase_client()
    stats = {"total": 0, "validated": 0, "unvalidated": 0, "error": None}
    if not supabase:
        stats["error"] = "Supabase client not initialized"
        return stats

    try:
        # Get total count
        response_total = supabase.table('opinions').select('UniqueID', count='exact').execute()
        if response_total.count is not None:
            stats["total"] = response_total.count
        else:
             log.error(f"Supabase error getting total count: {response_total.error}")
             stats["error"] = f"Failed to get total count: {response_total.error}"
             return stats # Return early if total count fails

        # Get validated count
        response_validated = supabase.table('opinions').select('UniqueID', count='exact').eq('validated', True).execute()
        if response_validated.count is not None:
            stats["validated"] = response_validated.count
        else:
            log.error(f"Supabase error getting validated count: {response_validated.error}")
            stats["error"] = f"Failed to get validated count: {response_validated.error}"
            # Don't return early, unvalidated might still work

        # Calculate unvalidated (more efficient than another query if total is accurate)
        if stats["total"] >= stats["validated"]:
             stats["unvalidated"] = stats["total"] - stats["validated"]
        else:
             # Fallback query if counts seem inconsistent
             log.warning("Total count less than validated count, querying unvalidated separately.")
             response_unvalidated = supabase.table('opinions').select('UniqueID', count='exact').eq('validated', False).execute()
             if response_unvalidated.count is not None:
                  stats["unvalidated"] = response_unvalidated.count
             else:
                  log.error(f"Supabase error getting unvalidated count: {response_unvalidated.error}")
                  stats["error"] = stats["error"] + f"; Failed to get unvalidated count: {response_unvalidated.error}" if stats["error"] else f"Failed to get unvalidated count: {response_unvalidated.error}"


    except Exception as e:
        log.error(f"Error getting database stats from Supabase: {e}", exc_info=True)
        stats["error"] = f"Unexpected error: {e}"

    return stats

# === Calendar Database Operations (Placeholder - Implement in GcalendarDbEM.py) ===
# Functions like save_calendar_entries, get_distinct_judges etc.
# should be implemented in GcalendarDbEM.py which can import the supabase client from here.

# === Obsolete SQLite Functions (Remove or Comment Out) ===
# def build_combo_db(...): # Remove
# def initialize_database(...): # Logic replaced
# def check_duplicate_by_hash(...): # Logic integrated into save


# === End of GdbEM.py ===
