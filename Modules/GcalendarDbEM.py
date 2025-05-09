# GcalendarDbEM.py
"""
Handles saving parsed calendar data to the Supabase 'calendar_entries' table.
"""
import logging
from GdbEM import get_supabase_client # Import the initialized client
from supabase import PostgrestAPIResponse

log = logging.getLogger(__name__)

def save_calendar_entries(calendar_data_list):
    """
    Saves a list of parsed calendar entry dictionaries to Supabase.

    Args:
        calendar_data_list (list): List of dictionaries, each representing a case entry.

    Returns:
        dict: Summary of insertion results {'inserted': count, 'error': count}.
    """
    if not calendar_data_list:
        log.warning("No calendar entries provided to save.")
        return {'inserted': 0, 'error': 0}

    supabase = get_supabase_client()
    if not supabase:
        log.error("Cannot save calendar entries: Supabase client not available.")
        return {'inserted': 0, 'error': len(calendar_data_list)}

    log.info(f"Attempting to insert {len(calendar_data_list)} calendar entries into Supabase table 'calendar_entries'...")

    try:
        # Ensure boolean fields are explicitly boolean for JSON compatibility
        for entry in calendar_data_list:
             entry['OralArgument'] = bool(entry.get('OralArgument', False))
             entry['IsConsolidated'] = bool(entry.get('IsConsolidated', False))
             # Convert list of days to comma-separated string if needed for DB schema
             if 'CalendarArgumentDays' in entry and isinstance(entry['CalendarArgumentDays'], list):
                  entry['CalendarArgumentDays'] = ",".join(entry['CalendarArgumentDays'])


        # Perform bulk insert
        # Note: Supabase might have limits on bulk insert size. Consider chunking if needed.
        response: PostgrestAPIResponse = supabase.table('calendar_entries').insert(calendar_data_list).execute()

        # Check response
        if hasattr(response, 'data') and response.data:
            inserted_count = len(response.data)
            log.info(f"Successfully inserted {inserted_count} calendar entries.")
            # Check if count matches input length - Supabase insert usually returns inserted rows
            if inserted_count != len(calendar_data_list):
                 log.warning(f"Inserted count ({inserted_count}) differs from input count ({len(calendar_data_list)}). Check for potential duplicates or partial failures if PK conflicts exist.")
            return {'inserted': inserted_count, 'error': len(calendar_data_list) - inserted_count}
        elif hasattr(response, 'error') and response.error:
            log.error(f"Supabase error inserting calendar entries: {response.error}")
            return {'inserted': 0, 'error': len(calendar_data_list)}
        else:
            # Handle unexpected response format
            log.error(f"Unexpected response from Supabase during calendar insert: {response}")
            return {'inserted': 0, 'error': len(calendar_data_list)}

    except Exception as e:
        log.error(f"Critical error during Supabase calendar insert: {e}", exc_info=True)
        return {'inserted': 0, 'error': len(calendar_data_list)}

# === End of GcalendarDbEM.py ===
