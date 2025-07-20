# GjudgeListEM.py
"""
Manages a reference list of known judge names for the G-EntryPoint-Manager (GEM) application.

This module is responsible for:
1.  Loading judge names from a predefined text file (e.g., `judges_2024_2025.txt`),
    which is expected to be located in the same directory as this module.
2.  Cleaning and normalizing these names by removing common prefixes (like "Hon.")
    and suffixes (like ", t/a") and standardizing whitespace.
3.  Storing the cleaned list of unique judge names in an in-memory set for efficient
    lookup and access (caching).
4.  Providing a function (`get_reference_judge_set()`) for other modules (e.g., `GstatusEM`)
    to retrieve this reference set, typically for validation or cross-referencing
    judge names found in scraped or database records.

A potential future enhancement could be an `is_judge_name_valid(name)` function
that directly checks a given name against this reference set.
"""
import os
import logging
import re # Imported re here as it's used in _clean_judge_name

log = logging.getLogger(__name__)

# Determine the directory where this module is located to find the judge list file.
MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
# Define the name of the text file containing the list of reference judge names.
JUDGE_LIST_FILE = os.path.join(MODULE_DIR, "judges_2024_2025.txt")

# In-memory cache for the set of judge names.
# This avoids re-reading and re-processing the file multiple times during an application run.
# Initialized to None to indicate it hasn't been loaded yet.
_judge_list_cache: set | None = None

def _clean_judge_name(name: str | None) -> str | None:
    """
    Cleans a raw judge name string by removing common titles, suffixes,
    and normalizing whitespace.

    Cleaning steps:
    - Removes "Hon." prefix (case-insensitive).
    - Removes ", t/a" suffix (case-insensitive, typically "trading as").
    - Normalizes multiple spaces into single spaces.
    - Strips leading/trailing whitespace.

    Args:
        name (str | None): The raw judge name string.

    Returns:
        str | None: The cleaned judge name, or None if the input was None or empty after cleaning.
    """
    if not name:
        return None

    # Remove "Hon." prefix, ignoring case, and strip whitespace.
    cleaned_name = re.sub(r"^\s*Hon\.\s*", "", name, flags=re.IGNORECASE).strip()
    # Remove ", t/a" suffix, ignoring case, and strip whitespace.
    # This pattern looks for a comma, optional spaces, "t/a", optional spaces, at the end of the string.
    cleaned_name = re.sub(r"\s*,\s*t/a\s*$", "", cleaned_name, flags=re.IGNORECASE).strip()
    # Replace multiple whitespace characters (spaces, tabs, newlines) with a single space.
    cleaned_name = re.sub(r"\s+", " ", cleaned_name).strip()

    # Optional considerations for further normalization (not currently implemented):
    # - Convert to a standard case (e.g., .title() or .lower()) for case-insensitive comparisons.
    # - Handle other suffixes like "Jr.", "Sr.", "III" (currently preserved).
    # - Remove other titles or post-nominals if necessary.

    return cleaned_name if cleaned_name else None # Return None if cleaning results in an empty string.

def load_reference_judge_list() -> set:
    """
    Loads the reference list of judge names from the `JUDGE_LIST_FILE`.

    Each line in the file is treated as a potential judge name. It applies
    `_clean_judge_name()` to each name and stores the unique, cleaned names
    in a set. This set is then cached in `_judge_list_cache`.

    If the file is not found or a read error occurs, an error is logged,
    and an empty set is cached and returned.

    Returns:
        set: A set of unique, cleaned judge names. Returns an empty set if
             the file cannot be loaded or is empty.
    """
    global _judge_list_cache
    # This check is actually redundant if get_reference_judge_set always calls this when cache is None.
    # However, if load_reference_judge_list could be called directly, it's fine.
    if _judge_list_cache is not None:
        log.debug("Judge list already in cache. Returning cached version.")
        return _judge_list_cache

    judge_name_set = set() # Use a set to ensure uniqueness.
    # Check if the judge list file exists.
    if not os.path.exists(JUDGE_LIST_FILE):
        log.error(f"Reference judge list file not found at: {JUDGE_LIST_FILE}")
        _judge_list_cache = judge_name_set # Cache an empty set to prevent re-attempts in this session.
        return judge_name_set

    try:
        # Open and read the judge list file.
        with open(JUDGE_LIST_FILE, 'r', encoding='utf-8') as f: # Specify encoding, UTF-8 is common.
            for line_number, line_content in enumerate(f, 1):
                # Clean each name read from the file.
                cleaned_judge_name = _clean_judge_name(line_content)
                if cleaned_judge_name: # Add to set if cleaning resulted in a non-empty name.
                    judge_name_set.add(cleaned_judge_name)
                elif line_content.strip() and not line_content.strip().startswith("#"): # Non-empty, non-comment line that became empty
                    log.debug(f"Line {line_number} ('{line_content.strip()}') in {JUDGE_LIST_FILE} resulted in empty name after cleaning.")

        log.info(f"Successfully loaded {len(judge_name_set)} unique reference judge names from {JUDGE_LIST_FILE}.")
        _judge_list_cache = judge_name_set # Populate the cache.
        return judge_name_set
    except IOError as e_io:
        # Handle file read errors (e.g., permission issues).
        log.error(f"IOError reading reference judge list file {JUDGE_LIST_FILE}: {e_io}", exc_info=True)
        _judge_list_cache = set() # Cache empty set on error.
        return set()
    except Exception as e_general:
        # Catch any other unexpected errors during file loading.
        log.error(f"An unexpected error occurred while loading the judge list from {JUDGE_LIST_FILE}: {e_general}", exc_info=True)
        _judge_list_cache = set() # Cache empty set on error.
        return set()

def get_reference_judge_set() -> set:
    """
    Returns the cached set of reference judge names.

    If the cache (`_judge_list_cache`) is not yet populated (is None),
    it calls `load_reference_judge_list()` to load the names from the file
    and populate the cache.

    Returns:
        set: A set of unique, cleaned reference judge names.
    """
    # Check if the cache has been populated.
    if _judge_list_cache is None:
        # If not, load the list (which also populates the cache).
        log.debug("Judge list cache is empty. Calling load_reference_judge_list().")
        return load_reference_judge_list()
    # Return the cached set.
    return _judge_list_cache

# Example usage block, typically for testing the module directly.
if __name__ == "__main__":
    # This block executes only when the script is run directly (e.g., `python GjudgeListEM.py`).
    # It's useful for quick verification of the module's functionality.
    # Ensure 're' is imported if _clean_judge_name uses it and this block is active.
    # import re # Already imported at the top level of the module.
    print("Attempting to load reference judge list...")
    reference_judges = get_reference_judge_set() # Get the set of judges.
    if reference_judges:
        print(f"Successfully loaded {len(reference_judges)} unique judge names:")
        # Print sorted list for consistent output.
        for judge_name_entry in sorted(list(reference_judges)):
            print(f"  - {judge_name_entry}")
    else:
        print("No judge names were loaded. Check logs for errors or if the list file is empty/missing.")

# === End of GjudgeListEM.py ===
