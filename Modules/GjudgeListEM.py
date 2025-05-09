# GjudgeListEM.py
"""
Loads and provides access to the reference list of known judge names.
"""
import os
import logging

log = logging.getLogger(__name__)

# Assumes the judge list file is in the same directory as this module
MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
JUDGE_LIST_FILE = os.path.join(MODULE_DIR, "judges_2024_2025.txt") # Name matches previous discussion

_judge_list_cache = None

def _clean_judge_name(name):
    """Cleans judge name: removes titles, suffixes like t/a, extra spaces."""
    if not name:
        return None
    # Remove titles like Hon.
    name = re.sub(r"^\s*Hon\.\s*", "", name, flags=re.IGNORECASE).strip()
    # Remove , t/a suffix (case-insensitive)
    name = re.sub(r"\s*,\s*t/a\s*$", "", name, flags=re.IGNORECASE).strip()
    # Normalize spacing
    name = re.sub(r"\s+", " ", name).strip()
    # Optional: Handle suffixes like Jr., Sr., III (keep them for now)
    # Optional: Convert to a standard case (e.g., lower or title) if needed for comparison
    return name

def load_reference_judge_list():
    """Loads the reference judge list from the text file."""
    global _judge_list_cache
    if _judge_list_cache is not None:
        return _judge_list_cache

    judge_set = set()
    if not os.path.exists(JUDGE_LIST_FILE):
        log.error(f"Reference judge list file not found: {JUDGE_LIST_FILE}")
        _judge_list_cache = judge_set # Cache empty set on error
        return judge_set

    try:
        with open(JUDGE_LIST_FILE, 'r') as f:
            for line in f:
                cleaned_name = _clean_judge_name(line)
                if cleaned_name:
                    judge_set.add(cleaned_name)
        log.info(f"Loaded {len(judge_set)} reference judge names from {JUDGE_LIST_FILE}")
        _judge_list_cache = judge_set
        return judge_set
    except IOError as e:
        log.error(f"Error reading judge list file {JUDGE_LIST_FILE}: {e}", exc_info=True)
        _judge_list_cache = set() # Cache empty set on error
        return set()
    except Exception as e:
        log.error(f"Unexpected error loading judge list: {e}", exc_info=True)
        _judge_list_cache = set()
        return set()

def get_reference_judge_set():
    """Returns the cached set of reference judge names."""
    if _judge_list_cache is None:
        return load_reference_judge_list()
    return _judge_list_cache

# Example usage (optional)
if __name__ == "__main__":
    import re # Need re for the cleaning function if run standalone
    print("Loading judge list...")
    judges = get_reference_judge_set()
    print(f"Found {len(judges)} judges:")
    for judge in sorted(list(judges)):
        print(f"- {judge}")

# === End of GjudgeListEM.py ===
