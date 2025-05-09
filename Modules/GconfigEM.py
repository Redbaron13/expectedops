# GconfigEM.py
# V2: Adapted for Supabase - Loads credentials from environment variables.
"""
Handles loading application configuration and Supabase credentials.
Removes SQLite-specific database file configurations.
"""
import json
import os
import logging
import datetime
from dotenv import load_dotenv # Added for .env support

log = logging.getLogger(__name__)

CONFIG_FILE = "config.json" # Still used for schedule, logging toggle, run counter

# --- Environment Variable Loading ---
def load_env():
    """Loads .env file for local development if it exists."""
    # Determine the project root based on this file's location
    # Assumes GconfigEM.py is in a 'Modules' subdirectory of the project root
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    dotenv_path = os.path.join(project_root, '.env')
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path=dotenv_path)
        # log.debug(f"Loaded environment variables from: {dotenv_path}") # Optional debug
    # else:
        # log.debug(".env file not found, relying on system environment variables.") # Optional debug

def get_supabase_url():
    """Gets Supabase URL from environment variables."""
    load_env() # Ensure .env is loaded if present
    url = os.environ.get("SUPABASE_URL")
    if not url:
        log.warning("SUPABASE_URL environment variable not set.")
    return url

def get_supabase_key():
    """Gets Supabase Service Role Key from environment variables."""
    load_env() # Ensure .env is loaded if present
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not key:
        log.warning("SUPABASE_SERVICE_KEY environment variable not set.")
    return key

# --- Configuration File Handling (for non-sensitive settings) ---

# Default schedule remains the same
DEFAULT_SCHEDULE = [
    {"time": "11:00", "type": "primary-1", "days": "Mon-Fri"},
    {"time": "17:30", "type": "primary-2", "days": "Mon-Fri"},
    {"time": "08:00", "type": "backup", "days": "Tue-Sat"},
    # Weekly check is hardcoded in GschedulerEM for now
]

DEFAULT_CONFIG = {
    "schedule": DEFAULT_SCHEDULE,
    "logging": True,
    "run_counter": 0,
    # Timestamps are now managed differently or might not be needed per-DB type
    # "last_run_timestamps": {} # Removed SQLite specific timestamps
}

def _get_config_path():
    """Gets the absolute path to the config file relative to the project root."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    return os.path.join(project_root, CONFIG_FILE)

def load_config():
    """Loads configuration from JSON file, creating/merging defaults if needed."""
    config_path = _get_config_path()
    if not os.path.exists(config_path):
        log.warning(f"Configuration file not found at {config_path}. Creating default config.")
        save_config(DEFAULT_CONFIG) # Save defaults first
        return DEFAULT_CONFIG.copy() # Return a copy

    try:
        with open(config_path, 'r') as f:
            config = json.load(f)

        # --- Merge with defaults to ensure all keys exist ---
        needs_saving = False
        if "schedule" not in config or not isinstance(config["schedule"], list):
            config["schedule"] = DEFAULT_CONFIG["schedule"]
            needs_saving = True
        if "logging" not in config or not isinstance(config["logging"], bool):
            config["logging"] = DEFAULT_CONFIG["logging"]
            needs_saving = True
        if "run_counter" not in config or not isinstance(config["run_counter"], int):
            config["run_counter"] = DEFAULT_CONFIG["run_counter"]
            needs_saving = True
        # Remove obsolete db_files and last_run_timestamps if they exist
        if "db_files" in config:
            del config["db_files"]
            needs_saving = True
        if "last_run_timestamps" in config:
             del config["last_run_timestamps"]
             needs_saving = True

        # Save back if any defaults were merged or obsolete keys removed
        if needs_saving:
            log.warning("Config updated with defaults or obsolete keys removed. Saving.")
            save_config(config)

        return config
    except json.JSONDecodeError as e:
        log.error(f"Error decoding JSON from config file {config_path}: {e}. Returning default config.", exc_info=True)
        return DEFAULT_CONFIG.copy()
    except Exception as e:
        log.error(f"Failed to load configuration from {config_path}: {e}. Returning default config.", exc_info=True)
        return DEFAULT_CONFIG.copy()

def save_config(data):
    """Saves configuration data (schedule, logging, counter) to JSON file."""
    config_path = _get_config_path()

    # Ensure structure before saving
    data.setdefault("schedule", DEFAULT_CONFIG["schedule"])
    data.setdefault("logging", DEFAULT_CONFIG["logging"])
    data.setdefault("run_counter", DEFAULT_CONFIG["run_counter"])

    # Remove obsolete keys just in case they were added back externally
    data.pop("db_files", None)
    data.pop("last_run_timestamps", None)

    try:
        with open(config_path, 'w') as f:
            json.dump(data, f, indent=4, sort_keys=True)
        log.info(f"Configuration saved successfully to {config_path}")
    except IOError as e:
        log.error(f"Could not write configuration file to {config_path}: {e}", exc_info=True)
    except Exception as e:
         log.error(f"An unexpected error occurred while saving config: {e}", exc_info=True)

# --- Helper Functions (Simplified) ---

def increment_run_counter():
    """Loads config, increments counter, and saves."""
    config = load_config()
    current_count = config.get("run_counter", 0)
    if not isinstance(current_count, int): current_count = 0
    config["run_counter"] = current_count + 1
    save_config(config)
    log.info(f"Run counter incremented to {config['run_counter']}")
    return config["run_counter"]

def reset_run_counter():
    """Loads config, resets counter to 0, and saves."""
    config = load_config()
    config["run_counter"] = 0
    save_config(config)
    log.info("Run counter reset to 0.")

def get_schedule():
    """Gets the schedule configuration from config.json."""
    config = load_config()
    return config.get("schedule", DEFAULT_CONFIG["schedule"])

def is_logging_enabled():
    """Checks if logging is enabled in config."""
    config = load_config()
    return config.get("logging", True)

def get_run_counter():
    """Gets the current run counter from config."""
    config = load_config()
    return config.get("run_counter", 0)

# Note: get_db_filenames() and get_last_run_timestamps() are removed as they were SQLite specific.

# === End of GconfigEM.py ===
