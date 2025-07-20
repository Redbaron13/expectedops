# GconfigEM.py
# V2: Adapted for Supabase - Loads credentials from environment variables.
"""
Handles loading application configuration and Supabase credentials for the G-EntryPoint-Manager (GEM).

This module is responsible for:
1. Loading Supabase URL and Service Key from environment variables (via a .env file
   during local development or system environment variables in production).
2. Managing a `config.json` file for non-sensitive settings like scheduling,
   logging toggle, and a run counter.
3. Providing default configurations if `config.json` is missing or corrupted.
4. Ensuring the configuration structure is up-to-date by merging loaded
   configurations with defaults and removing obsolete keys.
"""
import json
import os
import logging
# datetime is not used directly in this version, but good to keep if future schedule parsing needs it.
# import datetime
from dotenv import load_dotenv # For loading .env files

log = logging.getLogger(__name__)

CONFIG_FILE = "config.json" # Name of the configuration file for non-sensitive data.

# --- Environment Variable Loading ---
def load_env():
    """
    Loads environment variables from a .env file if it exists in the project root.

    The project root is determined by navigating up one level from the directory
    containing this script (GconfigEM.py, assumed to be in a 'Modules' subdirectory).
    This allows for easy local development by storing credentials in a .env file
    without hardcoding them. In production, environment variables should be set directly.
    """
    # Determine the project root based on this file's location.
    # Assumes GconfigEM.py is in a 'Modules' subdirectory of the project root.
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_script_dir)
    dotenv_path = os.path.join(project_root, '.env')

    # Load .env file if it exists
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path=dotenv_path)
        # log.debug(f"Loaded environment variables from: {dotenv_path}") # Optional: for debugging .env loading
    # else:
        # log.debug(".env file not found at {dotenv_path}, relying on system environment variables.") # Optional: for debugging

def get_supabase_url():
    """
    Retrieves the Supabase URL from environment variables.

    Ensures .env file is loaded (if present) before attempting to access the variable.

    Returns:
        str or None: The Supabase URL if found, otherwise None.
                     Logs a warning if the variable is not set.
    """
    load_env() # Ensure .env is loaded, especially for local development
    url = os.environ.get("SUPABASE_URL")
    if not url:
        # Log a warning if the Supabase URL is not found in environment variables
        log.warning("SUPABASE_URL environment variable not set.")
    return url

def get_supabase_key():
    """
    Retrieves the Supabase Service Role Key from environment variables.

    Ensures .env file is loaded (if present) before attempting to access the variable.

    Returns:
        str or None: The Supabase Service Role Key if found, otherwise None.
                     Logs a warning if the variable is not set.
    """
    load_env() # Ensure .env is loaded, especially for local development
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not key:
        # Log a warning if the Supabase key is not found in environment variables
        log.warning("SUPABASE_SERVICE_KEY environment variable not set.")
    return key

# --- Configuration File Handling (for non-sensitive settings) ---

# Default schedule for various tasks.
# 'type' can be used by the scheduler to identify different kinds of tasks.
# 'days' uses a simple "Mon-Fri" or "Tue,Thu" format (example, actual parsing might vary).
DEFAULT_SCHEDULE = [
    {"time": "11:00", "type": "primary-1", "days": "Mon-Fri"},
    {"time": "17:30", "type": "primary-2", "days": "Mon-Fri"},
    {"time": "08:00", "type": "backup", "days": "Tue-Sat"},
    # Note: A weekly check was previously hardcoded in GschedulerEM;
    # this structure allows defining various schedules.
]

# Default configuration dictionary for `config.json`.
# This ensures that the application has a fallback and a template for new configs.
DEFAULT_CONFIG = {
    "schedule": DEFAULT_SCHEDULE,
    "logging": True,  # General toggle for application logging verbosity
    "run_counter": 0, # Counter for application runs, can be used for various purposes
    # Obsolete keys like "last_run_timestamps" and "db_files" (SQLite specific)
    # are no longer part of the default and are actively removed if found.
}

def _get_config_path():
    """
    Constructs the absolute path to the `config.json` file.

    The path is determined relative to the project root, assuming this script
    (GconfigEM.py) is located within a 'Modules' subdirectory of the project root.

    Returns:
        str: The absolute path to the `config.json` file.
    """
    # Get the directory of the current script (GconfigEM.py)
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    # Navigate up one level to get the project root
    project_root = os.path.dirname(current_script_dir)
    # Join with the CONFIG_FILE name to get the full path
    return os.path.join(project_root, CONFIG_FILE)

def load_config():
    """
    Loads configuration from the `config.json` file.

    If the file doesn't exist, it creates one with default settings.
    If the file is found, it merges the loaded configuration with defaults to ensure
    all necessary keys are present and removes any obsolete keys.

    Returns:
        dict: A dictionary containing the application configuration.
              Returns a copy of DEFAULT_CONFIG in case of file not found or errors.

    Error Handling:
        - Logs a warning and creates a default config if `config.json` is not found.
        - Logs an error and returns default config if `config.json` is malformed (JSONDecodeError).
        - Logs an error and returns default config for other file I/O exceptions.
    """
    config_path = _get_config_path()

    # If config file doesn't exist, create it with defaults
    if not os.path.exists(config_path):
        log.warning(f"Configuration file not found at {config_path}. Creating default config.")
        save_config(DEFAULT_CONFIG) # Save the default configuration first
        return DEFAULT_CONFIG.copy() # Return a copy of the defaults

    try:
        # Attempt to read and parse the JSON configuration file
        with open(config_path, 'r') as f:
            config = json.load(f)

        # --- Merge loaded config with defaults to ensure all keys exist and remove obsolete ones ---
        needs_saving = False # Flag to track if the config file needs to be re-saved

        # Ensure 'schedule' key exists and is a list, otherwise use default
        if "schedule" not in config or not isinstance(config["schedule"], list):
            config["schedule"] = DEFAULT_CONFIG["schedule"]
            needs_saving = True
            log.info("Merged default schedule into configuration.")

        # Ensure 'logging' key exists and is a boolean, otherwise use default
        if "logging" not in config or not isinstance(config["logging"], bool):
            config["logging"] = DEFAULT_CONFIG["logging"]
            needs_saving = True
            log.info("Merged default logging setting into configuration.")

        # Ensure 'run_counter' key exists and is an integer, otherwise use default
        if "run_counter" not in config or not isinstance(config["run_counter"], int):
            config["run_counter"] = DEFAULT_CONFIG["run_counter"]
            needs_saving = True
            log.info("Merged default run_counter into configuration.")

        # Remove obsolete 'db_files' key (related to previous SQLite implementation)
        if "db_files" in config:
            del config["db_files"]
            needs_saving = True
            log.info("Removed obsolete 'db_files' key from configuration.")

        # Remove obsolete 'last_run_timestamps' key (related to previous SQLite implementation)
        if "last_run_timestamps" in config:
             del config["last_run_timestamps"]
             needs_saving = True
             log.info("Removed obsolete 'last_run_timestamps' key from configuration.")

        # If any changes were made (defaults merged or obsolete keys removed), save the config file
        if needs_saving:
            log.warning("Configuration was updated with defaults or had obsolete keys removed. Saving changes.")
            save_config(config)

        return config
    except json.JSONDecodeError as e:
        # Handle errors if the JSON file is malformed
        log.error(f"Error decoding JSON from config file {config_path}: {e}. "
                  "The file might be corrupted. Returning default configuration.", exc_info=True)
        return DEFAULT_CONFIG.copy() # Return defaults if JSON is invalid
    except Exception as e:
        # Handle other potential errors during file loading (e.g., permission issues)
        log.error(f"Failed to load configuration from {config_path} due to an unexpected error: {e}. "
                  "Returning default configuration.", exc_info=True)
        return DEFAULT_CONFIG.copy() # Return defaults for any other exception

def save_config(data):
    """
    Saves the provided configuration data to the `config.json` file.

    Ensures that essential configuration keys ('schedule', 'logging', 'run_counter')
    have default values if not present in the `data`. Also explicitly removes
    obsolete keys ('db_files', 'last_run_timestamps') before saving to maintain a clean config.

    Args:
        data (dict): The configuration dictionary to save.

    Raises:
        Logs an error if file I/O fails (e.g., IOError) or any other unexpected
        exception occurs during the save process.
    """
    config_path = _get_config_path()

    # Ensure essential structure before saving by adding defaults if keys are missing.
    # This prevents saving a partially incomplete config if `data` is missing some keys.
    data.setdefault("schedule", DEFAULT_CONFIG["schedule"])
    data.setdefault("logging", DEFAULT_CONFIG["logging"])
    data.setdefault("run_counter", DEFAULT_CONFIG["run_counter"])

    # Remove obsolete keys just in case they were somehow re-introduced externally or by older code.
    # Using .pop with a default value (None) avoids KeyError if the key is already gone.
    data.pop("db_files", None)
    data.pop("last_run_timestamps", None)

    try:
        # Write the configuration dictionary to the file in a pretty-printed JSON format
        with open(config_path, 'w') as f:
            json.dump(data, f, indent=4, sort_keys=True) # sort_keys for consistent ordering
        log.info(f"Configuration saved successfully to {config_path}")
    except IOError as e:
        # Handle file writing errors (e.g., permissions, disk full)
        log.error(f"Could not write configuration file to {config_path}: {e}", exc_info=True)
    except Exception as e:
        # Handle any other unexpected errors during saving
         log.error(f"An unexpected error occurred while saving config to {config_path}: {e}", exc_info=True)

# --- Helper Functions (Simplified for accessing config values) ---

def increment_run_counter():
    """
    Loads the current configuration, increments the 'run_counter', and saves the config.

    If 'run_counter' is missing or not an integer, it's initialized to 0 before incrementing.

    Returns:
        int: The new value of the run counter after incrementing.
    """
    config = load_config()
    current_count = config.get("run_counter", 0)
    # Ensure current_count is an integer before incrementing
    if not isinstance(current_count, int):
        log.warning(f"Run counter was not an integer ({current_count}). Resetting to 0 before incrementing.")
        current_count = 0
    config["run_counter"] = current_count + 1
    save_config(config)
    log.info(f"Run counter incremented to {config['run_counter']}")
    return config["run_counter"]

def reset_run_counter():
    """
    Loads the current configuration, resets the 'run_counter' to 0, and saves the config.
    """
    config = load_config()
    config["run_counter"] = 0
    save_config(config)
    log.info("Run counter reset to 0.")

def get_schedule():
    """
    Retrieves the 'schedule' list from the configuration.

    Returns:
        list: The schedule list from `config.json`, or the default schedule
              if not found or an error occurs during loading.
    """
    config = load_config()
    # Fallback to default schedule if not present in the loaded config
    return config.get("schedule", DEFAULT_CONFIG["schedule"])

def is_logging_enabled():
    """
    Checks if logging is enabled in the configuration.

    Returns:
        bool: True if logging is enabled, False otherwise. Defaults to True
              if the setting is not found or an error occurs.
    """
    config = load_config()
    # Fallback to default logging state (True) if not present
    return config.get("logging", DEFAULT_CONFIG["logging"]) # Defaulting to True

def get_run_counter():
    """
    Retrieves the current 'run_counter' value from the configuration.

    Returns:
        int: The current run counter value, or 0 if not found or an error occurs.
    """
    config = load_config()
    # Fallback to default run counter (0) if not present
    return config.get("run_counter", DEFAULT_CONFIG["run_counter"]) # Defaulting to 0

# Note: Functions like get_db_filenames() and get_last_run_timestamps()
# have been removed as they were specific to the previous SQLite-based implementation
# and are no longer relevant with Supabase.

# === End of GconfigEM.py ===
