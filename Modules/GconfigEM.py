# GconfigEM.py
# Added 'combo' database
"""
Handles loading and saving application configuration,
including database file names, schedule times, run counter, and timestamps.
Ensures DB filenames follow G[Name]EM.db format.
"""
import json
import os
import logging
import re # For filename validation
import datetime # For timestamps

log = logging.getLogger(__name__)

CONFIG_FILE = "config.json"
DB_FILENAME_PATTERN = re.compile(r"^G[a-zA-Z0-9]+EM\.db$") # Pattern G[Alphanumeric]EM.db

# Define standard database names following G...EM.db convention
# Added 'combo' database
DEFAULT_DB_NAMES = {
    "primary": "GPrimaryOpinionsEM.db",
    "backup": "GBackupOpinionsEM.db",
    "all_runs": "GAllRunsOpinionsEM.db",
    "test": "GTestOpinionsEM.db",
    "combo": "GComboEM.db" # New Combo Database
}

# Initialize default timestamps to None
DEFAULT_TIMESTAMPS = {key: None for key in DEFAULT_DB_NAMES}

DEFAULT_CONFIG = {
    "schedule": [
        {"time": "11:00", "type": "primary-1", "days": "Mon-Fri"},
        {"time": "17:30", "type": "primary-2", "days": "Mon-Fri"},
        {"time": "08:00", "type": "backup", "days": "Tue-Sat"},
    ],
    "db_files": DEFAULT_DB_NAMES,
    "logging": True,
    "run_counter": 0,
    "last_run_timestamps": DEFAULT_TIMESTAMPS
}

# --- Functions _get_config_path, _validate_db_filenames (mostly unchanged) ---
def _get_config_path():
    """Gets the absolute path to the config file relative to this script."""
    # Assuming config.json is in the project root where GmainEM.py is run.
    # Adjust if your structure is different.
    return os.path.abspath(CONFIG_FILE)

def _validate_db_filenames(db_files_dict):
    """Validates that filenames in the dictionary match the required pattern."""
    validated_dict = {}
    valid = True
    # Ensure all default keys are present
    for key, default_filename in DEFAULT_DB_NAMES.items():
        filename = db_files_dict.get(key, default_filename) # Get provided or default
        if isinstance(filename, str) and DB_FILENAME_PATTERN.match(filename):
            validated_dict[key] = filename
        else:
            log.warning(f"Invalid or missing DB filename format for key '{key}': '{filename}'. Using default '{default_filename}'.")
            validated_dict[key] = default_filename
            valid = False # Mark that correction occurred
    return validated_dict, valid

# --- Functions load_config, save_config (Updated to handle all default keys) ---
def load_config():
    """Loads configuration from JSON file, creating/merging defaults if needed and validating names."""
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
            config["schedule"] = DEFAULT_CONFIG["schedule"]  # Use the list of dicts
            needs_saving = True
        # DB Files (validate and ensure all keys)
        db_files_from_config = config.get("db_files", {})
        validated_db_files, names_were_corrected = _validate_db_filenames(db_files_from_config)
        if names_were_corrected or len(validated_db_files) != len(DEFAULT_DB_NAMES):
            config["db_files"] = validated_db_files # Update with validated/complete set
            needs_saving = True
        # Logging
        if "logging" not in config or not isinstance(config["logging"], bool):
            config["logging"] = DEFAULT_CONFIG["logging"]
            needs_saving = True
        # Run Counter
        if "run_counter" not in config or not isinstance(config["run_counter"], int):
            config["run_counter"] = DEFAULT_CONFIG["run_counter"]
            needs_saving = True
        # Timestamps (ensure all keys)
        timestamps_from_config = config.get("last_run_timestamps", {})
        if not isinstance(timestamps_from_config, dict): timestamps_from_config = {}
        updated_timestamps = False
        for key in DEFAULT_DB_NAMES.keys():
            if key not in timestamps_from_config:
                timestamps_from_config[key] = None
                updated_timestamps = True
        if updated_timestamps:
            config["last_run_timestamps"] = timestamps_from_config
            needs_saving = True

        # Save back if any defaults were merged, names corrected, or keys added
        if needs_saving:
            log.warning("Config updated with defaults, validated filenames, or new keys. Saving.")
            save_config(config)

        return config
    except json.JSONDecodeError as e:
        log.error(f"Error decoding JSON from config file {config_path}: {e}. Returning default config.", exc_info=True)
        return DEFAULT_CONFIG.copy()
    except Exception as e:
        log.error(f"Failed to load configuration from {config_path}: {e}. Returning default config.", exc_info=True)
        return DEFAULT_CONFIG.copy()


def save_config(data):
    """Saves configuration data to JSON file after validating DB filenames."""
    config_path = _get_config_path()

    # Ensure structure before saving, using defaults as base
    # Validate DB filenames before saving
    if "db_files" not in data or not isinstance(data["db_files"], dict):
         data["db_files"] = DEFAULT_DB_NAMES.copy()
         log.warning("Missing or invalid 'db_files' structure during save. Resetting to defaults.")
    else:
         # Validate potentially existing dict
         validated_db_files, _ = _validate_db_filenames(data["db_files"])
         data["db_files"] = validated_db_files # Save validated names

    # Ensure other top-level keys exist
    data.setdefault("schedule", DEFAULT_CONFIG["schedule"]) # Ensure schedule is a list
    data.setdefault("logging", DEFAULT_CONFIG["logging"])
    data.setdefault("run_counter", DEFAULT_CONFIG["run_counter"])

    # Ensure all expected timestamp keys exist
    current_timestamps = data.get("last_run_timestamps", {})
    if not isinstance(current_timestamps, dict): current_timestamps = {}
    for key in DEFAULT_DB_NAMES.keys():
        current_timestamps.setdefault(key, None) # Ensure key exists, default to None
    data["last_run_timestamps"] = current_timestamps

    try:
        with open(config_path, 'w') as f:
            json.dump(data, f, indent=4, sort_keys=True)
        log.info(f"Configuration saved successfully to {config_path}")
    except IOError as e:
        log.error(f"Could not write configuration file to {config_path}: {e}", exc_info=True)
    except Exception as e:
         log.error(f"An unexpected error occurred while saving config: {e}", exc_info=True)

# --- Helper Functions (increment_run_counter, reset_run_counter, update_last_run_timestamp, etc. - unchanged) ---
# ... [rest of the helper functions remain the same] ...

def increment_run_counter():
    """Loads config, increments counter, and saves."""
    config = load_config()
    current_count = config.get("run_counter", 0)
    if not isinstance(current_count, int): current_count = 0 # Handle potential corrupt data
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

def update_last_run_timestamp(db_type):
    """Loads config, updates timestamp for a specific db_type, and saves."""
    if db_type not in DEFAULT_DB_NAMES:
        log.warning(f"Attempted to update timestamp for unknown db_type '{db_type}'. Ignoring.")
        return
    config = load_config()
    now_iso = datetime.datetime.now().isoformat()
    # Ensure the timestamps dict exists
    if "last_run_timestamps" not in config or not isinstance(config["last_run_timestamps"], dict):
        config["last_run_timestamps"] = DEFAULT_TIMESTAMPS.copy()

    config["last_run_timestamps"][db_type] = now_iso
    save_config(config)
    log.info(f"Last run timestamp for '{db_type}' updated to {now_iso}")


def get_schedule(): """Gets the schedule configuration from config.json."""
    config=load_config(); return config.get("schedule", DEFAULT_CONFIG["schedule"]) # Returns the list of schedule entries






def get_db_filenames():
    """Gets the dictionary of validated database filenames from config."""
    config = load_config()
    # Ensure db_files exists and is validated before returning
    if "db_files" not in config or not isinstance(config["db_files"], dict):
        config = load_config() # Reload/create default if missing
    return config.get("db_files", DEFAULT_DB_NAMES.copy())

def is_logging_enabled():
    """Checks if logging is enabled in config."""
    config = load_config()
    return config.get("logging", True)

def get_run_counter():
    """Gets the current run counter from config."""
    config = load_config()
    return config.get("run_counter", 0)

def get_last_run_timestamps():
    """Gets the dictionary of last run timestamps from config."""
    config = load_config()
    # Ensure all keys exist, returning None if not set
    timestamps = config.get("last_run_timestamps", DEFAULT_TIMESTAMPS.copy())
    for key in DEFAULT_DB_NAMES.keys():
        timestamps.setdefault(key, None)
    return timestamps

# === End of GconfigEM.py ===