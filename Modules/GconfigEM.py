# GconfigEM.py
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
DEFAULT_DB_NAMES = {
    "primary": "GPrimaryOpinionsEM.db",
    "backup": "GBackupOpinionsEM.db",
    "all_runs": "GAllRunsOpinionsEM.db",
    "test": "GTestOpinionsEM.db"
}

# Initialize default timestamps to None
DEFAULT_TIMESTAMPS = {key: None for key in DEFAULT_DB_NAMES.keys()}

DEFAULT_CONFIG = {
    "schedule": {
        "primary": "10:10",
        "backup": "09:30"
    },
    "db_files": DEFAULT_DB_NAMES,
    "logging": True,
    "run_counter": 0, # Added run counter
    "last_run_timestamps": DEFAULT_TIMESTAMPS # Added timestamps per DB type
}

def _get_config_path():
    """Gets the absolute path to the config file relative to this script."""
    # Assumes config.json is in the same directory as the script using this module,
    # or potentially the parent directory if modules are structured differently.
    # For simplicity, let's assume it's where GmainEM.py runs from.
    # A more robust approach might search parent directories or use an env var.
    # Let's assume it's in the project root relative to GmainEM.py
    # This might need adjustment based on your project structure.
    # If GmainEM.py is in the root:
    # return os.path.abspath(CONFIG_FILE)
    # If GmainEM.py is one level above Modules:
    script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) # Go up one level from Modules
    return os.path.join(script_dir, CONFIG_FILE)


def _validate_db_filenames(db_files_dict):
    """Validates that filenames in the dictionary match the required pattern."""
    validated_dict = {}
    valid = True
    # Ensure all default keys are present, even if invalid in input dict
    for key, default_filename in DEFAULT_DB_NAMES.items():
        filename = db_files_dict.get(key, default_filename) # Get provided or default
        if isinstance(filename, str) and DB_FILENAME_PATTERN.match(filename):
            validated_dict[key] = filename
        else:
            log.warning(f"Invalid DB filename format for key '{key}': '{filename}'. Using default '{default_filename}'.")
            validated_dict[key] = default_filename
            valid = False
    return validated_dict, valid


def load_config():
    """Loads configuration from JSON file, creating/merging defaults if needed and validating names."""
    config_path = _get_config_path()
    if not os.path.exists(config_path):
        log.warning(f"Configuration file not found at {config_path}. Creating default config.")
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG.copy() # Return a copy

    try:
        with open(config_path, 'r') as f:
            config = json.load(f)

            # Ensure essential keys exist, merging with defaults if necessary
            config.setdefault("schedule", DEFAULT_CONFIG["schedule"])
            if not isinstance(config["schedule"], dict): config["schedule"] = DEFAULT_CONFIG["schedule"]

            config.setdefault("db_files", DEFAULT_CONFIG["db_files"].copy())
            if not isinstance(config["db_files"], dict): config["db_files"] = DEFAULT_CONFIG["db_files"].copy()

            config.setdefault("logging", DEFAULT_CONFIG["logging"])
            if not isinstance(config["logging"], bool): config["logging"] = DEFAULT_CONFIG["logging"]

            config.setdefault("run_counter", DEFAULT_CONFIG["run_counter"])
            if not isinstance(config["run_counter"], int): config["run_counter"] = DEFAULT_CONFIG["run_counter"]

            config.setdefault("last_run_timestamps", DEFAULT_CONFIG["last_run_timestamps"].copy())
            if not isinstance(config["last_run_timestamps"], dict): config["last_run_timestamps"] = DEFAULT_CONFIG["last_run_timestamps"].copy()


            # *** Validate DB Filenames ***
            # Pass only the db_files part for validation
            validated_db_files, all_names_were_valid = _validate_db_filenames(config["db_files"])
            config["db_files"] = validated_db_files # Update config with validated names

            # Ensure all expected timestamp keys exist
            current_timestamps = config["last_run_timestamps"]
            updated_timestamps = False
            for key in DEFAULT_DB_NAMES.keys():
                if key not in current_timestamps:
                    current_timestamps[key] = None
                    updated_timestamps = True
            config["last_run_timestamps"] = current_timestamps


            # Save back if any defaults were merged, names corrected, or timestamps added
            if not all_names_were_valid or updated_timestamps or "run_counter" not in config or "last_run_timestamps" not in config:
                log.warning("Config updated with defaults, validated filenames, or new keys. Saving.")
                save_config(config) # Save back the corrected/validated config

            return config
    except json.JSONDecodeError as e:
        log.error(f"Error decoding JSON from config file {config_path}: {e}. Returning default config.", exc_info=True)
        return DEFAULT_CONFIG.copy() # Return a copy to avoid modification
    except Exception as e:
        log.error(f"Failed to load configuration from {config_path}: {e}. Returning default config.", exc_info=True)
        return DEFAULT_CONFIG.copy() # Return a copy


def save_config(data):
    """Saves configuration data to JSON file after validating DB filenames."""
    config_path = _get_config_path()

    # Ensure structure before saving
    data.setdefault("schedule", DEFAULT_CONFIG["schedule"])
    data.setdefault("db_files", DEFAULT_CONFIG["db_files"].copy())
    data.setdefault("logging", DEFAULT_CONFIG["logging"])
    data.setdefault("run_counter", DEFAULT_CONFIG["run_counter"])
    data.setdefault("last_run_timestamps", DEFAULT_CONFIG["last_run_timestamps"].copy())


    # Validate DB filenames before saving
    if isinstance(data["db_files"], dict):
        validated_db_files, _ = _validate_db_filenames(data["db_files"])
        data["db_files"] = validated_db_files
    else:
        # Ensure db_files key exists with defaults if missing/invalid type
        data["db_files"] = DEFAULT_DB_NAMES.copy()
        log.warning("Missing or invalid 'db_files' structure during save. Resetting to defaults.")

    # Ensure all expected timestamp keys exist
    current_timestamps = data["last_run_timestamps"]
    if not isinstance(current_timestamps, dict): current_timestamps = {}
    for key in DEFAULT_DB_NAMES.keys():
        current_timestamps.setdefault(key, None)
    data["last_run_timestamps"] = current_timestamps

    try:
        with open(config_path, 'w') as f:
            json.dump(data, f, indent=4, sort_keys=True) # Sort keys for consistency
        log.info(f"Configuration saved successfully to {config_path}")
    except IOError as e:
        log.error(f"Could not write configuration file to {config_path}: {e}", exc_info=True)
    except Exception as e:
         log.error(f"An unexpected error occurred while saving config: {e}", exc_info=True)

# --- Helper Functions ---

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


def get_schedule_times():
    """Gets schedule times from config."""
    config = load_config()
    return config.get("schedule", DEFAULT_CONFIG["schedule"])

def get_db_filenames():
    """Gets the dictionary of validated database filenames from config."""
    config = load_config()
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