# GconfigEM.py
"""
Handles loading and saving application configuration,
including database file names and schedule times.
Ensures DB filenames follow G[Name]EM.db format.
"""
import json
import os
import logging
import re # For filename validation

log = logging.getLogger(__name__)

CONFIG_FILE = "config.json"
DB_FILENAME_PATTERN = re.compile(r"^G[a-zA-Z0-9]+EM\.db$") # Pattern G[Alphanumeric]EM.db

# Define standard database names following G...EM.db convention
DEFAULT_DB_NAMES = {
    "primary": "GPrimaryOpinionsEM.db", # Updated name
    "backup": "GBackupOpinionsEM.db",   # Updated name
    "all_runs": "GAllRunsOpinionsEM.db", # Updated name
    "test": "GTestOpinionsEM.db"        # Updated name
}

DEFAULT_CONFIG = {
    "schedule": {
        "primary": "10:10",
        "backup": "09:30"
    },
    "db_files": DEFAULT_DB_NAMES,
    "logging": True
}

def _get_config_path():
    """Gets the absolute path to the config file."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, CONFIG_FILE)

def _validate_db_filenames(db_files_dict):
    """Validates that filenames in the dictionary match the required pattern."""
    validated_dict = {}
    valid = True
    for key, filename in db_files_dict.items():
        if isinstance(filename, str) and DB_FILENAME_PATTERN.match(filename):
            validated_dict[key] = filename
        else:
            log.warning(f"Invalid DB filename format for key '{key}': '{filename}'. Using default.")
            # Fallback to default for that specific key if invalid
            validated_dict[key] = DEFAULT_DB_NAMES.get(key, f"GInvalidFilename{key.capitalize()}EM.db")
            valid = False
    return validated_dict, valid


def load_config():
    """Loads configuration from JSON file, creating default if not found and validating names."""
    config_path = _get_config_path()
    if not os.path.exists(config_path):
        log.warning(f"Configuration file not found at {config_path}. Creating default config.")
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG

    try:
        with open(config_path, 'r') as f:
            config = json.load(f)

            # Ensure essential keys exist, merging with defaults if necessary
            if "schedule" not in config or not isinstance(config["schedule"], dict):
                config["schedule"] = DEFAULT_CONFIG["schedule"]
            if "db_files" not in config or not isinstance(config["db_files"], dict):
                config["db_files"] = DEFAULT_CONFIG["db_files"].copy()
            else:
                # Ensure all default DB types exist in the loaded db_files dict
                current_db_files = config["db_files"]
                for key, default_value in DEFAULT_DB_NAMES.items():
                    if key not in current_db_files:
                         current_db_files[key] = default_value
                config["db_files"] = current_db_files # Update config dict

            if "logging" not in config or not isinstance(config["logging"], bool):
                config["logging"] = DEFAULT_CONFIG["logging"]

            # *** Validate DB Filenames ***
            validated_db_files, all_names_were_valid = _validate_db_filenames(config["db_files"])
            config["db_files"] = validated_db_files
            if not all_names_were_valid:
                log.warning("One or more DB filenames in config were invalid. Defaults used. Saving corrected config.")
                save_config(config) # Save back the corrected/validated filenames

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

    # Validate DB filenames before saving
    if "db_files" in data and isinstance(data["db_files"], dict):
        validated_db_files, _ = _validate_db_filenames(data["db_files"])
        data["db_files"] = validated_db_files
    else:
        # Ensure db_files key exists with defaults if missing/invalid type
        data["db_files"] = DEFAULT_DB_NAMES.copy()
        log.warning("Missing or invalid 'db_files' structure during save. Resetting to defaults.")


    try:
        with open(config_path, 'w') as f:
            json.dump(data, f, indent=4)
        log.info(f"Configuration saved successfully to {config_path}")
    except IOError as e:
        log.error(f"Could not write configuration file to {config_path}: {e}", exc_info=True)
    except Exception as e:
         log.error(f"An unexpected error occurred while saving config: {e}", exc_info=True)


def get_schedule_times():
    """Gets schedule times from config."""
    config = load_config()
    return config.get("schedule", DEFAULT_CONFIG["schedule"])

def get_db_filenames():
    """Gets the dictionary of validated database filenames from config."""
    config = load_config()
    # load_config now handles validation and defaults
    return config.get("db_files", DEFAULT_DB_NAMES.copy())

def is_logging_enabled():
    """Checks if logging is enabled in config."""
    config = load_config()
    return config.get("logging", True)

# === End of GconfigEM.py ===