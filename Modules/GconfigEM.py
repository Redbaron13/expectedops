# Modules/GconfigEM.py
# V3: Added configuration for HTML archival.
"""
Handles loading application configuration and Supabase credentials.
Removes SQLite-specific database file configurations.
"""
import json
import os
import logging
# import datetime # Not directly used in this version of the file
from dotenv import load_dotenv

log = logging.getLogger(__name__)

CONFIG_FILE = "config.json"

# --- Environment Variable Loading ---
def load_env():
    """Loads .env file for local development if it exists."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    dotenv_path = os.path.join(project_root, '.env')
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path=dotenv_path)
        # log.debug(f"Loaded environment variables from: {dotenv_path}")
    # else:
        # log.debug(".env file not found, relying on system environment variables.")

def get_supabase_url():
    """Gets Supabase URL from environment variables."""
    load_env()
    url = os.environ.get("SUPABASE_URL")
    if not url:
        log.warning("SUPABASE_URL environment variable not set.")
    return url

def get_supabase_key():
    """Gets Supabase Service Role Key from environment variables."""
    load_env()
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not key:
        log.warning("SUPABASE_SERVICE_KEY environment variable not set.")
    return key

# --- Configuration File Handling (for non-sensitive settings) ---

DEFAULT_SCHEDULE = [
    {"time": "11:00", "type": "primary-1", "days": "Mon-Fri"},
    {"time": "17:30", "type": "primary-2", "days": "Mon-Fri"},
    {"time": "08:00", "type": "backup", "days": "Tue-Sat"},
]

DEFAULT_CONFIG = {
    "schedule": DEFAULT_SCHEDULE,
    "logging": True,
    "run_counter": 0,
    "archive_pages": [
        {"key": "expected_opinions", "url": "https://www.njcourts.gov/attorneys/opinions/expected"},
        {"key": "supreme_appeals", "url": "https://www.njcourts.gov/courts/supreme/appeals"}
    ]
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
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG.copy()

    try:
        with open(config_path, 'r') as f:
            config = json.load(f)

        needs_saving = False
        for key, default_value in DEFAULT_CONFIG.items():
            if key not in config or not isinstance(config[key], type(default_value)):
                config[key] = default_value
                needs_saving = True
        
        # Remove obsolete keys if they exist from older versions
        obsolete_keys = ["db_files", "last_run_timestamps"]
        for obs_key in obsolete_keys:
            if obs_key in config:
                del config[obs_key]
                needs_saving = True

        if needs_saving:
            log.info("Config updated with defaults or obsolete keys removed. Saving.")
            save_config(config)

        return config
    except json.JSONDecodeError as e:
        log.error(f"Error decoding JSON from config file {config_path}: {e}. Returning default config.", exc_info=True)
        return DEFAULT_CONFIG.copy()
    except Exception as e:
        log.error(f"Failed to load configuration from {config_path}: {e}. Returning default config.", exc_info=True)
        return DEFAULT_CONFIG.copy()

def save_config(data):
    """Saves configuration data to JSON file."""
    config_path = _get_config_path()

    # Ensure all default sections are present before saving
    for key, default_value in DEFAULT_CONFIG.items():
        data.setdefault(key, default_value)

    # Remove obsolete keys just in case they were added back externally
    obsolete_keys = ["db_files", "last_run_timestamps"]
    for obs_key in obsolete_keys:
        data.pop(obs_key, None)

    try:
        with open(config_path, 'w') as f:
            json.dump(data, f, indent=4, sort_keys=True)
        log.info(f"Configuration saved successfully to {config_path}")
    except IOError as e:
        log.error(f"Could not write configuration file to {config_path}: {e}", exc_info=True)
    except Exception as e:
         log.error(f"An unexpected error occurred while saving config: {e}", exc_info=True)

def get_archive_pages_config():
    """Gets the list of pages to archive from config."""
    config = load_config()
    return config.get("archive_pages", DEFAULT_CONFIG["archive_pages"])

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

# === End of GconfigEM.py ===