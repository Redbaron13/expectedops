# GloggerEM.py
"""
Configures logging for the G-EntryPoint-Manager (GEM) application.

This module provides the `setup_logging` function to initialize
application-wide logging, including console and optional file logging.
"""
import logging
import os
import GconfigEM # Used to load configuration, e.g., to check if file logging is enabled.

# Define the directory where log files will be stored.
LOG_DIR = "logs"
# Define the full path to the application log file.
LOG_FILE = os.path.join(LOG_DIR, "application.log")

def setup_logging():
    """
    Sets up application-wide logging based on settings from `GconfigEM`.

    Purpose:
        Configures the Python root logger with appropriate handlers (console and file)
        and formatters. This function should be called once at application startup.

    Interaction with GconfigEM:
        - It calls `GconfigEM.load_config()` to fetch the application configuration.
        - It checks the "logging" boolean flag in the loaded configuration. If True,
          file logging is enabled. If False, or if the key is missing (defaults to True),
          only console logging will be active (in addition to the file logger if enabled).
        - If `GconfigEM.load_config()` fails (e.g., `config.json` is corrupted or
          unreadable), this function logs a warning and defaults to console-only
          logging at the WARNING level to ensure the failure is visible.

    Handlers:
        - Console Handler: Always configured. Outputs log messages to `sys.stderr`.
        - File Handler: Configured if the "logging" flag in `config.json` is True.
          Appends log messages to `logs/application.log`.

    Error Handling:
        - OSError during Log Directory Creation: If the `LOG_DIR` (e.g., "logs/")
          cannot be created (e.g., due to permission issues), an error message is
          printed to the console, and logging falls back to basic console-only
          configuration. File logging will be disabled.

    Behavior on GconfigEM.load_config() failure:
        If loading the configuration fails, a warning is printed, and logging is
        configured for console-only output with a log level of WARNING. This ensures
        that errors related to configuration loading are visible.
    """
    log_level = logging.INFO  # Default logging level for the application.
    handlers = []

    # --- Step 1: Ensure the log directory exists ---
    # The LOG_DIR is typically a subdirectory like "logs/" in the project root.
    if not os.path.exists(LOG_DIR):
        try:
            # Attempt to create the log directory if it doesn't exist.
            os.makedirs(LOG_DIR)
            print(f"Log directory '{LOG_DIR}' created.")
        except OSError as e:
            # This block executes if os.makedirs() fails (e.g., due to file permissions).
            print(f"Critical: Error creating log directory '{LOG_DIR}': {e}. "
                  "File logging will be disabled.")
            # Fallback to basic console logging if directory creation fails.
            # This ensures that some logging is still available, even if minimal.
            logging.basicConfig(level=logging.INFO,
                                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            # Log the error using the now-configured basic logger.
            logging.error(f"Could not create log directory '{LOG_DIR}'. File logging disabled.")
            return # Exit the function as file logging cannot be set up.

    # --- Step 2: Configure Console Handler (always active) ---
    console_handler = logging.StreamHandler() # Defaults to sys.stderr
    # Define a standard format for console log messages.
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    handlers.append(console_handler)

    # --- Step 3: Configure File Handler (based on configuration) ---
    try:
        # Load application configuration to check if file logging should be enabled.
        config = GconfigEM.load_config()
        # Get the 'logging' flag from config. Defaults to True if key is missing or load_config fails implicitly.
        # GconfigEM.load_config() itself returns a default config with 'logging': True if file is missing/corrupt.
        logging_enabled_in_config = config.get("logging", True)

        if logging_enabled_in_config:
            # If logging is enabled in the config, set up a file handler.
            # Mode 'a' appends to the log file if it exists, otherwise creates it.
            file_handler = logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8')
            # Define a more detailed format for file log messages.
            file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(funcName)s - %(lineno)d - %(message)s')
            file_handler.setFormatter(file_formatter)
            handlers.append(file_handler)
            # User feedback that file logging is active.
            print(f"File logging enabled. Logs will be written to: {os.path.abspath(LOG_FILE)}")
        else:
            # User feedback that file logging is intentionally disabled.
            print("File logging disabled via configuration (`logging` flag is false).")

    except Exception as e:
        # This block catches errors during GconfigEM.load_config() or other unexpected issues.
        print(f"Warning: Could not load logging configuration from GconfigEM: {e}. "
              "Defaulting to console-only logging at WARNING level.")
        log_level = logging.WARNING # Elevate log level to ensure this warning is visible.
        # Ensure only console handler is present if config loading failed.
        # This removes the file_handler if it was tentatively added before an error in config.get().
        handlers = [h for h in handlers if isinstance(h, logging.StreamHandler)]


    # --- Step 4: Configure the root logger with the determined handlers and level ---
    # This applies the configuration to the entire application's logging system.
    logging.basicConfig(level=log_level, handlers=handlers, force=True) # force=True to reconfigure if already configured by basicConfig

    # Log an initial message to confirm logging setup is complete.
    # This will go to console and file (if enabled).
    logging.info("Logging setup complete. Application starting...")
    if not any(isinstance(h, logging.FileHandler) for h in handlers):
        logging.warning("File logging is not active for this session.")


# Explicit call to setup_logging() is generally preferred.
# It should be done once from the main entry point of the application (e.g., in GmainEM.py)
# after any necessary paths or initial configurations are set up, but before other modules
# that might perform logging are imported or used extensively.
#
# Example usage in GmainEM.py:
#
# import GloggerEM
# GloggerEM.setup_logging() # Call early in the main script.
#
# # ... rest of your application ...
#
# Do not call setup_logging() here directly on module import,
# as it might execute before the application is fully ready or might be called multiple times
# if the module is imported in different places without a proper singleton pattern for setup.
# setup_logging() # This line is intentionally commented out.

# === End of GloggerEM.py ===