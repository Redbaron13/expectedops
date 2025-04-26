# GloggerEM.py
"""
Configures logging for the application.
"""
import logging
import os
import GconfigEM # Import config to check if logging is enabled

LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "application.log")

def setup_logging():
    """
    Sets up logging based on configuration.
    Logs to both console and a file if enabled in config.
    """
    # Ensure log directory exists
    if not os.path.exists(LOG_DIR):
        try:
            os.makedirs(LOG_DIR)
        except OSError as e:
            print(f"Error creating log directory '{LOG_DIR}': {e}")
            # Fallback to basic console logging if dir creation fails
            logging.basicConfig(level=logging.INFO,
                                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            logging.error(f"Could not create log directory '{LOG_DIR}'. File logging disabled.")
            return

    # Determine log level and handlers based on config
    log_level = logging.INFO # Default level
    handlers = []

    # Basic console handler (always add, level might be adjusted later)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    handlers.append(console_handler)

    # File handler (add if enabled in config)
    try:
        config = GconfigEM.load_config()
        logging_enabled = config.get("logging", True) # Default to True if key is missing
        log_to_file = logging_enabled # Use the config value directly

        if log_to_file:
            file_handler = logging.FileHandler(LOG_FILE, mode='a') # Append mode
            file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
            handlers.append(file_handler)
            print(f"File logging enabled. Logs will be written to: {LOG_FILE}")
        else:
            print("File logging disabled via configuration.")

    except Exception as e:
        # If config fails, default to console-only logging but report the error
        print(f"Warning: Could not load logging configuration: {e}. Defaulting to console logging.")
        log_level = logging.WARNING # Increase level to ensure this warning shows
        # Remove file handler if it was added before the exception
        handlers = [h for h in handlers if not isinstance(h, logging.FileHandler)]


    # Configure the root logger
    logging.basicConfig(level=log_level, handlers=handlers)

    # Test message
    logging.info("Logging setup complete.")

# Call setup_logging() when this module is imported?
# It's often better to call it explicitly once from the main entry point (GmainEM.py)
# setup_logging()

# === End of GloggerEM.py ===