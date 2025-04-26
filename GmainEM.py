# GmainEM.py
"""
Main entry point for the application. Sets up logging and calls the CLI handler.
"""
import GcliEM
import GloggerEM # Import the logger setup module
import sys

def main():
    # Set up logging as the first step
    GloggerEM.setup_logging()

    # Now proceed with the command line interface
    try:
        GcliEM.parse_arguments()
    except Exception as e:
        # Log any unhandled exceptions from the CLI/main logic
        import logging
        logging.critical(f"An unhandled error occurred: {e}", exc_info=True)
        print(f"Error: An unexpected error occurred. Check the log file '{GloggerEM.LOG_FILE}' for details.", file=sys.stderr)
        sys.exit(1) # Exit with an error code

if __name__ == "__main__":
    main()
# === End of GmainEM.py ===