# GmainEM.py
"""
GmainEM.py: Main entry point for the G-EntryPoint-Manager (GEM) application.

This script initializes the necessary environment, such as Python path adjustments
for module imports and logging configuration. It then invokes the main command-line
interface (CLI) handler from GcliEM.py. This script is intended to be the
primary executable for running GEM CLI commands.
"""
import os
import sys
import logging

# Ensure the 'Modules' directory, where this script resides, is added to the Python import path.
# This allows the script to correctly import sibling modules (e.g., GcliEM)
# regardless of the directory from which the script is executed.
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

import GcliEM

if __name__ == "__main__":
    GcliEM.main()