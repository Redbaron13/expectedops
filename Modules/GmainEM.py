# GmainEM.py
"""
Main entry point for ExpectedOps CLI commands.
"""
import os
import sys
import logging

# Ensure Modules directory is in path
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