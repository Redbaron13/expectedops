# GmainEM.py
"""
Main entry point for ExpectedOps CLI commands.
"""
import os
import sys
import logging

# Add Modules directory to path if needed
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

import GcliEM

if __name__ == "__main__":
    GcliEM.main()