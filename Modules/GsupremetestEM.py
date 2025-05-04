"""
Test module for Supreme Court scraping functionality.
Provides CLI interface for testing specific docket numbers.
"""
import sqlite3
import logging
import os
import GsupremescraperEM
from datetime import datetime

log = logging.getLogger(__name__)

# Constants - Update to use absolute paths
MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(MODULE_DIR)
SUPREME_TEST_DB = os.path.join(PROJECT_DIR, "supremesearch.db")

DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS supreme_searches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    search_timestamp TEXT NOT NULL,
    supreme_docket TEXT NOT NULL,
    appellate_docket TEXT,
    case_caption TEXT,
    county TEXT,
    state_agency TEXT,
    search_source TEXT,
    search_success INTEGER DEFAULT 0,
    error_message TEXT,
    raw_html TEXT,
    page_number INTEGER,
    json_response TEXT
);
"""

def init_test_db():
    """Initialize the test database."""
    try:
        os.makedirs(os.path.dirname(SUPREME_TEST_DB), exist_ok=True)
        conn = sqlite3.connect(SUPREME_TEST_DB)
        cursor = conn.cursor()
        cursor.execute(DB_SCHEMA)
        conn.commit()
        log.info(f"Initialized test database: {SUPREME_TEST_DB}")
        return conn
    except Exception as e:
        log.error(f"Failed to initialize test database: {e}", exc_info=True)
        raise

def search_supreme_docket(docket_number: str, save_results=True):
    """
    Search for a specific Supreme Court docket number and log details.
    """
    log.info(f"=== Starting Supreme Court search for docket: {docket_number} ===")
    
    conn = None
    try:
        if save_results:
            conn = init_test_db()
            cursor = conn.cursor()
        
        # Attempt search
        results = GsupremescraperEM.supreme_scraper.find_matching_case(
            docket_number,
            max_pages=20  # Increase pages for thorough testing
        )
        
        timestamp = datetime.now().isoformat()
        success = bool(results and results.get('app_docket'))
        
        if results:
            log.info("Search Results:")
            log.info(f"Supreme Docket: {results.get('sc_docket')}")
            log.info(f"Appellate Docket: {results.get('app_docket')}")
            log.info(f"Case Caption: {results.get('case_name')}")
            log.info(f"County: {results.get('county')}")
            log.info(f"State Agency: {results.get('state_agency')}")
            
            if save_results:
                cursor.execute("""
                    INSERT INTO supreme_searches
                    (search_timestamp, supreme_docket, appellate_docket, case_caption,
                     county, state_agency, search_source, search_success, raw_html)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    timestamp, docket_number, results.get('app_docket'),
                    results.get('case_name'), results.get('county'),
                    results.get('state_agency'), 'web', 1,
                    results.get('raw_html')
                ))
                conn.commit()
                
        else:
            log.warning(f"No results found for docket: {docket_number}")
            if save_results:
                cursor.execute("""
                    INSERT INTO supreme_searches
                    (search_timestamp, supreme_docket, search_source, 
                     search_success, error_message)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    timestamp, docket_number, 'web', 0,
                    "No matching case found"
                ))
                conn.commit()
        
        return results
        
    except Exception as e:
        log.error(f"Error during supreme search: {e}", exc_info=True)
        if save_results and conn:
            try:
                cursor.execute("""
                    INSERT INTO supreme_searches
                    (search_timestamp, supreme_docket, search_source, 
                     search_success, error_message)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    datetime.now().isoformat(), docket_number,
                    'web', 0, str(e)
                ))
                conn.commit()
            except Exception as db_e:
                log.error(f"Failed to save error to database: {db_e}")
        raise
    finally:
        if conn:
            conn.close()
