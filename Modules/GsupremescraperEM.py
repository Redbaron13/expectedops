"""
Handles scraping of Supreme Court case details from the NJ Courts website.
Matches Supreme Court docket numbers (A-##-YY) to find corresponding
Appellate Division numbers (A-####-YY) and additional case details.
"""
import requests
import logging
import time
import re
import json
from bs4 import BeautifulSoup
from typing import Dict, Optional, Tuple
import sqlite3
import GdbEM  # Add import for database connection

log = logging.getLogger(__name__)

# Constants (update these)
SUPREME_BASE_URL = "https://www.njcourts.gov/courts/supreme/appeals"  # Full direct URL
PAGE_URL = SUPREME_BASE_URL  # Base URL for pagination

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

class SupremeCourtScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._cache = {}  # Cache scraped results
        self._db_checked = set()  # Track which dockets we've checked in DB
        self.items_per_page = 20
        
    def _get_page_content(self, page: int = 1) -> Optional[Dict]:
        """Fetches a single page of Supreme Court cases and returns parsed JSON."""
        try:
            # First get the view ID from initial page load
            if page == 1:
                response = self.session.get(SUPREME_BASE_URL, timeout=30)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')
                view_dom_id = soup.find('div', class_='view-supreme-court-appeals')['id']
                log.debug(f"Found view DOM ID: {view_dom_id}")
            
            # Make AJAX request for data
            ajax_url = f"{SUPREME_BASE_URL}/views/ajax"
            data = {
                'view_name': 'supreme_court_appeals',
                'view_display_id': 'supreme_court_appeals_block',
                'page': page - 1,  # API uses 0-based indexing
                'view_dom_id': view_dom_id
            }
            
            response = self.session.post(ajax_url, data=data, timeout=30)
            response.raise_for_status()
            
            try:
                json_data = response.json()
                log.debug(f"Received JSON response for page {page}")
                return json_data
            except json.JSONDecodeError as e:
                log.error(f"Failed to parse JSON response: {e}")
                return None
                
        except Exception as e:
            log.error(f"Error fetching Supreme Court page {page}: {e}", exc_info=True)
            return None

    def _parse_case_details(self, case_element) -> Dict:
        """Extracts case details from JSON case data."""
        details = {
            'sc_docket': None,
            'app_docket': None,
            'county': None,
            'state_agency': None,
            'case_name': None,
            'raw_html': None
        }
        
        try:
            # Store raw HTML for analysis
            details['raw_html'] = str(case_element)
            
            # Parse HTML content from JSON
            soup = BeautifulSoup(case_element, 'html.parser')
            
            # Extract case name
            title_elem = soup.find('h2', class_='case-title')
            if title_elem:
                details['case_name'] = title_elem.text.strip()
            
            # Extract docket numbers with improved regex
            docket_elem = soup.find('div', class_='docket-number') or soup.find('div', class_='field-docket-number')
            if docket_elem:
                text = docket_elem.text
                sc_match = re.search(r'[^\d]?(A-\d{1,2}-\d{2})[^\d]?', text)
                app_match = re.search(r'[^\d]?(A-\d{4,}-\d{2})[^\d]?', text)
                if sc_match: details['sc_docket'] = sc_match.group(1)
                if app_match: details['app_docket'] = app_match.group(1)
                
            # Log what we found for debugging
            log.debug(f"Parsed details: SC={details['sc_docket']}, APP={details['app_docket']}, Name={details['case_name']}")
            
        except Exception as e:
            log.error(f"Error parsing case details: {e}", exc_info=True)
            
        return details

    def _search_database(self, case_caption: str, supreme_docket: str) -> Optional[Dict]:
        """Searches existing database entries for matching case caption."""
        if not case_caption or supreme_docket in self._db_checked:
            return None
            
        self._db_checked.add(supreme_docket)
        log.info(f"Searching database for case: {case_caption[:50]}...")
        
        db_files = GdbEM.get_db_filenames()
        primary_db = db_files.get("primary")
        if not primary_db:
            log.warning("Primary database not configured")
            return None
            
        try:
            conn = GdbEM.get_db_connection(primary_db)
            cursor = conn.cursor()
            
            # Search for exact caption match in Appellate cases
            cursor.execute("""
                SELECT AppDocketID, CaseName, LCCounty, StateAgency1
                FROM opinions 
                WHERE CaseName = ? 
                AND Venue = 'Appellate Division'
                ORDER BY ReleaseDate DESC
                LIMIT 1
            """, (case_caption,))
            
            row = cursor.fetchone()
            if row:
                details = {
                    'sc_docket': supreme_docket,
                    'app_docket': row['AppDocketID'],
                    'case_name': row['CaseName'],
                    'county': row['LCCounty'] or 'Statewide',
                    'state_agency': row['StateAgency1']
                }
                log.info(f"Found matching case in database: {details['app_docket']}")
                return details
                
        except Exception as e:
            log.error(f"Database search error: {e}", exc_info=True)
        finally:
            if conn:
                conn.close()
        return None

    def find_matching_case(self, search_docket: str, case_caption: str = None, max_pages: int = 10) -> Optional[Dict]:
        """
        Searches for case details first in database, then on Supreme Court site.
        Args:
            search_docket: Supreme Court docket number (A-##-YY)
            case_caption: Optional case caption for database search
            max_pages: Maximum pages to search on Supreme Court site
        """
        if not search_docket:
            return None
            
        # Check cache first
        cache_key = search_docket.upper()
        if cache_key in self._cache:
            return self._cache[cache_key]
            
        # Try database search first if we have a caption
        if case_caption:
            db_results = self._search_database(case_caption, search_docket)
            if db_results:
                self._cache[cache_key] = db_results
                return db_results
                
        # Fall back to web scraping if no database match
        search_docket = search_docket.upper()
        log.info(f"Searching Supreme Court site for: {search_docket}")
        
        for page in range(1, max_pages + 1):
            try:
                content = self._get_page_content(page)
                if not content:
                    continue
                    
                soup = BeautifulSoup(content, 'html.parser')
                cases = soup.find_all('div', class_='supreme-court-case')
                
                for case in cases:
                    details = self._parse_case_details(case)
                    if details['sc_docket'] and details['sc_docket'].upper() == search_docket:
                        log.info(f"Found matching case for {search_docket}")
                        self._cache[cache_key] = details
                        return details
                
                # Check if there are more pages
                next_button = soup.find('button', class_='load-more')
                if not next_button or 'disabled' in next_button.get('class', []):
                    break
                    
                time.sleep(3)  # Respect rate limiting
                
            except Exception as e:
                log.error(f"Error processing page {page}: {e}", exc_info=True)
                continue
        
        log.warning(f"No matching case found for {search_docket}")
        return None

# Create singleton instance
supreme_scraper = SupremeCourtScraper()
