# GsupremescraperEM.py
"""
Handles specialized scraping and parsing of New Jersey Supreme Court case information
from the NJ Courts website.

This module is primarily designed to:
1.  Fetch data from the Supreme Court appeals page, which often involves
    handling AJAX requests for paginated content.
2.  Parse HTML snippets (typically received within JSON responses from AJAX calls)
    to extract key details for Supreme Court cases.
3.  Identify and extract Supreme Court docket numbers (e.g., "A-XX-YY"),
    associated Appellate Division docket numbers (e.g., "A-XXXX-YY"),
    and the full case name/title.
4.  (Legacy) Attempt to find matching case details in a local SQLite database
    before resorting to web scraping.
5.  Cache results to minimize redundant scraping operations.

The core functionality is encapsulated within the `SupremeCourtScraper` class.
While this module focuses on docket numbers and high-level case names, the
extraction of very specific case name components (Plaintiff, Defendant, "In the Matter Of")
from the `case_name` string would typically be a post-processing step if required,
potentially using more complex regex or NLP techniques on the extracted `case_name`.
"""
import requests # For making HTTP requests.
import logging
import time # For adding delays between paginated requests.
import re   # For regular expression matching (e.g., for docket numbers).
import json # For parsing JSON responses from AJAX requests.
from bs4 import BeautifulSoup # For parsing HTML content.
from typing import Dict, Optional, Tuple # For type hinting.
import sqlite3 # Used by the legacy _search_database method.
import GdbEM   # Potentially for future integration with the main Supabase DB, though not fully used here yet.

log = logging.getLogger(__name__)

# --- Constants ---
# Base URL for the New Jersey Supreme Court appeals page.
SUPREME_BASE_URL = "https://www.njcourts.gov/courts/supreme/appeals"
PAGE_URL = SUPREME_BASE_URL  # Default URL for scraping operations.

# Standard User-Agent header to mimic a web browser.
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

class SupremeCourtScraper:
    """
    A class to scrape and parse New Jersey Supreme Court case information.

    Manages HTTP sessions, caches scraped data, and handles pagination
    of the Supreme Court appeals website which loads data via AJAX.
    It can also (as a legacy feature) query a local SQLite database.
    """
    def __init__(self):
        """
        Initializes the SupremeCourtScraper instance.

        Sets up an HTTP session with default headers and initializes
        an in-memory cache for scraped results and a set to track dockets
        already checked in the local database.
        """
        self.session = requests.Session() # Use a session object for persistent connections and headers.
        self.session.headers.update(HEADERS)
        self._cache: Dict[str, Optional[Dict]] = {}  # In-memory cache: {search_docket: details_dict}.
        self._db_checked: set[str] = set()  # Set of dockets already checked in the local SQLite DB.
        self.items_per_page = 20 # Default items per page, though actual might vary by site.
        self._view_dom_id: Optional[str] = None # Cached view DOM ID for AJAX requests.

    def _get_page_content(self, page: int = 1) -> Optional[Dict]:
        """
        Fetches a single page of Supreme Court case listings via an AJAX request.

        The first call (page=1) also fetches the main page to extract a necessary
        `view_dom_id` used in subsequent AJAX calls for pagination.

        Args:
            page (int, optional): The page number to fetch (1-based). Defaults to 1.
                                  The API itself uses 0-based indexing for 'page'.

        Returns:
            Optional[Dict]: A dictionary parsed from the JSON response of the AJAX call
                            if successful, otherwise None. The JSON typically contains
                            HTML snippets for cases.
        """
        try:
            # For the first page, we need to get the main page HTML to find the view_dom_id.
            if page == 1 and not self._view_dom_id:
                log.debug(f"Fetching initial page {SUPREME_BASE_URL} to get view_dom_id.")
                initial_response = self.session.get(SUPREME_BASE_URL, timeout=30)
                initial_response.raise_for_status() # Check for HTTP errors.
                soup = BeautifulSoup(initial_response.text, 'html.parser')
                # The view_dom_id is found in a div with class 'view-supreme-court-appeals'.
                view_div = soup.find('div', class_='view-supreme-court-appeals')
                if view_div and view_div.get('id'):
                    self._view_dom_id = view_div['id']
                    log.info(f"Successfully extracted view_dom_id: {self._view_dom_id}")
                else:
                    log.error("Could not find view_dom_id on the initial page load. AJAX requests may fail.")
                    return None

            if not self._view_dom_id:
                log.error("view_dom_id not available. Cannot make AJAX request.")
                return None

            # URL for the AJAX request (views/ajax).
            ajax_url = f"{SUPREME_BASE_URL}/views/ajax"
            # Data payload for the POST request, including view name, display ID, and page number.
            # The API uses 0-based page indexing, so subtract 1 from the requested page.
            ajax_payload = {
                'view_name': 'supreme_court_appeals',
                'view_display_id': 'supreme_court_appeals_block', # This is often fixed for a given view.
                'page': page - 1,
                'view_dom_id': self._view_dom_id # Crucial ID obtained from the page source.
            }
            log.debug(f"Making AJAX POST request to {ajax_url} for page {page} with payload: {ajax_payload}")
            ajax_response = self.session.post(ajax_url, data=ajax_payload, timeout=30)
            ajax_response.raise_for_status() # Check for HTTP errors.

            try:
                # The response is expected to be JSON.
                json_data = ajax_response.json()
                log.debug(f"Successfully received and parsed JSON response for page {page}.")
                return json_data # This JSON often contains HTML content for the cases.
            except json.JSONDecodeError as e_json:
                log.error(f"Failed to parse JSON response from AJAX call for page {page}: {e_json}. Response text: {ajax_response.text[:200]}")
                return None

        except requests.exceptions.RequestException as e_req:
            log.error(f"HTTP Error fetching Supreme Court page {page}: {e_req}", exc_info=True)
            return None
        except Exception as e_general: # Catch any other unexpected errors.
            log.error(f"Unexpected error fetching Supreme Court page {page}: {e_general}", exc_info=True)
            return None

    def _parse_case_details(self, case_html_snippet: str) -> Dict:
        """
        Extracts structured case details from an HTML snippet representing a single case.

        The HTML snippet is typically obtained from the JSON response of AJAX calls
        to the Supreme Court appeals page.

        Args:
            case_html_snippet (str): A string containing the HTML for one case entry.

        Returns:
            Dict: A dictionary containing extracted details:
                  'sc_docket' (str|None): Supreme Court docket number.
                  'app_docket' (str|None): Appellate Division docket number.
                  'county' (str|None): County (not reliably extracted from this snippet).
                  'state_agency' (str|None): State agency (not reliably extracted here).
                  'case_name' (str|None): The full case name/title.
                  'raw_html' (str): The original HTML snippet for reference/debugging.
                  Further parsing of `case_name` into Plaintiff/Defendant/etc. would be
                  a separate step if needed, as titles can be complex.
        """
        details = {
            'sc_docket': None, 'app_docket': None, 'county': None,
            'state_agency': None, 'case_name': None, 'raw_html': case_html_snippet
        }

        try:
            soup = BeautifulSoup(case_html_snippet, 'html.parser') # Parse the HTML snippet.

            # Extract case name/title. Usually in an <h2> with class 'case-title'.
            title_element = soup.find('h2', class_='case-title')
            if title_element:
                details['case_name'] = title_element.get_text(strip=True)
                # Note: Case name parsing (Plaintiff v. Defendant, "In the Matter Of", "et al.")
                # is complex. This scraper primarily extracts the full title.
                # Example of what could be done (needs robust regexes not included here):
                #   - Search for " v. " to split Plaintiff/Defendant.
                #   - Check for "In the Matter of" at the beginning.
                #   - Identify "et al." and similar suffixes.

            # Extract docket numbers. Usually in a <div> with class 'docket-number' or 'field-docket-number'.
            docket_element = soup.find('div', class_='docket-number') or \
                             soup.find('div', class_='field-content') # Fallback class if primary changes
            if docket_element:
                docket_text = docket_element.get_text(strip=True)
                # Regex for Supreme Court Docket (A-##-YY), ensuring it's not part of a larger number.
                sc_match = re.search(r'(A-\d{1,2}-\d{2})(?!\d)', docket_text) # Negative lookahead for no trailing digits
                # Regex for Appellate Docket (A-####-YY), ensuring it's not part of a larger number.
                app_match = re.search(r'(A-\d{4,}-\d{2})(?!\d)', docket_text)

                if sc_match: details['sc_docket'] = sc_match.group(1)
                if app_match: details['app_docket'] = app_match.group(1)

            log.debug(f"Parsed case details: SC Docket='{details['sc_docket']}', App Docket='{details['app_docket']}', Case Name='{details['case_name']}'")

        except Exception as e:
            log.error(f"Error parsing case details HTML snippet: {e}", exc_info=True)
            # Logged raw_html can be used for debugging the snippet that failed.

        return details

    def _search_database(self, case_caption: str, supreme_docket: str) -> Optional[Dict]:
        """
        (Legacy) Searches a local SQLite database for a matching case caption,
        primarily to find an associated Appellate Division docket number for a given
        Supreme Court case.

        Args:
            case_caption (str): The case caption to search for.
            supreme_docket (str): The Supreme Court docket number (used for logging
                                  and to populate the returned dict).

        Returns:
            Optional[Dict]: A dictionary with case details if a match is found in the
                            database, otherwise None. The dictionary includes
                            'sc_docket', 'app_docket', 'case_name', 'county', 'state_agency'.
        """
        if not case_caption or supreme_docket in self._db_checked:
            # Skip if no caption or if this SC docket was already checked to avoid redundant DB queries.
            return None

        self._db_checked.add(supreme_docket) # Mark this SC docket as checked in DB for this session.
        log.info(f"Searching local SQLite DB for case matching caption: '{case_caption[:50]}...' (for SC Docket: {supreme_docket})")

        # GdbEM.get_db_filenames() and GdbEM.get_db_connection() are from the original code.
        # This assumes GdbEM is configured for SQLite for this specific legacy function.
        # Modern GEM uses Supabase, so this part might be deprecated or need adaptation.
        db_files = GdbEM.get_db_filenames() # This would need to point to the relevant SQLite DB config.
        primary_db_path = db_files.get("primary") # Assuming "primary" is the key for the SQLite DB path.

        if not primary_db_path:
            log.warning("Primary SQLite database path not configured in GdbEM. Cannot perform local DB search.")
            return None

        conn = None # Initialize conn to None for finally block.
        try:
            conn = GdbEM.get_db_connection(primary_db_path) # Assumes this returns a sqlite3 connection.
            cursor = conn.cursor()

            # SQL query to find an Appellate Division case with an exact CaseName match.
            # Orders by ReleaseDate to get the most recent match if multiple exist.
            query = """
                SELECT AppDocketID, CaseName, LCCounty, StateAgency1
                FROM opinions
                WHERE CaseName = ?
                AND Venue = 'Appellate Division'
                ORDER BY ReleaseDate DESC
                LIMIT 1
            """
            cursor.execute(query, (case_caption,))
            row = cursor.fetchone() # Fetch the first matching row.

            if row:
                # If a match is found, construct the details dictionary.
                details = {
                    'sc_docket': supreme_docket, # Populate with the SC docket we are searching for.
                    'app_docket': row['AppDocketID'],
                    'case_name': row['CaseName'],
                    'county': row['LCCounty'] or 'Statewide', # Default to 'Statewide' if county is None.
                    'state_agency': row['StateAgency1']
                }
                log.info(f"Found matching case in local SQLite DB: SC Docket '{supreme_docket}' linked to App Docket '{details['app_docket']}'.")
                return details
            else:
                log.info(f"No exact match found in local SQLite DB for caption '{case_caption[:50]}...'.")

        except sqlite3.Error as e_sql: # Catch SQLite specific errors.
            log.error(f"SQLite database search error for caption '{case_caption[:50]}...': {e_sql}", exc_info=True)
        except Exception as e_gen: # Catch other potential errors.
            log.error(f"Unexpected error during local SQLite DB search for caption '{case_caption[:50]}...': {e_gen}", exc_info=True)
        finally:
            if conn:
                conn.close() # Ensure database connection is closed.
        return None

    def find_matching_case(self, search_docket_sc: str, case_caption: Optional[str] = None, max_pages: int = 10) -> Optional[Dict]:
        """
        Searches for NJ Supreme Court case details (especially linked Appellate Docket)
        for a given Supreme Court docket number.

        The search prioritizes:
        1. In-memory cache.
        2. (Legacy) Local SQLite database search if `case_caption` is provided.
        3. Live web scraping of the NJ Courts Supreme Court appeals page (with pagination).

        Args:
            search_docket_sc (str): The Supreme Court docket number to search for (e.g., "A-XX-YY").
            case_caption (Optional[str], optional): The case caption, used for the
                                                    legacy database search. Defaults to None.
            max_pages (int, optional): Maximum number of pages to scrape on the website
                                       if a web search is performed. Defaults to 10.

        Returns:
            Optional[Dict]: A dictionary containing case details if found (keys include
                            'sc_docket', 'app_docket', 'case_name'), otherwise None.
        """
        if not search_docket_sc:
            log.warning("find_matching_case called with no search_docket_sc.")
            return None

        search_docket_sc_upper = search_docket_sc.upper() # Normalize for cache key and comparison.

        # 1. Check in-memory cache first.
        if search_docket_sc_upper in self._cache:
            log.info(f"Found case {search_docket_sc_upper} in cache.")
            return self._cache[search_docket_sc_upper]

        # 2. (Legacy) Try database search if case_caption is available.
        if case_caption:
            db_results = self._search_database(case_caption, search_docket_sc_upper)
            if db_results:
                self._cache[search_docket_sc_upper] = db_results # Cache DB result.
                return db_results

        # 3. Fallback to web scraping.
        log.info(f"Case {search_docket_sc_upper} not in cache or DB (if caption provided). Proceeding with web scrape (max {max_pages} pages).")

        for page_num in range(1, max_pages + 1): # Iterate through pages.
            log.debug(f"Scraping page {page_num} for SC Docket {search_docket_sc_upper}...")
            # _get_page_content returns parsed JSON which may contain HTML snippets.
            page_json_data = self._get_page_content(page_num)

            if not page_json_data:
                log.warning(f"No content received for page {page_num} while searching for {search_docket_sc_upper}. Stopping scrape for this docket.")
                break # Stop if a page fetch fails or returns no data.

            # The actual case data is often in a list under a key like 'data' or part of a command's result.
            # This needs to be adapted based on the actual JSON structure from views/ajax.
            # Assuming the relevant HTML is in a list of commands, one of which is 'insert'.
            # The 'data' field of that command would contain the HTML.
            # This is a common pattern for Drupal AJAX views.
            case_html_content_list = []
            if isinstance(page_json_data, list):
                for command in page_json_data:
                    if command.get('command') == 'insert' and command.get('data'):
                        case_html_content_list.append(command['data'])
            elif isinstance(page_json_data, dict) and 'data' in page_json_data : # Less common for Drupal views/ajax but possible
                case_html_content_list.append(page_json_data['data'])


            if not case_html_content_list:
                log.info(f"No case HTML content found in JSON response for page {page_num}. Structure might have changed or page is empty.")
                # Check for pagination clues if list is empty but it's not an error in fetching.
                # This part assumes the JSON itself tells if there's a next page, which is not always the case.
                # Often, one checks if fewer items than expected were returned, or if a 'next' link is absent.
                # For this AJAX, if the 'insert' command's data is empty, it might be the end.
                # If the JSON structure is just a list of items directly, this logic would be simpler.
                # The original code used BeautifulSoup on `content` directly, which is not what `_get_page_content` returns.
                # Let's assume the 'insert' command's 'data' is the main HTML block to parse for cases.
                # If that block is empty, it might be end of results.
                
                # If the main content to parse is empty, it could mean no more results.
                # However, the old code checked for a next_button in the *parsed* content.
                # This needs to be reconciled. For now, if no case_html_content, assume end for this page.
                # A robust way is to see if the number of items returned is less than items_per_page.
                # Or if the JSON response itself indicates end of pages.
                log.info(f"No further case data in JSON response for page {page_num}. Assuming end of results for {search_docket_sc_upper}.")
                break


            # Wrap all content in a single root element to parse with BeautifulSoup easily
            # This is necessary because the 'data' might be a string of multiple sibling divs.
            combined_html_for_page = "<div>" + "".join(case_html_content_list) + "</div>"
            page_soup = BeautifulSoup(combined_html_for_page, 'html.parser')

            # Find all individual case entries within the page's combined HTML.
            # Class name 'supreme-court-case' is an assumption; inspect actual HTML from AJAX.
            # It could also be 'views-row' or similar generic Drupal view classes.
            case_elements_on_page = page_soup.find_all('div', class_='views-row') # Adjust class as per actual site structure
            if not case_elements_on_page: # Fallback if 'views-row' is not found
                 case_elements_on_page = page_soup.find_all('div') # Generic div if specific class fails, needs refinement
                 if case_elements_on_page: log.warning("Used generic 'div' to find case elements, parsing might be imprecise.")


            log.debug(f"Found {len(case_elements_on_page)} case elements on page {page_num} to parse.")
            found_on_this_page = False
            for case_element_html in case_elements_on_page:
                # Pass the individual case element (as string or Tag) to _parse_case_details.
                # _parse_case_details expects a string snippet.
                details = self._parse_case_details(str(case_element_html))
                if details.get('sc_docket') and details['sc_docket'].upper() == search_docket_sc_upper:
                    log.info(f"Found matching case for SC Docket {search_docket_sc_upper} on page {page_num}.")
                    self._cache[search_docket_sc_upper] = details # Cache the found result.
                    return details # Return as soon as match is found.
                if details.get('sc_docket'): # Log other dockets found for debugging.
                    log.debug(f"Parsed SC Docket {details['sc_docket']} on page {page_num} (searching for {search_docket_sc_upper}).")


            # Crude check for more pages: if we got a full page of items (approx), there might be more.
            # A proper Drupal AJAX view might have a 'pager' in the JSON or a 'load more' button.
            # The original code looked for a 'load-more' button in the *parsed soup of the last page's content*,
            # which is not how it typically works if the JSON itself doesn't have that button's state.
            # Let's assume if fewer than expected items, it's the last page.
            # This is a simplification; real Drupal views might need more specific pager parsing from JSON.
            if len(case_elements_on_page) < self.items_per_page: # Heuristic for last page.
                log.info(f"Page {page_num} had {len(case_elements_on_page)} items (less than {self.items_per_page}). Assuming end of results for {search_docket_sc_upper}.")
                break # Break from pagination loop.

            log.debug(f"SC Docket {search_docket_sc_upper} not found on page {page_num}. Sleeping before trying next page.")
            time.sleep(1)  # Be respectful: delay between page fetches.

        log.warning(f"No matching case found on website for SC Docket {search_docket_sc_upper} after checking {max_pages} pages.")
        self._cache[search_docket_sc_upper] = None # Cache negative result to avoid re-scraping.
        return None

# Create a singleton instance of the scraper for use by other modules.
supreme_scraper = SupremeCourtScraper()
