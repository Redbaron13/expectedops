# GscraperEM.py
# V6: Use zoneinfo for accurate timezone handling
"""
Handles the web scraping and parsing of court opinion data from the
New Jersey Courts website (njcourts.gov).

This module is responsible for:
1.  Fetching HTML content from the 'Expected Opinions' page.
2.  Parsing the HTML using `BeautifulSoup` to extract information about
    each listed opinion.
3.  Extracting key data points for each opinion, including:
    - Appellate Docket ID (AppDocketID) and any Linked Docket IDs.
    - Case Name (CaseName).
    - Release Date (ReleaseDate).
    - Opinion Type (DecisionTypeCode, DecisionTypeText) and Venue (e.g., Supreme, Appellate).
    - Lower Court information (LCdocketID, LCCounty, LowerCourtVenue, LowerCourtSubCaseType).
    - Involved State Agencies (StateAgency1, StateAgency2).
    - Case-specific notes (CaseNotes), and flags for consolidated or impounded records.
    - Opinion Status (opinionstatus), calculated based on release date and current time in Eastern Time.
4.  Utilizing regular expressions for complex pattern matching in text data.
5.  Employing `zoneinfo` (Python 3.9+) for accurate timezone conversions to Eastern Time,
    with a fallback mechanism for older Python versions.
6.  Interacting with `GsupremescraperEM` for specialized parsing of Supreme Court case details.

The primary function `fetch_and_parse_opinions()` orchestrates the fetching and parsing process.
Helper functions are used for specific tasks like text extraction, decision type mapping,
and detailed parsing of case titles and individual opinion entries from HTML article elements.
"""
import datetime
import requests # For making HTTP requests to fetch web page content.
from bs4 import BeautifulSoup # For parsing HTML content.
import logging
import re # For regular expression operations, used extensively in parsing.
import os
from dateutil.parser import parse as date_parse # For flexible parsing of date strings.

# Use zoneinfo for accurate timezone handling (requires Python 3.9+)
try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
except ImportError:
    # Fallback for older Python versions (less accurate, may not handle DST correctly)
    log_module_level = logging.getLogger(__name__) # Logger instance for module-level warnings.
    log_module_level.warning(
        "zoneinfo module not available (requires Python 3.9+). "
        "Using fixed UTC offset for time calculations. Daylight Saving Time changes will not be accurately handled."
    )
    from datetime import timezone, timedelta # Basic timezone objects.
    ZoneInfo = None # Flag to indicate zoneinfo is not available.

import GsupremescraperEM  # For fetching additional details specific to Supreme Court cases.

log = logging.getLogger(__name__) # Standard logger for this module.

# --- Constants ---
BASE_URL = "https://www.njcourts.gov"
# URL for the "Expected Opinions" page, which is the primary target for scraping.
EXPECTED_OPINIONS_URL = "https://www.njcourts.gov/attorneys/opinions/expected"
# URL for Supreme Court appeals, potentially used by GsupremescraperEM.
SUPREME_APPEALS_URL = "https://www.njcourts.gov/courts/supreme/appeals"
PAGE_URL = EXPECTED_OPINIONS_URL  # Main URL used by default for scraping.

# Standard User-Agent header to mimic a web browser, reducing likelihood of being blocked.
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# --- Timezone Configuration ---
EASTERN_TZ = None
if ZoneInfo: # Check if zoneinfo was successfully imported.
    try:
        # Preferred: Use 'America/New_York' for accurate Eastern Time, including DST.
        EASTERN_TZ = ZoneInfo("America/New_York")
        log.info("Using zoneinfo for America/New_York timezone for opinion status calculation.")
    except ZoneInfoNotFoundError:
        # Fallback if the specific timezone name is not found (should be rare).
        log.error("Timezone 'America/New_York' not found by zoneinfo. Falling back to fixed UTC-4 offset (EDT). DST might be incorrect.")
        EASTERN_TZ = datetime.timezone(datetime.timedelta(hours=-4), name="EDT_Fallback_NoZone") # Approx EDT.
else:
    # Fallback for Python < 3.9 where zoneinfo is not available.
    # This fixed offset does not account for Daylight Saving Time changes.
    log.warning("zoneinfo not available. Using fixed UTC-4 offset for opinion status. This may be inaccurate during Standard Time (EST).")
    EASTERN_TZ = datetime.timezone(datetime.timedelta(hours=-4), name="EDT_Fixed_NoZoneInfo") # Approx EDT.

# Time threshold for considering an opinion "released" on its publication date.
# Opinions are typically published around 10:30 AM Eastern Time.
RELEASE_TIME_THRESHOLD = datetime.time(10, 30, 0) # 10:30 AM

# --- Regular Expression Patterns ---
# Regex to identify Supreme Court docket numbers (e.g., A-12-23).
SUPREME_COURT_DOCKET_REGEX = re.compile(r"\b(A-\d{1,2}-\d{2})\b", re.IGNORECASE)
# Regex to identify Appellate Division docket numbers (e.g., A-1234-23).
APPELLATE_DOCKET_REGEX = re.compile(r"\b(A-\d{4,}-\d{2})\b", re.IGNORECASE)
# Regex for Tax Court docket numbers (e.g., 001234-2023 or 1234-2023).
TAX_COURT_DOCKET_REGEX = re.compile(r"\b(\d{6}-\d{4}|\d{4}-\d{4})\b", re.IGNORECASE)

# --- Mappings ---
# Maps keywords found in decision type badges/text to standardized codes, full text, and court venue.
DECISION_TYPE_MAP = {
    "unpublished appellate": ("appUNpub", "Unpublished Appellate", "Appellate Division"),
    "published appellate": ("appPUB", "Published Appellate", "Appellate Division"),
    "supreme": ("supreme", "Supreme Court", "Supreme Court"), # Typically derived from docket format.
    "unpublished tax": ("taxUNpub", "Unpublished Tax", "Tax Court"),
    "published tax": ("taxPUB", "Published Tax", "Tax Court"),
    "unpublished trial": ("trialUNpub", "Unpublished Trial", "Trial Court"),
    "published trial": ("trialPUB", "Published Trial", "Trial Court"),
}

# Maps full county names to their standard three-letter codes.
COUNTY_CODE_MAP = {
    "Atlantic County": "ATL", "Bergen County": "BER", "Burlington County": "BUR",
    "Camden County": "CAM", "Cape May County": "CPM", "Cumberland County": "CUM",
    "Essex County": "ESX", "Gloucester County": "GLO", "Hudson County": "HUD",
    "Hunterdon County": "HNT", "Mercer County": "MER", "Middlesex County": "MID",
    "Monmouth County": "MON", "Morris County": "MRS", "Ocean County": "OCN",
    "Passaic County": "PAS", "Salem County": "SLM", "Somerset County": "SOM",
    "Sussex County": "SSX", "Union County": "UNN", "Warren County": "WRN"
}

# Maps Lower Court (LC) docket patterns to their respective venue and sub-case type.
# Each entry is a tuple: (regex_pattern, venue_string, subtype_info_or_lambda, group_index_for_subtype_lambda).
LC_DOCKET_VENUE_MAP = [
    # Special Civil Part (DC, LT, SC) - patterns capture variations with or without county prefix.
    (re.compile(r'\b([A-Z]{3})-(DC|LT|SC)-(\d+)-(\d{2})\b', re.IGNORECASE), "Law Division", lambda m: f"Special Civil Part ({m.group(2).upper()})", 1),
    (re.compile(r'\b(DC|LT|SC)-(\d+)-(\d{2})\b', re.IGNORECASE), "Law Division", lambda m: f"Special Civil Part ({m.group(1).upper()})", 1),
    # Family Division subtypes (FV, FD, FM, FG, FP, other generic Family).
    (re.compile(r'\bF(V)-\d{2}-\d+-\d{2}\b', re.IGNORECASE), "Family Division", "Family Violence (FV)", 1),
    (re.compile(r'\bF(D)-\d{2}-\d+-\d{2}\b', re.IGNORECASE), "Family Division", "Dissolution (FD)", 1),
    (re.compile(r'\bF(M)-\d{2}-\d+-\d{2}\b', re.IGNORECASE), "Family Division", "Dissolution (FM)", 1), # FM often also dissolution/matrimonial.
    (re.compile(r'\bF(G)-\d{2}-\d+-\d{2}\b', re.IGNORECASE), "Family Division", "Guardianship (FG)", 1),
    (re.compile(r'\bF(P)-\d{2}-\d+-\d{2}\b', re.IGNORECASE), "Family Division", "Termination of Parental Rights (FP)", 1),
    (re.compile(r'\bF([A-Z])-\d{2}-\d+-\d{2}\b', re.IGNORECASE), "Family Division", lambda m: f"Other Family ({m.group(1).upper()})", 1), # Catches other F-prefixed dockets.
    # Chancery Division (Foreclosure, Probate, General Equity).
    (re.compile(r'\bF-\d+-\d{2}\b', re.IGNORECASE), "Chancery Division", "Foreclosure Part", None), # Note: F- alone is Foreclosure, not Family.
    (re.compile(r'\bP-\d+-\d{2}\b', re.IGNORECASE), "Chancery Division", "Probate Part", None),
    (re.compile(r'\bC-\d+-\d{2}\b', re.IGNORECASE), "Chancery Division", "General Equity Part", None),
    # Law Division (Civil Commitment, Civil Part, Criminal Part, Municipal Appeal).
    (re.compile(r'\bSVP-\d+-\d{2}\b', re.IGNORECASE), "Law Division", "Civil Commitment (SVP)", None), # Sexually Violent Predator
    (re.compile(r'\bL-\d+-\d{2}\b', re.IGNORECASE), "Law Division", "Civil Part", None),
    (re.compile(r'\b(\d{2}-\d{2}-\d{4})\b'), "Law Division", "Criminal Part", None), # Indictment numbers like YY-MM-NNNN
    (re.compile(r'\b(\d{4}-\d{2})\b'), "Law Division - Appellate Part", "Municipal Appeal", None), # Municipal court appeal
    # State Agency dockets.
    (re.compile(r'\b(H\d{4}-\d+)\b', re.IGNORECASE), "Agency", "Division on Civil Rights", None),
    (re.compile(r'\b(20\d{2}-\d+)\b', re.IGNORECASE), "Agency", "State Agency Docket", None), # Generic agency docket format
]

# --- Helper Functions ---
def _extract_text_safely(element, joiner=' '):
    """
    Safely extracts all text from a BeautifulSoup element and its children.
    Replaces non-breaking spaces ('\xa0') with regular spaces and strips whitespace.

    Args:
        element (bs4.element.Tag or None): The BeautifulSoup element to extract text from.
        joiner (str, optional): The string used to join multiple text nodes. Defaults to a space.

    Returns:
        str: The cleaned and joined text, or an empty string if element is None or has no text.
    """
    if element:
        # Find all text nodes, including those in nested tags.
        text_nodes = element.find_all(string=True, recursive=True)
        # Clean each text node: replace non-breaking spaces, strip whitespace.
        cleaned_text_fragments = [t.replace('\xa0', ' ').strip() for t in text_nodes]
        # Join non-empty fragments.
        return joiner.join(filter(None, cleaned_text_fragments))
    return "" # Return empty string if the element is None.

def _map_decision_info(type_string: str | None) -> tuple:
    """
    Maps a raw decision type string (often from a badge) to a standardized
    decision code, full decision text, and associated court venue.

    Args:
        type_string (str | None): The raw string indicating the decision type.

    Returns:
        tuple: (decision_code, decision_text, opinion_venue).
               Returns (None, original_type_string or "Unknown", "Unknown Court")
               if no mapping is found.
    """
    type_string_lower = type_string.lower().strip() if type_string else ""
    for key_prefix, (code, text, venue) in DECISION_TYPE_MAP.items():
        if type_string_lower.startswith(key_prefix):
            return code, text, venue
    # If no match, return unknowns but preserve original type string if possible.
    return (None, type_string if type_string else "Unknown", "Unknown Court")


def _parse_case_title_details(raw_title_text: str, opinion_type_venue: str = "Unknown Court") -> dict:
    """
    Parses the raw case title string, which often contains docket numbers,
    county information, and other details within parentheses, to extract
    structured data.

    This function attempts to identify:
    - Core Case Name.
    - Lower Court Docket ID (LCdocketID).
    - Lower Court County (LCCounty).
    - Operational Jurisdiction (OPJURISAPP, typically "Statewide").
    - Involved State Agencies (StateAgency1, StateAgency2).
    - Lower Court Venue and SubCaseType.
    - Case Notes (e.g., "RECORD IMPOUNDED", "CONSOLIDATED").
    - Flags for consolidated or impounded status.

    For Supreme Court cases, it may invoke `GsupremescraperEM` to find additional
    details like linked appellate dockets.

    Args:
        raw_title_text (str): The full text of the case title, including any
                              parenthetical information.
        opinion_type_venue (str, optional): The primary venue of the opinion (e.g.,
                                            "Supreme Court", "Appellate Division").
                                            This helps tailor parsing logic.
                                            Defaults to "Unknown Court".

    Returns:
        dict: A dictionary containing parsed details. Fields not found will be None
              or default values (e.g., 0 for boolean flags).
    """
    details = {
        'CaseName': raw_title_text.strip(), # Default to full text, refined later.
        'LCdocketID': None,
        'LCCounty': None,
        'OPJURISAPP': "Statewide", # Default, may be overridden if specific info found.
        'StateAgency1': None,
        'StateAgency2': None,
        'LowerCourtVenue': None,
        'LowerCourtSubCaseType': None,
        'CaseNotes': [], # List to accumulate notes.
        'caseconsolidated': 0, # Boolean flag (0 or 1).
        'recordimpounded': 0   # Boolean flag (0 or 1).
    }
    log.debug(f"Parsing raw title for venue '{opinion_type_venue}': '{raw_title_text[:100]}...'")

    # Separate core case name from parenthetical content.
    first_paren_index = raw_title_text.find('(')
    paren_content_full = ""
    core_name = raw_title_text.strip()
    if first_paren_index != -1:
        core_name = raw_title_text[:first_paren_index].strip()
        paren_content_full = raw_title_text[first_paren_index:].strip()
    details['CaseName'] = core_name

    # --- Extract flags and special notes from parenthetical content ---
    # Patterns for notes like "RECORD IMPOUNDED", "CONSOLIDATED".
    note_patterns = {'RECORD IMPOUNDED': 'recordimpounded', 'CONSOLIDATED': 'caseconsolidated', 'RESUBMITTED': None}
    remaining_paren_content = paren_content_full
    extracted_notes_from_flags = [] # Store notes like "Resubmitted"

    for note_text, flag_key in note_patterns.items():
        # Regex to find whole word, case-insensitive, surrounded by non-alphanumeric or start/end of string.
        pattern = r'(?:^|[\s,(;])\b(' + re.escape(note_text) + r')\b(?:$|[\s,);])'
        # Use finditer to handle multiple occurrences and remove them.
        matches = list(re.finditer(pattern, remaining_paren_content, re.IGNORECASE))
        offset = 0 # Adjust index after modifying string.
        found_this_note_flag = False
        for match in matches:
            found_this_note_flag = True
            start_idx, end_idx = match.span(0) # Get span of the whole match.
            # Adjust indices for previous removals.
            adj_start_idx, adj_end_idx = start_idx - offset, end_idx - offset
            # Remove the matched part from remaining_paren_content.
            remaining_paren_content = remaining_paren_content[:adj_start_idx] + remaining_paren_content[adj_end_idx:]
            offset += (adj_end_idx - adj_start_idx) # Update offset.
        if found_this_note_flag and flag_key: # If it's a flag to set (e.g., recordimpounded).
            details[flag_key] = 1
            log.info(f"Flag '{flag_key}' set to 1 for case '{core_name}' based on note '{note_text}'.")
        elif found_this_note_flag and not flag_key: # If it's a note to record (e.g., RESUBMITTED).
            extracted_notes_from_flags.append(note_text.title())

    # Clean up remaining parenthetical content (remove extra spaces, strip delimiters).
    remaining_paren_content = re.sub(r'\s+', ' ', remaining_paren_content).strip(' ,;()')
    log.debug(f"Parenthetical content after flag extraction: '{remaining_paren_content}'")

    # Split remaining parenthetical content by common delimiters (comma, semicolon, "AND").
    info_elements = [p.strip() for p in re.split(r'\s*[,;]\s*|\s+AND\s+', remaining_paren_content) if p.strip()]
    log.debug(f"Split info elements from parentheses: {info_elements}")

    # --- Process Info Elements for Dockets, County, Agency ---
    processed_indices = set() # Keep track of elements already processed.
    found_dockets_in_paren = [] # Store any dockets found.
    # Keywords indicating a state agency might be involved.
    agency_keywords = ["DEPARTMENT OF", "BOARD OF", "DIVISION OF", "BUREAU OF", "OFFICE OF", "COMMISSION"]
    # Check if CaseName itself suggests an agency case.
    is_agency_case_by_name = any(kw in details['CaseName'].upper() for kw in agency_keywords)
    appellate_docket_from_paren = None # Store Appellate Docket if found in parens.
    found_county_in_paren = None
    found_opjuris_in_paren = None
    found_agencies_in_paren = []

    # Iterate through elements extracted from parentheses.
    for i, element_text in enumerate(info_elements):
        element_has_been_processed = False
        # Try to match Appellate Docket (A-####-YY).
        app_match = APPELLATE_DOCKET_REGEX.search(element_text)
        if app_match:
            appellate_docket_from_paren = app_match.group(1).strip().upper()
            log.debug(f"Found potential Appellate Docket '{appellate_docket_from_paren}' in parenthetical element {i}: '{element_text}'.")
            processed_indices.add(i)
            element_has_been_processed = True # This element primarily identifies an AppDocket.

        # Try to match Lower Court / Agency dockets using LC_DOCKET_VENUE_MAP.
        for lc_pattern, lc_venue, lc_subtype_info, _ in LC_DOCKET_VENUE_MAP:
            lc_matches = list(lc_pattern.finditer(element_text))
            if lc_matches:
                for lc_match in lc_matches:
                    docket_string = lc_match.group(0).strip().upper()
                    # Avoid re-matching the appellate docket if it was also found by a broader LC pattern.
                    if docket_string == appellate_docket_from_paren:
                        continue
                    subtype = lc_subtype_info(lc_match) if callable(lc_subtype_info) else lc_subtype_info
                    found_dockets_in_paren.append({"docket": docket_string, "venue": lc_venue, "subtype": subtype})
                    log.debug(f"Found LC/Agency Docket: '{docket_string}' -> Venue: '{lc_venue}', Subtype: '{subtype}' in parenthetical element {i}: '{element_text}'.")
                processed_indices.add(i)
                element_has_been_processed = True # Element contained one or more LC dockets.

        # If an Appellate Docket was found in this element, prioritize that and skip other checks for this element.
        if appellate_docket_from_paren and element_has_been_processed:
            continue

        # If element wasn't primarily a known docket type, check for County, OPJuris, or Agency.
        if not element_has_been_processed:
            # County Check (full name or code)
            if not found_county_in_paren: # Only find first county mentioned.
                county_name_match = re.search(r'(?:COUNTY\s+OF\s+)?([A-Za-z\s]+?)\s+COUNTY\b', element_text, re.IGNORECASE)
                if county_name_match:
                    parsed_county_name = county_name_match.group(1).strip().title() + " County"
                    if parsed_county_name in COUNTY_CODE_MAP:
                        found_county_in_paren = parsed_county_name
                        log.debug(f"Found County by name: '{found_county_in_paren}' in parenthetical element {i}: '{element_text}'.")
                        processed_indices.add(i); continue
                # County code match (e.g., "ATL")
                county_code_match = re.search(r'\b([A-Z]{3})\b', element_text)
                if county_code_match:
                    code = county_code_match.group(1)
                    matched_county_name = next((name for name, c_code in COUNTY_CODE_MAP.items() if c_code == code), None)
                    if matched_county_name:
                        found_county_in_paren = matched_county_name
                        log.debug(f"Found County by code: '{code}' -> '{found_county_in_paren}' in parenthetical element {i}: '{element_text}'.")
                        processed_indices.add(i); continue

            # OPJuris Check (e.g., "Statewide")
            if not found_opjuris_in_paren and element_text.upper() == "STATEWIDE":
                found_opjuris_in_paren = "Statewide"
                log.debug(f"Found OPJuris: '{found_opjuris_in_paren}' in parenthetical element {i}: '{element_text}'.")
                processed_indices.add(i); continue

            # Agency Check (keywords and not a county)
            if any(kw in element_text.upper() for kw in agency_keywords) and "COUNTY" not in element_text.upper():
                found_agencies_in_paren.append(element_text.strip())
                processed_indices.add(i)
                is_agency_case_by_name = True # Mark as agency if mentioned in parens too.
                log.debug(f"Found Agency by keyword: '{element_text.strip()}' in parenthetical element {i}.")
                continue

    # --- Assign parsed details based on findings and opinion_type_venue ---
    details['LCCounty'] = found_county_in_paren
    if found_opjuris_in_paren:
        details['OPJURISAPP'] = found_opjuris_in_paren
    if found_agencies_in_paren:
        details['StateAgency1'] = found_agencies_in_paren[0]
        if len(found_agencies_in_paren) > 1:
            details['StateAgency2'] = found_agencies_in_paren[1]
        # Add any further agencies to notes.
        if len(found_agencies_in_paren) > 2:
            extracted_notes_from_flags.append(f"Other Mentioned Agencies: {', '.join(found_agencies_in_paren[2:])}")

    primary_lc_docket_info = found_dockets_in_paren[0] if found_dockets_in_paren else None

    # Logic specific to Supreme Court opinions.
    if opinion_type_venue == "Supreme Court":
        # Attempt to find the Supreme Court docket number (A-##-YY) in the raw title.
        sc_docket_match = SUPREME_COURT_DOCKET_REGEX.search(raw_title_text)
        if sc_docket_match:
            sc_docket_num = sc_docket_match.group(1).strip().upper()
            # Use GsupremescraperEM to get more details for this SC docket.
            sc_case_details = GsupremescraperEM.supreme_scraper.find_matching_case(
                sc_docket_num,
                case_caption=details['CaseName'] # Pass caption for better matching.
            )
            if sc_case_details:
                # If additional details found, populate them.
                if sc_case_details.get('app_docket'): # This is the linked Appellate docket.
                    details['LinkedDocketIDs'] = sc_case_details['app_docket']
                if sc_case_details.get('county') and not details['LCCounty']: # Prioritize paren if already found.
                    details['LCCounty'] = sc_case_details['county']
                if sc_case_details.get('state_agency') and not details['StateAgency1']:
                    details['StateAgency1'] = sc_case_details['state_agency']
                log.info(f"Fetched additional details for Supreme Court case '{sc_docket_num}' via GsupremescraperEM.")
            else:
                log.warning(f"No additional details found via GsupremescraperEM for Supreme Court case '{sc_docket_num}'.")

        # For SC cases, the LCdocketID is typically the linked Appellate docket.
        details['LCdocketID'] = appellate_docket_from_paren # This was extracted from parens.
        details['LowerCourtVenue'] = "Appellate Division" # Assume appealed from AppDiv.
        details['LowerCourtSubCaseType'] = None
        # Note the original LC if found in parens.
        if primary_lc_docket_info:
            note = (f"[Original LC: Docket={primary_lc_docket_info.get('docket','N/A')}, "
                    f"Venue={primary_lc_docket_info.get('venue','N/A')}" +
                    (f" ({primary_lc_docket_info.get('subtype')})" if primary_lc_docket_info.get('subtype') else "") + "]")
            extracted_notes_from_flags.insert(0, note) # Prepend this important note.
        # Log other info found in parens into notes for SC cases.
        if found_county_in_paren and (not primary_lc_docket_info or found_county_in_paren not in primary_lc_docket_info.get('venue','')):
            extracted_notes_from_flags.append(f"[Parenthetical County: {found_county_in_paren}]")
        if details['StateAgency1'] and not any(details['StateAgency1'] in note for note in extracted_notes_from_flags): # Avoid duplicate agency notes
             extracted_notes_from_flags.append(f"[Parenthetical Agency1: {details['StateAgency1']}]")
        if details['StateAgency2'] and not any(details['StateAgency2'] in note for note in extracted_notes_from_flags):
             extracted_notes_from_flags.append(f"[Parenthetical Agency2: {details['StateAgency2']}]")
        log.info(f"Supreme Court Case: LCdocketID (Appellate Link) set to '{details['LCdocketID']}'. Original LC info moved to notes.")
    else:  # For Appellate, Trial, Tax Court opinions.
        if primary_lc_docket_info:
            details['LCdocketID'] = primary_lc_docket_info.get('docket')
            details['LowerCourtVenue'] = primary_lc_docket_info.get('venue')
            details['LowerCourtSubCaseType'] = primary_lc_docket_info.get('subtype')

    # Add any remaining unprocessed elements from parentheses to notes.
    for i, element_text in enumerate(info_elements):
        if i not in processed_indices:
            extracted_notes_from_flags.append(element_text)

    # Finalize details.
    # If it's an agency case (by name or keywords), ensure LCCounty is 'NJ' (New Jersey statewide).
    if is_agency_case_by_name and details['LCCounty'] != 'NJ': # Check if already NJ to avoid redundant log.
        details['LCCounty'] = 'NJ'
        log.debug(f"Case '{core_name}' identified as agency-related. Set LCCounty to 'NJ'.")

    # If StateAgency1 is not filled but it's an agency case, try to extract from CaseName.
    if not details['StateAgency1'] and is_agency_case_by_name:
        for keyword in agency_keywords:
            # Regex to find keyword followed by capitalized words (potential agency name).
            agency_name_match = re.search(rf'\b({keyword}(?:\s+[A-Z][a-zA-Z]+)+)\b', details['CaseName'], re.IGNORECASE)
            if agency_name_match:
                details['StateAgency1'] = agency_name_match.group(1).strip()
                log.debug(f"Assigned StateAgency1 from CaseName: '{details['StateAgency1']}'.")
                break # Take the first match.

    # Default LowerCourtVenue if not determined, unless it's an agency case.
    if opinion_type_venue != "Supreme Court" and not details['LowerCourtVenue'] and details['LowerCourtVenue'] != "Agency":
        details['LowerCourtVenue'] = "Unknown"

    # Add a note if LC Docket is missing for non-SC, non-Agency cases.
    if opinion_type_venue != "Supreme Court" and \
       not details['LCdocketID'] and details['LowerCourtVenue'] not in ["Agency", "Unknown"]: # Avoid for Unknown venue too.
        missing_lc_note = "[LC Docket Missing in Parentheses]"
        if missing_lc_note not in extracted_notes_from_flags: # Avoid duplicate notes.
            extracted_notes_from_flags.append(missing_lc_note)
            log.warning(f"'{missing_lc_note}' added for case '{core_name}' (Venue: {opinion_type_venue}).")

    # Consolidate all collected notes into a single string.
    details['CaseNotes'] = ", ".join(sorted(list(set(filter(None, extracted_notes_from_flags))))) or None
    log.debug(f"Final parsed title details for '{core_name}' ({opinion_type_venue}): "
              f"LC Docket='{details['LCdocketID']}', LCVenue='{details['LowerCourtVenue']}', Notes='{details['CaseNotes']}'")
    return details


def _parse_case_article(article_element: BeautifulSoup, release_date_iso: str | None) -> list | None:
    """
    Parses a single HTML `<article>` element (or similar container) representing
    one opinion entry on the NJ Courts page.

    It extracts:
    - Primary Docket ID and Opinion Type/Venue from "badge" elements.
    - Calculates `opinionstatus` (0 for Expected, 1 for Released) based on the
      `release_date_iso` and the current time (using `EASTERN_TZ` and `RELEASE_TIME_THRESHOLD`).
    - Calls `_parse_case_title_details()` to get detailed information from the case title.
    - Handles cases where multiple primary dockets are listed in a single entry.

    Args:
        article_element (bs4.element.Tag): The BeautifulSoup Tag object for the
                                           HTML article/container.
        release_date_iso (str | None): The overall release date for opinions on the page,
                                   in "YYYY-MM-DD" format. Used for status calculation.

    Returns:
        list | None: A list of dictionaries, where each dictionary is a `case_data` record.
                     Returns a list with multiple items if the article lists multiple
                     consolidated primary dockets. Returns None if the article is invalid,
                     indicates "no opinions", or a critical parsing error occurs.
    """
    case_data_list = [] # To store one or more opinion records from this article.
    log.debug("Attempting to parse a case <article> element...")
    raw_title_text_for_log = "N/A (title not extracted yet)" # For logging in case of early failure.

    try:
        # --- Basic Structure Validation ---
        card_body = article_element.find('div', class_='card-body')
        if not card_body:
            log.warning("Skipping article: Missing 'div.card-body'.")
            return None
        # Check for "no opinions reported" message.
        no_opinions_text_match = card_body.find(string=re.compile(r'no\s+.*\s+opinions\s+reported', re.IGNORECASE))
        if no_opinions_text_match:
            log.info(f"Skipping article: Contains 'no opinions reported' text: '{no_opinions_text_match.strip()}'.")
            return None
        # Find the title div. Class name can vary slightly (e.g., with 'mb-1').
        title_div = card_body.find('div', class_=re.compile(r'card-title\b.*\btext-start\b'))
        if not title_div:
            log.warning("Skipping article: Missing 'div.card-title.text-start'.")
            return None
        raw_title_text_for_log = _extract_text_safely(title_div) # Get title for logging early.
        if not raw_title_text_for_log:
            log.warning("Skipping article: Case title is empty.")
            return None

        # --- Identify Opinion Type and Primary Docket from Badges ---
        # Badges usually contain the primary docket number and the opinion type.
        badge_spans = card_body.find_all('span', class_='badge')
        primary_docket_id = None
        primary_docket_badge_text = None # Store the text of the badge that gave the primary_docket_id.
        decision_code, decision_text, opinion_type_venue = None, None, "Unknown Court"

        # First pass: Identify the primary docket number and its associated venue.
        for span in badge_spans:
            span_text = _extract_text_safely(span).strip()
            if not span_text: continue

            # Check for Supreme, Appellate, Tax Court dockets first.
            sc_match = SUPREME_COURT_DOCKET_REGEX.search(span_text)
            if sc_match:
                primary_docket_id = sc_match.group(1).strip().upper()
                opinion_type_venue = "Supreme Court"
                decision_code, decision_text, _ = DECISION_TYPE_MAP["supreme"] # Supreme cases have a fixed type.
                primary_docket_badge_text = span_text; break
            app_match = APPELLATE_DOCKET_REGEX.search(span_text)
            if app_match:
                primary_docket_id = app_match.group(1).strip().upper()
                opinion_type_venue = "Appellate Division"
                primary_docket_badge_text = span_text; break
            tax_match = TAX_COURT_DOCKET_REGEX.search(span_text)
            if tax_match:
                primary_docket_id = tax_match.group(1).strip().upper()
                opinion_type_venue = "Tax Court"
                primary_docket_badge_text = span_text; break
            # Check for Trial Court dockets if others not found.
            for lc_pattern, _, _, _ in LC_DOCKET_VENUE_MAP:
                 match = lc_pattern.fullmatch(span_text) # fullmatch to ensure badge is just the docket.
                 if match:
                     primary_docket_id = match.group(0).strip().upper()
                     opinion_type_venue = "Trial Court"
                     primary_docket_badge_text = span_text; break
            if primary_docket_id: break # Found primary docket, exit loop.

        # Second pass (if not Supreme): Identify decision type from other badges.
        if opinion_type_venue != "Supreme Court":
            for span in badge_spans:
                 span_text = _extract_text_safely(span).strip()
                 # Skip if empty or if it's the badge already used for primary docket.
                 if not span_text or span_text == primary_docket_badge_text: continue
                 mapped_code, mapped_text, mapped_venue_from_type = _map_decision_info(span_text)
                 if mapped_code:
                     # If venue was unknown, set it from type. Or, ensure type matches known venue.
                     if opinion_type_venue == "Unknown Court" or mapped_venue_from_type == opinion_type_venue:
                         decision_code, decision_text = mapped_code, mapped_text
                         if opinion_type_venue == "Unknown Court":
                             opinion_type_venue = mapped_venue_from_type
                         break # Found decision type.

        if not primary_docket_id:
            log.warning(f"Could not determine Primary Docket ID for case: '{raw_title_text_for_log[:70]}...' (Venue guess: {opinion_type_venue}). Skipping article.")
            return None
        if not decision_code and opinion_type_venue != "Supreme Court": # Supreme Court type is fixed.
            log.warning(f"Could not determine Decision Type for docket {primary_docket_id} (Venue: {opinion_type_venue}). Defaulting type text.")
            decision_text = f"Unknown {opinion_type_venue} Type" # Provide a default text.

        # --- Calculate Opinion Status (Expected: 0, Released: 1) ---
        opinion_status = 0 # Default to 'Expected'.
        if release_date_iso and EASTERN_TZ: # Ensure we have a release date and timezone info.
            try:
                release_date_obj = datetime.datetime.strptime(release_date_iso, '%Y-%m-%d').date()
                # Combine the release date with the predefined release time threshold.
                # Make this datetime object timezone-aware using EASTERN_TZ.
                release_datetime_aware = datetime.datetime.combine(release_date_obj, RELEASE_TIME_THRESHOLD, tzinfo=EASTERN_TZ)
                # Get the current time, also localized to Eastern Time.
                now_eastern_aware = datetime.datetime.now(EASTERN_TZ)

                # If current Eastern time is at or after the release threshold on the release date.
                if now_eastern_aware >= release_datetime_aware:
                    opinion_status = 1 # Mark as 'Released'.
                log.debug(f"Status check for Docket {primary_docket_id}: Release Datetime (ET)='{release_datetime_aware}', "
                          f"Current Time (ET)='{now_eastern_aware}', Calculated Status='{opinion_status}'.")
            except ValueError as ve:
                log.warning(f"Date parsing error for status calculation (Docket {primary_docket_id}, Date '{release_date_iso}'): {ve}")
            except Exception as e_status:
                # Catch any other unexpected errors during status calculation.
                log.error(f"Unexpected error calculating opinion status for Docket {primary_docket_id}: {e_status}", exc_info=True)
        elif not release_date_iso:
            log.warning(f"Cannot calculate opinion status for Docket {primary_docket_id}: Release date is unknown.")
        else: # EASTERN_TZ is None
            log.warning(f"Cannot calculate opinion status for Docket {primary_docket_id}: Eastern Timezone information is unavailable (zoneinfo likely missing).")

        # --- Parse Title Details ---
        # Delegate complex title string parsing to _parse_case_title_details.
        title_details = _parse_case_title_details(raw_title_text_for_log, opinion_type_venue)

        # --- Handle multiple primary dockets if listed in the same badge ---
        # Example: "A-1234-22, A-1235-22" in one badge.
        all_primary_dockets_in_entry = [primary_docket_id] # Start with the one already identified.
        if primary_docket_badge_text: # If we have the text of the primary docket badge.
             primary_docket_regex_for_multi = None
             if opinion_type_venue == "Supreme Court": primary_docket_regex_for_multi = SUPREME_COURT_DOCKET_REGEX
             elif opinion_type_venue == "Appellate Division": primary_docket_regex_for_multi = APPELLATE_DOCKET_REGEX
             elif opinion_type_venue == "Tax Court": primary_docket_regex_for_multi = TAX_COURT_DOCKET_REGEX
             # No multi-docket regex for Trial Court as they are usually singular in badges.

             if primary_docket_regex_for_multi:
                  # Find all occurrences of the docket pattern in the badge text.
                  dockets_found_in_badge = primary_docket_regex_for_multi.findall(primary_docket_badge_text)
                  if len(dockets_found_in_badge) > 0: # If findall returns matches.
                      all_primary_dockets_in_entry = [d.strip().upper() for d in dockets_found_in_badge]
                      log.info(f"Expanded to multiple primary dockets from badge '{primary_docket_badge_text}': {all_primary_dockets_in_entry}")


        # --- Create Data Records for each primary docket found ---
        for i, current_docket_id in enumerate(all_primary_dockets_in_entry):
            # If multiple dockets, list others as "LinkedDocketIDs".
            linked_dockets_list = [d for idx, d in enumerate(all_primary_dockets_in_entry) if idx != i]
            case_data_entry = {
                "AppDocketID": current_docket_id, # This is the primary docket for this specific record.
                "ReleaseDate": release_date_iso,
                "LinkedDocketIDs": ", ".join(linked_dockets_list) if linked_dockets_list else None,
                "CaseName": title_details.get('CaseName'),
                "LCdocketID": title_details.get('LCdocketID'),
                "LCCounty": title_details.get('LCCounty'),
                "Venue": opinion_type_venue, # Overall venue (Supreme, Appellate, etc.).
                "LowerCourtVenue": title_details.get('LowerCourtVenue'),
                "LowerCourtSubCaseType": title_details.get('LowerCourtSubCaseType'),
                "OPJURISAPP": title_details.get('OPJURISAPP'),
                "DecisionTypeCode": decision_code,
                "DecisionTypeText": decision_text,
                "StateAgency1": title_details.get('StateAgency1'),
                "StateAgency2": title_details.get('StateAgency2'),
                "CaseNotes": title_details.get('CaseNotes'),
                "caseconsolidated": title_details.get('caseconsolidated', 0), # Default to 0 (false).
                "recordimpounded": title_details.get('recordimpounded', 0), # Default to 0 (false).
                "opinionstatus": opinion_status # Calculated status (0=Expected, 1=Released).
            }
            log.debug(f"Successfully parsed data record: AppDocketID='{case_data_entry['AppDocketID']}', "
                      f"Status='{case_data_entry['opinionstatus']}', CaseName='{case_data_entry['CaseName'][:50]}...'")
            case_data_list.append(case_data_entry)
        return case_data_list

    except Exception as e:
        # Catch-all for any error during parsing of a single article.
        log.error(f"Critical error parsing article (Title: '{raw_title_text_for_log[:70]}...'): {e}", exc_info=True)
        return None # Indicate failure for this article.

def fetch_and_parse_opinions(url: str = PAGE_URL) -> tuple[list, str | None]:
    """
    Fetches HTML content from the specified URL (defaulting to the NJ Courts
    Expected Opinions page) and parses all opinion articles found on the page.

    Args:
        url (str, optional): The URL to scrape. Defaults to `PAGE_URL`.

    Returns:
        tuple: A tuple containing:
            - list: A list of dictionaries, where each dictionary represents a parsed
                    opinion's data. Empty if no opinions found or error occurs.
            - str | None: The extracted release date for the opinions on the page
                          in "YYYY-MM-DD" ISO format. None if date cannot be determined.
    """
    log.info(f"Starting fetch and parse opinions from URL: {url}")
    all_parsed_opinions = []
    overall_release_date_iso = None # ISO format "YYYY-MM-DD"

    # --- Fetch HTML Content ---
    try:
        response = requests.get(url, headers=HEADERS, timeout=30) # 30-second timeout for the request.
        response.raise_for_status() # Raise HTTPError for bad responses (4XX or 5XX).
        log.info(f"Successfully fetched HTML content from {url}. Status: {response.status_code}.")
        html_content = response.text
    except requests.exceptions.RequestException as e:
        log.error(f"Failed to fetch URL '{url}': {e}", exc_info=True)
        print(f"Error: Connection failed for URL {url}. Cannot scrape opinions.")
        return [], None # Return empty list and None date on fetch failure.

    # --- Parse HTML with BeautifulSoup ---
    soup = BeautifulSoup(html_content, "html.parser") # Use standard html.parser.

    # --- Extract Overall Release Date for the Page ---
    try:
        # Date is usually in a specific h2 header within the view header.
        date_header_element = soup.select_one('div.view-header h2') # CSS selector for the date header.
        raw_date_text_from_header = _extract_text_safely(date_header_element) if date_header_element else None

        if raw_date_text_from_header:
            # Example header text: "Expected Opinions for publication on Monday, July 22, 2024"
            # Regex to extract the date part.
            date_match = re.search(r'on\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})', raw_date_text_from_header, re.IGNORECASE)
            extracted_date_str = date_match.group(1).strip() if date_match else None
            if extracted_date_str:
                log.info(f"Extracted raw date string from header: '{extracted_date_str}'")
                try:
                    # Use dateutil.parser for robust parsing of various date formats.
                    parsed_datetime_obj = date_parse(extracted_date_str)
                    overall_release_date_iso = parsed_datetime_obj.strftime('%Y-%m-%d') # Convert to ISO format.
                    log.info(f"Successfully parsed release date to ISO format: {overall_release_date_iso}")
                except Exception as date_parse_err:
                    log.warning(f"Could not parse extracted date string '{extracted_date_str}': {date_parse_err}. "
                                f"Will attempt to use raw string if compatible or proceed without date for status.")
                    overall_release_date_iso = extracted_date_str # Fallback to raw if compatible, else None.
            else:
                log.warning(f"Could not find date pattern ('on ...') in header text: '{raw_date_text_from_header}'.")
        else:
            log.warning("Release date header (div.view-header h2) not found on the page.")
    except Exception as e_date_extract:
        log.error(f"Error during overall release date extraction: {e_date_extract}", exc_info=True)

    if not overall_release_date_iso:
        log.error("Failed to determine the overall release date for opinions on this page. Opinion status calculations may be affected.")
        # Consider if this should halt further processing or just issue a warning.
        # For now, it proceeds, and status calculation will be impacted.
        print("Warning: Release date for the page could not be determined. Opinion statuses may be inaccurate.")

    # --- Find and Parse Opinion Articles ---
    # Opinions are typically within <article class="w-100"> or sometimes <div class="card">.
    # Prioritize 'main#main-content' for scoping search if it exists.
    main_content_area = soup.find('main', id='main-content') or soup # Fallback to whole soup if main not found.
    # Look for article tags first, then div.card as a fallback.
    article_elements = main_content_area.find_all('article', class_='w-100')
    if not article_elements:
        log.info("No <article class='w-100'> tags found. Trying 'div.card' as opinion containers.")
        article_elements = main_content_area.select('div.card') # Using select for more complex class checks if needed.

    log.info(f"Found {len(article_elements)} potential opinion containers/articles to parse.")

    processed_articles_count = 0
    skipped_articles_count = 0
    for article_html_element in article_elements:
        # Pass the determined overall_release_date_iso to the parsing function for each article.
        parsed_opinion_data_list = _parse_case_article(article_html_element, overall_release_date_iso)
        if parsed_opinion_data_list: # _parse_case_article returns a list of opinions (usually one).
            all_parsed_opinions.extend(parsed_opinion_data_list)
            processed_articles_count += len(parsed_opinion_data_list) # Count individual opinions if multi-docket.
        else:
            skipped_articles_count += 1 # Article was skipped or failed parsing.

    log.info(f"Opinion parsing complete. Total opinions extracted: {len(all_parsed_opinions)}. "
             f"Articles processed that yielded opinions: {processed_articles_count} (this counts individual dockets if expanded). "
             f"Articles skipped or yielding no data: {skipped_articles_count}.")

    if not all_parsed_opinions and len(article_elements) > 0 :
        log.warning("Found article elements but extracted zero opinions. Check parsing logic and website structure.")
    elif not article_elements:
        log.warning("No article elements found on the page. Website structure might have changed or page is empty.")

    return all_parsed_opinions, overall_release_date_iso

# === End of GscraperEM.py ===
