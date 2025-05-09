# Modules/GscraperEM.py
# V7: Modified fetch_and_parse_opinions to accept local file path.
"""
Handles fetching and parsing the NJ Courts 'Expected Opinions' page.
- V7: Modified fetch_and_parse_opinions to accept local file path.
- V6: Use zoneinfo for opinionstatus calculation.
- V5: Added opinionstatus field.
- V4: Corrected Supreme Court Docket Handling (A-##-YY).
- V3: Handles Supreme, Trial, Tax opinions; refined venue strings.
"""
import datetime
import requests
from bs4 import BeautifulSoup
import logging
import re
import os # Added for file path check
from dateutil.parser import parse as date_parse
try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
except ImportError:
    log_fallback = logging.getLogger(__name__) # Temporary logger for fallback
    log_fallback.warning("zoneinfo not available (requires Python 3.9+). Using fixed UTC offset for time calculations. DST changes will not be handled.")
    from datetime import timezone, timedelta
    ZoneInfo = None

import GsupremescraperEM

log = logging.getLogger(__name__)

# Constants
BASE_URL = "https://www.njcourts.gov"
EXPECTED_OPINIONS_URL = "https://www.njcourts.gov/attorneys/opinions/expected"
SUPREME_APPEALS_URL = "https://www.njcourts.gov/courts/supreme/appeals"
# PAGE_URL constant might be less relevant if we often pass specific URLs or file paths
# For default behavior, keep EXPECTED_OPINIONS_URL
DEFAULT_SCRAPE_SOURCE = EXPECTED_OPINIONS_URL

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

EASTERN_TZ = None
if ZoneInfo:
    try:
        EASTERN_TZ = ZoneInfo("America/New_York")
        log.info("Using zoneinfo for America/New_York timezone.")
    except ZoneInfoNotFoundError:
        log.error("Timezone 'America/New_York' not found by zoneinfo. Falling back to fixed offset.")
        EASTERN_TZ = datetime.timezone(datetime.timedelta(hours=-4), name="EDT_Fallback") # Fallback EDT
else:
    EASTERN_TZ = datetime.timezone(datetime.timedelta(hours=-4), name="EDT_Fixed")

RELEASE_TIME_THRESHOLD = datetime.time(10, 30, 0)

# Regex Patterns (Unchanged from your provided version)
SUPREME_COURT_DOCKET_REGEX = re.compile(r"\b(A-\d{1,2}-\d{2})\b", re.IGNORECASE)
APPELLATE_DOCKET_REGEX = re.compile(r"\b(A-\d{4,}-\d{2})\b", re.IGNORECASE)
TAX_COURT_DOCKET_REGEX = re.compile(r"\b(\d{6}-\d{4}|\d{4}-\d{4})\b", re.IGNORECASE)

# Mappings (Unchanged from your provided version)
DECISION_TYPE_MAP = {
    "unpublished appellate": ("appUNpub", "Unpublished Appellate", "Appellate Division"),
    "published appellate": ("appPUB", "Published Appellate", "Appellate Division"),
    "supreme": ("supreme", "Supreme Court", "Supreme Court"),
    "unpublished tax": ("taxUNpub", "Unpublished Tax", "Tax Court"),
    "published tax": ("taxPUB", "Published Tax", "Tax Court"),
    "unpublished trial": ("trialUNpub", "Unpublished Trial", "Trial Court"),
    "published trial": ("trialPUB", "Published Trial", "Trial Court"),
}
COUNTY_CODE_MAP = {
    "Atlantic County": "ATL", "Bergen County": "BER", "Burlington County": "BUR", "Camden County": "CAM", "Cape May County": "CPM", "Cumberland County": "CUM", "Essex County": "ESX", "Gloucester County": "GLO", "Hudson County": "HUD", "Hunterdon County": "HNT", "Mercer County": "MER", "Middlesex County": "MID", "Monmouth County": "MON", "Morris County": "MRS", "Ocean County": "OCN", "Passaic County": "PAS", "Salem County": "SLM", "Somerset County": "SOM", "Sussex County": "SSX", "Union County": "UNN", "Warren County": "WRN"
}
LC_DOCKET_VENUE_MAP = [
    (re.compile(r'\b([A-Z]{3})-(DC|LT|SC)-(\d+)-(\d{2})\b', re.IGNORECASE), "Law Division", lambda m: f"Special Civil Part ({m.group(2).upper()})", 1),
    (re.compile(r'\b(DC|LT|SC)-(\d+)-(\d{2})\b', re.IGNORECASE), "Law Division", lambda m: f"Special Civil Part ({m.group(1).upper()})", 1),
    (re.compile(r'\bF(V)-\d{2}-\d+-\d{2}\b', re.IGNORECASE), "Family Division", "Family Violence (FV)", 1),
    (re.compile(r'\bF(D)-\d{2}-\d+-\d{2}\b', re.IGNORECASE), "Family Division", "Dissolution (FD)", 1),
    (re.compile(r'\bF(M)-\d{2}-\d+-\d{2}\b', re.IGNORECASE), "Family Division", "Dissolution (FM)", 1),
    (re.compile(r'\bF(G)-\d{2}-\d+-\d{2}\b', re.IGNORECASE), "Family Division", "Guardianship (FG)", 1),
    (re.compile(r'\bF(P)-\d{2}-\d+-\d{2}\b', re.IGNORECASE), "Family Division", "Termination of Parental Rights (FP)", 1),
    (re.compile(r'\bF([A-Z])-\d{2}-\d+-\d{2}\b', re.IGNORECASE), "Family Division", lambda m: f"Other Family ({m.group(1).upper()})", 1),
    (re.compile(r'\bF-\d+-\d{2}\b', re.IGNORECASE), "Chancery Division", "Foreclosure Part", None),
    (re.compile(r'\bP-\d+-\d{2}\b', re.IGNORECASE), "Chancery Division", "Probate Part", None),
    (re.compile(r'\bC-\d+-\d{2}\b', re.IGNORECASE), "Chancery Division", "General Equity Part", None),
    (re.compile(r'\bSVP-\d+-\d{2}\b', re.IGNORECASE), "Law Division", "Civil Commitment (SVP)", None),
    (re.compile(r'\bL-\d+-\d{2}\b', re.IGNORECASE), "Law Division", "Civil Part", None),
    (re.compile(r'\b(\d{2}-\d{2}-\d{4})\b'), "Law Division", "Criminal Part", None), # Might be too broad
    (re.compile(r'\b(\d{4}-\d{2})\b'), "Law Division - Appellate Part", "Municipal Appeal", None), # Might be too broad
    (re.compile(r'\b(H\d{4}-\d+)\b', re.IGNORECASE), "Agency", "Division on Civil Rights", None),
    (re.compile(r'\b(20\d{2}-\d+)\b', re.IGNORECASE), "Agency", "State Agency Docket", None), # Generic agency
]

def _extract_text_safely(element, joiner=' '):
    if element:
        text_nodes = element.find_all(string=True, recursive=True)
        cleaned = [t.replace('\xa0', ' ').strip() for t in text_nodes]
        return joiner.join(filter(None, cleaned))
    return ""

def _map_decision_info(type_string):
    type_string_lower = type_string.lower().strip() if type_string else ""
    for key, values in DECISION_TYPE_MAP.items():
        if type_string_lower.startswith(key): return values
    return (None, type_string if type_string else "Unknown", "Unknown Court")

def _parse_case_title_details(raw_title_text, opinion_type_venue="Unknown Court"):
    # This function is complex and assumed to be as per your existing version.
    # For brevity, I'm keeping your existing logic for this helper.
    # Ensure it's the same as your GscraperEM.py version.
    # Key aspects:
    # - Extracts CaseName, LCdocketID, LCCounty, OPJURISAPP, StateAgency1/2,
    #   LowerCourtVenue, LowerCourtSubCaseType, CaseNotes, caseconsolidated, recordimpounded.
    # - Uses GsupremescraperEM for Supreme Court cases.
    # - Handles complex parenthetical information.
    # Placeholder for brevity:
    details = {
        'CaseName': raw_title_text.strip(), 'LCdocketID': None, 'LCCounty': None,
        'OPJURISAPP': "Statewide", 'StateAgency1': None, 'StateAgency2': None,
        'LowerCourtVenue': None, 'LowerCourtSubCaseType': None, 'CaseNotes': [],
        'caseconsolidated': 0, 'recordimpounded': 0, 'LinkedDocketIDs': None
    }
    # --- Start of your _parse_case_title_details logic ---
    # (This is a copy of your logic from the provided GscraperEM.py)
    log.debug(f"Parsing raw title ({opinion_type_venue}): {raw_title_text[:100]}...")
    first_paren_index = raw_title_text.find('(')
    paren_content_full = ""
    core_name = raw_title_text.strip()
    if first_paren_index != -1:
        core_name = raw_title_text[:first_paren_index].strip()
        paren_content_full = raw_title_text[first_paren_index:].strip()
    details['CaseName'] = core_name
    note_patterns = {'RECORD IMPOUNDED': 'recordimpounded', 'CONSOLIDATED': 'caseconsolidated', 'RESUBMITTED': None}
    remaining_paren_content = paren_content_full
    extracted_notes = []
    for note_text, flag_key in note_patterns.items():
        pattern = r'(?:^|[\s,(;])\b(' + re.escape(note_text) + r')\b(?:$|[\s,);])'
        matches = list(re.finditer(pattern, remaining_paren_content, re.IGNORECASE))
        offset = 0
        found = False
        for match in matches:
            found = True
            start, end = match.span(0)
            adj_s, adj_e = start - offset, end - offset
            remaining_paren_content = remaining_paren_content[:adj_s] + remaining_paren_content[adj_e:]
            offset += (adj_e - adj_s)
        if found and flag_key:
            details[flag_key] = 1
            log.info(f"Set flag '{flag_key}'=1 for '{core_name}'.")
        elif found and not flag_key: # e.g. RESUBMITTED
            extracted_notes.append(note_text.title()) # Add as a note

    remaining_paren_content = re.sub(r'\s+', ' ', remaining_paren_content).strip(' ,;()')
    log.debug(f"Parens after flags: '{remaining_paren_content}'")
    info_elements = [p.strip() for p in re.split(r'\s*[,;]\s*|\s+AND\s+', remaining_paren_content) if p.strip()]
    log.debug(f"Elements: {info_elements}")

    processed_indices = set()
    found_dockets = [] # Stores dicts: {"docket": str, "venue": str, "subtype": str or None}
    agency_kw = ["DEPARTMENT OF", "BOARD OF", "DIVISION OF", "BUREAU OF", "OFFICE OF", "COMMISSION"]
    # is_agency = any(kw in details['CaseName'].upper() for kw in agency_kw) # Check later
    
    app_docket_sc = None # Potential A-####-YY from Supreme Court section
    found_county = None
    found_opjuris = None # For OPJURISAPP
    found_agencies = []

    for i, element in enumerate(info_elements):
        element_processed_for_docket = False
        # Check for Appellate Docket (A-####-YY) - usually relevant for SC cases pointing to prior AppDiv docket
        app_match = APPELLATE_DOCKET_REGEX.search(element)
        if app_match:
            app_docket_sc = app_match.group(1).strip().upper()
            log.debug(f"Found potential App Docket (maybe for SC context) '{app_docket_sc}' in element '{element}' (idx {i}).")
            # Don't mark processed_indices.add(i) yet, as this element might also contain LC info

        # Check other dockets (LC/Agency)
        for pattern, venue, subtype_info, _ in LC_DOCKET_VENUE_MAP:
            matches = list(pattern.finditer(element))
            if matches:
                for match in matches:
                    docket_str = match.group(0).strip().upper()
                    # Avoid re-adding the AppDocketID if it was already identified as the main AppDocket
                    if docket_str == app_docket_sc: # Compare with the specific A-####-YY found above
                        continue
                    
                    subtype = subtype_info(match) if callable(subtype_info) else subtype_info
                    found_dockets.append({"docket": docket_str, "venue": venue, "subtype": subtype})
                    log.debug(f" Found LC/Agency Docket: {docket_str} -> Venue: {venue}, Subtype: {subtype} (in element '{element}', idx {i})")
                processed_indices.add(i) # Mark this element as processed for dockets
                element_processed_for_docket = True # This element yielded docket(s)
        
        if element_processed_for_docket: # If we found dockets, this element is largely explained
            continue

        # If element wasn't primarily a known docket, check for County, OPJuris, Agency
        if not found_county: # Only find first county
            county_name_match = re.search(r'(?:COUNTY\s+OF\s+)?([A-Za-z\s]+?)\s+COUNTY\b', element, re.IGNORECASE)
            if county_name_match:
                county_full_name = county_name_match.group(1).strip().title() + " County"
                if county_full_name in COUNTY_CODE_MAP:
                    found_county = county_full_name
                    log.debug(f"Found County by name: {found_county} in element '{element}' (idx {i})")
                    processed_indices.add(i); continue
            
            # Check for 3-letter county code if full name not found
            # This is a bit risky as other 3-letter codes might exist.
            # Let's make it specific to common patterns like (XXX)
            county_code_match = re.search(r'\(([A-Z]{3})\)', element) # e.g. (ATL)
            if not county_code_match: # or a standalone code
                 county_code_match = re.fullmatch(r'[A-Z]{3}', element)

            if county_code_match:
                code = county_code_match.group(1)
                matched_county_name = next((name for name, c_code in COUNTY_CODE_MAP.items() if c_code == code), None)
                if matched_county_name:
                    found_county = matched_county_name
                    log.debug(f"Found County by code '{code}': {found_county} in element '{element}' (idx {i})")
                    processed_indices.add(i); continue
        
        if not found_opjuris: # Only find first OPJuris if explicitly stated
            if element.upper() == "STATEWIDE":
                found_opjuris = "Statewide"
                log.debug(f"Found OPJuris: {found_opjuris} in element '{element}' (idx {i})")
                processed_indices.add(i); continue
        
        # Check for State Agencies
        if any(kw in element.upper() for kw in agency_kw) and "COUNTY" not in element.upper():
            # Avoid mistaking things like "DEPARTMENT OF CORRECTIONS, ESSEX COUNTY" as purely agency
            agency_name = element.strip()
            found_agencies.append(agency_name)
            log.debug(f"Found potential State Agency: {agency_name} in element '{element}' (idx {i})")
            processed_indices.add(i); # is_agency = True; # Set later based on context
            continue
            
    # --- Assign results based on parsed info and opinion_type_venue ---
    details['LCCounty'] = found_county
    if found_opjuris: details['OPJURISAPP'] = found_opjuris
    
    if found_agencies:
        details['StateAgency1'] = found_agencies[0]
        if len(found_agencies) > 1: details['StateAgency2'] = found_agencies[1]
        if len(found_agencies) > 2: extracted_notes.append(f"Other Agencies: {', '.join(found_agencies[2:])}")

    primary_lc_info = found_dockets[0] if found_dockets else None

    if opinion_type_venue == "Supreme Court":
        sc_docket_match_in_title = SUPREME_COURT_DOCKET_REGEX.search(raw_title_text) # Search entire title string
        sc_docket_from_title = sc_docket_match_in_title.group(1).strip().upper() if sc_docket_match_in_title else None

        if sc_docket_from_title:
            log.info(f"Supreme Court case. Primary SC Docket (from title): {sc_docket_from_title}. Searching for details...")
            # Pass case caption to supreme scraper search function for better matching
            sc_details = GsupremescraperEM.supreme_scraper.find_matching_case(
                search_docket=sc_docket_from_title,
                case_caption=details['CaseName'] # Use the core name
            )
            if sc_details:
                log.info(f"Supreme scraper found: AppDocket='{sc_details.get('app_docket')}', County='{sc_details.get('county')}', Agency='{sc_details.get('state_agency')}'")
                # The AppDocketID from SC scraper is the underlying AppDiv docket (A-####-YY)
                if sc_details.get('app_docket'):
                    details['LinkedDocketIDs'] = sc_details['app_docket'] # This is the A-####-YY
                
                # If LCCounty is not already found from parens, use SC scraper's county
                if not details['LCCounty'] and sc_details.get('county'):
                    details['LCCounty'] = sc_details['county']
                
                # If StateAgency1 is not already found, use SC scraper's agency
                if not details['StateAgency1'] and sc_details.get('state_agency'):
                    details['StateAgency1'] = sc_details['state_agency']
            else:
                log.warning(f"No additional details found via supreme_scraper for SC Docket {sc_docket_from_title}.")
        else:
            log.warning(f"Supreme Court case type, but no SC Docket (A-##-YY) found in raw title: {raw_title_text}")

        # For SC cases, the LCdocketID field in DB should store the underlying App. Div. (A-####-YY) if available.
        # This was stored in app_docket_sc if found in parens, or from LinkedDocketIDs now.
        details['LCdocketID'] = details.get('LinkedDocketIDs') # This is the A-####-YY
        details['LowerCourtVenue'] = "Appellate Division" # Original venue was App Div
        details['LowerCourtSubCaseType'] = None

        # Add any originally found LC dockets (from trial court level) to notes
        if primary_lc_info:
            note = f"[Original Trial LC: Docket={primary_lc_info.get('docket','N/A')}, Venue={primary_lc_info.get('venue','N/A')}"
            if primary_lc_info.get('subtype'): note += f" ({primary_lc_info.get('subtype')})"
            note += "]"
            extracted_notes.insert(0, note)
            # If LCCounty was not set by supreme_scraper or from parens, try to infer from this original LC
            if not details['LCCounty'] and primary_lc_info.get('venue') != "Agency":
                 # Simplistic inference, requires county to be in venue string (e.g. "Essex County Law Div")
                 for county_name_map, code in COUNTY_CODE_MAP.items():
                     if county_name_map.split(" ")[0].upper() in primary_lc_info.get('venue', '').upper(): # Check "Essex" in "Essex County..."
                         details['LCCounty'] = county_name_map
                         log.debug(f"Inferred LCCounty '{details['LCCounty']}' for SC case from original trial LC venue.")
                         break


    else: # Appellate, Trial, Tax cases
        if primary_lc_info:
            details['LCdocketID'] = primary_lc_info.get('docket')
            details['LowerCourtVenue'] = primary_lc_info.get('venue')
            details['LowerCourtSubCaseType'] = primary_lc_info.get('subtype')
        
        # If LCCounty is not set and venue suggests a county (and not agency)
        if not details['LCCounty'] and details.get('LowerCourtVenue') and "Agency" not in details['LowerCourtVenue']:
             # Attempt to infer county from venue string
             venue_str_for_county_check = details['LowerCourtVenue']
             for element_text in info_elements: # Also check original info_elements
                 if details['LCCounty']: break
                 for county_name_map, code in COUNTY_CODE_MAP.items():
                     if county_name_map.upper() in element_text.upper() or code in element_text:
                         details['LCCounty'] = county_name_map
                         log.debug(f"Inferred LCCounty '{details['LCCounty']}' for non-SC case from info_elements/venue.")
                         break
    
    # General handling for is_agency and LCCounty='NJ'
    is_agency_case = any(kw in details['CaseName'].upper() for kw in agency_kw) or \
                     (details.get('StateAgency1') is not None) or \
                     (details.get('LowerCourtVenue') == "Agency")

    if is_agency_case and details.get('LCCounty') != 'NJ':
        # For true state agency appeals, LCCounty should be 'NJ' or null if statewide.
        # Using 'NJ' to signify it's a NJ state agency appeal rather than a specific county.
        # If a county was found, it might be the agency's location, not the case origin in the same way.
        details['LCCounty'] = 'NJ' 
        log.debug("Set LCCounty='NJ' due to agency indicators.")

    # Backfill StateAgency1 from CaseName if not found but keywords exist
    if not details.get('StateAgency1') and any(kw in details['CaseName'].upper() for kw in agency_kw):
        for keyword in agency_kw: # Try to extract full agency name
            match = re.search(rf'\b({keyword}(?:\s+[A-Z&][a-zA-Z&]+)+)\b', details['CaseName'], re.IGNORECASE)
            if match:
                details['StateAgency1'] = match.group(1).strip().title()
                log.debug(f"Assigned StateAgency1 from CaseName: {details['StateAgency1']}")
                break

    # Add remaining unprocessed parenthetical elements to notes
    for i, element in enumerate(info_elements):
        if i not in processed_indices:
            # Avoid adding elements that are just county names if county already captured
            is_already_captured_county = False
            if details['LCCounty']:
                county_short_name = details['LCCounty'].replace(" County","")
                if county_short_name.upper() in element.upper() or \
                   COUNTY_CODE_MAP.get(details['LCCounty']) == element.upper():
                    is_already_captured_county = True
            if not is_already_captured_county:
                 extracted_notes.append(element)


    # Default LowerCourtVenue if still None (for App/Trial/Tax)
    if opinion_type_venue != "Supreme Court" and not details['LowerCourtVenue']:
        details['LowerCourtVenue'] = "Unknown" # Default if no specific LC venue parsed

    # Add note if LC Docket ID is missing for non-SC cases that are not Agency
    if opinion_type_venue != "Supreme Court" and not details['LCdocketID'] and \
       details.get('LowerCourtVenue') != "Agency" and \
       (not is_agency_case): # Further refinement: only if not an agency case
        note = "[LC Docket Missing]"
        if note not in extracted_notes:
            extracted_notes.append(note)
            log.warning(f"Added note '{note}' for '{core_name}' ({opinion_type_venue}).")

    details['CaseNotes'] = ", ".join(sorted(list(set(filter(None, extracted_notes))))) or None
    log.debug(f"Parsed title FINAL ({opinion_type_venue}): CoreName='{details['CaseName']}', LC Docket='{details['LCdocketID']}', LCVenue='{details['LowerCourtVenue']}', LCCounty='{details['LCCounty']}', Notes='{details['CaseNotes']}'")
    return details
    # --- End of your _parse_case_title_details logic ---

def _parse_case_article(article_element, release_date_iso):
    # This function is complex and assumed to be as per your existing version.
    # For brevity, I'm keeping your existing logic for this helper.
    # Ensure it's the same as your GscraperEM.py version.
    # Key aspects:
    # - Extracts primary_docket_id, opinion_type_venue, decision_code/text.
    # - Calculates opinion_status using timezone.
    # - Calls _parse_case_title_details.
    # - Handles multiple primary dockets.
    # Placeholder for brevity:
    case_data_list = []
    raw_title_text = "N/A" # For error logging
    try:
        # --- Start of your _parse_case_article logic ---
        # (This is a copy of your logic from the provided GscraperEM.py)
        log.debug("Parsing case article...")
        card_body = article_element.find('div', class_='card-body')
        if not card_body: log.warning("Missing card-body in article."); return None
        
        no_opinions = card_body.find(string=re.compile(r'no\s+.*\s+opinions\s+reported', re.IGNORECASE))
        if no_opinions: log.info(f"Skipping 'No opinions reported' entry."); return None
        
        title_div = card_body.find('div', class_=re.compile(r'card-title\b.*\btext-start\b'))
        if not title_div: log.warning("Missing title div in card-body."); return None
        
        raw_title_text = _extract_text_safely(title_div)
        if not raw_title_text: log.warning("Title text is empty."); return None

        # Identify Opinion Type and Primary Docket from Badges
        badge_spans = card_body.find_all('span', class_='badge')
        primary_docket_id, primary_docket_badge_text = None, None
        decision_code, decision_text, opinion_type_venue = None, None, "Unknown Court"

        # First pass: Find primary docket ID and establish opinion_type_venue
        for span in badge_spans:
            span_text = _extract_text_safely(span).strip()
            if not span_text: continue
            
            sc_match = SUPREME_COURT_DOCKET_REGEX.search(span_text)
            if sc_match: 
                primary_docket_id = sc_match.group(1).strip().upper()
                opinion_type_venue = "Supreme Court"
                # For SC, decision type is fixed
                decision_code, decision_text, _ = DECISION_TYPE_MAP["supreme"]
                primary_docket_badge_text = span_text; break 
            
            app_match = APPELLATE_DOCKET_REGEX.search(span_text)
            if app_match:
                primary_docket_id = app_match.group(1).strip().upper()
                opinion_type_venue = "Appellate Division"
                primary_docket_badge_text = span_text; break

            tax_match = TAX_COURT_DOCKET_REGEX.search(span_text)
            if tax_match:
                primary_docket_id = tax_match.group(1).strip().upper() # Tax dockets can be like 001234-2023 or 1234-2023
                opinion_type_venue = "Tax Court"
                primary_docket_badge_text = span_text; break
            
            # Check for Trial Court dockets (more complex patterns)
            for pattern, _, _, _ in LC_DOCKET_VENUE_MAP:
                 # Use fullmatch for trial dockets if they are expected to be the sole content of a badge
                 # Or search if they can be part of a larger string
                 match = pattern.search(span_text) # Using search for broader match
                 if match and match.group(0).strip().upper() == span_text.upper(): # Ensure it's the main content
                    primary_docket_id = match.group(0).strip().upper()
                    opinion_type_venue = "Trial Court"
                    primary_docket_badge_text = span_text; break
            if primary_docket_id: break # Found primary docket

        if not primary_docket_id:
            log.warning(f"No Primary Docket ID badge found for article with title starting: '{raw_title_text[:50]}...'. Skipping article.")
            return None

        # Second pass (if not Supreme Court): Find decision type text from other badges
        if opinion_type_venue != "Supreme Court":
            for span in badge_spans:
                 span_text = _extract_text_safely(span).strip()
                 if not span_text or span_text == primary_docket_badge_text: continue # Skip empty or primary docket badge
                 
                 mapped_code, mapped_text, mapped_venue_from_type = _map_decision_info(span_text)
                 if mapped_code:
                     # Ensure the type's venue matches the established opinion_type_venue or helps define it
                     if opinion_type_venue == "Unknown Court" or opinion_type_venue == mapped_venue_from_type:
                         decision_code, decision_text = mapped_code, mapped_text
                         if opinion_type_venue == "Unknown Court": # If venue wasn't set by docket, set by type
                             opinion_type_venue = mapped_venue_from_type
                         break # Found decision type
        
        if not decision_code and opinion_type_venue != "Supreme Court":
            log.warning(f"No decision type badge found for {primary_docket_id} ({opinion_type_venue}). Defaulting type text.")
            decision_text = f"Unknown {opinion_type_venue} Type" # e.g. "Unknown Appellate Division Type"
            # decision_code remains None

        # Calculate Opinion Status
        opinion_status = 0 # Default to Expected (0)
        if release_date_iso and EASTERN_TZ:
            try:
                release_date_obj = datetime.datetime.strptime(release_date_iso, '%Y-%m-%d').date()
                release_dt_aware = datetime.datetime.combine(release_date_obj, RELEASE_TIME_THRESHOLD, tzinfo=EASTERN_TZ)
                now_aware = datetime.datetime.now(EASTERN_TZ)
                if now_aware >= release_dt_aware:
                    opinion_status = 1 # Released (1)
                log.debug(f"Status check for {primary_docket_id}: ReleaseDateTime_ET={release_dt_aware}, Now_ET={now_aware}, Status={opinion_status}")
            except ValueError as ve: log.warning(f"Date parse error for status check on '{release_date_iso}': {ve}")
            except Exception as e_stat: log.error(f"Error calculating opinion status for {primary_docket_id}: {e_stat}", exc_info=True)
        elif not release_date_iso: log.warning(f"Cannot calculate opinion status for {primary_docket_id}: Release date is unknown.")
        else: log.warning(f"Cannot calculate opinion status for {primary_docket_id}: Timezone info (EASTERN_TZ) is unavailable.")

        # Parse Title Details
        title_details = _parse_case_title_details(raw_title_text, opinion_type_venue)

        # Handle multiple primary dockets if they were grouped in the primary_docket_badge_text
        all_primary_dockets = [primary_docket_id] # Start with the one found
        if primary_docket_badge_text:
             primary_regex_for_multi = None
             if opinion_type_venue == "Supreme Court": primary_regex_for_multi = SUPREME_COURT_DOCKET_REGEX
             elif opinion_type_venue == "Appellate Division": primary_regex_for_multi = APPELLATE_DOCKET_REGEX
             elif opinion_type_venue == "Tax Court": primary_regex_for_multi = TAX_COURT_DOCKET_REGEX
             # Trial court dockets are usually unique enough not to be comma-separated in a single badge
             
             if primary_regex_for_multi:
                  found_in_badge = primary_regex_for_multi.findall(primary_docket_badge_text)
                  if len(found_in_badge) > 1: # If more than one of the same type found
                      all_primary_dockets = sorted(list(set(d.strip().upper() for d in found_in_badge))) # Unique, sorted
                      log.info(f"Multiple primary dockets of type {opinion_type_venue} found in badge '{primary_docket_badge_text}': {all_primary_dockets}")

        # Create Data Records
        for i, current_pd_id in enumerate(all_primary_dockets):
            # LinkedDocketIDs in this context are other primary dockets from the same badge
            # The _parse_case_title_details handles SC's underlying AppDiv docket in its 'LinkedDocketIDs'
            linked_badge_dockets = [d for j, d in enumerate(all_primary_dockets) if i != j]
            
            # Combine linked dockets from badge with those from title parsing (especially for SC cases)
            final_linked_dockets = set(linked_badge_dockets)
            if title_details.get('LinkedDocketIDs'): # This is A-####-YY for SC cases
                final_linked_dockets.add(title_details['LinkedDocketIDs'])
            
            # Ensure the current_pd_id itself isn't in its own linked list
            final_linked_dockets.discard(current_pd_id)

            case_data = {
                "AppDocketID": current_pd_id, 
                "ReleaseDate": release_date_iso,
                "LinkedDocketIDs": ", ".join(sorted(list(final_linked_dockets))) if final_linked_dockets else None,
                "CaseName": title_details.get('CaseName'), 
                "LCdocketID": title_details.get('LCdocketID'),
                "LCCounty": title_details.get('LCCounty'), 
                "Venue": opinion_type_venue,
                "LowerCourtVenue": title_details.get('LowerCourtVenue'),
                "LowerCourtSubCaseType": title_details.get('LowerCourtSubCaseType'),
                "OPJURISAPP": title_details.get('OPJURISAPP'),
                "DecisionTypeCode": decision_code, 
                "DecisionTypeText": decision_text,
                "StateAgency1": title_details.get('StateAgency1'), 
                "StateAgency2": title_details.get('StateAgency2'),
                "CaseNotes": title_details.get('CaseNotes'),
                "caseconsolidated": title_details.get('caseconsolidated', 0),
                "recordimpounded": title_details.get('recordimpounded', 0),
                "opinionstatus": opinion_status
            }
            log.debug(f"Parsed data record: AppDocket={case_data['AppDocketID']}, Release={case_data['ReleaseDate']}, Status={case_data['opinionstatus']}, Venue={case_data['Venue']}")
            case_data_list.append(case_data)
        
        return case_data_list
        # --- End of your _parse_case_article logic ---
    except Exception as e:
        log.error(f"Critical error parsing article with title '{raw_title_text[:70]}...': {e}", exc_info=True)
        return None


def fetch_and_parse_opinions(url_or_file_path=DEFAULT_SCRAPE_SOURCE):
    """Fetches HTML from a URL or reads from a local file, then parses opinion articles."""
    log.info(f"Processing opinions from source: {url_or_file_path}")
    opinions_data_list, release_date_str_iso = [], None
    html_content = None

    if os.path.exists(url_or_file_path) and os.path.isfile(url_or_file_path):
        log.info(f"Reading HTML from local file: {url_or_file_path}")
        try:
            with open(url_or_file_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
            log.info(f"Local HTML file '{url_or_file_path}' read successfully.")
        except IOError as e:
            log.error(f"IOError reading local HTML file {url_or_file_path}: {e}")
            print(f"Error: Could not read file {url_or_file_path}.")
            return [], None
        except Exception as e:
            log.error(f"Unexpected error reading local HTML file {url_or_file_path}: {e}", exc_info=True)
            return [], None
    else:
        log.info(f"Fetching HTML from URL: {url_or_file_path}")
        try:
            response = requests.get(url_or_file_path, headers=HEADERS, timeout=30)
            response.raise_for_status()
            html_content = response.text
            log.info(f"Fetch from {url_or_file_path} OK, content length: {len(html_content)}")
        except requests.exceptions.RequestException as e:
            log.error(f"RequestException: Fetch fail for {url_or_file_path}: {e}")
            print(f"Error: Connection failure for {url_or_file_path}.")
            return [], None

    if not html_content:
        log.error("No HTML content obtained to parse.")
        return [], None

    soup = BeautifulSoup(html_content, "html.parser")

    # Extract Release Date from H2 header
    try:
        date_header_element = soup.select_one('div.view-header h2') # More specific selector
        raw_date_text_from_header = _extract_text_safely(date_header_element) if date_header_element else None
        
        if raw_date_text_from_header:
            log.debug(f"Raw date text from header: '{raw_date_text_from_header}'")
            # Example: "Expected Opinions for Friday, May 9, 2025" or "Opinions Posted on May 9, 2025"
            # More flexible regex to capture various date formats
            date_match = re.search(r'(?:for|on|as\s+of)\s+([A-Za-z]+,\s+[A-Za-z]+\s+\d{1,2},\s+\d{4}|\w+\s+\d{1,2},\s+\d{4})', raw_date_text_from_header, re.IGNORECASE)
            parsed_date_str_from_header = date_match.group(1).strip() if date_match else None

            if parsed_date_str_from_header:
                log.info(f"Extracted date string from header: '{parsed_date_str_from_header}'")
                try:
                    release_datetime_obj = date_parse(parsed_date_str_from_header)
                    release_date_str_iso = release_datetime_obj.strftime('%Y-%m-%d')
                    log.info(f"Parsed release date from header: {release_date_str_iso}")
                except Exception as date_err:
                    log.warning(f"Date parsing failed for header string '{parsed_date_str_from_header}': {date_err}. Using raw string.")
                    # Fallback to trying to parse the raw string if regex fails but text exists
                    try:
                        release_datetime_obj = date_parse(raw_date_text_from_header.split("on ",1)[1]) # Try simple split
                        release_date_str_iso = release_datetime_obj.strftime('%Y-%m-%d')
                        log.info(f"Fallback Parsed release date: {release_date_str_iso}")
                    except:
                        release_date_str_iso = raw_date_text_from_header # As last resort, use unparsed
            else:
                log.warning(f"No date pattern matched in H2 header: '{raw_date_text_from_header}'.")
        else:
            log.warning("Release date H2 header not found or empty in HTML.")
    except Exception as e:
        log.error(f"Error extracting or parsing release date from header: {e}", exc_info=True)

    if not release_date_str_iso:
        log.error("Critical: Release date could not be determined from the source. Further processing might be affected.")
        # Decide if to proceed or not. For testing archived files, this might be acceptable if date is in filename.
        # print("Warning: Release date is unknown for this source.")

    # Find opinion articles/cards
    main_content_area = soup.find('main', id='main-content') or soup # Fallback to whole soup
    # Common patterns for article containers
    potential_article_elements = main_content_area.find_all('article', class_='w-100') 
    if not potential_article_elements:
        potential_article_elements = main_content_area.select('div.card.views-row') # Another common pattern
    if not potential_article_elements:
        potential_article_elements = main_content_area.select('div.views-row') # More generic

    log.info(f"Found {len(potential_article_elements)} potential opinion containers/articles in the source.")

    processed_article_count, skipped_article_count = 0, 0
    for article_el in potential_article_elements:
        parsed_cases_from_article = _parse_case_article(article_el, release_date_str_iso)
        if parsed_cases_from_article:
            opinions_data_list.extend(parsed_cases_from_article)
            processed_article_count += 1 # Count articles, not individual dockets if multi-docket article
        else:
            skipped_article_count += 1
            
    total_opinions_extracted = len(opinions_data_list)
    log.info(f"HTML parsing complete. Processed Articles: {processed_article_count}, Skipped Articles: {skipped_article_count}, Total Opinions Extracted: {total_opinions_extracted}.")
    if total_opinions_extracted == 0 and processed_article_count == 0 and skipped_article_count > 0:
        log.warning("Processed 0 opinion entries, all articles were skipped. Check article parsing logic or HTML structure.")
    elif total_opinions_extracted == 0 and len(potential_article_elements) > 0:
        log.warning("Found potential article elements but extracted 0 opinions. Check selectors and parsing logic within _parse_case_article.")
    elif total_opinions_extracted == 0 and len(potential_article_elements) == 0:
        log.warning("No potential opinion containers found in the HTML. Check page structure or selectors.")

    return opinions_data_list, release_date_str_iso

# === End of GscraperEM.py ===