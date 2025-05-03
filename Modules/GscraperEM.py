# GscraperEM.py
# V4: Corrected Supreme Court Docket Handling (A-##-YY)
"""
Handles fetching and parsing the NJ Courts 'Expected Opinions' page.
*** V4 Updates ***
Corrected Supreme Court Docket Handling: AppDocketID = A-##-YY (short), LCdocketID = A-####-YY (long, from parens).
Moves all other SC paren info (except Statewide) to CaseNotes.
*** V3 Updates ***
Handles Supreme Court cases (extracting SC Docket, App Docket, and Orig LC info).
Handles Trial and Tax Court opinions.
Refined LC_DOCKET_VENUE_MAP venue/subtype strings.
Improved parsing logic based on opinion type (Supreme, Appellate, Trial, Tax).
*** Prior Updates ***
Refined Special Civil Part docket extraction and added specific CaseNotes format.
Updated County Code mapping based on user input. Corrected Special Civil Part venue string.
Added extraction for 'Consolidated' and 'Record Impounded' flags. Set LCCounty='NJ' for agency cases.
Added patterns for Municipal Appeal and Probate dockets. Defaults OPJURISAPP to Statewide.
Adds note if LC Docket ID is missing.
"""
import datetime
import requests
from bs4 import BeautifulSoup
import logging
import re
import os
from dateutil.parser import parse as date_parse
from dateutil.tz import gettz # For timezone awareness if needed

log = logging.getLogger(__name__)

# Constants
BASE_URL = "https://www.njcourts.gov"
PAGE_URL = os.path.join(BASE_URL, "attorneys/opinions/expected")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# Regex Patterns
# Corrected Supreme Court Docket Regex (A-##-YY or A-#-YY)
SUPREME_COURT_DOCKET_REGEX = re.compile(r"\b(A-\d{1,2}-\d{2})\b", re.IGNORECASE)
# Standard Appellate Docket Regex (A-####-YY)
APPELLATE_DOCKET_REGEX = re.compile(r"\b(A-\d{4,}-\d{2})\b", re.IGNORECASE)
# Tax Court Docket Regex (e.g., 00xxxx-YYYY or NNNN-YYYY) - needs verification
TAX_COURT_DOCKET_REGEX = re.compile(r"\b(\d{6}-\d{4}|\d{4}-\d{4})\b", re.IGNORECASE) # Example: 001234-2023 or 1234-2023


# Mappings for Decision Type and Venue (of the opinion being released)
# Updated Supreme Court definition based on docket correction
DECISION_TYPE_MAP = {
    "unpublished appellate": ("appUNpub", "Unpublished Appellate", "Appellate Division"),
    "published appellate": ("appPUB", "Published Appellate", "Appellate Division"),
    "supreme": ("supreme", "Supreme Court", "Supreme Court"), # Key used if SC docket found
    "unpublished tax": ("taxUNpub", "Unpublished Tax", "Tax Court"),
    "published tax": ("taxPUB", "Published Tax", "Tax Court"),
    "unpublished trial": ("trialUNpub", "Unpublished Trial", "Trial Court"),
    "published trial": ("trialPUB", "Published Trial", "Trial Court"),
}

# --- County Code Mapping (Unchanged) ---
COUNTY_CODE_MAP = {
    "Atlantic County": "ATL", "Bergen County": "BER", "Burlington County": "BUR",
    "Camden County": "CAM", "Cape May County": "CPM", "Cumberland County": "CUM",
    "Essex County": "ESX", "Gloucester County": "GLO", "Hudson County": "HUD",
    "Hunterdon County": "HNT", "Mercer County": "MER", "Middlesex County": "MID",
    "Monmouth County": "MON", "Morris County": "MRS", "Ocean County": "OCN",
    "Passaic County": "PAS", "Salem County": "SLM", "Somerset County": "SOM",
    "Sussex County": "SSX", "Union County": "UNN", "Warren County": "WRN"
}


# --- Lower Court Venue/Subtype Mapping (Unchanged from V3) ---
# This map is primarily for identifying dockets *within the parenthetical text*
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
    (re.compile(r'\b(\d{2}-\d{2}-\d{4})\b'), "Law Division", "Criminal Part", None),
    (re.compile(r'\b(\d{4}-\d{2})\b'), "Law Division - Appellate Part", "Municipal Appeal", None),
    (re.compile(r'\b(H\d{4}-\d+)\b', re.IGNORECASE), "Agency", "Division on Civil Rights", None),
    (re.compile(r'\b(20\d{2}-\d+)\b', re.IGNORECASE), "Agency", "State Agency Docket", None),
]

# --- Helper Functions ---
def _extract_text_safely(element, joiner=' '):
    """Safely extracts and joins text from a BeautifulSoup element."""
    if element:
        text_nodes = element.find_all(string=True, recursive=True)
        cleaned_text = [text.replace('\xa0', ' ').strip() for text in text_nodes]
        return joiner.join(filter(None, cleaned_text))
    return ""

def _map_decision_info(type_string):
    """Maps raw decision type text to code, text, and opinion venue."""
    type_string_lower = type_string.lower().strip() if type_string else ""
    for key, values in DECISION_TYPE_MAP.items():
        # Use "startswith" for more flexible matching (e.g., "Supreme Court Opinion")
        if type_string_lower.startswith(key):
            return values # Returns (code, text, venue) tuple
    # If no known type matched, return unknown
    return (None, type_string if type_string else "Unknown", "Unknown Court")


# --- Case Title Parsing (for parenthetical info) ---
# Updated to handle SC specific note extraction
def _parse_case_title_details(raw_title_text, opinion_type_venue="Unknown Court"):
    """
    Parses the raw case title string to extract details from parenthetical info.
    Handles different expectations based on the opinion_type_venue (Supreme, Appellate, Trial, Tax).
    For Supreme Court, finds App Div docket (A-####-YY) for LCdocketID and moves other info to CaseNotes.
    """
    details = {
        'CaseName': raw_title_text.strip(),
        'LCdocketID': None, # Specific meaning varies by opinion type
        'LCCounty': None,
        'OPJURISAPP': "Statewide", # Default
        'StateAgency1': None, 'StateAgency2': None,
        'LowerCourtVenue': None, # Venue corresponding to LCdocketID
        'LowerCourtSubCaseType': None, # Subtype corresponding to LCdocketID
        'CaseNotes': [], # Start as list
        'caseconsolidated': 0,
        'recordimpounded': 0,
        # No longer need AppellateDocketForSupreme - LCdocketID serves this for SC cases
        # No longer need OriginalLCTextForSupreme - info goes direct to notes
    }
    log.debug(f"Parsing raw title ({opinion_type_venue}): {raw_title_text[:100]}...")

    # Separate core name from parenthetical info
    first_paren_index = raw_title_text.find('(')
    paren_content_full = ""
    core_name = raw_title_text.strip()
    if first_paren_index != -1:
        core_name = raw_title_text[:first_paren_index].strip()
        paren_content_full = raw_title_text[first_paren_index:].strip()
    details['CaseName'] = core_name

    # --- Extract Binary Flags and Remove from Paren Content ---
    note_patterns_text = {
        'RECORD IMPOUNDED': 'recordimpounded',
        'CONSOLIDATED': 'caseconsolidated',
        'RESUBMITTED': None # Keep as note
    }
    remaining_paren_content = paren_content_full
    extracted_notes_for_field = [] # Notes destined for the CaseNotes field

    for note_text, flag_key in note_patterns_text.items():
        note_pattern = r'(?:^|[\s,(;])\b(' + re.escape(note_text) + r')\b(?:$|[\s,);])'
        matches = list(re.finditer(note_pattern, remaining_paren_content, re.IGNORECASE))
        offset = 0
        found_flag = False
        for match in matches:
            matched_string = match.group(1)
            log.debug(f"Found flag pattern '{matched_string}' in paren content.")
            found_flag = True
            start, end = match.span(0)
            adj_start, adj_end = start - offset, end - offset
            remaining_paren_content = remaining_paren_content[:adj_start] + remaining_paren_content[adj_end:]
            offset += (adj_end - adj_start)

        if found_flag and flag_key:
            details[flag_key] = 1
            log.info(f"Set binary flag '{flag_key}'=1 for case '{core_name}'.")
        elif found_flag and not flag_key:
             extracted_notes_for_field.append(note_text.title()) # Add 'Resubmitted' etc. to notes

    remaining_paren_content = re.sub(r'\s+', ' ', remaining_paren_content).strip(' ,;()')
    log.debug(f"Paren content after flag removal: '{remaining_paren_content}'")

    # --- Process remaining parenthetical content ---
    info_elements = [p.strip() for p in re.split(r'\s*[,;]\s*|\s+AND\s+', remaining_paren_content) if p.strip()]
    log.debug(f"Elements from remaining parens: {info_elements}")

    processed_elements_indices = set() # Track indices of elements used
    found_dockets_details = [] # Store details of *all* dockets found
    agency_keywords = ["DEPARTMENT OF", "BOARD OF", "DIVISION OF", "BUREAU OF", "OFFICE OF", "COMMISSION"]
    is_agency_appeal = any(keyword in details['CaseName'].upper() for keyword in agency_keywords)
    found_appellate_docket_for_sc = None # Store the A-####-YY if found for SC case
    found_county = None
    found_opjuris = None # Store if 'Statewide' specifically found
    found_agencies = []

    # --- Process elements ---
    for i, element in enumerate(info_elements):
        element_processed = False # Track if this element yields useful info

        # 1. Check for Appellate Docket (A-####-YY) -> Needed for SC LCdocketID
        app_match = APPELLATE_DOCKET_REGEX.search(element)
        if app_match:
            found_appellate_docket_for_sc = app_match.group(1).strip().upper()
            log.debug(f"Found potential Appellate Docket '{found_appellate_docket_for_sc}' in parens (element {i}).")
            processed_elements_indices.add(i)
            element_processed = True
            # Continue checking element for other info like county

        # 2. Check for Lower Court / Agency dockets using the map
        for pattern, venue, subtype_info, _ in LC_DOCKET_VENUE_MAP:
            matches = list(pattern.finditer(element))
            if matches:
                for match in matches:
                    docket_str = match.group(0).strip()
                    # Skip if it's the long A-# we already found
                    if docket_str.upper() == found_appellate_docket_for_sc: continue

                    subtype = None
                    if subtype_info:
                        if callable(subtype_info): subtype = subtype_info(match)
                        else: subtype = subtype_info
                    docket_detail = {"docket": docket_str, "venue": venue, "subtype": subtype, "element_index": i}
                    found_dockets_details.append(docket_detail)
                    log.debug(f"  Found LC/Agency Docket Detail: {docket_str} -> Venue: {venue}, Subtype: {subtype} (element {i})")
                processed_elements_indices.add(i)
                element_processed = True
                # Don't break inner loop, element might contain multiple dockets

        # 3. Check for County (if not already found and element not fully processed)
        if not found_county and not element_processed: # Only check if not used for docket
            county_name_match = re.search(r'(?:COUNTY\s+OF\s+)?([A-Za-z\s]+?)\s+COUNTY\b', element, re.IGNORECASE)
            if county_name_match:
                 county_name = county_name_match.group(1).strip().title() + " County"
                 if county_name in COUNTY_CODE_MAP:
                     found_county = county_name
                     log.debug(f"Extracted LCCounty (Full Name): {found_county} (element {i})")
                     processed_elements_indices.add(i)
                     element_processed = True

            if not found_county: # Check code only if full name not found
                 code_match = re.search(r'\b([A-Z]{3})\b', element)
                 if code_match:
                     county_name_from_code = next((name for name, c in COUNTY_CODE_MAP.items() if c == code_match.group(1)), None)
                     if county_name_from_code:
                         found_county = county_name_from_code
                         log.debug(f"Extracted LCCounty (Code '{code_match.group(1)}'): {found_county} (element {i})")
                         processed_elements_indices.add(i)
                         element_processed = True

        # 4. Check for OPJURISAPP (Statewide) (if not already found and element not fully processed)
        if not found_opjuris and not element_processed:
             # Use exact match for statewide within an element to avoid partials
             if element.upper() == "STATEWIDE":
                 found_opjuris = "Statewide"
                 log.debug(f"Confirmed OPJURISAPP: Statewide (element {i})")
                 processed_elements_indices.add(i)
                 element_processed = True
             # Add other specific jurisdictions if needed

        # 5. Check for Agency Names (if element not fully processed)
        if not element_processed:
            if any(keyword in element.upper() for keyword in agency_keywords) and "COUNTY" not in element.upper():
                found_agencies.append(element.strip())
                processed_elements_indices.add(i)
                is_agency_appeal = True
                log.debug(f"Found potential agency name: {element.strip()} (element {i})")
                element_processed = True


    # --- Assign Extracted Info and Handle Leftovers ---
    details['LCCounty'] = found_county
    if found_opjuris: details['OPJURISAPP'] = found_opjuris # Override default only if explicitly found

    if found_agencies:
        details['StateAgency1'] = found_agencies[0]
        if len(found_agencies) > 1: details['StateAgency2'] = found_agencies[1]
        # Add remaining agencies to notes
        if len(found_agencies) > 2: extracted_notes_for_field.append(f"Other Agencies: {', '.join(found_agencies[2:])}")

    # Determine primary LC Docket/Venue based on non-Appellate dockets found
    primary_lc_docket_info = None
    if found_dockets_details:
        primary_lc_docket_info = found_dockets_details[0] # Default to first found
        for docket_info in found_dockets_details:
             if docket_info.get("venue") != "Unknown": # Prioritize known venues
                 primary_lc_docket_info = docket_info
                 break

    # --- Type-Specific Assignments ---
    if opinion_type_venue == "Supreme Court":
        details['LCdocketID'] = found_appellate_docket_for_sc # A-####-YY becomes LCdocketID
        details['LowerCourtVenue'] = "Appellate Division" # Appeal is FROM App Div
        details['LowerCourtSubCaseType'] = None
        # All other found info (LC Dockets, County, Agencies, unprocessed elements) goes into notes
        if primary_lc_docket_info:
             orig_lc_text = f"[Original LC: Docket={primary_lc_docket_info.get('docket', 'N/A')}, Venue={primary_lc_docket_info.get('venue', 'N/A')}"
             if primary_lc_docket_info.get('subtype'): orig_lc_text += f" ({primary_lc_docket_info.get('subtype')})"
             orig_lc_text += "]"
             extracted_notes_for_field.insert(0, orig_lc_text)
        # Add county if found and not part of primary LC note already
        if found_county and (not primary_lc_docket_info or found_county not in primary_lc_docket_info.get('venue','')):
             extracted_notes_for_field.append(f"[County: {found_county}]")
        # Add agencies
        if details['StateAgency1']: extracted_notes_for_field.append(f"[Agency1: {details['StateAgency1']}]")
        if details['StateAgency2']: extracted_notes_for_field.append(f"[Agency2: {details['StateAgency2']}]")
        # Add unprocessed elements to notes
        for i, element in enumerate(info_elements):
             if i not in processed_elements_indices:
                  # Don't add back the App Docket or Statewide if OPJURISAPP is set
                  if element.upper() == found_appellate_docket_for_sc: continue
                  if element.upper() == "STATEWIDE" and details['OPJURISAPP'] == "Statewide": continue
                  extracted_notes_for_field.append(element)

        log.info(f"Supreme Court Case: LCdocketID='{details['LCdocketID']}', Orig LC info moved to notes.")

    else: # Appellate, Trial, Tax
        if primary_lc_docket_info:
            details['LCdocketID'] = primary_lc_docket_info.get('docket')
            details['LowerCourtVenue'] = primary_lc_docket_info.get('venue')
            details['LowerCourtSubCaseType'] = primary_lc_docket_info.get('subtype')
        # Add unprocessed elements to notes
        for i, element in enumerate(info_elements):
             if i not in processed_elements_indices:
                  # Don't add back Statewide if OPJURISAPP is set
                  if element.upper() == "STATEWIDE" and details['OPJURISAPP'] == "Statewide": continue
                  extracted_notes_for_field.append(element)

    # Set LCCounty='NJ' for Agency cases if not already set
    if is_agency_appeal and details['LCCounty'] != 'NJ':
        details['LCCounty'] = "NJ"
        log.debug("Setting LCCounty='NJ' for Agency appeal.")
        if not details['StateAgency1']: # Backfill Agency1 from case name
             for keyword in agency_keywords:
                 match = re.search(rf'\b({keyword}(?:\s+[A-Z][a-zA-Z]+)+)\b', details['CaseName'], re.IGNORECASE)
                 if match:
                     details['StateAgency1'] = match.group(1).strip()
                     log.debug(f"Assigned StateAgency1 from case name: {details['StateAgency1']}")
                     break

    # Default Lower Court Venue if still unknown (unless SC case)
    if opinion_type_venue != "Supreme Court" and not details['LowerCourtVenue']:
        details['LowerCourtVenue'] = "Unknown"


    # Add missing LC docket note if applicable (excluding SC where LC = App Div)
    if opinion_type_venue != "Supreme Court":
        lc_id_field = details.get('LCdocketID')
        # Add note if LCdocketID is empty AND it's not expected (e.g., not an Agency case)
        if not lc_id_field and details['LowerCourtVenue'] != "Agency":
            note_text = "[LC Docket Missing]"
            if note_text not in extracted_notes_for_field:
                 extracted_notes_for_field.append(note_text)
                 log.warning(f"'{note_text}' for case '{core_name}' (Type: {opinion_type_venue}).")

    # Final notes assembly
    final_notes_list = sorted(list(set(filter(None, extracted_notes_for_field))))
    details['CaseNotes'] = ", ".join(final_notes_list) if final_notes_list else None

    log.debug(f"Parsed title details FINAL ({opinion_type_venue}): Name='{details['CaseName'][:50]}...', LC Docket='{details['LCdocketID']}', LC Venue='{details['LowerCourtVenue']}', County='{details['LCCounty']}', OPJuris='{details['OPJURISAPP']}', Notes='{details['CaseNotes']}', Consol={details['caseconsolidated']}, Impound={details['recordimpounded']}")
    return details


# --- _parse_case_article (Updated for SC Docket Logic) ---
def _parse_case_article(article_element, release_date_iso):
    """
    Parses a single <article> element containing case information.
    Handles different logic based on detected opinion type (Supreme, Appellate, Trial, Tax).
    Correctly identifies SC dockets (A-##-YY).
    """
    case_data_list = []
    log.debug("Parsing case article...")
    raw_title_text = "N/A" # For error logging
    try:
        card_body = article_element.find('div', class_='card-body')
        if not card_body:
            log.warning("Article missing card-body div. Skipping.")
            return None

        no_opinions_message = card_body.find(string=re.compile(r'no\s+.*\s+opinions\s+reported', re.IGNORECASE))
        if no_opinions_message:
            log.info(f"Skipping 'No opinions reported' message: '{no_opinions_message.strip()}'")
            return None

        title_div = card_body.find('div', class_=re.compile(r'card-title\b.*\btext-start\b'))
        if not title_div:
            log.warning("Could not find title div (card-title text-start). Skipping article.")
            return None

        raw_title_text = _extract_text_safely(title_div)
        if not raw_title_text:
            log.warning("Title div found but contained no text. Skipping article.")
            return None

        # --- Identify Opinion Type and Primary Docket from Badges ---
        badge_spans = card_body.find_all('span', class_='badge')
        primary_docket_id = None
        primary_docket_badge_text = None # Store the text where the primary docket was found
        raw_decision_type_text = None
        decision_code, decision_text, opinion_type_venue = None, None, "Unknown Court"

        # First pass: Find the primary docket ID - this determines the type
        for span in badge_spans:
            span_text = _extract_text_safely(span).strip()
            if not span_text: continue

            # Check for Supreme Court Docket (A-##-YY) FIRST
            sc_match = SUPREME_COURT_DOCKET_REGEX.search(span_text)
            if sc_match:
                primary_docket_id = sc_match.group(1).strip().upper()
                primary_docket_badge_text = span_text
                opinion_type_venue = "Supreme Court"
                decision_code, decision_text, _ = DECISION_TYPE_MAP["supreme"]
                log.debug(f"Identified Opinion Type: Supreme Court based on docket '{primary_docket_id}' from badge '{span_text}'")
                break # Found primary docket and type

            # Check for Appellate Docket (A-####-YY)
            app_match = APPELLATE_DOCKET_REGEX.search(span_text)
            if app_match:
                primary_docket_id = app_match.group(1).strip().upper()
                primary_docket_badge_text = span_text
                opinion_type_venue = "Appellate Division" # Assume App Div if long A-# found
                log.debug(f"Identified Opinion Type: Appellate Division based on docket '{primary_docket_id}' from badge '{span_text}'")
                # Decision type (pub/unpub) will be found in second pass
                break

            # Check for Tax Court Docket
            tax_match = TAX_COURT_DOCKET_REGEX.search(span_text)
            if tax_match:
                primary_docket_id = tax_match.group(1).strip().upper()
                primary_docket_badge_text = span_text
                opinion_type_venue = "Tax Court"
                log.debug(f"Identified Opinion Type: Tax Court based on docket '{primary_docket_id}' from badge '{span_text}'")
                break

            # Check for Trial Court Dockets (using LC map)
            # This is less reliable as badges might contain other numbers
            # Only do this if no other type was identified yet
            for pattern, _, _, _ in LC_DOCKET_VENUE_MAP:
                 # Use fullmatch - the badge *is* the docket
                 match = pattern.fullmatch(span_text)
                 if match:
                     primary_docket_id = match.group(0).strip().upper()
                     primary_docket_badge_text = span_text
                     opinion_type_venue = "Trial Court"
                     log.debug(f"Identified Opinion Type: Trial Court based on docket '{primary_docket_id}' from badge '{span_text}'")
                     break # Stop inner loop
            if opinion_type_venue == "Trial Court": break # Stop outer loop

        # Second pass: Find the decision type text (Pub/Unpub) if not SC
        if opinion_type_venue != "Supreme Court":
            for span in badge_spans:
                 span_text = _extract_text_safely(span).strip()
                 if not span_text or span_text == primary_docket_badge_text: continue

                 mapped_code, mapped_text, mapped_venue = _map_decision_info(span_text)
                 # Ensure the mapped venue matches the docket type found
                 # Or if docket type was unknown initially, use the type badge venue
                 if mapped_code and (mapped_venue == opinion_type_venue or opinion_type_venue == "Unknown Court"):
                     decision_code, decision_text = mapped_code, mapped_text
                     if opinion_type_venue == "Unknown Court": # Update venue if found via type badge
                          opinion_type_venue = mapped_venue
                     raw_decision_type_text = span_text
                     log.debug(f"Found Decision Type Text: {decision_text} from badge '{span_text}' (Matches Venue: {opinion_type_venue})")
                     break # Found type

        # --- CRITICAL: Primary Docket ID is required ---
        if not primary_docket_id:
            log.warning(f"Could not find Primary Docket ID badge for case '{raw_title_text[:50]}...'. Skipping article.")
            log.debug(f"Badges found: {[(_extract_text_safely(s)) for s in badge_spans]}")
            return None

        # If decision type still not found, set defaults
        if not decision_code:
             log.warning(f"Could not determine decision type (pub/unpub) for {primary_docket_id}. Using defaults.")
             decision_text = f"Unknown {opinion_type_venue} Type"
             # Assign a generic code? or leave None? Let's leave None
             # decision_code = f"{opinion_type_venue.lower().replace(' ','')[:4]}UNK"


        # --- Parse Title Details (Parenthetical Info) ---
        title_details = _parse_case_title_details(raw_title_text, opinion_type_venue)


        # Handle potential multiple primary dockets listed in the badge
        all_primary_dockets = [primary_docket_id] # Start with the one identified
        if primary_docket_badge_text:
             # Try finding others matching the primary type's regex
             docket_regex = None
             if opinion_type_venue == "Supreme Court": docket_regex = SUPREME_COURT_DOCKET_REGEX
             elif opinion_type_venue == "Appellate Division": docket_regex = APPELLATE_DOCKET_REGEX
             elif opinion_type_venue == "Tax Court": docket_regex = TAX_COURT_DOCKET_REGEX

             if docket_regex:
                  found_in_badge = docket_regex.findall(primary_docket_badge_text)
                  if len(found_in_badge) > 1:
                       all_primary_dockets = [d.strip().upper() for d in found_in_badge]
                       log.debug(f"Found multiple primary dockets in badge: {all_primary_dockets}")
             # Add logic for Trial Court if needed, though less likely in single badge


        # --- Create Data Records ---
        for i, current_primary_docket in enumerate(all_primary_dockets):
            linked_dockets = [d for j, d in enumerate(all_primary_dockets) if i != j]

            # Assign fields based on parsed details and opinion type
            case_data = {
                "AppDocketID": current_primary_docket,
                "ReleaseDate": release_date_iso,
                "LinkedDocketIDs": ", ".join(linked_dockets) if linked_dockets else None,
                "CaseName": title_details.get('CaseName'),
                "LCdocketID": title_details.get('LCdocketID'), # Correctly set by title parser based on type
                "LCCounty": title_details.get('LCCounty'),
                "Venue": opinion_type_venue, # Venue of the court issuing this opinion
                "LowerCourtVenue": title_details.get('LowerCourtVenue'),
                "LowerCourtSubCaseType": title_details.get('LowerCourtSubCaseType'),
                "OPJURISAPP": title_details.get('OPJURISAPP'),
                "DecisionTypeCode": decision_code,
                "DecisionTypeText": decision_text,
                "StateAgency1": title_details.get('StateAgency1'),
                "StateAgency2": title_details.get('StateAgency2'),
                "CaseNotes": title_details.get('CaseNotes'),
                "caseconsolidated": title_details.get('caseconsolidated', 0),
                "recordimpounded": title_details.get('recordimpounded', 0)
            }
            log.debug(f"Parsed data record: AppD={case_data['AppDocketID']}, LCK={case_data['LCdocketID']}, LCVen={case_data['LowerCourtVenue']}, Venue={case_data['Venue']}, Notes={case_data['CaseNotes']}")
            case_data_list.append(case_data)

        return case_data_list

    except Exception as e:
        log.error(f"Error parsing case article (starts with '{raw_title_text[:50]}'): {e}", exc_info=True)
        log.debug(f"Article HTML on error: {article_element.prettify()[:500]}")
        return None # Return None on error


# --- fetch_and_parse_opinions (Unchanged from V3) ---
def fetch_and_parse_opinions(url=PAGE_URL):
    """Fetches the HTML from the URL and parses all opinion articles."""
    log.info(f"Attempting to fetch opinions from: {url}")
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        log.info(f"Successfully fetched URL. Status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        log.error(f"Failed to fetch URL {url}: {e}", exc_info=True)
        print(f"Error: Could not connect to {url}. Check network connection and URL.")
        return [], None

    html = response.text
    soup = BeautifulSoup(html, "html.parser")
    opinions = []
    release_date_str_iso = None

    # --- Extract Release Date ---
    try:
        date_header = soup.select_one('div.view-header h2')
        if date_header:
            date_text = _extract_text_safely(date_header)
            match = re.search(r'on\s+(.+)', date_text, re.IGNORECASE)
            if match:
                raw_date_str = match.group(1).strip()
                log.info(f"Extracted raw release date string: '{raw_date_str}'")
                try:
                    tzinfos = {"ET": gettz("America/New_York"), "EDT": gettz("America/New_York"), "EST": gettz("America/New_York")}
                    release_date_dt = date_parse(raw_date_str, tzinfos=tzinfos)
                    release_date_str_iso = release_date_dt.strftime('%Y-%m-%d')
                    log.info(f"Parsed release date to ISO format: {release_date_str_iso}")
                except Exception as date_err:
                    log.warning(f"Could not parse date string '{raw_date_str}': {date_err}. Storing raw.")
                    release_date_str_iso = raw_date_str
            else:
                log.warning(f"Could not find date pattern ('on ...') in H2 tag: '{date_text}'")
        else:
            log.warning("Could not find 'div.view-header h2' containing the release date.")
    except Exception as e:
        log.error(f"Error extracting release date: {e}", exc_info=True)

    if not release_date_str_iso:
        log.error("Release date could not be determined from page header. Records will lack ReleaseDate.")
        print("Warning: Could not determine the release date for these opinions.")

    # --- Find Case Articles ---
    main_content = soup.find('main', id='main-content')
    search_area = main_content if main_content else soup
    potential_articles = search_area.find_all('article', class_='w-100')
    if not potential_articles:
         potential_articles = search_area.select('div.card')
         if not potential_articles:
              log.error("Cannot find any suitable article containers (article.w-100 or div.card).")
              return [], release_date_str_iso
         else:
              log.warning(f"Using fallback selector 'div.card', found {len(potential_articles)} potential containers.")
    else:
         log.info(f"Found {len(potential_articles)} potential article containers using 'article.w-100'.")


    processed_count = 0
    skipped_count = 0
    for article_container in potential_articles:
        parsed_data_list = _parse_case_article(article_container, release_date_str_iso)
        if parsed_data_list:
            opinions.extend(parsed_data_list)
            processed_count += len(parsed_data_list)
        else:
            skipped_count += 1

    log.info(f"Parsing complete. Successfully processed {processed_count} opinion entries. Skipped {skipped_count} containers/messages.")
    if processed_count == 0 and skipped_count > 0:
         log.warning("Processed 0 opinions, but skipped some containers. Check if 'No opinions reported' messages were correctly handled or if parsing errors occurred.")
    elif processed_count == 0 and skipped_count == 0:
         log.warning("Processed 0 opinions and skipped 0 containers. No opinion data found on the page structure.")

    return opinions, release_date_str_iso

# === End of GscraperEM.py ===