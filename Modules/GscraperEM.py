# GscraperEM.py
"""
Handles fetching and parsing the NJ Courts 'Expected Opinions' page.
Refined Special Civil Part docket extraction and added specific CaseNotes format.
Updated County Code mapping based on user input.
Corrected Special Civil Part venue string.
"""
import datetime
import requests
from bs4 import BeautifulSoup
import logging
import re
import os
from dateutil.parser import parse as date_parse

log = logging.getLogger(__name__)

# Constants
BASE_URL = "https://www.njcourts.gov"
PAGE_URL = os.path.join(BASE_URL, "attorneys/opinions/expected")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# Regex to find typical NJ Appellate Docket numbers (A-####-## format)
APPELLATE_DOCKET_REGEX = re.compile(r"(A-\d{4,}-\d{2})", re.IGNORECASE)

# Mappings for Decision Type and Venue (of the opinion being released)
DECISION_TYPE_MAP = {
    "unpublished appellate": ("appUNpub", "Unpublished Appellate", "Appellate"),
    "published appellate": ("appPUB", "Published Appellate", "Appellate"),
    "supreme": ("supreme", "Supreme Court", "Supreme"),
    "unpublished tax": ("taxUNpub", "Unpublished Tax", "Tax"),
    "published tax": ("taxPUB", "Published Tax", "Tax"),
    "unpublished trial": ("trialUNpub", "Unpublished Trial", "Trial"),
    "published trial": ("trialPUB", "Published Trial", "Trial"),
}

# --- County Code Mapping ---
# Updated based on user provided list (May 1, 2025)
COUNTY_CODE_MAP = {
    "Atlantic County": "ATL", "Bergen County": "BER", "Burlington County": "BUR",
    "Camden County": "CAM", "Cape May County": "CPM", "Cumberland County": "CUM",
    "Essex County": "ESX", "Gloucester County": "GLO", "Hudson County": "HUD",
    "Hunterdon County": "HNT", "Mercer County": "MER", "Middlesex County": "MID",
    "Monmouth County": "MON", "Morris County": "MRS", "Ocean County": "OCN",
    "Passaic County": "PAS", "Salem County": "SLM", "Somerset County": "SOM",
    "Sussex County": "SSX", "Union County": "UNN", "Warren County": "WRN"
}


# --- Lower Court Venue/Subtype Mapping ---
# NOTE: Order matters - more specific patterns first. Venue updated for Special Civil.
LC_DOCKET_VENUE_MAP = [
    # Special Civil Part (with 3-letter prefix) - Captures prefix, type, digits, year
    (re.compile(r'\b([A-Z]{3})-(DC|LT|SC)-(\d+)-(\d{2})\b', re.IGNORECASE), "Law Division - Special Civil Part", lambda m: f"Special Civil ({m.group(2).upper()})", 1), # Venue Updated
    # Special Civil Part (without 3-letter prefix - less common but possible)
    (re.compile(r'\b(DC|LT|SC)-(\d+)-(\d{2})\b', re.IGNORECASE), "Law Division - Special Civil Part", lambda m: f"Special Civil ({m.group(1).upper()})", 1), # Venue Updated
    # Family (FV, FD, FM, FG, FP, Other F*)
    (re.compile(r'\bF(V)-\d{2}-\d+-\d{2}\b', re.IGNORECASE), "Family", "Family Violence (FV)", 1),
    (re.compile(r'\bF(D)-\d{2}-\d+-\d{2}\b', re.IGNORECASE), "Family", "Dissolution (FD)", 1),
    (re.compile(r'\bF(M)-\d{2}-\d+-\d{2}\b', re.IGNORECASE), "Family", "Dissolution (FM)", 1),
    (re.compile(r'\bF(G)-\d{2}-\d+-\d{2}\b', re.IGNORECASE), "Family", "Guardianship (FG)", 1),
    (re.compile(r'\bF(P)-\d{2}-\d+-\d{2}\b', re.IGNORECASE), "Family", "Termination of Parental Rights (FP)", 1),
    (re.compile(r'\bF([A-Z])-\d{2}-\d+-\d{2}\b', re.IGNORECASE), "Family", lambda m: f"Other Family ({m.group(1).upper()})", 1),
    # Chancery (Foreclosure, General Equity)
    (re.compile(r'\bF-\d+-\d{2}\b', re.IGNORECASE), "Chancery Division", "Foreclosure", None),
    (re.compile(r'\bC-\d{6}-\d{2}\b', re.IGNORECASE), "Chancery Division", "General Equity", None),
    # Law Division (SVP - Civil Commitment, Standard Law)
    (re.compile(r'\bSVP-\d{3}-\d{2}\b', re.IGNORECASE), "Law Division", "Civil Commitment (SVP)", None),
    (re.compile(r'\bL-\d{4}-\d{2}\b', re.IGNORECASE), "Law Division", None, None),
    # Criminal (Indictment/Accusation format)
    (re.compile(r'\b\d{2}-\d{2}-\d{4}\b'), "Criminal", None, None),
]

# --- Helper Functions ---
def _extract_text_safely(element, joiner=' '):
    """Safely extracts and joins text from a BeautifulSoup element."""
    if element:
        return joiner.join(filter(None, [text.strip() for text in element.find_all(string=True, recursive=True)]))
    return ""

def _map_decision_info(type_string):
    """Maps raw decision type text to code, text, and opinion venue."""
    type_string_lower = type_string.lower().strip() if type_string else ""
    for key, values in DECISION_TYPE_MAP.items():
        if key in type_string_lower:
            return values
    log.warning(f"Unrecognized decision type string: '{type_string}'. Storing raw.")
    return (None, type_string if type_string else "Unknown", None)

# --- Case Title Parsing ---
def _parse_case_title_details(raw_title_text):
    """
    Parses the raw case title string to extract various details.
    Prioritizes Special Civil Part dockets and adds specific notes.
    """
    details = {
        'CaseName': raw_title_text.strip(), 'LCdocketID': None, 'LCCounty': None,
        'OPJURISAPP': None, 'StateAgency1': None, 'StateAgency2': None,
        'LowerCourtVenue': None, 'LowerCourtSubCaseType': None, 'CaseNotes': []
    }
    log.debug(f"Parsing raw title: {raw_title_text[:100]}...")

    first_paren_index = raw_title_text.find('(')
    paren_content_full = ""
    core_name = raw_title_text
    if first_paren_index != -1:
        core_name = raw_title_text[:first_paren_index].strip()
        paren_content_full = raw_title_text[first_paren_index:]
    details['CaseName'] = core_name

    note_patterns_text = ['RECORD IMPOUNDED', 'CONSOLIDATED', 'RESUBMITTED']
    remaining_paren_content = paren_content_full
    extracted_notes = []
    for note_text in note_patterns_text:
        note_pattern = r'(?:^|[\s,(])(' + re.escape(note_text) + r')(?:$|[\s,)])'
        matches = list(re.finditer(note_pattern, remaining_paren_content, re.IGNORECASE))
        offset = 0
        for match in matches:
            extracted_notes.append(match.group(1).strip().title())
            start, end = match.span(1)
            adj_start = start - offset
            adj_end = end - offset
            remaining_paren_content = remaining_paren_content[:adj_start] + remaining_paren_content[adj_end:]
            offset += (adj_end - adj_start)
        remaining_paren_content = remaining_paren_content.replace('()','').replace('(,','(').replace(',)',')').strip(' ,')

    parts_in_parens = re.findall(r'\(([^()]*?(?:\([^()]*\)[^()]*?)*?)\)', remaining_paren_content)
    info_elements_combined = []
    if parts_in_parens:
        combined_info_str = " , ".join(parts_in_parens)
        info_elements_combined = [p.strip() for p in re.split(r'\s*,\s*(?![^()]*\))|\s+AND\s+', combined_info_str) if p.strip()]
        log.debug(f"Extracted elements from parens: {info_elements_combined}")

    processed_elements = set()
    agency_keywords = ["DEPARTMENT OF", "BOARD OF", "DIVISION OF", "BUREAU OF", "OFFICE OF"]
    is_agency_appeal = any(keyword in details['CaseName'].upper() for keyword in agency_keywords)
    found_dockets_details = []
    primary_lc_docket_info = None

    for element in info_elements_combined:
        if element in processed_elements: continue
        for pattern, venue, subtype_info, group_index_for_subtype in LC_DOCKET_VENUE_MAP:
            matches = list(pattern.finditer(element))
            if matches:
                log.debug(f"Pattern {pattern.pattern} matched in element '{element}'")
                for match in matches:
                    docket_str = match.group(0)
                    subtype = None
                    if subtype_info:
                        if callable(subtype_info): subtype = subtype_info(match)
                        else: subtype = subtype_info
                    docket_detail = {"docket": docket_str, "venue": venue, "subtype": subtype, "match_obj": match}
                    found_dockets_details.append(docket_detail)
                    log.debug(f"  Found LC Docket: {docket_str} -> Venue: {venue}, Subtype: {subtype}")
                    if primary_lc_docket_info is None:
                        primary_lc_docket_info = docket_detail
                processed_elements.add(element)
                break

    if primary_lc_docket_info:
        details['LowerCourtVenue'] = primary_lc_docket_info['venue']
        details['LowerCourtSubCaseType'] = primary_lc_docket_info['subtype']

    if found_dockets_details:
        all_docket_strings = [d['docket'] for d in found_dockets_details]
        details['LCdocketID'] = ", ".join(all_docket_strings)
        log.debug(f"Assigned Primary LC Venue: {details['LowerCourtVenue']}, Subtype: {details['LowerCourtSubCaseType']}")
        log.debug(f"Combined LC Dockets field: {details['LCdocketID']}")

    for element in info_elements_combined:
        if element in processed_elements or details['LCCounty']: continue
        county_match = re.search(r'(?:COUNTY\s+OF\s+)?([A-Z\s]+?)\s+COUNTY', element, re.IGNORECASE)
        if county_match:
            county_name = county_match.group(1).strip().title()
            details['LCCounty'] = f"{county_name} County"
            log.debug(f"Extracted LCCounty: {details['LCCounty']}")
            processed_elements.add(element)

    for element in info_elements_combined:
        if element in processed_elements or details['OPJURISAPP']: continue
        if "STATEWIDE" in element.upper():
            details['OPJURISAPP'] = "Statewide"
            log.debug("Extracted OPJURISAPP: Statewide")
            processed_elements.add(element)

    found_agencies = []
    for element in info_elements_combined:
        if element in processed_elements: continue
        if any(keyword in element.upper() for keyword in agency_keywords):
            found_agencies.append(element)
            processed_elements.add(element)
            is_agency_appeal = True

    if found_agencies:
         details['StateAgency1'] = found_agencies[0].strip()
         log.debug(f"Extracted StateAgency1: {details['StateAgency1']}")
         if len(found_agencies) > 1:
              details['StateAgency2'] = found_agencies[1].strip()
              log.debug(f"Extracted StateAgency2: {details['StateAgency2']}")
              if len(found_agencies) > 2:
                  other_agencies_note = f"Other Agencies: {', '.join(a.strip() for a in found_agencies[2:])}"
                  extracted_notes.append(other_agencies_note)
                  log.debug(f"Added note for other agencies: {other_agencies_note}")

    if is_agency_appeal and not details['LowerCourtVenue']:
         details['LowerCourtVenue'] = "Agency"
         log.debug("Setting LowerCourtVenue to 'Agency' based on case name/paren content.")
         if not details['StateAgency1']:
             for keyword in agency_keywords:
                 if keyword in details['CaseName'].upper():
                     match = re.search(f"({keyword}[^,(]*)", details['CaseName'], re.IGNORECASE)
                     if match:
                         details['StateAgency1'] = match.group(1).strip()
                         log.debug(f"Assigned StateAgency1 from case name: {details['StateAgency1']}")
                         break

    if not details['LowerCourtVenue']:
        details['LowerCourtVenue'] = "Unknown"
        log.debug("Setting LowerCourtVenue to 'Unknown'.")

    special_civil_note = None
    # *** Venue check updated to match new string ***
    if details['LowerCourtVenue'] == "Law Division - Special Civil Part" and found_dockets_details:
        primary_sc_docket_str = None
        # *** Venue check updated ***
        if primary_lc_docket_info and primary_lc_docket_info['venue'] == "Law Division - Special Civil Part":
            primary_sc_docket_str = primary_lc_docket_info['docket']
        else:
            for d_info in found_dockets_details:
                 # *** Venue check updated ***
                if d_info['venue'] == "Law Division - Special Civil Part":
                    primary_sc_docket_str = d_info['docket']
                    break

        if primary_sc_docket_str:
            log.debug(f"Generating Special Civil note for docket: {primary_sc_docket_str}")
            ccc_match = re.match(r'([A-Z]{3})-', primary_sc_docket_str, re.IGNORECASE)
            ccc = None
            if ccc_match:
                ccc = ccc_match.group(1).upper()
                log.debug(f"Extracted CCC '{ccc}' from docket string.")
                special_civil_note = f"[{primary_sc_docket_str}]" # Use full docket string as it contains CCC
            else:
                if details['LCCounty'] and details['LCCounty'] in COUNTY_CODE_MAP:
                    ccc = COUNTY_CODE_MAP[details['LCCounty']]
                    log.debug(f"Using CCC '{ccc}' from LCCounty map.")
                    special_civil_note = f"[{ccc}-{primary_sc_docket_str}]" # Prepend CCC if not in docket
                else:
                    log.warning(f"Could not determine 3-letter county code for Special Civil case. LCCounty: {details['LCCounty']}")
                    special_civil_note = f"[{primary_sc_docket_str}]"
            log.info(f"Generated Special Civil Note: {special_civil_note}")

    final_notes_list = extracted_notes
    if special_civil_note:
        final_notes_list.insert(0, special_civil_note) # Add special note at the beginning

    for element in info_elements_combined:
        if element not in processed_elements:
            final_notes_list.append(element.strip())

    details['CaseNotes'] = ", ".join(filter(None, final_notes_list)) # Join notes

    if details['StateAgency1'] and details['StateAgency1'].strip():
        agency_name_to_check = details['StateAgency1'].strip()
        normalized_case_name = re.sub(r'[\s()]+', '', details['CaseName'].upper())
        normalized_agency_name = re.sub(r'[\s()]+', '', agency_name_to_check.upper())
        if normalized_agency_name not in normalized_case_name:
             details['CaseName'] += f" ({agency_name_to_check})"
             log.debug(f"Appended agency '{agency_name_to_check}' to case name.")

    log.debug(f"Parsed title details FINAL: Name='{details['CaseName'][:50]}...', LC Docket='{details['LCdocketID']}', LC Venue='{details['LowerCourtVenue']}', Notes='{details['CaseNotes']}'")
    return details

# --- _parse_case_article ---
def _parse_case_article(article_element):
    """Parses a single <article> element containing case information."""
    case_data_list = []
    log.debug("Parsing case article...")
    try:
        card_body = article_element.find('div', class_='card-body')
        if not card_body:
            log.warning("Article missing card-body div. Skipping.")
            return None

        no_opinions_div = card_body.find('div', class_='card-title text-start mb-2')
        if no_opinions_div:
            no_opinions_text = _extract_text_safely(no_opinions_div)
            if "no" in no_opinions_text.lower() and "opinions reported" in no_opinions_text.lower():
                log.info(f"Skipping 'No opinions reported' message: '{no_opinions_text}'")
                return None

        title_div = card_body.find('div', class_='card-title fw-bold text-start')
        if not title_div:
             log.warning("Could not find primary title div (card-title fw-bold text-start). Skipping article.")
             return None

        raw_title_text = _extract_text_safely(title_div)
        if not raw_title_text:
             log.warning("Title div found but contained no text. Skipping article.")
             return None
        title_details = _parse_case_title_details(raw_title_text)

        badge_spans = card_body.find_all('span', class_='badge')
        raw_docket_id_text, raw_decision_type_text = None, None
        for span in badge_spans:
            span_text = _extract_text_safely(span)
            if APPELLATE_DOCKET_REGEX.search(span_text):
                 raw_docket_id_text = span_text
            elif any(k in span_text.lower() for k in DECISION_TYPE_MAP.keys()):
                 raw_decision_type_text = span_text

        if not raw_docket_id_text:
            case_name_for_log = title_details.get('CaseName', raw_title_text[:50] + "...")
            log.warning(f"Could not find Appellate Docket ID badge for case '{case_name_for_log}'. Skipping.")
            return None

        decision_code, decision_text, venue = _map_decision_info(raw_decision_type_text)

        app_docket_ids = APPELLATE_DOCKET_REGEX.findall(raw_docket_id_text)
        if not app_docket_ids:
            log.warning(f"Regex failed to extract AppDocket IDs from text '{raw_docket_id_text}'. Skipping.")
            return None

        for i, primary_docket in enumerate(app_docket_ids):
            linked_dockets = [d for j, d in enumerate(app_docket_ids) if i != j]

            case_data = {
                "AppDocketID": primary_docket.strip(),
                "LinkedDocketIDs": ", ".join(linked_dockets) if linked_dockets else None,
                "CaseName": title_details.get('CaseName'),
                "LCdocketID": title_details.get('LCdocketID'),
                "LCCounty": title_details.get('LCCounty'),
                "Venue": venue,
                "LowerCourtVenue": title_details.get('LowerCourtVenue'),
                "LowerCourtSubCaseType": title_details.get('LowerCourtSubCaseType'),
                "OPJURISAPP": title_details.get('OPJURISAPP'),
                "DecisionTypeCode": decision_code,
                "DecisionTypeText": decision_text,
                "StateAgency1": title_details.get('StateAgency1'),
                "StateAgency2": title_details.get('StateAgency2'),
                "CaseNotes": title_details.get('CaseNotes'),
                "ReleaseDate": ""
            }
            log.debug(f"Parsed data: AppD={case_data['AppDocketID']}, LCVen={case_data['LowerCourtVenue']}, LCSub={case_data['LowerCourtSubCaseType']}, Ag1={case_data['StateAgency1']}, Notes={case_data['CaseNotes'][:50]}...")
            case_data_list.append(case_data)

        return case_data_list

    except Exception as e:
        log.error(f"Error parsing case article: {e}", exc_info=True)
        return None

# --- fetch_and_parse_opinions ---
def fetch_and_parse_opinions(url=PAGE_URL):
    """Fetches the HTML from the URL and parses all opinion articles."""
    log.info(f"Attempting to fetch opinions from: {url}")
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        log.info(f"Successfully fetched URL. Status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        log.error(f"Failed to fetch URL {url}: {e}", exc_info=True)
        return [], None

    html = response.text
    soup = BeautifulSoup(html, "html.parser")
    opinions = []
    release_date_str_iso = None

    try:
        date_header_div = soup.find('div', class_='view-header')
        if date_header_div:
            h2_tag = date_header_div.find('h2')
            if h2_tag:
                match = re.search(r'on\s+(.+)', h2_tag.get_text(), re.IGNORECASE)
                if match:
                    raw_date_str = match.group(1).strip()
                    log.info(f"Extracted raw release date string: {raw_date_str}")
                    try:
                        release_date_dt = date_parse(raw_date_str)
                        release_date_str_iso = release_date_dt.strftime('%Y-%m-%d')
                        log.info(f"Parsed release date to ISO format: {release_date_str_iso}")
                    except ValueError:
                        log.warning(f"Could not parse date string '{raw_date_str}'. Storing raw.")
                        release_date_str_iso = raw_date_str
                else:
                    log.warning("Could not find date pattern ('on ...') in H2 tag.")
            else:
                log.warning("Could not find H2 tag within view-header.")
        else:
            log.warning("Could not find 'div.view-header' containing the release date.")
    except Exception as e:
        log.error(f"Error extracting release date: {e}", exc_info=True)

    if not release_date_str_iso:
        log.warning("Release date not determined from page header.")

    content_wrapper = soup.find('div', attrs={'data-drupal-views-infinite-scroll-content-wrapper': ''})
    if not content_wrapper:
        log.error("Could not find main content wrapper 'data-drupal-views-infinite-scroll-content-wrapper'. Trying fallback: find all 'article' tags.")
        case_articles = soup.find_all('article', class_='w-100')
        if not case_articles:
             log.error("Fallback failed. Cannot find any <article class='w-100'> tags.")
             return [], release_date_str_iso
        else:
             log.warning(f"Using fallback, found {len(case_articles)} potential articles.")
    else:
        case_articles = content_wrapper.find_all('article', class_='w-100')
        log.info(f"Found {len(case_articles)} potential case articles within content wrapper.")

    if not case_articles:
        log.info("No case articles found on the page.")
        return [], release_date_str_iso

    processed_count = 0
    skipped_count = 0
    for article in case_articles:
        parsed_data_list = _parse_case_article(article)
        if parsed_data_list:
            for parsed_data in parsed_data_list:
                 parsed_data["ReleaseDate"] = release_date_str_iso # Assign date extracted earlier
                 opinions.append(parsed_data)
                 processed_count += 1
        else:
            skipped_count += 1

    log.info(f"Parsing complete. Processed {processed_count} entries. Skipped {skipped_count} articles.")
    return opinions, release_date_str_iso

# === End of GscraperEM.py ===