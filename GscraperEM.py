# GscraperEM.py
"""
Handles fetching and parsing the NJ Courts 'Expected Opinions' page.
Corrected SyntaxError on line 175.
Stores multiple LC Dockets comma-separated.
Revised parsing logic for LC dockets, venue, subtype, and agencies.
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
DOCKET_REGEX = re.compile(r"(A-\d{4,}-\d{2})", re.IGNORECASE)

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

# --- Lower Court Venue/Subtype Mapping ---
LC_DOCKET_VENUE_MAP = [
    (re.compile(r'\bF(V)-\d{2}-\d+-\d{2}\b', re.IGNORECASE), "Family", "Family Violence (FV)", 1),
    (re.compile(r'\bF(D)-\d{2}-\d+-\d{2}\b', re.IGNORECASE), "Family", "Dissolution (FD)", 1),
    (re.compile(r'\bF(M)-\d{2}-\d+-\d{2}\b', re.IGNORECASE), "Family", "Dissolution (FM)", 1),
    (re.compile(r'\bF(G)-\d{2}-\d+-\d{2}\b', re.IGNORECASE), "Family", "Guardianship (FG)", 1),
    (re.compile(r'\bF(P)-\d{2}-\d+-\d{2}\b', re.IGNORECASE), "Family", "Termination of Parental Rights (FP)", 1),
    (re.compile(r'\bF([A-Z])-\d{2}-\d+-\d{2}\b', re.IGNORECASE), "Family", lambda m: f"Other Family ({m.group(1).upper()})", 1),
    (re.compile(r'\bF-\d+-\d{2}\b', re.IGNORECASE), "Chancery", "Foreclosure", None),
    (re.compile(r'\bC-\d{6}-\d{2}\b', re.IGNORECASE), "Chancery", "General Equity", None),
    (re.compile(r'\bSVP-\d{3}-\d{2}\b', re.IGNORECASE), "Law Division", "Civil Commitment (SVP)", None), # Venue updated
    (re.compile(r'\b[A-Z]{3}-(DC)-\d+-\d{2}\b', re.IGNORECASE), "Special Civil Part", "Special Civil (DC)", 1),
    (re.compile(r'\b[A-Z]{3}-(SC)-\d+-\d{2}\b', re.IGNORECASE), "Special Civil Part", "Small Claims (SC)", 1),
    (re.compile(r'\b[A-Z]{3}-(LT)-\d+-\d{2}\b', re.IGNORECASE), "Special Civil Part", "Landlord Tenant (LT)", 1),
    (re.compile(r'\bL-\d{4}-\d{2}\b', re.IGNORECASE), "Law Division", None, None), # Venue updated
    (re.compile(r'\b\d{2}-\d{2}-\d{4}\b'), "Criminal", None, None),
]

FAMILY_SUBTYPE_MAP = {
    'V': 'Family Violence', 'D': 'Dissolution', 'M': 'Dissolution',
    'G': 'Guardianship', 'P': 'Termination of Parental Rights'
}

# --- Helper Functions ---
def _extract_text_safely(element, joiner=' '):
    if element:
        return joiner.join(filter(None, [text.strip() for text in element.find_all(string=True, recursive=True)]))
    return ""

def _map_decision_info(type_string):
    type_string_lower = type_string.lower().strip() if type_string else ""
    for key, values in DECISION_TYPE_MAP.items():
        if key in type_string_lower:
            return values
    log.warning(f"Unrecognized decision type string: '{type_string}'. Storing raw.")
    return (None, type_string if type_string else "Unknown", None)

# --- Case Title Parsing ---
def _parse_case_title_details(raw_title_text):
    """ Parses title string. Stores multiple LC Dockets comma-separated in LCdocketID. """
    details = {
        'CaseName': raw_title_text.strip(), 'LCdocketID': None, 'LCCounty': None,
        'OPJURISAPP': None, 'StateAgency1': None, 'StateAgency2': None,
        'LowerCourtVenue': None, 'LowerCourtSubCaseType': None, 'CaseNotes': []
    }

    # 1. Separate Core Name from Parenthetical Content
    first_paren_index = raw_title_text.find('(')
    paren_content_full = ""
    core_name = raw_title_text
    if first_paren_index != -1:
        core_name = raw_title_text[:first_paren_index].strip()
        paren_content_full = raw_title_text[first_paren_index:]
    details['CaseName'] = core_name

    # 2. Extract Specific Note Groups First
    note_patterns_text = ['RECORD IMPOUNDED', 'CONSOLIDATED', 'RESUBMITTED']
    remaining_paren_content = paren_content_full
    extracted_notes = []
    for note_text in note_patterns_text:
        note_pattern = r'([\s,(]*)(' + re.escape(note_text) + r')([\s,)]*)'
        matches = list(re.finditer(note_pattern, remaining_paren_content, re.IGNORECASE))
        offset = 0
        for match in matches:
            extracted_notes.append(match.group(2).strip().title())
            start, end = match.span(); start -= offset; end -= offset
            remaining_paren_content = remaining_paren_content[:start] + remaining_paren_content[end:]
            offset += (end - start)
        remaining_paren_content = remaining_paren_content.replace('()','').strip(' ,')

    # 3. Process Remaining Parenthetical Content
    parts_in_parens = re.findall(r'\(([^()]*?(?:\([^()]*\)[^()]*?)*?)\)', remaining_paren_content)
    info_elements_combined = []
    if parts_in_parens:
        combined_info_str = " , ".join(parts_in_parens)
        info_elements_combined = [p.strip() for p in re.split(r'\s*,\s*|\s+AND\s+', combined_info_str) if p.strip()]

    processed_elements = set()
    agency_keywords = ["DEPARTMENT OF", "BOARD OF", "DIVISION OF", "BUREAU OF"]
    is_agency_appeal = any(keyword in details['CaseName'].upper() for keyword in agency_keywords)

    # 4. Extract ALL LC Dockets & Determine Venue/Subtype (based on *first* found)
    found_dockets_details = []
    for i, element in enumerate(info_elements_combined):
        if element in processed_elements: continue
        for pattern, venue, default_subtype, subtype_func_or_idx in LC_DOCKET_VENUE_MAP:
            matches = list(pattern.finditer(element))
            if matches:
                for match in matches:
                    match_str = match.group(0)
                    subtype = default_subtype
                    if subtype_func_or_idx is not None:
                        if callable(subtype_func_or_idx): subtype = subtype_func_or_idx(match)
                        elif isinstance(subtype_func_or_idx, int):
                            subtype_code = match.group(subtype_func_or_idx).upper()
                            if venue == "Family": subtype = FAMILY_SUBTYPE_MAP.get(subtype_code, subtype_code)
                            elif venue == "Special Civil Part": subtype = default_subtype
                    found_dockets_details.append({"docket": match_str, "venue": venue, "subtype": subtype})
                processed_elements.add(element)
                log.debug(f"LC Docket Parse: Found {len(matches)} in '{element}' -> Venue: {venue}, Subtype: {subtype}")

    # Assign Venue/Subtype based on the *first* docket found
    if found_dockets_details:
        first_docket_info = found_dockets_details[0]
        details['LowerCourtVenue'] = first_docket_info['venue']
        details['LowerCourtSubCaseType'] = first_docket_info['subtype']
        all_docket_strings = [d['docket'] for d in found_dockets_details]
        details['LCdocketID'] = ", ".join(all_docket_strings)
        log.debug(f"Combined LC Dockets: {details['LCdocketID']}")

    # 5. Extract County
    for i, element in enumerate(info_elements_combined):
        if element in processed_elements or details['LCCounty']: continue
        county_match = re.search(r'([A-Z\s]+?)\s+COUNTY', element, re.IGNORECASE)
        if county_match:
            details['LCCounty'] = county_match.group(1).strip().title() + " County"
            processed_elements.add(element)

    # 6. Extract OPJURISAPP (Statewide)
    for i, element in enumerate(info_elements_combined):
        if element in processed_elements or details['OPJURISAPP']: continue
        if "STATEWIDE" in element.upper():
            details['OPJURISAPP'] = "Statewide"
            processed_elements.add(element)

    # 7. Extract State Agency & Set Agency Venue if applicable
    found_agencies = []
    for i, element in enumerate(info_elements_combined):
        if element in processed_elements: continue
        if any(keyword in element.upper() for keyword in agency_keywords):
            found_agencies.append(element)
            processed_elements.add(element)
            is_agency_appeal = True

    if found_agencies: # Added missing colon here
         details['StateAgency1'] = found_agencies[0]
         if len(found_agencies) > 1:
              details['StateAgency2'] = found_agencies[1]
              if len(found_agencies) > 2: extracted_notes.append(f"Other Agencies: {', '.join(found_agencies[2:])}")

    if is_agency_appeal and not details['LowerCourtVenue']:
         details['LowerCourtVenue'] = "Agency"
         if not details['StateAgency1'] and parts_in_parens:
              for part in parts_in_parens:
                   if any(keyword in part.upper() for keyword in agency_keywords):
                        details['StateAgency1'] = part
                        processed_elements.add(part)
                        break
    if not details['LowerCourtVenue']: details['LowerCourtVenue'] = "Unknown"

    # 8. Add remaining unprocessed elements and notes
    temp_case_notes = extracted_notes
    for i, element in enumerate(info_elements_combined):
        if element not in processed_elements:
            temp_case_notes.append(element)
    details['CaseNotes'] = ", ".join(filter(None, temp_case_notes))

    # 9. Append Agency Name to CaseName if needed
    if details['StateAgency1'] and details['StateAgency1'].strip():
        agency_name_to_check = details['StateAgency1'].strip()
        # Check if already seems present (simple check)
        if agency_name_to_check.upper() not in details['CaseName'].upper():
             details['CaseName'] += f" ({agency_name_to_check})"
             log.debug(f"Appended agency '{agency_name_to_check}' to case name.")

    log.debug(f"Parsed title details FINAL: Name='{details['CaseName'][:50]}...', LC Docket='{details['LCdocketID']}', LC Venue='{details['LowerCourtVenue']}', Notes='{details['CaseNotes']}'")
    return details

# --- _parse_case_article (Unchanged) ---
def _parse_case_article(article_element):
    case_data_list = []
    log.debug("Parsing case article...")
    try:
        card_body = article_element.find('div', class_='card-body')
        if not card_body: return None
        no_opinions_div = card_body.find('div', class_='card-title text-start mb-2')
        if no_opinions_div:
            no_opinions_text = _extract_text_safely(no_opinions_div)
            if "no" in no_opinions_text.lower() and "opinions reported" in no_opinions_text.lower():
                log.info(f"Skipping 'No opinions reported' message: '{no_opinions_text}'")
                return None
        title_div = card_body.find('div', class_='card-title fw-bold text-start')
        if not title_div:
             log.warning("Could not find title div. Skipping article.")
             return None
        raw_title_text = _extract_text_safely(title_div)
        title_details = _parse_case_title_details(raw_title_text)
        badge_spans = card_body.find_all('span', class_='badge')
        raw_docket_id_text, raw_decision_type_text = None, None
        for span in badge_spans:
            span_text = _extract_text_safely(span)
            if DOCKET_REGEX.search(span_text): raw_docket_id_text = span_text
            elif any(k in span_text.lower() for k in ["appellate", "supreme", "tax", "trial", "published", "unpublished"]):
                 raw_decision_type_text = span_text
        if not raw_docket_id_text:
            log.warning(f"Could not find AppDocket ID badge for '{title_details.get('CaseName', 'Unknown')}'. Skipping.")
            return None
        decision_code, decision_text, venue = _map_decision_info(raw_decision_type_text)
        app_docket_ids = DOCKET_REGEX.findall(raw_docket_id_text)
        if not app_docket_ids:
            log.warning(f"Could not extract AppDocket IDs from '{raw_docket_id_text}'. Skipping.")
            return None
        for i, primary_docket in enumerate(app_docket_ids):
            linked_dockets = [d for j, d in enumerate(app_docket_ids) if i != j]
            case_data = {
                "AppDocketID": primary_docket.strip(),
                "LinkedDocketIDs": ", ".join(linked_dockets) if linked_dockets else "",
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
            log.debug(f"Parsed data: AppD={case_data['AppDocketID']}, LCVen={case_data['LowerCourtVenue']}, LCSub={case_data['LowerCourtSubCaseType']}, Ag={case_data['StateAgency1']}, Name={case_data['CaseName'][:30]}...")
            case_data_list.append(case_data)
        return case_data_list
    except Exception as e:
        log.error(f"Error parsing case article: {e}", exc_info=True)
        return None

# --- fetch_and_parse_opinions (Unchanged) ---
def fetch_and_parse_opinions(url=PAGE_URL):
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
                else: log.warning("Could not find date pattern ('on ...') in H2.")
            else: log.warning("Could not find H2 tag within view-header.")
        else: log.warning("Could not find 'div.view-header'.")
    except Exception as e: log.error(f"Error extracting release date: {e}", exc_info=True)
    if not release_date_str_iso: log.warning("Release date not determined.")

    content_wrapper = soup.find('div', attrs={'data-drupal-views-infinite-scroll-content-wrapper': ''})
    if not content_wrapper:
        log.error("Could not find main content wrapper. Trying fallback.")
        case_articles = soup.find_all('article', class_='w-100')
        if not case_articles:
             log.error("Fallback failed. Cannot parse opinions.")
             return [], release_date_str_iso
        else: log.warning("Using fallback to find articles.")
    else:
        case_articles = content_wrapper.find_all('article', class_='w-100')
        log.info(f"Found {len(case_articles)} potential case articles.")

    if not case_articles:
        log.info("No case articles found.")
        return [], release_date_str_iso

    processed_count = 0
    skipped_count = 0
    for article in case_articles:
        parsed_data_list = _parse_case_article(article)
        if parsed_data_list:
            for parsed_data in parsed_data_list:
                 parsed_data["ReleaseDate"] = release_date_str_iso if release_date_str_iso else ""
                 opinions.append(parsed_data)
                 processed_count +=1
        else: skipped_count += 1

    log.info(f"Parsing complete. Processed {processed_count} entries. Skipped {skipped_count} articles.")
    return opinions, release_date_str_iso

# === End of GscraperEM.py ===