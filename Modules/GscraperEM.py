# GscraperEM.py
# V6: Use zoneinfo for accurate timezone handling
"""
Handles fetching and parsing the NJ Courts 'Expected Opinions' page.
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
import os
from dateutil.parser import parse as date_parse
# Use zoneinfo for accurate timezone handling (requires Python 3.9+)
try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
except ImportError:
    # Fallback for older Python versions (less accurate, no DST handling)
    log.warning("zoneinfo not available (requires Python 3.9+). Using fixed UTC offset for time calculations. DST changes will not be handled.")
    from datetime import timezone, timedelta
    ZoneInfo = None # Flag that zoneinfo is not available

log = logging.getLogger(__name__)

# Constants
BASE_URL = "https://www.njcourts.gov"
PAGE_URL = os.path.join(BASE_URL, "attorneys/opinions/expected")
HEADERS = { # User Agent remains the same
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# Define Eastern Timezone using zoneinfo if available
EASTERN_TZ = None
if ZoneInfo:
    try:
        EASTERN_TZ = ZoneInfo("America/New_York")
        log.info("Using zoneinfo for America/New_York timezone.")
    except ZoneInfoNotFoundError:
        log.error("Timezone 'America/New_York' not found by zoneinfo. Falling back to fixed offset.")
        EASTERN_TZ = datetime.timezone(datetime.timedelta(hours=-4), name="EDT_Fallback") # Fallback EDT
else:
    # Fallback fixed offset if zoneinfo failed or wasn't imported
    EASTERN_TZ = datetime.timezone(datetime.timedelta(hours=-4), name="EDT_Fixed") # Assume EDT

RELEASE_TIME_THRESHOLD = datetime.time(10, 30, 0) # 10:30 AM


# Regex Patterns (Unchanged)
SUPREME_COURT_DOCKET_REGEX = re.compile(r"\b(A-\d{1,2}-\d{2})\b", re.IGNORECASE)
APPELLATE_DOCKET_REGEX = re.compile(r"\b(A-\d{4,}-\d{2})\b", re.IGNORECASE)
TAX_COURT_DOCKET_REGEX = re.compile(r"\b(\d{6}-\d{4}|\d{4}-\d{4})\b", re.IGNORECASE)

# Mappings (Unchanged)
DECISION_TYPE_MAP = { # ... remains same ...
    "unpublished appellate": ("appUNpub", "Unpublished Appellate", "Appellate Division"),
    "published appellate": ("appPUB", "Published Appellate", "Appellate Division"),
    "supreme": ("supreme", "Supreme Court", "Supreme Court"),
    "unpublished tax": ("taxUNpub", "Unpublished Tax", "Tax Court"),
    "published tax": ("taxPUB", "Published Tax", "Tax Court"),
    "unpublished trial": ("trialUNpub", "Unpublished Trial", "Trial Court"),
    "published trial": ("trialPUB", "Published Trial", "Trial Court"),
}
COUNTY_CODE_MAP = { # ... remains same ...
    "Atlantic County": "ATL", "Bergen County": "BER", "Burlington County": "BUR", "Camden County": "CAM", "Cape May County": "CPM", "Cumberland County": "CUM", "Essex County": "ESX", "Gloucester County": "GLO", "Hudson County": "HUD", "Hunterdon County": "HNT", "Mercer County": "MER", "Middlesex County": "MID", "Monmouth County": "MON", "Morris County": "MRS", "Ocean County": "OCN", "Passaic County": "PAS", "Salem County": "SLM", "Somerset County": "SOM", "Sussex County": "SSX", "Union County": "UNN", "Warren County": "WRN"
}
LC_DOCKET_VENUE_MAP = [ # ... remains same ...
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

# --- Helper Functions (_extract_text_safely, _map_decision_info - Unchanged) ---
# ... (code remains the same) ...
def _extract_text_safely(element, joiner=' '):
    if element: text_nodes = element.find_all(string=True, recursive=True); cleaned = [t.replace('\xa0', ' ').strip() for t in text_nodes]; return joiner.join(filter(None, cleaned))
    return ""
def _map_decision_info(type_string):
    type_string_lower = type_string.lower().strip() if type_string else ""
    for key, values in DECISION_TYPE_MAP.items():
        if type_string_lower.startswith(key): return values
    return (None, type_string if type_string else "Unknown", "Unknown Court")


# --- _parse_case_title_details (Unchanged) ---
def _parse_case_title_details(raw_title_text, opinion_type_venue="Unknown Court"):
    """Parses the raw case title string to extract details from parenthetical info."""
    # ... (code remains the same as V6) ...
    details = { 'CaseName': raw_title_text.strip(), 'LCdocketID': None, 'LCCounty': None, 'OPJURISAPP': "Statewide", 'StateAgency1': None, 'StateAgency2': None, 'LowerCourtVenue': None, 'LowerCourtSubCaseType': None, 'CaseNotes': [], 'caseconsolidated': 0, 'recordimpounded': 0 }
    log.debug(f"Parsing raw title ({opinion_type_venue}): {raw_title_text[:100]}...")
    first_paren_index = raw_title_text.find('('); paren_content_full = ""; core_name = raw_title_text.strip()
    if first_paren_index != -1: core_name = raw_title_text[:first_paren_index].strip(); paren_content_full = raw_title_text[first_paren_index:].strip()
    details['CaseName'] = core_name
    note_patterns = {'RECORD IMPOUNDED': 'recordimpounded', 'CONSOLIDATED': 'caseconsolidated', 'RESUBMITTED': None}
    remaining_paren_content = paren_content_full; extracted_notes = []
    for note_text, flag_key in note_patterns.items():
        pattern = r'(?:^|[\s,(;])\b(' + re.escape(note_text) + r')\b(?:$|[\s,);])'; matches = list(re.finditer(pattern, remaining_paren_content, re.IGNORECASE)); offset = 0; found = False
        for match in matches: found = True; start, end = match.span(0); adj_s, adj_e = start - offset, end - offset; remaining_paren_content = remaining_paren_content[:adj_s] + remaining_paren_content[adj_e:]; offset += (adj_e - adj_s)
        if found and flag_key: details[flag_key] = 1; log.info(f"Set flag '{flag_key}'=1 for '{core_name}'.")
        elif found and not flag_key: extracted_notes.append(note_text.title())
    remaining_paren_content = re.sub(r'\s+', ' ', remaining_paren_content).strip(' ,;()'); log.debug(f"Parens after flags: '{remaining_paren_content}'")
    info_elements = [p.strip() for p in re.split(r'\s*[,;]\s*|\s+AND\s+', remaining_paren_content) if p.strip()]; log.debug(f"Elements: {info_elements}")
    processed_indices = set(); found_dockets = []; agency_kw = ["DEPARTMENT OF", "BOARD OF", "DIVISION OF", "BUREAU OF", "OFFICE OF", "COMMISSION"]; is_agency = any(kw in details['CaseName'].upper() for kw in agency_kw); app_docket_sc = None; found_county = None; found_opjuris = None; found_agencies = []
    for i, element in enumerate(info_elements): # Process elements
        element_processed = False
        app_match = APPELLATE_DOCKET_REGEX.search(element) # Find A-####-YY
        if app_match: app_docket_sc = app_match.group(1).strip().upper(); log.debug(f"Found potential App Docket '{app_docket_sc}' (elem {i})."); processed_indices.add(i); element_processed=True # Mark processed for AppDocket part
        # Check other dockets (LC/Agency)
        for pattern, venue, subtype_info, _ in LC_DOCKET_VENUE_MAP:
            matches = list(pattern.finditer(element))
            if matches:
                for match in matches:
                    docket_str = match.group(0).strip();
                    if docket_str.upper() == app_docket_sc: continue # Skip if it is the A-####-YY
                    subtype = subtype_info(match) if callable(subtype_info) else subtype_info
                    found_dockets.append({"docket": docket_str, "venue": venue, "subtype": subtype}); log.debug(f" Found LC/Agency: {docket_str} -> {venue}, {subtype} (elem {i})")
                processed_indices.add(i); element_processed = True # Mark fully processed if any LC docket found
        if element_processed and app_docket_sc: continue # Skip other checks if AppDocket found in this element (allow county/agency in same element?) - maybe remove continue later
        # Check County, OPJuris, Agency only if element wasn't primarily a known docket
        if not element_processed:
            if not found_county:
                 county_match = re.search(r'(?:COUNTY\s+OF\s+)?([A-Za-z\s]+?)\s+COUNTY\b', element, re.IGNORECASE)
                 if county_match: name = county_match.group(1).strip().title()+" County";
                                  if name in COUNTY_CODE_MAP: found_county=name; log.debug(f"Found County: {found_county} (elem {i})"); processed_indices.add(i); continue
                 code_match = re.search(r'\b([A-Z]{3})\b', element);
                 if code_match: name=next((n for n,c in COUNTY_CODE_MAP.items() if c==code_match.group(1)), None);
                                if name: found_county=name; log.debug(f"Found County Code: {code_match.group(1)}->{found_county} (elem {i})"); processed_indices.add(i); continue
            if not found_opjuris:
                if element.upper() == "STATEWIDE": found_opjuris="Statewide"; log.debug(f"Found OPJuris: {found_opjuris} (elem {i})"); processed_indices.add(i); continue
            if any(kw in element.upper() for kw in agency_kw) and "COUNTY" not in element.upper(): found_agencies.append(element.strip()); processed_indices.add(i); is_agency=True; log.debug(f"Found Agency: {element.strip()} (elem {i})"); continue
    # Assign results
    details['LCCounty'] = found_county;
    if found_opjuris: details['OPJURISAPP'] = found_opjuris
    if found_agencies: details['StateAgency1']=found_agencies[0];
                       if len(found_agencies)>1: details['StateAgency2']=found_agencies[1];
                       if len(found_agencies)>2: extracted_notes.append(f"Other Agencies: {', '.join(found_agencies[2:])}")
    primary_lc = found_dockets[0] if found_dockets else None
    # Type specific logic
    if opinion_type_venue == "Supreme Court":
        details['LCdocketID'] = app_docket_sc; details['LowerCourtVenue'] = "Appellate Division"; details['LowerCourtSubCaseType'] = None
        if primary_lc: extracted_notes.insert(0, f"[Original LC: Docket={primary_lc.get('docket','N/A')}, Venue={primary_lc.get('venue','N/A')}" + (f" ({primary_lc.get('subtype')})" if primary_lc.get('subtype') else "") + "]")
        # Add other processed info to notes for SC cases
        if found_county and (not primary_lc or found_county not in primary_lc.get('venue','')): extracted_notes.append(f"[County: {found_county}]")
        if details['StateAgency1']: extracted_notes.append(f"[Agency1: {details['StateAgency1']}]");
        if details['StateAgency2']: extracted_notes.append(f"[Agency2: {details['StateAgency2']}]");
        for i, element in enumerate(info_elements): # Add unprocessed elements
             if i not in processed_indices: extracted_notes.append(element)
        log.info(f"SC Case: LCdocketID='{details['LCdocketID']}', Orig LC->notes.")
    else: # App/Trial/Tax
        if primary_lc: details['LCdocketID']=primary_lc.get('docket'); details['LowerCourtVenue']=primary_lc.get('venue'); details['LowerCourtSubCaseType']=primary_lc.get('subtype')
        for i, element in enumerate(info_elements): # Add unprocessed elements
             if i not in processed_indices: extracted_notes.append(element)
    # Final details
    if is_agency and details['LCCounty'] != 'NJ': details['LCCounty']='NJ'; log.debug("Set LCCounty=NJ for Agency.")
    if not details['StateAgency1'] and is_agency: # Backfill agency
         for keyword in agency_kw: match=re.search(rf'\b({keyword}(?:\s+[A-Z][a-zA-Z]+)+)\b', details['CaseName'], re.IGNORECASE);
                                    if match: details['StateAgency1']=match.group(1).strip(); log.debug(f"Assigned Agency1: {details['StateAgency1']}"); break
    if opinion_type_venue != "Supreme Court" and not details['LowerCourtVenue']: details['LowerCourtVenue']="Unknown"
    if opinion_type_venue != "Supreme Court" and not details['LCdocketID'] and details['LowerCourtVenue'] != "Agency":
        note="[LC Docket Missing]";
        if note not in extracted_notes: extracted_notes.append(note); log.warning(f"'{note}' for '{core_name}' ({opinion_type_venue}).")
    details['CaseNotes'] = ", ".join(sorted(list(set(filter(None, extracted_notes))))) or None
    log.debug(f"Parsed title FINAL ({opinion_type_venue}): LC Docket='{details['LCdocketID']}', Notes='{details['CaseNotes']}'")
    return details


# --- _parse_case_article (Updated for zoneinfo) ---
def _parse_case_article(article_element, release_date_iso):
    """ Parses a single <article> element. Calculates 'opinionstatus' using timezone."""
    case_data_list = []
    log.debug("Parsing case article...")
    raw_title_text = "N/A"
    try:
        # ... (Initial checks for card-body, no opinions, title_div - same as before) ...
        card_body = article_element.find('div', class_='card-body')
        if not card_body: log.warning("Missing card-body."); return None
        no_opinions = card_body.find(string=re.compile(r'no\s+.*\s+opinions\s+reported', re.IGNORECASE))
        if no_opinions: log.info(f"Skipping 'No opinions'."); return None
        title_div = card_body.find('div', class_=re.compile(r'card-title\b.*\btext-start\b'))
        if not title_div: log.warning("Missing title div."); return None
        raw_title_text = _extract_text_safely(title_div)
        if not raw_title_text: log.warning("Title empty."); return None

        # --- Identify Opinion Type and Primary Docket from Badges (same as V6) ---
        # ... (Code to find primary_docket_id, opinion_type_venue, decision_code/text remains same) ...
        badge_spans = card_body.find_all('span', class_='badge'); primary_docket_id, primary_docket_badge_text = None, None; decision_code, decision_text, opinion_type_venue = None, None, "Unknown Court"
        for span in badge_spans: # Find primary docket first
            span_text = _extract_text_safely(span).strip(); found_primary = False;
            if not span_text: continue
            sc_match = SUPREME_COURT_DOCKET_REGEX.search(span_text)
            if sc_match: primary_docket_id = sc_match.group(1).strip().upper(); opinion_type_venue = "Supreme Court"; decision_code, decision_text, _ = DECISION_TYPE_MAP["supreme"]; found_primary = True; break
            app_match = APPELLATE_DOCKET_REGEX.search(span_text)
            if app_match: primary_docket_id = app_match.group(1).strip().upper(); opinion_type_venue = "Appellate Division"; found_primary = True; break
            tax_match = TAX_COURT_DOCKET_REGEX.search(span_text)
            if tax_match: primary_docket_id = tax_match.group(1).strip().upper(); opinion_type_venue = "Tax Court"; found_primary = True; break
            for pattern, _, _, _ in LC_DOCKET_VENUE_MAP: # Check Trial
                 match = pattern.fullmatch(span_text)
                 if match: primary_docket_id = match.group(0).strip().upper(); opinion_type_venue = "Trial Court"; found_primary = True; break
            if found_primary: primary_docket_badge_text = span_text; break
        if opinion_type_venue != "Supreme Court": # Find decision type text
            for span in badge_spans:
                 span_text = _extract_text_safely(span).strip();
                 if not span_text or span_text == primary_docket_badge_text: continue
                 mapped_code, mapped_text, mapped_venue = _map_decision_info(span_text)
                 if mapped_code and (mapped_venue == opinion_type_venue or opinion_type_venue == "Unknown Court"):
                     decision_code, decision_text = mapped_code, mapped_text;
                     if opinion_type_venue == "Unknown Court": opinion_type_venue = mapped_venue;
                     break
        if not primary_docket_id: log.warning(f"No Primary Docket ID badge for '{raw_title_text[:50]}...' ({opinion_type_venue})."); return None
        if not decision_code and opinion_type_venue != "Supreme Court": log.warning(f"No type for {primary_docket_id}."); decision_text = f"Unknown {opinion_type_venue} Type"

        # --- Calculate Opinion Status (using zoneinfo if available) ---
        opinion_status = 0 # Default to Expected
        if release_date_iso and EASTERN_TZ: # Check if TZ object is available
            try:
                release_date_obj = datetime.datetime.strptime(release_date_iso, '%Y-%m-%d').date()
                # Combine release date with threshold time, make it timezone-aware
                release_dt_aware = datetime.datetime.combine(release_date_obj, RELEASE_TIME_THRESHOLD, tzinfo=EASTERN_TZ)
                # Get current time, localized to the same timezone
                now_aware = datetime.datetime.now(EASTERN_TZ)

                if now_aware >= release_dt_aware:
                    opinion_status = 1 # Released
                log.debug(f"Status check for {primary_docket_id}: ReleaseDT={release_dt_aware}, Now={now_aware}, Status={opinion_status}")
            except ValueError as e: log.warning(f"Date parse error for status check '{release_date_iso}': {e}")
            except Exception as e: log.error(f"Error calculating opinion status {primary_docket_id}: {e}", exc_info=True)
        elif not release_date_iso: log.warning(f"Cannot calc status {primary_docket_id}: Release date unknown.")
        else: log.warning(f"Cannot calc status {primary_docket_id}: Timezone info unavailable.")

        # --- Parse Title Details ---
        title_details = _parse_case_title_details(raw_title_text, opinion_type_venue)

        # --- Handle multiple primary dockets ---
        # ... (code remains same as V6) ...
        all_primary_dockets = [primary_docket_id]
        if primary_docket_badge_text:
             primary_regex = None
             if opinion_type_venue == "Supreme Court": primary_regex = SUPREME_COURT_DOCKET_REGEX
             elif opinion_type_venue == "Appellate Division": primary_regex = APPELLATE_DOCKET_REGEX
             elif opinion_type_venue == "Tax Court": primary_regex = TAX_COURT_DOCKET_REGEX
             if primary_regex:
                  found = primary_regex.findall(primary_docket_badge_text)
                  if len(found) > 0: all_primary_dockets = [d.strip().upper() for d in found]


        # --- Create Data Records (add opinionstatus) ---
        for i, current_primary_docket in enumerate(all_primary_dockets):
            linked_dockets = [d for j, d in enumerate(all_primary_dockets) if i != j]
            case_data = {
                "AppDocketID": current_primary_docket, "ReleaseDate": release_date_iso,
                "LinkedDocketIDs": ", ".join(linked_dockets) if linked_dockets else None,
                "CaseName": title_details.get('CaseName'), "LCdocketID": title_details.get('LCdocketID'),
                "LCCounty": title_details.get('LCCounty'), "Venue": opinion_type_venue,
                "LowerCourtVenue": title_details.get('LowerCourtVenue'),
                "LowerCourtSubCaseType": title_details.get('LowerCourtSubCaseType'),
                "OPJURISAPP": title_details.get('OPJURISAPP'),
                "DecisionTypeCode": decision_code, "DecisionTypeText": decision_text,
                "StateAgency1": title_details.get('StateAgency1'), "StateAgency2": title_details.get('StateAgency2'),
                "CaseNotes": title_details.get('CaseNotes'),
                "caseconsolidated": title_details.get('caseconsolidated', 0),
                "recordimpounded": title_details.get('recordimpounded', 0),
                "opinionstatus": opinion_status # Add calculated status
            }
            log.debug(f"Parsed data record: AppD={case_data['AppDocketID']}, Status={case_data['opinionstatus']}")
            case_data_list.append(case_data)
        return case_data_list

    except Exception as e: log.error(f"Error parsing article '{raw_title_text[:50]}': {e}", exc_info=True); return None

# --- fetch_and_parse_opinions (Updated for zoneinfo logging) ---
def fetch_and_parse_opinions(url=PAGE_URL):
    """Fetches the HTML from the URL and parses all opinion articles."""
    log.info(f"Fetching opinions: {url}"); opinions, release_date_str_iso = [], None
    try: response = requests.get(url, headers=HEADERS, timeout=30); response.raise_for_status(); log.info("Fetch OK")
    except requests.exceptions.RequestException as e: log.error(f"Fetch fail {url}: {e}"); print(f"Error: Connect fail {url}."); return [], None
    html = response.text; soup = BeautifulSoup(html, "html.parser")
    # Extract Release Date
    try:
        date_header = soup.select_one('div.view-header h2'); date_text = _extract_text_safely(date_header) if date_header else None
        if date_text: match = re.search(r'on\s+(.+)', date_text, re.IGNORECASE); raw_date_str = match.group(1).strip() if match else None
        if raw_date_str:
            log.info(f"Extracted date string: '{raw_date_str}'")
            # Try parsing with dateutil (handles various formats)
            try: release_date_dt = date_parse(raw_date_str); release_date_str_iso = release_date_dt.strftime('%Y-%m-%d'); log.info(f"Parsed release date: {release_date_str_iso}")
            except Exception as date_err: log.warning(f"Date parse fail '{raw_date_str}': {date_err}. Using raw."); release_date_str_iso = raw_date_str
        else: log.warning("No date pattern in H2.")
    except Exception as e: log.error(f"Error extracting date: {e}", exc_info=True)
    if not release_date_str_iso: log.error("Release date undetermined."); print("Warning: Release date unknown.")
    # Find articles
    search_area = soup.find('main', id='main-content') or soup
    potential_articles = search_area.find_all('article', class_='w-100') or search_area.select('div.card')
    log.info(f"Found {len(potential_articles)} potential containers.")
    # Parse articles
    processed_count, skipped_count = 0, 0
    for article in potential_articles:
        parsed_list = _parse_case_article(article, release_date_str_iso) # Pass date
        if parsed_list: opinions.extend(parsed_list); processed_count += len(parsed_list)
        else: skipped_count += 1
    log.info(f"Parsing complete. Processed: {processed_count}, Skipped: {skipped_count}.")
    if processed_count==0 and skipped_count>0: log.warning("Processed 0 opinions, check skips.")
    elif processed_count==0 and skipped_count==0: log.warning("No opinion data found.")
    return opinions, release_date_str_iso

# === End of GscraperEM.py ===