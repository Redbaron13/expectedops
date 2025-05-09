# GcalendarParserEM.py
"""
Parses NJ Appellate Division calendar PDF files to extract case information.
Handles date ranges, judge names (including t/a), argument types, locations,
and consolidated cases based on the refined strategy.
"""

import pdfplumber
import re
import os
import uuid
import datetime
import logging
from dateutil.parser import parse as date_parse
from collections import defaultdict

# Import judge cleaning utility
from GjudgeListEM import _clean_judge_name

log = logging.getLogger(__name__)

# --- Regex Patterns ---
# More robust patterns needed, these are starting points
AGENDA_PATTERN = re.compile(r"^\s*AGENDA:\s*(\w+,\s+\w+\s+\d{1,2},\s+\d{4})\s*,\s*PART\s+(\w+)", re.IGNORECASE)
JUDGES_LINE_PATTERN = re.compile(r"^\s*JUDGES:\s*([A-Z,\s\-\.\']+(\s*,\s*t/a)?)(?:,\s*([A-Z,\s\-\.\']+(\s*,\s*t/a)?))*", re.IGNORECASE) # Captures judge names, handles t/a
# Docket pattern - needs to handle optional (e) suffix and potentially other variations
DOCKET_PATTERN = re.compile(r"\b(A-\d{4,}-\d{2}(?:-T\d{1,2})?(?:\s*\(e\))?)\b", re.IGNORECASE)
TIME_PATTERN = re.compile(r"(\d{1,2}:\d{2}\s*[AP]M)")
ITEM_NUMBER_PATTERN = re.compile(r"^\s*(\d+)\s+") # Starts with a number
CONSOL_PATTERN = re.compile(r"\bConsol\b", re.IGNORECASE)
LOCATION_PATTERN_1 = re.compile(r"^\s*([A-Z\s]+(?:CRTHSE|COURTHOUSE|COMPLEX).*)", re.IGNORECASE) # Line likely containing location
LOCATION_PATTERN_2 = re.compile(r"^\s*(REMOTE\s+ARGUMENT|WAIVER\s+CALENDAR.*)", re.IGNORECASE) # Remote or Waiver location line

def normalize_text(text):
    """Basic text cleaning."""
    if not text: return ""
    text = text.replace('\r', '').replace('\n', ' ').strip()
    text = re.sub(r'\s+', ' ', text)
    return text

def parse_judges_from_line(line):
    """Extracts and cleans judge names from a 'JUDGES:' line."""
    match = JUDGES_LINE_PATTERN.search(line)
    if not match:
        return []
    # Extract all judge names mentioned, split by comma, clean each
    full_judge_string = line.split(":", 1)[1] # Get text after "JUDGES:"
    judges = []
    # Split carefully, handling potential spaces around commas
    raw_names = [name.strip() for name in full_judge_string.split(',') if name.strip()]
    for name in raw_names:
        cleaned = _clean_judge_name(name) # Use cleaning function from GjudgeListEM
        if cleaned:
            judges.append(cleaned)
    return judges

def parse_calendar_pdf(pdf_path):
    """
    Parses a single calendar PDF file.

    Returns:
        tuple: (list_of_case_data_dicts, new_filename_str) or (None, None) on failure.
    """
    extracted_cases = []
    calendar_dates = set()
    argument_days = set()
    calendar_id = str(uuid.uuid4())
    processing_ts = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # State variables for parsing context
    current_date_iso = None
    current_part = None
    current_location = "Unknown"
    current_part_judges = []
    current_case_judges = []
    current_oral_argument = None # True, False, or None if not in section
    last_item_number = 0
    consolidated_block = [] # Stores dockets within a consolidation block

    log.info(f"Starting parsing of PDF: {pdf_path}")

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, 1):
                log.debug(f"Processing Page {page_num}...")
                # Use extract_text with layout preservation options if needed
                text = page.extract_text(x_tolerance=2, y_tolerance=3, layout=False)
                if not text:
                    log.warning(f"No text extracted from page {page_num}.")
                    continue

                lines = text.split('\n')
                i = 0
                while i < len(lines):
                    line = lines[i].strip()
                    i += 1 # Increment here, adjust if lookahead needed

                    if not line: continue # Skip empty lines

                    # --- Detect State Changes ---
                    agenda_match = AGENDA_PATTERN.search(line)
                    if agenda_match:
                        date_str = agenda_match.group(1)
                        current_part = agenda_match.group(2).strip()
                        current_oral_argument = None # Reset argument type
                        current_case_judges = [] # Reset case judges
                        current_part_judges = [] # Reset part judges
                        current_location = "Unknown" # Reset location
                        last_item_number = 0 # Reset item numbering for new section
                        log.debug(f"Found AGENDA: Date='{date_str}', Part='{current_part}'")
                        try:
                            dt = date_parse(date_str)
                            calendar_dates.add(dt.date())
                            argument_days.add(dt.strftime('%A'))
                            current_date_iso = dt.strftime('%Y-%m-%d')
                            # Look for location and judges on subsequent lines
                            if i < len(lines): # Location line
                                loc_line = lines[i].strip()
                                loc_match1 = LOCATION_PATTERN_1.match(loc_line)
                                loc_match2 = LOCATION_PATTERN_2.match(loc_line)
                                if loc_match1:
                                    current_location = normalize_text(loc_match1.group(1))
                                elif loc_match2:
                                    loc_text = loc_match2.group(1).upper()
                                    if "REMOTE" in loc_text: current_location = "Virtual Hearing"
                                    elif "WAIVER" in loc_text: current_location = "Waiver Calendar (No Location)"
                                else:
                                    log.warning(f"Could not parse location line: {loc_line}")
                                i += 1
                            if i < len(lines): # Judges line
                                judges_line = lines[i].strip()
                                if judges_line.upper().startswith("JUDGES:"):
                                     current_part_judges = parse_judges_from_line(judges_line)
                                     log.debug(f"Part Judges: {current_part_judges}")
                                     i += 1
                        except Exception as e:
                            log.error(f"Error parsing date or subsequent lines for agenda '{date_str}': {e}")
                            current_date_iso = None
                        continue # Move to next line after processing agenda block

                    # Detect Argument Type Section
                    if "ORAL ARGUMENT" in line.upper() and current_oral_argument is None:
                        current_oral_argument = True
                        current_case_judges = [] # Reset case judges for new section
                        log.debug("Entering ORAL ARGUMENT section.")
                        continue
                    # Use WAIVER only if not part of "WAIVER CALENDAR" location line
                    if "WAIVER" in line.upper() and "CALENDAR" not in line.upper() and current_oral_argument is None:
                        current_oral_argument = False
                        current_case_judges = [] # Reset case judges for new section
                        log.debug("Entering WAIVER section.")
                        continue

                    # Detect Case-Specific Judges (only if within an argument section)
                    if current_oral_argument is not None and line.upper().startswith("JUDGES:"):
                        current_case_judges = parse_judges_from_line(line)
                        log.debug(f"Found Case/Group Judges: {current_case_judges}")
                        continue

                    # --- Detect Case Entry ---
                    item_match = ITEM_NUMBER_PATTERN.match(line)
                    docket_match = DOCKET_PATTERN.search(line)

                    # Must be in a section and find item number + docket
                    if current_oral_argument is not None and item_match and docket_match:
                        item_number = int(item_match.group(1))
                        # Reset consolidation block if item number isn't sequential or resets
                        if item_number <= last_item_number:
                            consolidated_block = []
                        last_item_number = item_number

                        docket_number = docket_match.group(1).strip()
                        is_consolidated = bool(CONSOL_PATTERN.search(line))

                        # Extract Time (for Oral)
                        time_str = None
                        if current_oral_argument:
                            time_match = TIME_PATTERN.search(line)
                            if time_match: time_str = time_match.group(1)
                            # Look on next line too if not on current (simple lookahead)
                            elif i < len(lines):
                                next_line_time_match = TIME_PATTERN.search(lines[i])
                                if next_line_time_match: time_str = next_line_time_match.group(1)

                        # --- Extract Caption (Multi-line capable) ---
                        caption_lines = []
                        # Start caption from current line after removing item#, docket, time, consol
                        caption_part = line.replace(item_match.group(0), '', 1) # Remove item number prefix
                        caption_part = caption_part.replace(docket_number, '', 1) # Remove first docket occurrence
                        if time_str: caption_part = caption_part.replace(time_str, '', 1)
                        if is_consolidated: caption_part = CONSOL_PATTERN.sub('', caption_part)
                        caption_lines.append(caption_part.strip())

                        # Look ahead for more caption lines
                        caption_line_index = i
                        while caption_line_index < len(lines):
                            next_line = lines[caption_line_index].strip()
                            # Stop if we hit the next item number, judges line, section break, or empty line
                            if not next_line or ITEM_NUMBER_PATTERN.match(next_line) or next_line.upper().startswith("JUDGES:") or AGENDA_PATTERN.search(next_line) or "ORAL ARGUMENT" in next_line.upper() or "WAIVER" in next_line.upper():
                                break
                            # Stop if it looks like another docket number (heuristic)
                            if DOCKET_PATTERN.search(next_line):
                                 break
                            caption_lines.append(next_line)
                            caption_line_index += 1
                        # Don't advance main loop index 'i' here, let the outer loop handle it

                        caption = normalize_text(" ".join(caption_lines))

                        # Determine assigned judges
                        assigned_judges = current_case_judges if current_case_judges else current_part_judges

                        # Handle Consolidation Block
                        if is_consolidated:
                            if not consolidated_block: # Start of a new block
                                consolidated_block = [docket_number]
                            else: # Add to existing block
                                consolidated_block.append(docket_number)
                            # Look ahead for more dockets in this block if needed (more complex)
                        else:
                            # If previous was consolidated, process that block now
                            if consolidated_block:
                                for idx, consol_docket in enumerate(consolidated_block):
                                    linked_dockets = [d for jdx, d in enumerate(consolidated_block) if idx != jdx]
                                    # Find the data associated with consol_docket (might need to store temporarily)
                                    # This part is tricky - need to associate the *first* case's details (caption, judges etc.) with all dockets in the block
                                    # For now, create entry using current details but correct docket/links
                                    consol_case_data = {
                                        # ... copy most fields from current case_data ...
                                        "AppDocketID": consol_docket,
                                        "LinkedDocketIDs": ",".join(linked_dockets) if linked_dockets else None,
                                        "IsConsolidated": True,
                                        # Ensure other fields are consistent for the block
                                        "CalendarID": calendar_id, "ProcessingTimestamp": processing_ts,
                                        "HearingDate": current_date_iso, "HearingTime": time_str,
                                        "CourtPart": current_part, "Location": current_location,
                                        "ArgumentType": "Oral Argument" if current_oral_argument else "Submission", # Use True/False field instead
                                        "OralArgument": current_oral_argument,
                                        "ItemNumber": item_number, # Maybe use first item number of block?
                                        "CaseName": caption, # Use caption from first case in block
                                        "AssignedJudges": ",".join(assigned_judges),
                                        "PresidingJudgesPart": ",".join(current_part_judges),
                                    }
                                    extracted_cases.append(consol_case_data)
                                consolidated_block = [] # Reset block after processing
                            # Process the current non-consolidated case
                            case_data = {
                                "CalendarID": calendar_id, "ProcessingTimestamp": processing_ts,
                                "HearingDate": current_date_iso, "HearingTime": time_str,
                                "CourtPart": current_part, "Location": current_location,
                                "ArgumentType": "Oral Argument" if current_oral_argument else "Submission", # Use True/False field instead
                                "OralArgument": current_oral_argument,
                                "ItemNumber": item_number,
                                "AppDocketID": docket_number,
                                "LinkedDocketIDs": None,
                                "CaseName": caption,
                                "AssignedJudges": ",".join(assigned_judges),
                                "PresidingJudgesPart": ",".join(current_part_judges),
                                "IsConsolidated": False,
                            }
                            extracted_cases.append(case_data)

                        log.debug(f"Parsed Item {item_number}: Docket={docket_number}, Oral={current_oral_argument}, Consol={is_consolidated}")

            # After processing all lines on a page, check if the last case was consolidated
            if consolidated_block:
                 # Process the last consolidated block similarly to above
                 # This requires storing the details of the first case in the block temporarily
                 log.warning(f"Consolidated block processing at end of page/file needs refinement.") # Mark for improvement
                 # Simplified: Append last block using last known details
                 first_case_details = extracted_cases[-len(consolidated_block)] if extracted_cases else {} # Get details from first case added in block
                 for idx, consol_docket in enumerate(consolidated_block):
                     linked_dockets = [d for jdx, d in enumerate(consolidated_block) if idx != jdx]
                     consol_case_data = {
                         "AppDocketID": consol_docket,
                         "LinkedDocketIDs": ",".join(linked_dockets) if linked_dockets else None,
                         "IsConsolidated": True,
                         "CalendarID": calendar_id, "ProcessingTimestamp": processing_ts,
                         "HearingDate": first_case_details.get("HearingDate"),
                         "HearingTime": first_case_details.get("HearingTime"),
                         "CourtPart": first_case_details.get("CourtPart"),
                         "Location": first_case_details.get("Location"),
                         "OralArgument": first_case_details.get("OralArgument"),
                         "ItemNumber": first_case_details.get("ItemNumber"), # Use first item number
                         "CaseName": first_case_details.get("CaseName"), # Use first caption
                         "AssignedJudges": first_case_details.get("AssignedJudges"),
                         "PresidingJudgesPart": first_case_details.get("PresidingJudgesPart"),
                     }
                     # Avoid duplicating if already added (simple check)
                     if not any(c['AppDocketID'] == consol_docket and c['IsConsolidated'] for c in extracted_cases[-len(consolidated_block):]):
                          extracted_cases.append(consol_case_data)


    except Exception as e:
        log.error(f"Error parsing PDF file {pdf_path}: {e}", exc_info=True)
        return None, None # Indicate failure

    # --- Post-processing ---
    if not calendar_dates:
        log.error(f"No dates found in calendar PDF: {pdf_path}")
        return None, None

    try:
        start_date = min(calendar_dates)
        end_date = max(calendar_dates)
        date_range_str = f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
        year_week = f"{start_date.strftime('%Y')}_{start_date.isocalendar()[1]:02d}" # YYYY_WW format
        new_filename = f"{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}.pdf"

        # Add overall calendar info to each case record
        final_cases = []
        # Filter out potential duplicates created during consolidated processing
        seen_dockets_in_calendar = set()
        for case in extracted_cases:
             case["CalendarDateRange"] = date_range_str
             case["CalendarYearWeek"] = year_week
             case["CalendarArgumentDays"] = sorted(list(argument_days))
             # Add OralArgument boolean field based on ArgumentType logic
             case["OralArgument"] = case.get("ArgumentType") == "Oral Argument"
             case.pop("ArgumentType", None) # Remove the old string field if desired

             # Simple duplicate check within this calendar parse run
             docket_id = case.get("AppDocketID")
             if docket_id not in seen_dockets_in_calendar:
                  final_cases.append(case)
                  seen_dockets_in_calendar.add(docket_id)
             elif case.get("IsConsolidated"):
                  # Allow adding if it's part of consolidation (needs better check)
                  final_cases.append(case)
             else:
                  log.warning(f"Potential duplicate docket {docket_id} detected during final processing of {calendar_id}. Skipping.")


        log.info(f"Finished parsing {pdf_path}. Found {len(final_cases)} case entries. Date range: {date_range_str}. New filename: {new_filename}")
        return final_cases, new_filename

    except Exception as e:
        log.error(f"Error during post-processing or filename generation for {pdf_path}: {e}", exc_info=True)
        return None, None


# === End of GcalendarParserEM.py ===
