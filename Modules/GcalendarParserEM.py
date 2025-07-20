# GcalendarParserEM.py
"""
Parses New Jersey Appellate Division calendar PDF files to extract structured
case information.

This module uses the `pdfplumber` library to read text content from PDF files.
It then applies a series of regular expressions and state-based parsing logic
to identify and extract details about scheduled court calendar events.

Key information extracted includes:
- Calendar agenda details: Date, Part (e.g., "A", "B"), Location, and Judges assigned to the Part.
- Argument type sections: Distinguishes between "ORAL ARGUMENT" and "WAIVER" sections.
- Case-specific judges: Judges assigned to a particular case or group of cases,
  if different from the Part judges.
- Individual case entries:
    - Item Number on the calendar.
    - Primary Appellate Docket Number (e.g., "A-1234-22").
    - Hearing Time (for oral arguments).
    - Case Caption/Name (handles multi-line captions).
    - Consolidation status and linked dockets within a consolidated block.
- Overall calendar metadata: Date range covered by the PDF, ISO year & week.

The primary function `parse_calendar_pdf()` processes a single PDF file and
returns a list of dictionaries, each representing a parsed case entry, along with
a suggested new filename for the processed PDF based on its date range.
Judge names are cleaned using a utility from `GjudgeListEM`.
"""

import pdfplumber # For reading text content from PDF files.
import re         # For regular expression matching.
import os
import uuid       # For generating a unique CalendarID for each parsed PDF.
import datetime
import logging
from dateutil.parser import parse as date_parse # For flexible date string parsing.
from collections import defaultdict # Not actively used in V1, but useful for more complex grouping.

# Import judge cleaning utility from GjudgeListEM.
from GjudgeListEM import _clean_judge_name

log = logging.getLogger(__name__)

# --- Regular Expression Patterns ---
# Pattern to detect an agenda line, capturing date and part.
# Example: "AGENDA: MONDAY, JULY 22, 2024 , PART A"
AGENDA_PATTERN = re.compile(r"^\s*AGENDA:\s*(\w+,\s+\w+\s+\d{1,2},\s+\d{4})\s*,\s*PART\s+(\w+)", re.IGNORECASE)

# Pattern to detect a line listing judges, capturing the names.
# Handles "Hon." prefixes and ", t/a" suffixes implicitly via _clean_judge_name.
# Example: "JUDGES: SMITH, JONES, t/a, RODRIGUEZ"
JUDGES_LINE_PATTERN = re.compile(r"^\s*JUDGES:\s*([A-Z,\s\-\.\']+(\s*,\s*t/a)?)(?:,\s*([A-Z,\s\-\.\']+(\s*,\s*t/a)?))*", re.IGNORECASE)

# Pattern to find Appellate Division docket numbers.
# Handles optional "-T<num>" suffix and "(e)" for eCourts.
# Example: "A-1234-22", "A-1234-22-T1", "A-1234-22 (e)"
DOCKET_PATTERN = re.compile(r"\b(A-\d{4,}-\d{2}(?:-T\d{1,2})?(?:\s*\(e\))?)\b", re.IGNORECASE)

# Pattern to detect time strings like "9:30 AM" or "10:00PM".
TIME_PATTERN = re.compile(r"(\d{1,2}:\d{2}\s*[AP]M)")

# Pattern to detect an item number at the beginning of a line. Example: "1. "
ITEM_NUMBER_PATTERN = re.compile(r"^\s*(\d+)\s+")

# Pattern to detect "Consol" indicating a consolidated case.
CONSOL_PATTERN = re.compile(r"\bConsol\b", re.IGNORECASE)

# Patterns to detect lines describing the location of the hearing.
# Pattern 1: Captures lines with common courthouse keywords.
LOCATION_PATTERN_1 = re.compile(r"^\s*([A-Z\s]+(?:CRTHSE|COURTHOUSE|COMPLEX).*)", re.IGNORECASE)
# Pattern 2: Captures lines indicating remote arguments or waiver calendars.
LOCATION_PATTERN_2 = re.compile(r"^\s*(REMOTE\s+ARGUMENT|WAIVER\s+CALENDAR.*)", re.IGNORECASE)

def normalize_text(text: str | None) -> str:
    """
    Performs basic cleaning of text extracted from the PDF.
    - Replaces carriage returns and newlines with spaces.
    - Replaces multiple whitespace characters with a single space.
    - Strips leading/trailing whitespace.

    Args:
        text (str | None): The text to clean.

    Returns:
        str: The cleaned text, or an empty string if input is None.
    """
    if not text:
        return ""
    text = text.replace('\r', '').replace('\n', ' ').strip() # Standardize line breaks and strip.
    text = re.sub(r'\s+', ' ', text) # Collapse multiple spaces.
    return text

def parse_judges_from_line(line: str) -> list[str]:
    """
    Extracts and cleans judge names from a line identified as containing judge information
    (typically starting with "JUDGES:").

    It splits the line by commas and applies `_clean_judge_name` (from `GjudgeListEM`)
    to each extracted name.

    Args:
        line (str): The line of text containing judge names.

    Returns:
        list[str]: A list of cleaned judge names. Returns an empty list if no
                   valid names are found or the line doesn't match the expected format.
    """
    # Search for the "JUDGES:" pattern. While the line might start with "JUDGES:",
    # this regex was originally for broader matching; specific slicing is now used.
    match = JUDGES_LINE_PATTERN.search(line) # This regex might be overly broad if line always starts with "JUDGES:".
    if not match:
        # Fallback or stricter check if line must start with "JUDGES:"
        if line.upper().strip().startswith("JUDGES:"):
            log.debug(f"JUDGES_LINE_PATTERN did not match but line starts with 'JUDGES:'. Proceeding with split. Line: '{line}'")
        else:
            log.warning(f"Line does not appear to be a valid judges line: '{line}'")
            return []

    # Get the part of the string after "JUDGES:".
    try:
        full_judge_string = line.split(":", 1)[1].strip()
    except IndexError:
        log.warning(f"Could not split judge line by ':' as expected: '{line}'")
        return []

    judges = []
    # Split the string of names by comma. Handles names like "DOE, JOHN P., t/a".
    raw_names = [name.strip() for name in full_judge_string.split(',') if name.strip()]
    for raw_name_part in raw_names:
        # Clean each individual name part using the utility from GjudgeListEM.
        cleaned_name = _clean_judge_name(raw_name_part)
        if cleaned_name:
            judges.append(cleaned_name)
        elif raw_name_part: # Log if a non-empty part becomes empty after cleaning.
            log.debug(f"Raw judge name part '{raw_name_part}' resulted in empty string after cleaning.")
    return judges

def parse_calendar_pdf(pdf_path: str) -> tuple[list[dict] | None, str | None]:
    """
    Parses a single NJ Appellate Division calendar PDF file to extract structured
    information about scheduled case events.

    Args:
        pdf_path (str): The file system path to the PDF calendar file.

    Returns:
        tuple: A tuple containing:
            - list[dict] | None: A list of dictionaries, where each dictionary
              represents a parsed case entry. Returns None if a critical error occurs
              during parsing or if no valid data is found.
            - str | None: A suggested new filename for the processed PDF, formatted as
              "YYYYMMDD-YYYYMMDD.pdf" based on the min/max dates found in the calendar.
              Returns None if dates cannot be determined.
    """
    extracted_cases = [] # List to store dictionaries of parsed case data.
    calendar_dates_found = set() # To store all unique dates mentioned in AGENDAs.
    argument_days_of_week = set() # To store unique days of the week (e.g., "Monday").
    calendar_id = str(uuid.uuid4()) # Unique ID for this parsing run/calendar document.
    processing_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # --- State variables to maintain context during line-by-line parsing ---
    current_date_iso = None       # ISO format date (YYYY-MM-DD) of the current agenda.
    current_part_letter = None    # Part letter (e.g., "A", "B") from the agenda.
    current_location_text = "Unknown Location" # Default location.
    current_judges_for_part = []  # Judges assigned to the current Part.
    current_judges_for_case_group = [] # Judges for a specific case or group under an argument type.
    current_is_oral_argument = None # Boolean: True for Oral, False for Waiver, None if not in section.
    last_item_number_parsed = 0       # Track item numbers to detect resets/new sections.
    consolidated_docket_block = [] # Temporarily stores dockets part of a "Consol" block.

    log.info(f"Starting parsing of PDF calendar: {pdf_path}")

    try:
        with pdfplumber.open(pdf_path) as pdf: # Open the PDF file.
            for page_num, pdf_page in enumerate(pdf.pages, 1): # Iterate through each page.
                log.debug(f"Processing Page {page_num} of '{os.path.basename(pdf_path)}'...")
                # Extract text from the page. x_tolerance and y_tolerance help with layout.
                # layout=False provides a more raw text stream, which might be better for regex.
                page_text_content = pdf_page.extract_text(x_tolerance=2, y_tolerance=3, layout=False)
                if not page_text_content:
                    log.warning(f"No text extracted from page {page_num} of '{pdf_path}'.")
                    continue # Skip empty pages.

                lines = page_text_content.split('\n') # Split page content into lines.
                line_idx = 0
                while line_idx < len(lines): # Iterate through lines on the page.
                    current_line = lines[line_idx].strip()
                    line_idx += 1 # Increment line index for next iteration.

                    if not current_line: continue # Skip empty lines.

                    # --- State Change Detection: Agenda, Argument Type, Case-Specific Judges ---
                    agenda_match_obj = AGENDA_PATTERN.search(current_line)
                    if agenda_match_obj:
                        # Found an "AGENDA:" line, signifies a new section with date, part, location, judges.
                        raw_date_str = agenda_match_obj.group(1)
                        current_part_letter = agenda_match_obj.group(2).strip()
                        # Reset context for the new agenda.
                        current_is_oral_argument = None
                        current_judges_for_case_group = []
                        current_judges_for_part = []
                        current_location_text = "Unknown Location"
                        last_item_number_parsed = 0 # Item numbering resets for each new agenda/part.
                        log.debug(f"New AGENDA detected: Date='{raw_date_str}', Part='{current_part_letter}'")
                        try:
                            # Parse the date string from the agenda line.
                            parsed_date_obj = date_parse(raw_date_str)
                            calendar_dates_found.add(parsed_date_obj.date()) # Store unique dates.
                            argument_days_of_week.add(parsed_date_obj.strftime('%A')) # Store day of week.
                            current_date_iso = parsed_date_obj.strftime('%Y-%m-%d')

                            # Look for Location and Judges on the immediately following lines.
                            if line_idx < len(lines): # Check if there's a next line for location.
                                location_line_text = lines[line_idx].strip()
                                location_match_type1 = LOCATION_PATTERN_1.match(location_line_text)
                                location_match_type2 = LOCATION_PATTERN_2.match(location_line_text)
                                if location_match_type1:
                                    current_location_text = normalize_text(location_match_type1.group(1))
                                elif location_match_type2:
                                    location_text_upper = location_match_type2.group(1).upper()
                                    if "REMOTE" in location_text_upper: current_location_text = "Virtual Hearing"
                                    elif "WAIVER" in location_text_upper: current_location_text = "Waiver Calendar (No Location)"
                                else:
                                    log.warning(f"Could not parse location from line: '{location_line_text}' following agenda.")
                                line_idx += 1 # Consume the location line.
                            if line_idx < len(lines): # Check for judges line.
                                judges_line_text = lines[line_idx].strip()
                                if judges_line_text.upper().startswith("JUDGES:"):
                                     current_judges_for_part = parse_judges_from_line(judges_line_text)
                                     log.debug(f"Part Judges for Part {current_part_letter}: {current_judges_for_part}")
                                     line_idx += 1 # Consume the judges line.
                        except Exception as e_agenda_parse:
                            log.error(f"Error parsing date or subsequent lines for agenda '{raw_date_str}': {e_agenda_parse}")
                            current_date_iso = None # Invalidate date if parsing fails.
                        continue # Move to next line after processing agenda block.

                    # Detect "ORAL ARGUMENT" section header.
                    if "ORAL ARGUMENT" in current_line.upper() and current_is_oral_argument is None:
                        current_is_oral_argument = True
                        current_judges_for_case_group = [] # Reset case/group judges.
                        log.debug("Entering ORAL ARGUMENT section.")
                        continue
                    # Detect "WAIVER" section header (ensure not part of "WAIVER CALENDAR" location).
                    if "WAIVER" in current_line.upper() and "CALENDAR" not in current_line.upper() and current_is_oral_argument is None:
                        current_is_oral_argument = False
                        current_judges_for_case_group = [] # Reset case/group judges.
                        log.debug("Entering WAIVER (Submission) section.")
                        continue

                    # Detect case-specific "JUDGES:" line (must be within an Oral/Waiver section).
                    if current_is_oral_argument is not None and current_line.upper().startswith("JUDGES:"):
                        current_judges_for_case_group = parse_judges_from_line(current_line)
                        log.debug(f"Found Case/Group specific Judges: {current_judges_for_case_group}")
                        continue

                    # --- Case Entry Detection and Parsing ---
                    item_num_match = ITEM_NUMBER_PATTERN.match(current_line)
                    docket_num_match = DOCKET_PATTERN.search(current_line)

                    # A line is considered a case entry if we are in an Oral/Waiver section
                    # AND it starts with an item number AND contains a docket number.
                    if current_is_oral_argument is not None and item_num_match and docket_num_match:
                        parsed_item_number = int(item_num_match.group(1))
                        # If item number resets or is not sequential, current consolidation block ends.
                        if parsed_item_number <= last_item_number_parsed:
                            if consolidated_docket_block:
                                log.debug(f"Item number reset/out of sequence ({parsed_item_number} after {last_item_number_parsed}). "
                                          f"Ending previous consolidation block: {consolidated_docket_block}")
                                # Note: Logic to process this premature end of block might be complex if details are spread.
                                # Current simplified model processes block when a non-consolidated item is hit, or at end of page/file.
                            consolidated_docket_block = []
                        last_item_number_parsed = parsed_item_number

                        parsed_docket_number = docket_num_match.group(1).strip()
                        is_marked_consolidated = bool(CONSOL_PATTERN.search(current_line))

                        # Extract Hearing Time (primarily for Oral Arguments).
                        hearing_time_str = None
                        if current_is_oral_argument: # Times are usually only for oral arguments.
                            time_str_match = TIME_PATTERN.search(current_line)
                            if time_str_match:
                                hearing_time_str = time_str_match.group(1)
                            # Simple lookahead for time on the next line if not on current.
                            elif line_idx < len(lines):
                                next_line_time_match = TIME_PATTERN.search(lines[line_idx])
                                if next_line_time_match: hearing_time_str = next_line_time_match.group(1)

                        # --- Multi-line Caption Extraction ---
                        caption_parts_list = []
                        # Start caption from current line after removing item#, docket, time, consol markers.
                        line_for_caption = current_line.replace(item_num_match.group(0), '', 1)
                        line_for_caption = line_for_caption.replace(parsed_docket_number, '', 1) # Remove first docket.
                        if hearing_time_str: line_for_caption = line_for_caption.replace(hearing_time_str, '', 1)
                        if is_marked_consolidated: line_for_caption = CONSOL_PATTERN.sub('', line_for_caption)
                        caption_parts_list.append(line_for_caption.strip())

                        # Look ahead for subsequent lines that are part of the same caption.
                        caption_lookahead_idx = line_idx
                        while caption_lookahead_idx < len(lines):
                            next_potential_caption_line = lines[caption_lookahead_idx].strip()
                            # Heuristics to stop caption gathering:
                            # - Empty line.
                            # - Line starts with another item number.
                            # - Line indicates new Judges list.
                            # - Line indicates a new Agenda or Argument Type section.
                            # - Line contains another docket number (likely start of new case not properly numbered).
                            if not next_potential_caption_line or \
                               ITEM_NUMBER_PATTERN.match(next_potential_caption_line) or \
                               next_potential_caption_line.upper().startswith("JUDGES:") or \
                               AGENDA_PATTERN.search(next_potential_caption_line) or \
                               "ORAL ARGUMENT" in next_potential_caption_line.upper() or \
                               ("WAIVER" in next_potential_caption_line.upper() and "CALENDAR" not in next_potential_caption_line.upper()) or \
                               DOCKET_PATTERN.search(next_potential_caption_line):
                                break # End of current caption.
                            caption_parts_list.append(next_potential_caption_line)
                            caption_lookahead_idx += 1
                        # The main loop index `line_idx` is NOT advanced here; it's advanced by the outer loop.

                        full_caption = normalize_text(" ".join(caption_parts_list))

                        # Determine which set of judges applies to this case.
                        final_assigned_judges = current_judges_for_case_group if current_judges_for_case_group else current_judges_for_part

                        # --- Handle Consolidation Logic ---
                        if is_marked_consolidated:
                            if not consolidated_docket_block: # This is the first case in a new "Consol" block.
                                consolidated_docket_block = [parsed_docket_number]
                            else: # Add subsequent "Consol" case to the current block.
                                consolidated_docket_block.append(parsed_docket_number)
                        else: # Current case is NOT marked "Consol".
                            # If there was an active consolidation block, it ends here. Process it.
                            if consolidated_docket_block:
                                log.debug(f"Processing consolidated block: {consolidated_docket_block} due to non-consolidated item {parsed_item_number}.")
                                # The details (caption, judges, time etc.) of the *first* case in a
                                # consolidated block are typically applied to all dockets in that block.
                                # This requires storing those first-case details when the block started.
                                # For simplicity, this implementation uses the current (last) case's details for prior consol dockets.
                                # A more robust approach would store the 'anchor' case details of the block.
                                for consol_idx, consol_docket_id in enumerate(consolidated_docket_block):
                                    linked_consol_dockets = [d for i, d in enumerate(consolidated_docket_block) if i != consol_idx]
                                    consol_case_entry = {
                                        "CalendarID": calendar_id, "ProcessingTimestamp": processing_timestamp,
                                        "HearingDate": current_date_iso, "HearingTime": hearing_time_str, # Time might apply to whole block.
                                        "CourtPart": current_part_letter, "Location": current_location_text,
                                        "OralArgument": current_is_oral_argument,
                                        "ItemNumber": parsed_item_number - len(consolidated_docket_block) + consol_idx, # Attempt to reconstruct item num.
                                        "AppDocketID": consol_docket_id,
                                        "LinkedDocketIDs": ",".join(linked_consol_dockets) if linked_consol_dockets else None,
                                        "CaseName": full_caption, # Caption of the *last* item before block processing.
                                        "AssignedJudges": ",".join(final_assigned_judges),
                                        "PresidingJudgesPart": ",".join(current_judges_for_part), # Part judges remain same.
                                        "IsConsolidated": True,
                                    }
                                    extracted_cases.append(consol_case_entry)
                                consolidated_docket_block = [] # Reset block.

                            # Process the current non-consolidated case.
                            case_data = {
                                "CalendarID": calendar_id, "ProcessingTimestamp": processing_timestamp,
                                "HearingDate": current_date_iso, "HearingTime": hearing_time_str,
                                "CourtPart": current_part_letter, "Location": current_location_text,
                                "OralArgument": current_is_oral_argument,
                                "ItemNumber": parsed_item_number,
                                "AppDocketID": parsed_docket_number,
                                "LinkedDocketIDs": None, # Not part of a processed consol block here.
                                "CaseName": full_caption,
                                "AssignedJudges": ",".join(final_assigned_judges),
                                "PresidingJudgesPart": ",".join(current_judges_for_part),
                                "IsConsolidated": False,
                            }
                            extracted_cases.append(case_data)
                        log.debug(f"Parsed Item {parsed_item_number}: Docket={parsed_docket_number}, Oral={current_is_oral_argument}, Consol={is_marked_consolidated}, Caption='{full_caption[:50]}...'")

            # --- End of Page/File Consolidation Check ---
            # After processing all lines on a page, if a consolidated block is still active, process it.
            # This primarily handles cases where a PDF ends mid-consolidation block.
            if line_idx >= len(lines) and consolidated_docket_block: # If at end of lines for page
                 log.debug(f"Processing consolidated block at end of page: {consolidated_docket_block}")
                 # Similar logic as above for processing the block.
                 # This part needs careful state management if details are from first item of block.
                 # Using last known caption/details for simplicity here.
                 last_known_caption = extracted_cases[-1]['CaseName'] if extracted_cases and not extracted_cases[-1]['IsConsolidated'] else "Caption from last consolidated item"
                 last_known_item_num = extracted_cases[-1]['ItemNumber'] if extracted_cases and not extracted_cases[-1]['IsConsolidated'] else last_item_number_parsed

                 for consol_idx, consol_docket_id in enumerate(consolidated_docket_block):
                     linked_consol_dockets = [d for i, d in enumerate(consolidated_docket_block) if i != consol_idx]
                     consol_case_entry = {
                         "CalendarID": calendar_id, "ProcessingTimestamp": processing_timestamp,
                         "HearingDate": current_date_iso, "HearingTime": hearing_time_str, # Use last known time
                         "CourtPart": current_part_letter, "Location": current_location_text,
                         "OralArgument": current_is_oral_argument,
                         "ItemNumber": last_known_item_num, # Use item number of the (last) consol item
                         "AppDocketID": consol_docket_id,
                         "LinkedDocketIDs": ",".join(linked_consol_dockets) if linked_consol_dockets else None,
                         "CaseName": last_known_caption, # Use last known caption.
                         "AssignedJudges": ",".join(final_assigned_judges if final_assigned_judges else current_judges_for_part),
                         "PresidingJudgesPart": ",".join(current_judges_for_part),
                         "IsConsolidated": True,
                     }
                     # Avoid adding if it was the *very last* item processed and already added.
                     # This check is imperfect but aims to prevent simple duplicates from this specific end-of-page logic.
                     if not (len(extracted_cases) > 0 and extracted_cases[-1]['AppDocketID'] == consol_docket_id and extracted_cases[-1]['IsConsolidated']):
                        extracted_cases.append(consol_case_entry)
                 consolidated_docket_block = [] # Reset block.

    except Exception as e_pdf_parse:
        log.error(f"Critical error parsing PDF file '{pdf_path}': {e_pdf_parse}", exc_info=True)
        return None, None # Indicate failure for the entire PDF.

    # --- Post-Parsing Processing ---
    if not calendar_dates_found:
        log.error(f"No calendar dates found in PDF: {pdf_path}. Cannot generate metadata or filename.")
        return None, None # Cannot proceed without dates.

    try:
        # Determine date range and suggested filename.
        min_date = min(calendar_dates_found)
        max_date = max(calendar_dates_found)
        date_range_display_str = f"{min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}"
        # Filename based on start date, or range if different.
        if min_date == max_date:
            new_suggested_filename = f"{min_date.strftime('%Y%m%d')}_AppDivCalendar.pdf"
        else:
            new_suggested_filename = f"{min_date.strftime('%Y%m%d')}-{max_date.strftime('%Y%m%d')}_AppDivCalendar.pdf"

        # ISO Year and Week number from the start date for categorization.
        calendar_iso_year_week = f"{min_date.strftime('%Y')}_{min_date.isocalendar()[1]:02d}"

        final_case_list = []
        # Temporary set to help remove exact duplicates that might arise from complex consol logic.
        # An exact duplicate here means all fields are identical including AppDocketID.
        temp_seen_for_dedupe = set()

        for case_entry_dict in extracted_cases:
            # Add common calendar metadata to each case record.
            case_entry_dict["CalendarDateRange"] = date_range_display_str
            case_entry_dict["CalendarYearWeek"] = calendar_iso_year_week
            # Ensure ArgumentDays is a sorted list of unique day names.
            case_entry_dict["CalendarArgumentDays"] = sorted(list(argument_days_of_week))
            # 'OralArgument' field (boolean) is already set during parsing.

            # Create a frozenset of items for robust deduplication of entire records.
            # This helps if the consolidation logic accidentally creates identical entries.
            record_tuple_for_dedupe = tuple(sorted(case_entry_dict.items()))
            if record_tuple_for_dedupe not in temp_seen_for_dedupe:
                final_case_list.append(case_entry_dict)
                temp_seen_for_dedupe.add(record_tuple_for_dedupe)
            else:
                log.warning(f"Exact duplicate record removed during final processing: Docket {case_entry_dict.get('AppDocketID')}, CalendarID {calendar_id}")

        log.info(f"Finished parsing '{pdf_path}'. Found {len(final_case_list)} unique case entries. "
                 f"Date range: {date_range_display_str}. Suggested new filename: '{new_suggested_filename}'")
        return final_case_list, new_suggested_filename

    except Exception as e_post_proc:
        log.error(f"Error during post-processing or filename generation for '{pdf_path}': {e_post_proc}", exc_info=True)
        return None, None # Indicate failure.

# === End of GcalendarParserEM.py ===
[end of Modules/GcalendarParserEM.py]
