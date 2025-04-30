#
# """
#Scrapes the NJ Courts Expected Opinions page, extracts case information,
#and stores it in primary and backup SQLite databases with scheduling and validation.
#"""
import requests
import bs4
import schedule
import time
import sqlite3
import hashlib
import datetime
import re
import uuid
import argparse
import sys
import os
import glob
import random
import string
import json
import subprocess
from dateutil.parser import parse as date_parse
import select
def prompt_input(prompt_text):
    """Prompt user for input, exit if 'cancel' is entered."""
    response = input(prompt_text).strip()
    if response.lower() == "cancel":
        print("Cancelled by user.")
        sys.exit(0)
    return response


def print_terminal_multiplexer_instructions():
    print("\n--- Terminal Multiplexer Guidance ---")
    in_screen = "STY" in os.environ
    in_tmux = "TMUX" in os.environ
    if in_screen:
        print("You are running inside a 'screen' session.")
        print("Detach: Press Ctrl+a then d")
        print("Reattach: Run 'screen -r' from your terminal.")
    elif in_tmux:
        print("You are running inside a 'tmux' session.")
        print("Detach: Press Ctrl+b then d")
        print("Reattach: Run 'tmux attach' from your terminal.")
    else:
        print("You are NOT running inside 'screen' or 'tmux'.")
        print("For long-running jobs, consider using one of these tools:")
        print("  To use screen:")
        print("    1. Start a session:   screen -S mysession")
        print("    2. Run this script inside the session.")
        print("    3. Detach:            Ctrl+a then d")
        print("    4. Reattach:          screen -r mysession")
        print("  To use tmux:")
        print("    1. Start a session:   tmux new -s mysession")
        print("    2. Run this script inside the session.")
        print("    3. Detach:            Ctrl+b then d")
        print("    4. Reattach:          tmux attach -t mysession")
        print("--------------------------------------------------")
        while True:
            choice = prompt_input("Would you like to [Q]uit and run the script using tmux or screen, or [C]ontinue without? (Q/C or 'cancel'): ").strip().upper()
            if choice == "Q":
                print("Exiting. Please start a 'screen' or 'tmux' session and rerun this script inside it.")
                sys.exit(0)
            elif choice == "C":
                print("Continuing without screen/tmux. Note: Detach/reattach will not be available.")
                break
            else:
                print("Please enter 'Q' to quit, 'C' to continue, or 'cancel' to exit.")
    print("--------------------------------------------------\n")

    
# --- Configuration ---
URL = "https://www.njcourts.gov/attorneys/opinions/expected"
PRIMARY_DB = "primary_opinions.db"
BACKUP_DB = "backup_opinions.db"
TEST_DB = "test_opinions.db"
LOG_DIR = "logs"
PRIMARY_RUN_TIME = "10:30"
BACKUP_RUN_TIME = "09:30"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36" # Be a good citizen
MAX_TEST_DBS = 3
STATUS_FILE = "status.json"

# --- Database Setup ---
DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS opinions (
    UniqueID TEXT PRIMARY KEY,
    AppDocketID TEXT NOT NULL,
    LinkedDocketIDs TEXT,
    CaseName TEXT,
    LCdocketID TEXT,
    LCdocketID2 TEXT,
    LCdocketID3 TEXT,
    LCCounty TEXT,
    OpApplication TEXT,
    DecisionType TEXT,
    CaseNotes TEXT,
    ReleaseDate TEXT NOT NULL,
    ScrapeTimestamp TEXT NOT NULL,
    RunType TEXT NOT NULL, -- 'Primary' or 'Backup' or 'Test'
    DuplicateFlag INTEGER DEFAULT 0, -- 0 = No, 1 = Yes (based on hash)
    OfficialFlag INTEGER DEFAULT 0, -- 1 = Explicitly approved, 0 = Unofficial
    ApprovalTimestamp TEXT -- When approved or auto-inserted
);

CREATE INDEX IF NOT EXISTS idx_appdocketid ON opinions (AppDocketID);
CREATE INDEX IF NOT EXISTS idx_releasedate ON opinions (ReleaseDate);
CREATE INDEX IF NOT EXISTS idx_runtype ON opinions (RunType);
CREATE INDEX IF NOT EXISTS idx_casetype ON opinions (DecisionType);
"""

def init_db(db_name):
    """Initializes the SQLite database and creates the table if it doesn't exist."""
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        cursor.executescript(DB_SCHEMA)
        conn.commit()
        print(f"Database '{db_name}' initialized successfully.")
    except sqlite3.Error as e:
        print(f"Error initializing database {db_name}: {e}", file=sys.stderr)
        sys.exit(1) # Exit if DB can't be initialized
    finally:
        if conn:
            conn.close()

# --- Web Scraping and Parsing ---

def parse_case_title(title_text):
    """
    Robust parser for NJ case titles from HTML.
    - Case name: everything before the first parenthesis group.
    - Dockets: all dockets from all parenthesis groups.
    - County/applicability: from first parenthesis group.
    - Notes: from any remaining parenthesis groups.
    """
    data = {
        'CaseName': None,
        'LCdocketID': None,
        'LCdocketID2': None,
        'LCdocketID3': None,
        'LCCounty': None,
        'Applicability': None,
        'OpApplication': None,
        'CaseNotes': None,
        'MissingLCDocketFlag': False
    }

    # Regex for common NJ docket formats (expand as needed)
    docket_patterns = [
        r'[A-Z]{1,3}-\d{2}-\d{4}',
        r'[A-Z]{1,3}-\d{4}-\d{2}',
        r'[A-Z]{1,3}-\d{3,5}-\d{2}',
        r'[A-Z]{1,3}-\d{2,4}',
        r'\d{2}-\d{2}-\d{4}',
        r'\d{2}-\d{4}',
        r'[A-Z]{1,3}-\d{2,4}-\d{2,4}-\d{2,4}',
        r'\d{2}-\d{2}-\d{4}',
        r'\d{2}-\d{2}-\d{4}',  # e.g., 19-09-2304
        r'\d{6,}-\d{4}',       # e.g., 006902-2020
    ]
    docket_regex = re.compile('|'.join(docket_patterns))

    # Find all parenthesis groups
    paren_groups = re.findall(r'\([^)]+\)', title_text)
    # Case name is everything before the first parenthesis group
    case_name = title_text.split('(')[0].strip().rstrip(',')
    data['CaseName'] = case_name

    # Extract dockets from all paren groups
    all_dockets = []
    for group in paren_groups:
        all_dockets.extend(docket_regex.findall(group))

    # Assign up to 3 dockets
    if all_dockets:
        data['LCdocketID'] = all_dockets[0]
        if len(all_dockets) > 1:
            data['LCdocketID2'] = all_dockets[1]
        if len(all_dockets) > 2:
            data['LCdocketID3'] = all_dockets[2]
    else:
        data['MissingLCDocketFlag'] = True

    # County and applicability from first paren group (if present)
    county = None
    applicability = None
    if paren_groups:
        first_group = paren_groups[0].strip('()')
        # County
        county_match = re.search(r'([A-Z][A-Z\s]+) COUNTY', first_group, re.IGNORECASE)
        if county_match:
            county = county_match.group(1).strip().title() + " County"
        # Applicability
        if 'STATEWIDE' in first_group.upper():
            applicability = 'Statewide'
    data['LCCounty'] = county
    data['Applicability'] = applicability
    data['OpApplication'] = applicability

    # Notes: any paren group that is not the first and not a docket/county/applicability
    notes = []
    for group in paren_groups[1:]:
        group_clean = group.strip('()')
        if not docket_regex.search(group_clean) and 'COUNTY' not in group_clean.upper() and 'STATEWIDE' not in group_clean.upper():
            notes.append(group_clean)
    data['CaseNotes'] = "; ".join(notes) if notes else None

    return data


def map_decision_type(type_string):
    """Maps the text description to a short code."""
    type_string_lower = type_string.lower().strip()
    if "unpublished appellate" in type_string_lower:
        return "appUNpub"
    elif "published appellate" in type_string_lower:
        return "appPUB"
    elif "supreme" in type_string_lower:
        return "Supreme"
    elif "unpublished tax" in type_string_lower:
        return "taxUNpub"
    elif "published tax" in type_string_lower:
        return "taxPUB"
    elif "unpublished trial" in type_string_lower:
        return "trialUNpub"
    elif "published trial" in type_string_lower:
        return "trialPUB"
    # Add other mappings as needed (e.g., Rule Hearing)
    else:
        return type_string # Fallback

def fetch_and_parse_opinions(url):
    """Fetches and parses the NJ Courts expected opinions page."""
    opinions = []
    release_date_str = None
    try:
        headers = {'User-Agent': USER_AGENT}
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status() # Raise an exception for bad status codes
        soup = bs4.BeautifulSoup(response.text, 'html.parser')

        # --- Extract Release Date ---
        # Find the h2 tag containing the release date text
        header_div = soup.find('div', class_='view-header')
        if header_div:
            h2_tag = header_div.find('h2')
            if h2_tag:
                # Extract text like "Expected opinions for release on April 22, 2025"
                match = re.search(r'on\s+(.+)', h2_tag.get_text(), re.IGNORECASE)
                if match:
                    try:
                        # Attempt to parse the date string
                        release_date_dt = date_parse(match.group(1).strip())
                        release_date_str = release_date_dt.strftime('%Y-%m-%d') # Standard format
                    except ValueError:
                        print(f"Warning: Could not parse date from '{match.group(1).strip()}'", file=sys.stderr)
                        release_date_str = match.group(1).strip() # Use raw string as fallback
                else:
                     print("Warning: Could not find date pattern in H2 tag.", file=sys.stderr)
            else:
                 print("Warning: H2 tag not found within view-header.", file=sys.stderr)
        else:
            print("Warning: view-header div not found.", file=sys.stderr)

        if not release_date_str:
            print("Warning: Release date could not be determined. Using today's date as fallback.", file=sys.stderr)
            release_date_str = datetime.date.today().strftime('%Y-%m-%d')


        # --- Extract Opinion Details ---
        # Find all opinion articles within the main content area
        # The structure seems to be nested: find the container first
        main_content_wrapper = soup.find('div', class_='views-infinite-scroll-content-wrapper')
        if not main_content_wrapper:
             print("Error: Could not find main content wrapper 'views-infinite-scroll-content-wrapper'. Page structure might have changed.", file=sys.stderr)
             return [], release_date_str # Return empty list if structure is broken

        articles = main_content_wrapper.find_all('article', class_='w-100')

        if not articles:
            print("Warning: No opinion articles found. Check page structure or content.", file=sys.stderr)


        for article in articles:
            card_body = article.find('div', class_='card-body')
            if not card_body:
                continue # Skip if structure is unexpected

            title_div = card_body.find('div', class_='card-title')
            if not title_div:
                continue # Skip if structure is unexpected

            title_text = title_div.get_text(strip=True)

            # Check for "No opinions reported" messages first
            if title_text.startswith("No") and "opinions reported" in title_text:
                print(f"Info: Found '{title_text}'")
                # Optionally log this, but don't add to the main opinions list
                continue # Skip to the next article

            # --- This seems to be a valid case entry ---
            parsed_title = parse_case_title(title_text)

            # Find docket number and decision type badges
            badge_spans = card_body.find_all('span', class_='badge')
            app_docket_ids_raw = None
            decision_type_raw = None
            for span in badge_spans:
                text = span.get_text(strip=True)
                # Check if it looks like an appellate docket ID (A-####-##)
                if re.match(r'^[A]-\d{4}-\d{2}', text):
                    app_docket_ids_raw = text
                else:
                    # Assume the other badge is the decision type
                    decision_type_raw = text

            if not app_docket_ids_raw:
                print(f"Warning: Could not find Appellate Docket ID for case '{parsed_title.get('CaseName', 'Unknown')}'. Skipping.", file=sys.stderr)
                continue # Cannot proceed without the primary key

            if not decision_type_raw:
                 print(f"Warning: Could not find Decision Type for case '{parsed_title.get('CaseName', 'Unknown')}'. Using 'Unknown'.", file=sys.stderr)
                 decision_type_raw = "Unknown"


            decision_type = map_decision_type(decision_type_raw)

            # Handle multiple/consolidated docket numbers
            app_docket_ids = re.findall(r'(A-\d{4}-\d{2})', app_docket_ids_raw)

            if not app_docket_ids:
                 print(f"Warning: Could not parse individual docket IDs from '{app_docket_ids_raw}' for case '{parsed_title.get('CaseName', 'Unknown')}'. Skipping.", file=sys.stderr)
                 continue


            # Create a separate record for each primary AppDocketID
            for i, primary_docket in enumerate(app_docket_ids):
                linked_dockets = [d for j, d in enumerate(app_docket_ids) if i != j]

                opinion_data = {
                    "AppDocketID": primary_docket.strip(),
                    "LinkedDocketIDs": ", ".join(linked_dockets) if linked_dockets else None,
                    "CaseName": parsed_title.get('CaseName'),
                    "LCdocketID": parsed_title.get('LCdocketID'),
                    "LCdocketID2": parsed_title.get('LCdocketID2'),
                    "LCdocketID3": parsed_title.get('LCdocketID3'),
                    "LCCounty": parsed_title.get('LCCounty'),
                    "OpApplication": parsed_title.get('OpApplication'),
                    "DecisionType": decision_type,
                    "CaseNotes": parsed_title.get('CaseNotes'),
                    "ReleaseDate": release_date_str,
                    "MissingLCDocketFlag": parsed_title.get('MissingLCDocketFlag', False),
                    # UniqueID, Hash, Timestamps, RunType added during insertion
                }
                opinions.append(opinion_data)

    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL {url}: {e}", file=sys.stderr)
    except Exception as e:
        print(f"Error parsing HTML: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc() # Print full traceback for parsing errors

    return opinions, release_date_str

# --- Database Operations ---

def generate_unique_id(data, run_type, scrape_timestamp):
    """Generates a unique ID incorporating key data points and a hash."""
    # Create a stable string representation of the core data
    core_data_str = f"{data.get('AppDocketID', '')}|{data.get('CaseName', '')}|{data.get('ReleaseDate', '')}|{data.get('DecisionType', '')}"
    # Use SHA-256 hash of the core data
    data_hash = hashlib.sha256(core_data_str.encode('utf-8')).hexdigest()

    # Generate a UUID based on the hash and a namespace (e.g., DNS namespace)
    # This ensures the same input data always produces the same UUID
    # Using AppDocketID + ReleaseDate as the name for UUID generation
    name_for_uuid = f"{data.get('AppDocketID', 'NO_DOCKET')}-{data.get('ReleaseDate', 'NO_DATE')}"
    generated_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, name_for_uuid)

    # Combine elements for the final UniqueID string
    # Format: UUID-RunType-Timestamp-First8Hash
    unique_id_str = f"{generated_uuid}-{run_type}-{scrape_timestamp.replace(':', '').replace('-', '').replace(' ', '')}-{data_hash[:8]}"
    return unique_id_str, data_hash # Return both the full ID and the hash

def check_duplicate(cursor, data_hash):
    """Checks if a record with the same data hash already exists."""
    # Note: This simple hash check might have collisions, though unlikely with SHA-256.
    # A more robust check would compare more fields.
    # We generate UUID based on AppDocketID and ReleaseDate, so checking for that UUID might be better.
    # Let's stick to the hash check as initially requested for the DuplicateFlag.
    cursor.execute("SELECT 1 FROM opinions WHERE SUBSTR(UniqueID, -8) = ?", (data_hash[:8],))
    return cursor.fetchone() is not None

def insert_data(db_name, data_list, run_type, release_date, official_flag=0, approval_timestamp=None):
    """Inserts the list of opinion data into the specified database."""
    if not data_list:
        print(f"No data to insert into {db_name}.")
        return 0

    inserted_count = 0
    conn = None
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        scrape_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        approval_timestamp = approval_timestamp or scrape_timestamp

        for record in data_list:
            unique_id, data_hash = generate_unique_id(record, run_type, scrape_timestamp)
            cursor.execute("SELECT 1 FROM opinions WHERE AppDocketID = ? AND ReleaseDate = ?",
                           (record['AppDocketID'], record['ReleaseDate']))
            duplicate_exists = cursor.fetchone() is not None

            data_tuple = (
                unique_id,
                record['AppDocketID'],
                record['LinkedDocketIDs'],
                record['CaseName'],
                record['LCdocketID'],
                record.get('LCdocketID2'),
                record.get('LCdocketID3'),
                record['LCCounty'],
                record['OpApplication'],
                record['DecisionType'],
                record['CaseNotes'],
                record['ReleaseDate'],
                scrape_timestamp,
                run_type,
                1 if duplicate_exists else 0, # DuplicateFlag
                official_flag,
                approval_timestamp
            )

            try:
                cursor.execute("""
                    INSERT OR IGNORE INTO opinions (
                        UniqueID, AppDocketID, LinkedDocketIDs, CaseName, LCdocketID, LCdocketID2, LCdocketID3,
                        LCCounty, OpApplication, DecisionType, CaseNotes, ReleaseDate,
                        ScrapeTimestamp, RunType, DuplicateFlag, OfficialFlag, ApprovalTimestamp
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, data_tuple)

                if cursor.rowcount > 0:
                    inserted_count += 1

            except sqlite3.Error as e:
                print(f"Error inserting record for {record.get('AppDocketID', 'N/A')}: {e}", file=sys.stderr)

        conn.commit()
        print(f"Successfully inserted {inserted_count} new records into {db_name}.")
        if len(data_list) - inserted_count > 0:
            print(f"Skipped {len(data_list) - inserted_count} duplicate records (based on AppDocketID + ReleaseDate).")

    except sqlite3.Error as e:
        print(f"Database error with {db_name}: {e}", file=sys.stderr)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
    return inserted_count

def get_primary_data_for_date(db_name, target_date):
    """Retrieves primary run data for a specific release date."""
    data = {}
    conn = None
    try:
        conn = sqlite3.connect(db_name)
        # Ensure compatibility with older Python versions lacking row_factory
        # conn.row_factory = sqlite3.Row # Access columns by name
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM opinions
            WHERE ReleaseDate = ? AND RunType = 'Primary'
            ORDER BY ScrapeTimestamp DESC
        """, (target_date,))
        rows = cursor.fetchall()
        # Manually create dictionaries from tuples
        colnames = [desc[0] for desc in cursor.description]
        for row in rows:
            row_dict = dict(zip(colnames, row))
            # Use AppDocketID as key for easy comparison
            if row_dict.get('AppDocketID'):
                 # Store the most recent primary record for this docket/date
                 if row_dict['AppDocketID'] not in data or row_dict['ScrapeTimestamp'] > data[row_dict['AppDocketID']]['ScrapeTimestamp']:
                      data[row_dict['AppDocketID']] = row_dict

    except sqlite3.Error as e:
        print(f"Error fetching primary data for {target_date} from {db_name}: {e}", file=sys.stderr)
    finally:
        if conn:
            conn.close()
    return data


def fix_county_and_applicability(db_name):
    """Update prior records to ensure LCCounty includes 'COUNTY' and Applicability/OpApplication is set properly."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    updated_count = 0

    # Update LCCounty to include 'COUNTY' if not already present and not null/empty
    cursor.execute("""
        UPDATE opinions
        SET LCCounty = TRIM(LCCounty) || ' COUNTY'
        WHERE LCCounty IS NOT NULL
          AND LCCounty != ''
          AND UPPER(LCCounty) NOT LIKE '%COUNTY'
    """)
    updated_count += cursor.rowcount

    # Set Applicability and OpApplication to 'Statewide' if they contain 'STATEWIDE' (case-insensitive)
    cursor.execute("""
        UPDATE opinions
        SET Applicability = 'Statewide', OpApplication = 'Statewide'
        WHERE (Applicability IS NULL OR Applicability = '' OR Applicability NOT LIKE 'Statewide')
          AND (
                (CaseNotes LIKE '%STATEWIDE%' COLLATE NOCASE)
             OR (LCCounty LIKE '%STATEWIDE%' COLLATE NOCASE)
             OR (OpApplication LIKE '%STATEWIDE%' COLLATE NOCASE)
          )
    """)
    updated_count += cursor.rowcount

    conn.commit()
    conn.close()
    print(f"Updated {updated_count} records in {db_name} for county/applicability formatting.")


# --- UI and Logging ---

def display_data(data_list, release_date, discrepancies=None):
    """Prints the collected data and discrepancies to the console."""
    print("\n" + "="*60)
    print(f"NJ Courts Expected Opinions")
    print(f"Release Date: {release_date if release_date else 'Unknown'}")
    print(f"Scraped: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Found {len(data_list)} potential opinion entries.")
    print("="*60 + "\n")

    if discrepancies:
        print("--- Verification Discrepancies ---")
        if discrepancies['missing_in_primary']:
            print(" Cases found now but MISSING in yesterday's Primary run:")
            for d in discrepancies['missing_in_primary']: print(f"  - {d}")
        if discrepancies['missing_in_backup']:
            print("\n Cases found in yesterday's Primary run but MISSING now:")
            for d in discrepancies['missing_in_backup']: print(f"  - {d}")
        # Add more detailed comparison later if needed (e.g., changed fields)
        print("---------------------------------\n")


    if not data_list:
        print("No specific case details found or parsed.")
        return

    headers = ["AppDocketID", "DecisionType", "CaseName", "LCCounty", "LCdocketID", "OpApplication", "CaseNotes", "LinkedDocketIDs"]
    # Calculate max widths for alignment (optional, can be simple print)
    # Simple print for now:
    print(f"{'AppDocketID':<15} | {'Type':<10} | {'Case Name':<40} | {'County':<15} | {'LC Docket':<15} | {'LC Docket2':<15} | {'LC Docket3':<15} | {'Notes':<20} | {'MissingLC?'}")
    print("-"*160)
    for item in data_list:
        app_docket_id = item.get('AppDocketID') or ''
        decision_type = item.get('DecisionType') or ''
        case_name = (item.get('CaseName') or '')
        lc_county = item.get('LCCounty') or ''
        lc_docket_id = item.get('LCdocketID') or ''
        lc_docket_id2 = item.get('LCdocketID2') or ''
        lc_docket_id3 = item.get('LCdocketID3') or ''
        case_notes = item.get('CaseNotes') or ''
        missing_flag = "YES" if item.get('MissingLCDocketFlag') else ""
        print(f"{app_docket_id:<15} | {decision_type:<10} | {case_name:<40} | {lc_county:<15} | {lc_docket_id:<15} | {lc_docket_id2:<15} | {lc_docket_id3:<15} | {case_notes:<20} | {missing_flag}")
        if item.get('LinkedDocketIDs'):
            print(f"{' ':<15} | {' ':<10} | {'Linked: ' + item['LinkedDocketIDs']:<40}")


def log_output(log_filename, data_list, release_date, run_type, discrepancies=None):
    """Logs the collected data and discrepancies to a file."""
    if not os.path.exists(LOG_DIR):
        try:
            os.makedirs(LOG_DIR)
        except OSError as e:
            print(f"Error creating log directory {LOG_DIR}: {e}", file=sys.stderr)
            return # Cannot log if directory fails

    filepath = os.path.join(LOG_DIR, log_filename)
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write("="*60 + "\n")
            f.write(f"NJ Courts Expected Opinions Log\n")
            f.write(f"Run Type: {run_type}\n")
            f.write(f"Release Date: {release_date if release_date else 'Unknown'}\n")
            f.write(f"Scrape Timestamp: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Found {len(data_list)} potential opinion entries.\n")
            f.write("="*60 + "\n\n")

            if discrepancies:
                f.write("--- Verification Discrepancies ---\n")
                if discrepancies['missing_in_primary']:
                    f.write(" Cases found now but MISSING in yesterday's Primary run:\n")
                    for d in discrepancies['missing_in_primary']: f.write(f"  - {d}\n")
                if discrepancies['missing_in_backup']:
                    f.write("\n Cases found in yesterday's Primary run but MISSING now:\n")
                    for d in discrepancies['missing_in_backup']: f.write(f"  - {d}\n")
                f.write("---------------------------------\n\n")


            if not data_list:
                f.write("No specific case details found or parsed.\n")
                return

            # Simple CSV-like format for log
            headers = ["AppDocketID", "LinkedDocketIDs", "CaseName", "LCdocketID", "LCCounty", "OpApplication", "DecisionType", "CaseNotes", "ReleaseDate"]
            f.write("\t".join(headers) + "\n")
            for item in data_list:
                 row = [
                     item.get('AppDocketID', ''),
                     item.get('LinkedDocketIDs', ''),
                     item.get('CaseName', ''),
                     item.get('LCdocketID', ''),
                     item.get('LCCounty', ''),
                     item.get('OpApplication', ''),
                     item.get('DecisionType', ''),
                     item.get('CaseNotes', ''),
                     item.get('ReleaseDate', '')
                 ]
                 f.write("\t".join(map(str, row)) + "\n") # Ensure all are strings

        print(f"Output logged to '{filepath}'")
    except IOError as e:
        print(f"Error writing to log file {filepath}: {e}", file=sys.stderr)


def prompt_validation(data_list, timeout=600):
    """Asks the user to validate the primary data before insertion. Auto-approves after timeout."""
    if not data_list:
        print("No data collected, skipping validation.")
        return False, 0 # Nothing to validate

    print("\n--- Primary Data Validation ---")
    print("Review the data displayed above.")
    print("This data will be saved to the PRIMARY database.")
    print("If no response is given within 10 minutes, the data will be auto-inserted as unofficial.")
    print("Type 'y' to approve, 'n' to reject, or 'quit' to exit.")

    start_time = time.time()
    while True:
        print(f"You have {int(timeout - (time.time() - start_time))} seconds left to respond...", end='\r', flush=True)
        if sys.stdin in select.select([sys.stdin], [], [], 1)[0]:
            response = sys.stdin.readline().strip().lower()
            if response == "quit":
                print("Exiting by user request.")
                sys.exit(0)
            elif response == 'y':
                return True, 1
            elif response == 'n':
                print("Data NOT saved to primary database based on user input.")
                return False, 0
            else:
                print("Invalid input. Please enter 'Y', 'N', or 'quit'.")
        if time.time() - start_time > timeout:
            print("\nNo response received. Auto-inserting data as unofficial.")
            return True, 0  # Insert as unofficial


def countdown(seconds, mode=""):
    """
    Displays a countdown timer before a run.
    User can type 'quit' to exit or 'skip' to skip this run.
    """
    label = f"{mode} run" if mode else "run"
    print(f"\nStarting {label} in {seconds} seconds.")
    print("Type 'quit' to exit, or 'skip' to skip this run.")
    for i in range(seconds, 0, -1):
        print(f"{i}...", end=' ', flush=True)
        # Wait up to 1 second for user input
        if sys.stdin in select.select([sys.stdin], [], [], 1)[0]:
            user_input = sys.stdin.readline().strip().lower()
            if user_input == "quit":
                print("\nExiting by user request.")
                sys.exit(0)
            elif user_input == "skip":
                print("\nSkipping this run by user request.")
                return "skip"
        time.sleep(1)
    print("Go!", flush=True)
    return None


# --- Scheduled Job Functions ---

def update_status_file(run_type, last_run_time, next_run_time, run_count):
    status = {
        "last_run_type": run_type,
        "last_run_time": last_run_time.strftime("%Y-%m-%d %H:%M:%S"),
        "next_run_time": next_run_time.strftime("%Y-%m-%d %H:%M:%S"),
        "run_count": run_count
    }
    with open(STATUS_FILE, "w") as f:
        json.dump(status, f, indent=2)

def primary_run(db_name=PRIMARY_DB, run_mode='Primary'):
    """Job for the primary data collection run."""
    print(f"\n--- Starting {run_mode} Run ({datetime.datetime.now()}) ---")
    opinions, release_date = fetch_and_parse_opinions(URL)
    display_data(opinions, release_date)

    log_filename = f"{run_mode.lower()}_run_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_output(log_filename, opinions, release_date, run_mode)

    validated = False
    official_flag = 0
    if run_mode == 'Primary':
        validated, official_flag = prompt_validation(opinions)
    elif run_mode == 'Backup':
        validated, official_flag = prompt_validation(opinions)
    elif run_mode == 'Test':
        print("Test mode: Auto-validating data for insertion after 1 minute.")
        # Wait 1 minute for possible user input, then auto-approve as unofficial
        validated, official_flag = prompt_validation(opinions, timeout=60)

    if validated:
        approval_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        insert_data(db_name, opinions, run_mode, release_date, official_flag=official_flag, approval_timestamp=approval_timestamp)
    else:
        print(f"{run_mode} data insertion skipped.")

    print(f"--- {run_mode} Run Finished ({datetime.datetime.now()}) ---")


def backup_run(primary_db=PRIMARY_DB, backup_db=BACKUP_DB, run_mode='Backup'):
    """Job for the backup/verification run."""
    print(f"\n--- Starting {run_mode} Run ({datetime.datetime.now()}) ---")
    current_opinions, current_release_date = fetch_and_parse_opinions(URL)

    # Compare with yesterday's primary run (or today's if run manually after primary)
    comparison_date = current_release_date
    if not comparison_date:
        print("Warning: Cannot determine release date for comparison. Skipping verification.", file=sys.stderr)
        primary_data_map = {}
    else:
        print(f"Comparing against primary data for release date: {comparison_date}")
        primary_data_map = get_primary_data_for_date(primary_db, comparison_date)

    # Perform comparison
    discrepancies = {'missing_in_primary': [], 'missing_in_backup': []}
    current_dockets = {o['AppDocketID'] for o in current_opinions}
    primary_dockets = set(primary_data_map.keys())

    discrepancies['missing_in_primary'] = list(current_dockets - primary_dockets)
    discrepancies['missing_in_backup'] = list(primary_dockets - current_dockets)

    # Display and Log
    display_data(current_opinions, current_release_date, discrepancies)
    log_filename = f"{run_mode.lower()}_run_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_output(log_filename, current_opinions, current_release_date, run_mode, discrepancies)

    # Validation for backup run (same as primary)
    validated = False
    official_flag = 0
    validated, official_flag = prompt_validation(current_opinions)
    if validated:
        approval_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        insert_data(backup_db, current_opinions, run_mode, current_release_date, official_flag=official_flag, approval_timestamp=approval_timestamp)
    else:
        print(f"{run_mode} data insertion skipped.")

    print(f"--- {run_mode} Run Finished ({datetime.datetime.now()}) ---")


# --- Database Customization ---

def random_word():
    # You can expand this list or use an external word list
    words = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india"]
    return random.choice(words)

def random_alphanum(length=3, avoid_last=None):
    chars = string.ascii_letters + string.digits
    while True:
        result = ''.join(random.choices(chars, k=length))
        if not avoid_last or result[-1] != avoid_last:
            return result

def prompt_for_db_customization(db_type):
    # Prompt for prefix
    prefix = input("What is the new database prefix? Type skip to skip or AG to auto generate: ").strip()
    if prefix.lower() == "skip":
        prefix = ""
    elif prefix.upper() == "AG":
        prefix = random_alphanum()
    elif len(prefix) > 25:
        print("Prefix too long, truncating to 25 chars.")
        prefix = prefix[:25]

    # Prompt for name
    name = input("What is the new database name? Type skip to skip or AG to auto generate: ").strip()
    if name.lower() == "skip":
        name = ""
    elif name.upper() == "AG":
        name = random_word()

    # Prompt for suffix
    suffix = input("What is the new database suffix? Type skip to skip or AG to auto generate: ").strip()
    if suffix.lower() == "skip":
        suffix = ""
    elif suffix.upper() == "AG":
        suffix = random_alphanum()

    # Compose base name
    base = f"{db_type}_opinions.db"
    if name:
        base = f"{name}.db"
    return prefix, base, suffix

def select_db_to_delete(db_list):
    print("Existing databases:")
    for idx, db in enumerate(db_list):
        print(f"{idx+1}: {db}")
    while True:
        choice = input("Enter the number of the database to delete, or type 'skip' to skip: ").strip()
        if choice.lower() == "skip":
            return None
        if choice.isdigit() and 1 <= int(choice) <= len(db_list):
            return db_list[int(choice)-1]
        print("Invalid selection.")


# --- Manual Mode Setup ---

def prompt_manual_mode():
    print("\n--- Manual Mode Setup ---")
    manual = prompt_input("Run in manual mode (ignore scheduler)? (Y/N or 'cancel'): ").strip().upper()
    if manual != "Y":
        return None

    try:
        seconds = int(prompt_input("Seconds until manual run (default 10, or 'cancel'): ").strip() or "10")
    except ValueError:
        seconds = 10

    run_next_sched = prompt_input("Still run next scheduled instance? (Y/N or 'cancel'): ").strip().upper() == "Y"
    modify_sched = prompt_input("Modify scheduled times? (Y/N or 'cancel'): ").strip().upper() == "Y"
    reset_sched = False
    new_primary, new_backup = None, None

    if modify_sched:
        reset_sched = prompt_input("Reset to default times? (Y/N or 'cancel'): ").strip().upper() == "Y"
        if not reset_sched:
            new_primary = prompt_input(f"New primary run time (current: {PRIMARY_RUN_TIME}, format HH:MM, or 'cancel'): ").strip() or PRIMARY_RUN_TIME
            new_backup = prompt_input(f"New backup run time (current: {BACKUP_RUN_TIME}, format HH:MM, or 'cancel'): ").strip() or BACKUP_RUN_TIME

    add_or_validate = prompt_input("In manual mode, add new data to DB or only validate? (add/validate or 'cancel'): ").strip().lower()
    lock_db = prompt_input("Lock current DB and create new one? (Y/N or 'cancel'): ").strip().upper() == "Y"
    db_type = prompt_input("Which DB to use? (Primary/Backup/Test/User or 'cancel'): ").strip().lower()
    db_name = None
    if db_type == "user":
        db_name = prompt_input("Specify database filename (or 'cancel'): ").strip()
    elif db_type == "primary":
        db_name = PRIMARY_DB
    elif db_type == "backup":
        db_name = BACKUP_DB
    elif db_type == "test":
        db_name = TEST_DB

    return {
        "seconds": seconds,
        "run_next_sched": run_next_sched,
        "modify_sched": modify_sched,
        "reset_sched": reset_sched,
        "new_primary": new_primary,
        "new_backup": new_backup,
        "add_or_validate": add_or_validate,
        "lock_db": lock_db,
        "db_type": db_type,
        "db_name": db_name
    }

def prompt_test_mode_times():
    print("\n--- Test Mode Scheduler Setup ---")
    primary_time = input(f"Test mode: Enter primary run time (default {PRIMARY_RUN_TIME}): ").strip() or PRIMARY_RUN_TIME
    backup_time = input(f"Test mode: Enter backup run time (default {BACKUP_RUN_TIME}): ").strip() or BACKUP_RUN_TIME
    return primary_time, backup_time

def robust_error_report(e, context=""):
    print(f"\n[ERROR] {context}: {e}", file=sys.stderr)
    import traceback
    traceback.print_exc()

# --- Main Execution ---

def list_test_dbs():
    return [f for f in glob.glob("*test_opinions*.db")]

def prompt_for_db_name(base, prefix, suffix):
    name = base
    if prefix:
        name = f"{prefix}_{name}"
    if suffix:
        name = f"{name}_{suffix}"
    return name

def get_next_run_time():
    """Returns the datetime and label of the next scheduled run."""
    now = datetime.datetime.now()
    today = now.date()
    times = [
        (datetime.datetime.combine(today, datetime.datetime.strptime(BACKUP_RUN_TIME, "%H:%M").time()), "Backup"),
        (datetime.datetime.combine(today, datetime.datetime.strptime(PRIMARY_RUN_TIME, "%H:%M").time()), "Primary"),
    ]
    # Add tomorrow's times if already passed
    times += [
        (datetime.datetime.combine(today + datetime.timedelta(days=1), datetime.datetime.strptime(BACKUP_RUN_TIME, "%H:%M").time()), "Backup"),
        (datetime.datetime.combine(today + datetime.timedelta(days=1), datetime.datetime.strptime(PRIMARY_RUN_TIME, "%H:%M").time()), "Primary"),
    ]
    # Find the next run time after now
    next_times = [(t, label) for t, label in times if t > now]
    next_time, label = min(next_times, key=lambda x: x[0])
    return next_time, label

def scheduler_with_hide():
    in_screen = "STY" in os.environ
    in_tmux = "TMUX" in os.environ
    if in_screen:
        print("You are running inside a 'screen' session.")
        print("Detach with Ctrl+a then d. Reattach with 'screen -r <session>'.")
    elif in_tmux:
        print("You are running inside a 'tmux' session.")
        print("Detach with Ctrl+b then d. Reattach with 'tmux attach'.")
    else:
        print("Tip: For long-running jobs, consider running this script inside a 'screen' or 'tmux' session.")
        print("If you are NOT in 'screen' or 'tmux', Ctrl+a/Ctrl+b then d will NOT detach. Use another terminal to manage your session if needed.")
    print("Type 'cancel' to exit at any time.")
    last_status_update = 0
    run_count = 0
    while True:
        schedule.run_pending()
        now = time.time()
        # Update status file every 10 minutes
        if now - last_status_update > 600:
            next_run, label = get_next_run_time()
            update_status_file(label, datetime.datetime.now(), next_run, run_count)
            last_status_update = now
        # Countdown to next run
        next_run, label = get_next_run_time()
        delta = next_run - datetime.datetime.now()
        hours, remainder = divmod(int(delta.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)
        print(f"\rNext scheduled run: {label} at {next_run.strftime('%Y-%m-%d %H:%M:%S')} | T-minus {hours:02}:{minutes:02}:{seconds:02} | Type 'cancel' to exit.", end='', flush=True)
        import select
        if sys.stdin in select.select([sys.stdin], [], [], 1)[0]:
            user_input = sys.stdin.readline().strip().lower()
            if user_input == "cancel":
                print("\nScript cancelled by user.")
                sys.exit(0)
        time.sleep(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NJ Courts Expected Opinions Scraper")
    parser.add_argument('--test', action='store_true', help="Run in test mode: performs one run immediately using test.db")
    parser.add_argument('--clear-test-db', action='store_true', help="Clear test database before running")
    parser.add_argument('--db-prefix', type=str, help="Prefix for test or live database name")
    parser.add_argument('--db-suffix', type=str, help="Suffix for test or live database name")
    parser.add_argument('--save-db', action='store_true', help="Save a copy of the current database with prefix/suffix")
    parser.add_argument('--db-type', choices=['test', 'live'], default='test', help="Which database type to use or save")
    parser.add_argument('--override-db-limit', action='store_true', help="Override the test DB limit")
    parser.add_argument(
        '--time',
        type=str,
        help="Specify test run type: 'primary' (default if --test) or 'backup'. Simulates that run."
    )
    parser.add_argument(
        '--countdown',
        type=int,
        default=60,
        help="Countdown in seconds before test run (default: 60)"
    )
    parser.add_argument('--run-now', choices=['primary', 'backup'], help="Run a job immediately before starting the scheduler")
    parser.add_argument('--fix-county-applicability', action='store_true', help="Fix county/applicability formatting in the database")
    parser.add_argument('--status', action='store_true', help="Show current status and exit")

    args = parser.parse_args()

    print_terminal_multiplexer_instructions()  # <-- Add this line here

    if args.status:
        if os.path.exists(STATUS_FILE):
            with open(STATUS_FILE) as f:
                print(f.read())
        else:
            print("No status file found.")
        sys.exit(0)

    if args.test and args.db_type == "live":
        print("Test runs cannot modify the live database. Exiting.")
        sys.exit(1)

    # Determine DB name
    db_base = "test_opinions.db" if args.db_type == "test" else "primary_opinions.db"
    db_name = prompt_for_db_name(db_base, args.db_prefix, args.db_suffix)

    # Handle test DB limit
    if args.db_type == "test":
        test_dbs = list_test_dbs()
        if db_name not in test_dbs and len(test_dbs) >= MAX_TEST_DBS and not args.override_db_limit:
            print(f"More than {MAX_TEST_DBS} test databases exist.")
            print("Existing test DBs:", test_dbs)
            print("You must delete one or use override.")
            db_to_delete = select_db_to_delete(test_dbs)
            if db_to_delete:
                os.remove(db_to_delete)
                print(f"Deleted {db_to_delete}.")
            else:
                override = input("Type 'override' to continue anyway, or anything else to exit: ").strip().lower()
                if override == "override":
                    # Generate override DB name
                    db_id = "T"
                    rand3 = random_alphanum(3)
                    # Ensure last char is not repeated
                    while rand3[-1] == db_id:
                        rand3 = random_alphanum(3)
                    db_name = f"database_override{rand3}{db_id}.db"
                    print(f"Override DB name: {db_name}")
                else:
                    print("Exiting.")
                    sys.exit(1)
        elif db_name in test_dbs:
            print(f"Using existing test DB: {db_name}")
        else:
            # Prompt for customization if desired
            customize = input("Would you like to customize the new test database name/prefix/suffix? (Y/N): ").strip().upper()
            if customize == "Y":
                prefix, base, suffix = prompt_for_db_customization("test")
                db_name = prompt_for_db_name(base, prefix, suffix)
                print(f"Custom DB name: {db_name}")

    # Optionally clear test DB
    if args.clear_test_db and os.path.exists(db_name):
        os.remove(db_name)
        print(f"Cleared test database: {db_name}")

    # Optionally save DB
    if args.save_db:
        save_name = prompt_for_db_name(db_base, args.db_prefix, args.db_suffix)
        if os.path.exists(db_base):
            import shutil
            shutil.copy(db_base, save_name)
            print(f"Saved database as: {save_name}")
        else:
            print(f"Base database {db_base} does not exist to save.")
        sys.exit(0)

    # Initialize the chosen DB
    init_db(db_name)

    # Manual mode prompt
    manual_opts = prompt_manual_mode()
    if manual_opts:
        countdown(manual_opts["seconds"], mode="manual")
        opinions, release_date = fetch_and_parse_opinions(URL)
        display_data(opinions, release_date)
        # ...rest of manual mode logic (validation, insertion, etc.)...

    # Test mode: prompt for times
    if args.test:
        test_primary, test_backup = prompt_test_mode_times()
        print(f"Test mode: Primary at {test_primary}, Backup at {test_backup}")
        countdown(args.countdown, mode="test")
        primary_run(db_name=db_name, run_mode='Test')
        sys.exit(0)

    # Manual run if requested
    if args.run_now:
        if args.run_now == 'primary':
            primary_run(db_name=PRIMARY_DB, run_mode='Primary')
        elif args.run_now == 'backup':
            backup_run(primary_db=PRIMARY_DB, backup_db=BACKUP_DB, run_mode='Backup')

    # Fix county/applicability if requested
    if args.fix_county_applicability:
        fix_county_and_applicability(db_name)
        sys.exit(0)

    # Continue with scheduler as normal
    print("--- Starting Scheduler ---")
    print(f"Primary run scheduled daily at {PRIMARY_RUN_TIME}")
    print(f"Backup/Verify run scheduled daily at {BACKUP_RUN_TIME}")
    print(f"Using Primary DB: {PRIMARY_DB}, Backup DB: {BACKUP_DB}")
    print(f"Logging to directory: {LOG_DIR}")
    print("Press Ctrl+C to exit.")

    # --- Schedule the jobs ---
    schedule.every().day.at(PRIMARY_RUN_TIME).do(primary_run, db_name=PRIMARY_DB, run_mode='Primary')
    schedule.every().day.at(BACKUP_RUN_TIME).do(backup_run, primary_db=PRIMARY_DB, backup_db=BACKUP_DB, run_mode='Backup')

    # --- Run scheduler loop ---
    try:
        scheduler_with_hide()
    except KeyboardInterrupt:
        print("\nScheduler stopped by user.")
    except Exception as e:
         print(f"\nAn unexpected error occurred in the scheduler loop: {e}", file=sys.stderr)
         import traceback
         traceback.print_exc()
    finally:
         print("Exiting application.")