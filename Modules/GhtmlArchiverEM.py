# Modules/GhtmlArchiverEM.py
import os
import datetime
import logging
import requests
import GconfigEM # To get the list of pages

log = logging.getLogger(__name__)

# Use the same HEADERS as in GscraperEM for consistency
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# Define a base directory for storing archived HTML files
MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(MODULE_DIR)
DEFAULT_ARCHIVE_DIR = os.path.join(PROJECT_ROOT, "html_archive")


def ensure_archive_dir_exists(base_dir, page_key):
    """Ensures that the directory for a specific page's archives exists."""
    target_dir = os.path.join(base_dir, page_key)
    if not os.path.exists(target_dir):
        try:
            os.makedirs(target_dir)
            log.info(f"Created archive directory: {target_dir}")
        except OSError as e:
            log.error(f"Error creating archive directory {target_dir}: {e}")
            return None
    return target_dir

def fetch_and_save_html(page_config, base_archive_dir=DEFAULT_ARCHIVE_DIR):
    """
    Fetches HTML content for a single page configuration and saves it.

    Args:
        page_config (dict): A dictionary with 'key' and 'url'.
        base_archive_dir (str): The root directory to save archives.

    Returns:
        bool: True if successful, False otherwise.
    """
    page_key = page_config.get("key")
    url = page_config.get("url")

    if not page_key or not url:
        log.error(f"Invalid page configuration provided: {page_config}")
        return False

    log.info(f"Attempting to archive HTML for '{page_key}' from URL: {url}")

    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4XX or 5XX)
        html_content = response.text
        log.info(f"Successfully fetched HTML for '{page_key}'. Length: {len(html_content)}")

        archive_subdir = ensure_archive_dir_exists(base_archive_dir, page_key)
        if not archive_subdir:
            log.error(f"Could not ensure archive subdirectory for '{page_key}'. Skipping save.")
            return False

        current_date_str = datetime.datetime.now().strftime("%Y-%m-%d")
        filename = f"{current_date_str}_{page_key}.html"
        filepath = os.path.join(archive_subdir, filename)

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html_content)
        log.info(f"Successfully saved HTML for '{page_key}' to: {filepath}")
        return True

    except requests.exceptions.RequestException as e:
        log.error(f"Failed to fetch HTML for '{page_key}' from {url}: {e}")
    except IOError as e:
        log.error(f"Failed to save HTML for '{page_key}' to {filepath}: {e}") # Corrected filepath variable
    except Exception as e:
        log.error(f"An unexpected error occurred while archiving '{page_key}': {e}", exc_info=True)
    
    return False

def archive_all_configured_pages():
    """
    Iterates through all pages defined in the configuration and archives them.
    """
    log.info("Starting HTML archival process for all configured pages.")
    pages_to_archive = GconfigEM.get_archive_pages_config()
    
    if not pages_to_archive:
        log.warning("No pages configured for HTML archival. Please check 'archive_pages' in config.")
        print("Warning: No pages configured for archival.")
        return

    success_count = 0
    failure_count = 0

    for page_config in pages_to_archive:
        if fetch_and_save_html(page_config):
            success_count += 1
        else:
            failure_count += 1
    
    log.info(f"HTML archival process finished. Success: {success_count}, Failures: {failure_count}")
    print(f"\nHTML Archival Summary:")
    print(f"  Successfully archived: {success_count} pages")
    print(f"  Failed to archive:   {failure_count} pages")

if __name__ == "__main__":
    print("Running HTML Archiver directly...")
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
    GconfigEM.load_env() # Ensure env is loaded for any config dependency
    archive_all_configured_pages()