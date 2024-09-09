from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
from bs4 import BeautifulSoup
from datetime import datetime
import time
import uuid
import logging
import db_operations

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration Constants
GECKO_DRIVER_PATH = 'C:/Users/1/Downloads/geckodriver-v0.35.0-win32/geckodriver.exe'
FIREFOX_BINARY_PATH = 'C:/Program Files/Mozilla Firefox/firefox.exe'
CONNECTION_STRING = (
    r'DRIVER={SQL Server};'
    r'SERVER=DESKTOP-A0VMN38\SQLEXPRESS;'
    r'DATABASE=GBGB;'
    r'Trusted_Connection=yes;'
)

# Initialize WebDriver
def init_webdriver():
    options = Options()
    options.binary_location = FIREFOX_BINARY_PATH
    service = Service(executable_path=GECKO_DRIVER_PATH)
    driver = webdriver.Firefox(service=service, options=options)
    return driver

# Extract data from race page and insert into the database
def extract_data_from_page(driver, cursor, meeting_id):
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'html.parser')
    events = soup.select('.form-guide-field-event')

    for event in events:
        race_name = event.select_one('.meeting-event__header-race').text.strip()
        rows = event.select('.form-guide-field-selection-mobile')
        
        for row in rows:
            greyhound_name = row.select_one('.form-guide-field-selection-mobile__name').text.strip()
            trainer_name = row.select_one('.form-guide-field-selection-mobile__trainer').text.strip().replace('T.', '').strip()
            rating = row.select_one('.form-guide-field-selection-mobile__stat-title:contains("Rating") + .form-guide-field-selection-mobile__stat-value')
            rating_text = rating.text.strip() if rating else 'N/A'
            
            # Use the db_operations module to insert race data
            db_operations.insert_race_data(cursor, race_name, greyhound_name, trainer_name, meeting_id)

# Process each meeting and extract data
def process_meeting(driver, cursor, wait, row, meeting_id, meeting_date_str):
    meeting_title = row.find_element(By.CSS_SELECTOR, '.meeting-row__title').text.strip()
    
    # Use the db_operations module to insert meeting data
    db_operations.insert_meeting(cursor, meeting_id, meeting_date_str, meeting_title)

    try:
        fields_button = row.find_element(By.XPATH, './/a[contains(text(), "Fields")]')
        fields_button.click()
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.form-guide-field-event')))
        extract_data_from_page(driver, cursor, meeting_id)
        driver.back()
        wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.meeting-row')))
    except NoSuchElementException:
        logging.warning(f"No 'Fields' button found for meeting '{meeting_title}'")

# Main loop to process the meetings
def process_meetings(driver, cursor):
    wait = WebDriverWait(driver, 10)
    processed_meetings = set()

    while True:
        try:
            meeting_rows = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.meeting-row')))
            if len(processed_meetings) >= len(meeting_rows):
                logging.info("All meetings processed.")
                break

            for i, row in enumerate(meeting_rows):
                if i in processed_meetings:
                    continue

                try:
                    # Fetch the date and meeting title (h2 and row element)
                    h2_element = row.find_element(By.XPATH, 'preceding-sibling::h2[@class="meeting-list__title"]')
                    meeting_date_str = parse_meeting_date(h2_element.text)
                    meeting_id = uuid.uuid4()

                    # Process individual meeting
                    process_meeting(driver, cursor, wait, row, meeting_id, meeting_date_str)
                    processed_meetings.add(i)

                except (StaleElementReferenceException, NoSuchElementException) as e:
                    logging.error(f"Error processing row {i}: {e}")

        except Exception as e:
            logging.error(f"General error during meeting processing: {e}")
            break

# Parse the meeting date from the <h2> element
def parse_meeting_date(date_str):
    current_year = datetime.now().year
    date_obj = datetime.strptime(date_str, "%A, %B %d").replace(year=current_year)
    return date_obj.strftime("%Y-%m-%d %H:%M:%S")

# Entry point
def main():
    driver = init_webdriver()
    conn = db_operations.init_db_connection(CONNECTION_STRING)
    cursor = conn.cursor()

    driver.get('https://www.thegreyhoundrecorder.com.au/form-guides/uk/')
    try:
        process_meetings(driver, cursor)
    finally:
        conn.commit()
        db_operations.close_db_connection(conn)
        driver.quit()
        logging.info("Database connection closed and WebDriver shut down.")

if __name__ == "__main__":
    main()
