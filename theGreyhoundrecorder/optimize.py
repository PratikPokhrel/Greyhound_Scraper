import requests
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

# Extract data from race page
def extract_data_from_page(driver, meeting_id):
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'html.parser')
    events = soup.select('.form-guide-field-event')
    
    extracted_race_data = []

    for event in events:
        race_name = event.select_one('.meeting-event__header-race').text.strip()
        rows = event.select('.form-guide-field-selection-mobile')
        
        for row in rows:
            greyhound_name = row.select_one('.form-guide-field-selection-mobile__name').text.strip()
            trainer_name = row.select_one('.form-guide-field-selection-mobile__trainer').text.strip().replace('T.', '').strip()
            rating = row.select_one('.form-guide-field-selection-mobile__stat-title:contains("Rating") + .form-guide-field-selection-mobile__stat-value')
            rating_text = rating.text.strip() if rating else 'N/A'

            extracted_race_data.append({
                'race_name': race_name,
                'greyhound_name': greyhound_name,
                'trainer_name': trainer_name,
                'rating_text': rating_text,
                'meeting_id': meeting_id
            })
    
    return extracted_race_data

# Retry wrapper to handle StaleElementReferenceException
def retry_find_element(wait, by, value, max_retries=3):
    for _ in range(max_retries):
        try:
            element = wait.until(EC.presence_of_element_located((by, value)))
            return element
        except StaleElementReferenceException as e:
            logging.warning(f"StaleElementReferenceException encountered, retrying... {e}")
            time.sleep(1)  # Add a short delay before retrying
    raise StaleElementReferenceException(f"Element could not be retrieved after {max_retries} attempts")

# Process each meeting and collect data
def process_meeting(driver, wait, row, meeting_id, meeting_date_str):
    meeting_title = retry_find_element(wait, By.CSS_SELECTOR, '.meeting-row__title').text.strip()

    meeting_data = {
        'meeting_id': meeting_id,
        'meeting_date': meeting_date_str,
        'meeting_title': meeting_title,
        'race_data': []
    }

    try:
        fields_button = retry_find_element(wait, By.XPATH, './/a[contains(text(), "Fields")]')
        fields_button.click()
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.form-guide-field-event')))
        race_data = extract_data_from_page(driver, meeting_id)
        meeting_data['race_data'] = race_data
        driver.back()
        wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.meeting-row')))
    except NoSuchElementException:
        logging.warning(f"No 'Fields' button found for meeting '{meeting_title}'")
    
    return meeting_data

# Main loop to process the meetings and collect all data
def process_meetings(driver):
    wait = WebDriverWait(driver, 10)
    processed_meetings = set()
    all_meeting_data = []

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
                    # Retry locating the h2 element for the meeting date
                    h2_element = retry_find_element(wait, By.XPATH, 'preceding-sibling::h2[@class="meeting-list__title"]')
                    meeting_date_str = parse_meeting_date(h2_element.text)
                    meeting_id = uuid.uuid4()

                    # Process individual meeting and collect data
                    meeting_data = process_meeting(driver, wait, row, meeting_id, meeting_date_str)
                    all_meeting_data.append(meeting_data)

                    processed_meetings.add(i)

                except (StaleElementReferenceException, NoSuchElementException) as e:
                    logging.error(f"Error processing row {i}: {e}")

        except Exception as e:
            logging.error(f"General error during meeting processing: {e}")
            break

    return all_meeting_data

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
        # Collect all data
        all_meeting_data = process_meetings(driver)

        # Once data is collected, insert everything into the database
        for meeting in all_meeting_data:
            db_operations.insert_meeting(cursor, meeting['meeting_id'], meeting['meeting_date'], meeting['meeting_title'])
            for race in meeting['race_data']:
                db_operations.insert_race_data(cursor, race['race_name'], race['greyhound_name'], race['trainer_name'], race['meeting_id'])

        conn.commit()
        logging.info("Data successfully written to the database.")

    finally:
        db_operations.close_db_connection(conn)
        driver.quit()
        logging.info("Database connection closed and WebDriver shut down.")

if __name__ == "__main__":
    main()
