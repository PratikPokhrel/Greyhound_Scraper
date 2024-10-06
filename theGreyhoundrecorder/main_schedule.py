import schedule
import time
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
from bs4 import BeautifulSoup
from datetime import datetime
import pyodbc
import uuid

def scrape_races():
    # Path to the GeckoDriver executable
    gecko_driver_path = 'C:/Users/1/Downloads/geckodriver-v0.35.0-win32/geckodriver.exe'

    # Path to the Firefox binary (adjust this if Firefox is installed in a non-default location)
    firefox_binary_path = 'C:/Program Files/Mozilla Firefox/firefox.exe'

    # Setup Firefox options
    options = Options()
    options.binary_location = firefox_binary_path  # Set the binary location

    # Setup the WebDriver (Firefox)
    service = Service(executable_path=gecko_driver_path)
    driver = webdriver.Firefox(service=service, options=options)

    # MSSQL Connection Setup
    connection_string = (
        r'DRIVER={SQL Server};'
        r'SERVER=DESKTOP-A0VMN38\SQLEXPRESS;'
        r'DATABASE=GBGB;'
        r'Trusted_Connection=yes;'
    )
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    # Open the website with the list of races
    driver.get('https://www.thegreyhoundrecorder.com.au/form-guides/uk/')

    # Wait for the page to load completely
    wait = WebDriverWait(driver, 10)

    def extract_data_from_page(meeting_id):
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')

        # Extract the header information
        guide_field_events = soup.select('.form-guide-field-event')

        for guide_field_event in guide_field_events:
            rows = guide_field_event.select('.form-guide-field-selection-mobile')
            race_name = guide_field_event.select_one('.meeting-event__header-race').text.strip()
            for row in rows:
                name = row.select_one('.form-guide-field-selection-mobile__name').text.strip()
                trainer = row.select_one('.form-guide-field-selection-mobile__trainer').text.strip()

                # Scrape the value where the stat-title is 'Career'
                career_stat = row.find('span', text='Career')
                career_value = ''
                if career_stat:
                    career_value = career_stat.find_next('span', class_='form-guide-field-selection-mobile__stat-value').text.strip()

                rating = row.select_one('.form-guide-field-selection-mobile__stat-title:contains("Rating") + .form-guide-field-selection-mobile__stat-value').text.strip()

                if trainer.startswith('T.'):
                    trainer = trainer[2:].strip()

                # Insert data into UpcomingMeetingRaces
                cursor.execute("""
                    INSERT INTO UpcomingMeetingRaces (ID, RaceName, GreyhoundName, TrainerName, Career, UpcomingMeetingID)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (uuid.uuid4(), race_name, name, trainer.replace("()", "").strip(), career_value, meeting_id))

        conn.commit()

    # Track processed meetings
    processed_meetings = set()

    try:
        # Locate all meeting titles
        meeting_titles = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.meeting-list__title')))

        # Locate meeting rows
        meeting_rows = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.meeting-row')))

        for i in range(len(meeting_rows)):
            if i in processed_meetings:
                continue

            try:
                meeting_rows = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.meeting-row')))
                row = meeting_rows[i]
                h2_element = row.find_element(By.XPATH, 'preceding-sibling::h2[@class="meeting-list__title"]')

                if h2_element:
                    current_year = datetime.now().year
                    date_obj = datetime.strptime(h2_element.text, "%A, %B %d")
                    date_with_year = date_obj.replace(year=current_year)
                    mssql_datetime_str = date_with_year.strftime("%Y-%m-%d %H:%M:%S")

                meeting_title = row.find_element(By.CSS_SELECTOR, '.meeting-row__title').text.strip()

                meeting_id = uuid.uuid4()
                cursor.execute("""
                    INSERT INTO UpcomingMeetings (MeetingID, MeetingDate, Name, AddedOn)
                    VALUES (?, ?, ?, ?) """, (meeting_id, mssql_datetime_str, meeting_title, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

                try:
                    fields_button = row.find_element(By.XPATH, './/a[contains(text(), "Fields")]')
                except NoSuchElementException:
                    continue

                fields_button.click()
                driver.refresh()
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.form-guide-field-event')))

                extract_data_from_page(meeting_id)

                processed_meetings.add(i)

                driver.back()
                wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.meeting-row')))

            except (StaleElementReferenceException, NoSuchElementException) as e:
                continue

    except Exception as e:
        pass

    conn.close()
    driver.quit()

# Schedule the function to run every 20 minutes
schedule.every(10).minutes.do(scrape_races)

while True:
    schedule.run_pending()
    time.sleep(1)
