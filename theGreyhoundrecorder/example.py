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
import time
import uuid

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
            name = row.select_one(
            '.form-guide-field-selection-mobile__name').text.strip()
            trainer = row.select_one(
            '.form-guide-field-selection-mobile__trainer').text.strip()
             # Scrape the value where the stat-title is 'Career'
            career_stat = row.find('span', text='Career')
            career_value= ''
            if career_stat:
             career_value = career_stat.find_next('span', class_='form-guide-field-selection-mobile__stat-value').text.strip()
             print(f"Career: {career_value}")

            rating = row.select_one(
            '.form-guide-field-selection-mobile__stat-title:contains("Rating") + .form-guide-field-selection-mobile__stat-value').text.strip()
            if trainer.startswith('T.'):
             trainer = trainer[2:].strip()
             print("TRAINER NAME", trainer)
         # Insert data into UpcomingMeetingRaces
            cursor.execute("""
             INSERT INTO UpcomingMeetingRaces (ID, RaceName, GreyhoundName, TrainerName, Career, UpcomingMeetingID)
             VALUES (?, ?, ?, ?, ?, ?)
             """, (uuid.uuid4(), race_name, name, trainer.replace("()", "").strip(), career_value,  meeting_id))

    conn.commit()


# Track processed meetings
processed_meetings = set()

while True:
    try:
        # Locate all meeting titles
        meeting_titles = wait.until(EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, '.meeting-list__title')))

        # Print all meeting titles
        for title in meeting_titles:
            print(f"Meeting Title: {title.text.strip()}")

        # Locate meeting rows
        meeting_rows = wait.until(EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, '.meeting-row')))

        # Break the loop if all meetings have been processed
        if len(processed_meetings) >= len(meeting_rows):
            print("All meetings processed.")
            break

        for i in range(len(meeting_rows)):
            if i in processed_meetings:
                continue  # Skip already processed meetings

            try:
                # Re-locate the meeting row in case of stale element
                meeting_rows = wait.until(EC.presence_of_all_elements_located(
                    (By.CSS_SELECTOR, '.meeting-row')))
                row = meeting_rows[i]
                h2_element = row.find_element(
                    By.XPATH, 'preceding-sibling::h2[@class="meeting-list__title"]')
                # Print or process the <h2> element
                if h2_element:
                 print(f'Found <h2> Title: {h2_element.text}')
                 current_year = datetime.now().year

                 # Parse the date string into a datetime object (without year)
                 date_obj = datetime.strptime(h2_element.text, "%A, %B %d")

                 # Replace the year with the current year
                 date_with_year = date_obj.replace(year=current_year)

                 # Format the datetime object to MSSQL datetime format
                 mssql_datetime_str = date_with_year.strftime(
                     "%Y-%m-%d %H:%M:%S")

                else:
                  print('No <h2> title found for this meeting-row.')
                # Extract meeting row title
                meeting_title = row.find_element(
                    By.CSS_SELECTOR, '.meeting-row__title').text.strip()
                print(f"Meeting Row Title: {meeting_title}")

                # Insert meeting title and date into UpcomingMeetings
                meeting_id = uuid.uuid4()
                cursor.execute("""
                    INSERT INTO UpcomingMeetings (MeetingID, MeetingDate, Name, AddedOn)
                    VALUES (?, ?, ?, ?) """, (meeting_id, mssql_datetime_str, meeting_title, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

                # Check if the "Fields" button exists within the current row
                try:
                    fields_button = row.find_element(
                        By.XPATH, './/a[contains(text(), "Fields")]')
                except NoSuchElementException:
                    print(f"No 'Fields' button found in row {i}.")
                    continue

                # Click the "Fields" button
                fields_button.click()

                # Wait for the new page to load
                wait.until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, '.form-guide-field-event')))

                # Extract data from the new page
                extract_data_from_page(meeting_id)

                # Mark this meeting as processed
                processed_meetings.add(i)

                # Navigate back to the original page
                driver.back()

                # Wait for the main meeting list to load again
                wait.until(EC.presence_of_all_elements_located(
                    (By.CSS_SELECTOR, '.meeting-row')))

            except (StaleElementReferenceException, NoSuchElementException) as e:
                print(f"An error occurred in row processing (row {i}): {e}")
                continue

    except Exception as e:
        print(f"An error occurred during page processing: {e}")
        break  # Exit the loop on general exceptions

# Close the database connection and browser
conn.close()
driver.quit()
