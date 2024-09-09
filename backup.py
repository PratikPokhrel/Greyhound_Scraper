from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
from bs4 import BeautifulSoup
import time
import csv

# Path to the GeckoDriver executable
gecko_driver_path = 'C:/Users/1/Downloads/geckodriver-v0.35.0-win32/geckodriver.exe'

# Path to the Firefox binary (adjust this if Firefox is installed in a non-default location)
# Modify this path if necessary
firefox_binary_path = 'C:/Program Files/Mozilla Firefox/firefox.exe'

# Setup Firefox options
options = Options()
options.binary_location = firefox_binary_path  # Set the binary location

# Setup the WebDriver (Firefox)
service = Service(executable_path=gecko_driver_path)
driver = webdriver.Firefox(service=service, options=options)

# Open the website with the list of races
driver.get('https://www.thegreyhoundrecorder.com.au/form-guides/uk/')

# Wait for the page to load completely
wait = WebDriverWait(driver, 10)

data_list = []

# Specify the CSV file name
csv_file_name = 'scraped_data.csv'


def extract_data_from_page():
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'html.parser')

    # Extract the header information
    header = soup.select_one('.form-guide-field-event')
    if header:
        distance = header.select_one(
            '.meeting-event__header-distance').text.strip()
        class_ = header.select_one('.meeting-event__header-class').text.strip()
        prize = header.select_one('.meeting-event__header-prize').text.strip()
        time = header.select_one('.meeting-event__header-time').text.strip()
        rc = header.select_one('.meeting-event__header-race').text.strip()

        # Print the extracted race information
        print(f"Race: {rc}")
        print(f"Distance: {distance}")
        print(f"Class: {class_}")
        print(f"Prize: {prize}")
        print(f"Time: {time}")
        print()

        # Create a dictionary for the race data
        race_data = {
            'Distance': distance,
            'Class': class_,
            'Prize': prize,
            'Time': time
        }
        # data_list.append(race_data)

    rows = soup.select('.form-guide-field-selection-mobile')
    for row in rows:
        name = row.select_one(
            '.form-guide-field-selection-mobile__name').text.strip()
        trainer = row.select_one(
            '.form-guide-field-selection-mobile__trainer').text.strip()
        rating = row.select_one(
            '.form-guide-field-selection-mobile__stat-title:contains("Rating") + .form-guide-field-selection-mobile__stat-value').text.strip()
        race = header.select_one('.meeting-event__header-race').text.strip()

        # Create a dictionary for the current row
        row_data = {
            'Name': name,
            'Trainer': trainer.replace("()", "").strip(),
            'Rating': rating,
            'Race': race  # Include the race name in each row's data
        }

        # Append the dictionary to the list
        data_list.append(row_data)

        # Print the data (optional)
        print(f"Name: {name}")
        print(f"Trainer: {trainer}")
        print(f"Rating: {rating}")
        print()


# Track processed meetings
processed_meetings = set()

while True:
    try:
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

                # Extract meeting row title
                meeting_title = row.find_element(
                    By.CSS_SELECTOR, '.meeting-row__title').text.strip()
                print(f"Meeting Title: {meeting_title}")

                # Extract additional meeting information if needed
                meeting_data = {
                    'Meeting Title': meeting_title
                }
                data_list.append(meeting_data)

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
                extract_data_from_page()

                # Mark this meeting as processed
                processed_meetings.add(i)

                # Navigate back to the original page
                driver.back()

            except (StaleElementReferenceException, NoSuchElementException) as e:
                print(f"An error occurred in row processing (row {i}): {e}")
                continue

    except Exception as e:
        print(f"An error occurred during page processing: {e}")
        break  # Exit the loop on general exceptions

# Open the CSV file for writing
with open(csv_file_name, mode='w', newline='', encoding='utf-8') as csv_file:
    # Create a CSV writer object
    csv_writer = csv.DictWriter(csv_file, fieldnames=[
                                'Meeting Title', 'Race', 'Name', 'Trainer', 'Rating', 'Our $'])

    # Write the header row
    csv_writer.writeheader()

    # Write the data rows
    for row_data in data_list:
        csv_writer.writerow(row_data)

print(f"Data successfully written to {csv_file_name}")
# Close the browser
driver.quit()
