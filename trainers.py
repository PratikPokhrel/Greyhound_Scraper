import requests
from bs4 import BeautifulSoup
import csv

# URL of the website
url = 'https://www.gbgb.org.uk/gbgb-trainer-list/'

# Send a GET request to the website
response = requests.get(url)

# Parse the HTML content
soup = BeautifulSoup(response.content, 'html.parser')

# Find the table
table = soup.find('table')

# Extract table headers
headers = []
for th in table.find_all('th'):
    headers.append(th.text.strip())

# Extract table rows
rows = []
for tr in table.find_all('tr')[1:]:  # Skip the header row
    cells = tr.find_all('td')
    row = [cell.text.strip() for cell in cells]
    rows.append(row)

# Save the data to a CSV file
with open('trainers_list.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(headers)  # Write the headers
    writer.writerows(rows)    # Write the data rows

print("Data has been saved to trainers_list.csv")
