import requests
from datetime import *
import time
import csv

DATE_API_URL = "https://api.gbgb.org.uk/api/results?page=1&itemsPerPage=1000000&date={}"
DETAILS_API_URL = "https://api.gbgb.org.uk/api/results/meeting/{}?meeting={}"

race_data = []
dump_data = []
meeting_id_numbers = []

# Initialize query_date and target_date
query_date = datetime.strptime('2024-05-15', '%Y-%m-%d')
target_date = datetime.strptime('2024-09-07', '%Y-%m-%d')

# Placeholder lists to store meeting ids and race data
meeting_id_numbers = []
dump_data = []

print('query_date:', query_date)
print('target_date:', target_date)

# Begin timer
start_time = time.time()
print("Beginning Data Query\n")

# Loop through each day from query_date to target_date
while query_date <= target_date:
    query_date_str = query_date.strftime('%Y-%m-%d')
    
    # Attempt API request for the current query_date
    response = requests.get(DATE_API_URL.format(query_date_str))
    print("QUERYING FOR ", query_date_str)
    
    # Check for successful API call
    if response.status_code != 200:
        print(
            "Error {}: Bad API call for date {}".format(
                response.status_code, query_date_str
            )
        )
        query_date += timedelta(days=1)
        continue  # Move to the next date
    
    response = response.json()  # Convert to JSON object (Python Dictionary)

    if response["items"]:
        for race in response["items"]:
            meeting_id = race["meetingId"]
            print("MEETING ID", meeting_id)
            if meeting_id not in meeting_id_numbers:
                meeting_id_numbers.append(meeting_id)
            dump_data.append(race)
    else:
        print("Finished querying date: {}".format(query_date_str))
    
    # Increment the query_date by one day
    query_date += timedelta(days=1)

# End timer and show the duration of the process
end_time = time.time()
print(f"Data query completed in {end_time - start_time} seconds")

# Print details of recorded data from initial API calls.
print("Length of dump_data :", len(dump_data), "\n")

print(meeting_id_numbers)
print("Length of meeting_id_numbers: ", len(meeting_id_numbers))

# Iterate through all collected meetingId numbers
n = 1
for meeting_id in meeting_id_numbers:

    # Query new API for detailed meetingId view
    try:
        response = requests.get(DETAILS_API_URL.format(meeting_id, meeting_id))
        print("Querying meeting id {} of {}".format(n, len(meeting_id_numbers)))
        if response.status_code != 200:
            print("Meeting API Response Status: ", response.status_code)
        response = response.json()
    # Error handling
    except Exception as e:
        print("Failed to retrieve data, Error: ", e)
        break

    # Iterate through response dictionary list
    for meeting_info in response:
        # Check to see if data is present
        if "races" in meeting_info:
            race_data.append(meeting_info)
        else:
            print("No races found for meeting ID: ", meeting_id)

    # Sleep 2 seconds to prevent rapid API calls
    # time.sleep(1)
    n += 1

# Set csv fields
csv_file = "data_csv1.csv"
csv_fields = [
    "meetingDate",
    "meetingId",
    "trackName",
    "raceTime",
    "raceDate",
    'raceId',
    'raceTitle',
    'raceNumber',
    'raceType', 
    'raceHandicap',
    'raceClass',
    'raceDistance',
    'racePrizes', 
    'raceGoing',
    'raceForecast',
    'raceTricast',
    'trapNumber',
    'trapHandicap',
    'dogId',
    'dogName',
    'dogSire',
    'dogDam',
    'dogBorn',
    'dogColour',
    'dogSex',
    'dogSeason',
    'trainerName',
    'ownerName',
    'SP',
    'resultPosition',
    'resultMarketPos',
    'resultMarketCnt',
    'resultPriceNumerator',
    'resultPriceDenominator',
    'resultBtnDistance',
    'resultSectionalTime',
    'resultComment',
    'resultRunTime',
    'resultDogWeight',
    'resultAdjustedTime'
]

# Directly write to CSV file without creating a JSON file
with open(csv_file, 'w', newline='') as csvFile:
    writer = csv.DictWriter(csvFile, fieldnames=csv_fields)
    writer.writeheader()
    for meeting in race_data:
        meeting_data = meeting.copy()
        races = meeting_data.pop('races')
        for race in races:
            traps = race.pop('traps')
            for trap in traps:
                row = {**meeting_data, **race, **trap}
                writer.writerow(row)

print("Converted data to CSV file successfully.")
