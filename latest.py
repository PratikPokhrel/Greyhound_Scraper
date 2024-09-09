import requests
import csv
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

# API endpoints
results_url = "https://api.gbgb.org.uk/api/results"
meeting_details_url = "https://api.gbgb.org.uk/api/results/meeting/{meetingID}?meeting={meetingID}"

# Function to fetch results data
def fetch_results(page, items_per_page, date):
    response = requests.get(results_url, params={'page': page, 'itemsPerPage': items_per_page, 'date': date})
    response.raise_for_status()
    return response.json().get('items', [])

# Function to fetch meeting details data
def fetch_meeting_details(meeting_id):
    url = meeting_details_url.format(meetingID=meeting_id)
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

# Save data to CSV (append mode)
def append_to_csv(filename, data, headers):
    if not data:
        return
    file_exists = False
    try:
        with open(filename, 'r') as csvfile:
            file_exists = True
    except FileNotFoundError:
        pass

    with open(filename, 'a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        if not file_exists:  # Write header only if the file doesn't exist
            writer.writeheader()
        for row in data:
            writer.writerow(row)

# Main function to fetch and save data
def main(start_date, end_date, items_per_page=100):
    current_date = start_date
    results_filename = 'results_latest.csv'
    meeting_details_filename = 'meeting_details_latest.csv'
    
    while current_date <= end_date:
        print(f"Fetching data for {current_date.strftime('%Y-%m-%d')}...")
        page = 1
        while True:
            results = fetch_results(page, items_per_page, current_date.strftime('%Y-%m-%d'))
            if not results:
                break
            # Save results to CSV as they are fetched
            if results:
                results_headers = results[0].keys()
                append_to_csv(results_filename, results, results_headers)

            # Fetch and save meeting details
            with ThreadPoolExecutor(max_workers=10) as executor:
                future_to_meeting_id = {executor.submit(fetch_meeting_details, result['meetingId']): result['meetingId'] for result in results}
                
                for future in as_completed(future_to_meeting_id):
                    try:
                        meeting_details = future.result()
                        # Save meeting details to CSV as they are fetched
                        if meeting_details:
                            meeting_details_headers = meeting_details[0].keys()
                            append_to_csv(meeting_details_filename, meeting_details, meeting_details_headers)
                    except Exception as e:
                        print(f"Error fetching details for meeting ID {future_to_meeting_id[future]}: {e}")

            page += 1
            time.sleep(0.2)  # To avoid overloading the API server
        
        current_date += timedelta(days=1)

if __name__ == "__main__":
    start_date = datetime(2024, 8, 1)
    end_date = datetime.now()
    main(start_date, end_date, items_per_page=50)
