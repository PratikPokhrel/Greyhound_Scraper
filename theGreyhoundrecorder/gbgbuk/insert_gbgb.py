import pandas as pd
import pyodbc
import os
from datetime import datetime

# Connection string for MSSQL Server
connection_string = (
    r'DRIVER={SQL Server};'
    r'SERVER=DESKTOP-A0VMN38\SQLEXPRESS;'
    r'DATABASE=GBGB;'
    r'Trusted_Connection=yes;'
)

# Get the current directory where the Python script is located
current_dir = os.path.dirname(os.path.abspath(__file__))

# Define the file path for the CSV file, assuming it's in the same folder as the Python script
csv_file_path = os.path.join(current_dir, 'data_csv1.csv')

# Read the CSV file into a pandas DataFrame with a different encoding
try:
    df = pd.read_csv(csv_file_path, encoding='ISO-8859-1')  # Try using 'ISO-8859-1' or 'cp1252' if needed
    print("CSV file loaded successfully.")
except UnicodeDecodeError:
    print("Error decoding the file. Try another encoding such as 'utf-16' or 'ISO-8859-1'.")
except Exception as e:
    print(f"An error occurred: {e}")

# Convert columns to numeric types where necessary and handle invalid data
def clean_data(df):
    # Define columns that should be floats
    float_columns = [
        'raceDistance', 'resultSectionalTime', 'resultRunTime', 
        'resultDogWeight', 'resultAdjustedTime'
    ]
    
    # Apply rounding to 4 decimal places or the precision your database expects
    for column in float_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0).round(4)  # Adjust to .round(2) if SQL expects less precision
    
    # Define columns that should be integers
    int_columns = [
        'meetingId', 'raceId', 'raceGoing', 'trapNumber', 'dogId',
        'resultPosition', 'resultMarketPos', 'resultMarketCnt', 
        'resultPriceNumerator', 'resultPriceDenominator'
    ]
    for column in int_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0).astype(int)
    
    # Convert date columns to datetime.date
    date_columns = ['meetingDate', 'raceDate', 'dogBorn']
    for column in date_columns:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors='coerce').dt.date
    
    return df

df = clean_data(df)

# Check if any non-numeric or anomalous values exist in raceDistance
invalid_race_distance_rows = df[~df['raceDistance'].apply(lambda x: isinstance(x, (int, float)))]
if not invalid_race_distance_rows.empty:
    print(f"Invalid values in 'raceDistance':\n{invalid_race_distance_rows}")

# Define a function to insert data into SQL Server
# Clean up NaN values in the DataFrame
df = df.where(pd.notnull(df), None)

# Clean up NaN values in the DataFrame (replace NaN with default value None)
df = df.where(pd.notnull(df), None)

# Ensure raceHandicap is mapped correctly to integers (assuming boolean-like)
df['raceHandicap'] = df['raceHandicap'].replace({'True': 1, 'False': 0, 'Yes': 1, 'No': 0, None: 0})

# Convert raceHandicap to integers, handling any remaining non-numeric data
df['raceHandicap'] = pd.to_numeric(df['raceHandicap'], errors='coerce').fillna(0).astype(int)

# Convert date columns to the appropriate format, specifying dayfirst=True if dates are in dd/mm/yyyy format
date_columns = ['meetingDate', 'raceDate', 'dogBorn']
for column in date_columns:
    df[column] = pd.to_datetime(df[column], errors='coerce', dayfirst=True).dt.date

# Ensure numeric columns are consistent
df['resultSectionalTime'] = pd.to_numeric(df['resultSectionalTime'], errors='coerce').fillna(0)
df['resultRunTime'] = pd.to_numeric(df['resultRunTime'], errors='coerce').fillna(0)

def safe_strftime(date, format='%Y-%m-%d'):
    if pd.isna(date):
        return 'NaT'  # or another placeholder
    return date.strftime(format)


def insert_data_to_sql(df, table_name, conn_string, limit):
     # Create the connection to SQL Server
    conn = pyodbc.connect(conn_string)
    cursor = conn.cursor()

    # Limit the DataFrame to only the first 'limit' rows
    df_limited = df.head(limit)

    for index, row in df_limited.iterrows():
        try:
            insert_query = f"""
            INSERT INTO {table_name} (
                meetingDate, meetingId, trackName, raceDate, raceId, raceTitle, raceNumber, raceType,
                raceHandicap, raceClass, raceDistance, racePrizes, raceGoing, raceForecast, raceTricast,
                trapNumber, trapHandicap, dogId, dogName, dogSire, dogDam, dogBorn, dogColour, dogSex, dogSeason,
                trainerName, ownerName, SP, resultPosition, resultMarketPos, resultMarketCnt, resultPriceNumerator,
                resultPriceDenominator, resultBtnDistance, resultSectionalTime, resultComment, resultRunTime,
                resultDogWeight, resultAdjustedTime
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            # Prepare the values for insertion
            values = (
                safe_strftime(row['meetingDate']), row['meetingId'], row['trackName'],  safe_strftime(row['raceDate']),
                row['raceId'], row['raceTitle'], row['raceNumber'], row['raceType'], row['raceHandicap'],
                row['raceClass'], row['raceDistance'], row['racePrizes'], row['raceGoing'], row['raceForecast'],
                row['raceTricast'], row['trapNumber'], row['trapHandicap'], row['dogId'], row['dogName'],
                row['dogSire'], row['dogDam'], safe_strftime(row['dogBorn']), row['dogColour'], row['dogSex'], row['dogSeason'],
                row['trainerName'], row['ownerName'], row['SP'], row['resultPosition'], row['resultMarketPos'],
                row['resultMarketCnt'], row['resultPriceNumerator'], row['resultPriceDenominator'], 
                row['resultBtnDistance'], row['resultSectionalTime'], row['resultComment'], row['resultRunTime'],
                row['resultDogWeight'], row['resultAdjustedTime']
            )

            cursor.execute(insert_query, values)

        except Exception as e:
            print(f"Error inserting row {index}: {e}")
            print(f"Problematic row: {row}")
            continue

    conn.commit()
    cursor.close()
    conn.close()

# Call the function to insert only 100 rows into the SQL table 'Test_Table'
insert_data_to_sql(df, 'Test_Table', connection_string, limit=1000000)


