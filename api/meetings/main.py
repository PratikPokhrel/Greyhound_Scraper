from flask_cors import CORS  # Import CORS
import uuid
from flask import Flask, jsonify, request, send_file, make_response
import pyodbc
import csv
# from io import BytesIO, TextIOWrapper
import io  # Import the io module


app = Flask(__name__)
CORS(app)  # Enable CORS for all routes
# Database connection string
connection_string = (
    r'DRIVER={SQL Server};'
    r'SERVER=DESKTOP-A0VMN38\SQLEXPRESS;'
    r'DATABASE=GBGB;'
    r'Trusted_Connection=yes;'
)

# Function to create a database connection
def get_db_connection():
    conn = pyodbc.connect(connection_string)
    return conn

# API to get all upcoming meetings
@app.route('/api/upcoming-meetings', methods=['GET'])
def get_upcoming_meetings():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM dbo.UpcomingMeetings')
    rows = cursor.fetchall()
    meetings = [
        {
            "MeetingID": str(row.MeetingID),
            "MeetingDate": row.MeetingDate,
            "Name": row.Name,
            "AddedOn": row.AddedOn
        } for row in rows
    ]
    conn.close()
    return jsonify(meetings)
# Function to query data
def get_dog_performance():
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    
    query = """
    SELECT TOP 20
        dogName,
        COUNT(*) AS TotalRaces,
        SUM(CASE WHEN resultPosition = 1 THEN 1 ELSE 0 END) AS Wins,
        CAST(SUM(CASE WHEN resultPosition = 1 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100 AS WinRatePercentage
    FROM dbo.vwMain
    WHERE trackName = 'Crayford'
    GROUP BY dogName
    HAVING COUNT(*) >= 10
    ORDER BY WinRatePercentage DESC;
    """
    
    cursor.execute(query)
    result = cursor.fetchall()
    
    dogs_performance = []
    
    for row in result:
        dogs_performance.append({
            'dogName': row[0],
            'TotalRaces': row[1],
            'Wins': row[2],
            'WinRatePercentage': row[3]
        })
    
    conn.close()
    return dogs_performance

@app.route('/dog-performance', methods=['GET'])
def dog_performance():
    data = get_dog_performance()
    return jsonify(data)

# API to get all races by MeetingID
@app.route('/api/races/<meeting_id>', methods=['GET'])
def get_races_by_meeting_id(meeting_id):
    try:
        meeting_id = uuid.UUID(meeting_id)  # Ensure it's a valid UUID
    except ValueError:
        return jsonify({"error": "Invalid Meeting ID"}), 400
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT ID, RaceName,RaceTime, RaceDistance, GreyhoundName, TrainerName, Career
        FROM dbo.UpcomingMeetingRaces
        WHERE UpcomingMeetingID = ?
    ''', (meeting_id,))
    
    rows = cursor.fetchall()
    races = [
        {
            "ID": str(row.ID),
            "RaceName": row.RaceName,
            "RaceTime": row.RaceTime,
            "RaceDistance": row.RaceDistance,
            "GreyhoundName": row.GreyhoundName,
            "TrainerName": row.TrainerName,
            "Career": row.Career
        } for row in rows
    ]
    conn.close()
    
    if not races:
        return jsonify({"message": "No races found for this meeting"}), 404
    
    return jsonify(races)
# Function to query data
def get_trainer_dog_performance():
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    
    query = """
    SELECT TOP 20
        trainerName,
        dogName,
        COUNT(*) AS TotalRaces,
        SUM(CASE WHEN resultPosition = 1 THEN 1 ELSE 0 END) AS Wins,
        CAST(SUM(CASE WHEN resultPosition = 1 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100 AS WinRatePercentage
    FROM dbo.vwMain
    WHERE trackName = 'Crayford'
    GROUP BY trainerName, dogName
    HAVING COUNT(*) >= 10
    ORDER BY WinRatePercentage DESC;
    """
    
    cursor.execute(query)
    result = cursor.fetchall()
    
    performance_data = []
    
    for row in result:
        performance_data.append({
            'trainerName': row[0],
            'dogName': row[1],
            'TotalRaces': row[2],
            'Wins': row[3],
            'WinRatePercentage': row[4]
        })
    
    conn.close()
    return performance_data

@app.route('/trainer-dog-performance', methods=['GET'])
def trainer_dog_performance():
    data = get_trainer_dog_performance()
    return jsonify(data)
# Function to query race data based on trackName and export to CSV
# Function to query race data based on trackName and export to CSV


    track_name = request.args.get('trackName')
    if not track_name:
        return jsonify({'error': 'trackName is required'}), 400

    csv_output = get_race_data_csv(track_name)
    return send_file(
        csv_output,
        mimetype='text/csv',
        as_attachment=True,
        download_name=f'{track_name}_race_data.csv'
    )
# Function to get the latest races for a given dog name
def get_latest_races(dog_name):
    query = """
    SELECT TOP 10
        m.meetingDate,
        m.trackName,
        m.raceTime,
        m.raceDate,
        m.raceId,
        m.raceTitle,
        m.resultPosition,
        m.resultPriceNumerator,
        m.resultPriceDenominator,
        m.resultRunTime,
        m.resultComment,
        w.dogName AS Winner,
        m.raceDistance,
        m.raceNumber,
        m.trapNumber,
        w.trainerName,
        m.resultDogWeight,
        w.raceClass

    FROM 
        dbo.vwMain m
    LEFT JOIN 
        dbo.vwMain w ON m.raceId = w.raceId AND w.resultPosition = 1
    WHERE 
        m.dogName = ?
    ORDER BY 
        m.raceDate DESC, m.raceTime DESC;
    """

    # Connect to the database and execute the query
    with pyodbc.connect(connection_string) as conn:
        cursor = conn.cursor()
        cursor.execute(query, (dog_name,))
        return cursor.fetchall()

# Flask route to get races for a specific dog name
@app.route('/races/<dog_name>', methods=['GET'])
def get_races(dog_name):
    try:
        # Fetch the latest races for the given dog name
        races = get_latest_races(dog_name)
        races_list = []
        for race in races:
            races_list.append({
                "meetingDate": race[0],
                "trackName": race[1],
                "raceTime": race[2],
                "raceDate": race[3],
                "raceId": race[4],
                "raceTitle": race[5],
                "resultPosition": race[6],
                "resultPriceNumerator": race[7],
                "resultPriceDenominator": race[8],
                "resultRunTime": race[9],
                "resultComment": race[10],
                "winner": race[11],
                "raceDistance": race[12],
                "raceNumber": race[13],
                "trapNumber": race[14],  # Corrected index for trapNumber
                "trainerName": race[15],  # Corrected index for trapNumber
                "resultDogWeight": race[16],  # Corrected index for trapNumber
                "raceClass": race[17]
            })
        # Return the list of races as a JSON response
        return jsonify(races_list)
    except Exception as e:
        # Return an error response if something goes wrong
        return jsonify({"error": str(e)}), 500  

@app.route('/getRaceData', methods=['GET'])
def get_race_data():
    dog_name = request.args.get('dogName')
    if not dog_name:
        return jsonify({"error": "Missing dogName parameter"}), 400

    query = """
    SELECT 
        trackName,
        raceDistance,
        AVG(CAST(resultRunTime AS FLOAT)) AS AverageRaceTime
    FROM 
        dbo.vwMain
    WHERE 
        resultRunTime IS NOT NULL AND dogName = ?
    GROUP BY 
        trackName, 
        raceDistance
    ORDER BY 
        trackName, 
        raceDistance;
    """
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(query, (dog_name,))
    rows = cursor.fetchall()
    conn.close()

    race_data = []
    for row in rows:
        race_data.append({
            'trackName': row.trackName,
            'raceDistance': row.raceDistance,
            'AverageRaceTime': row.AverageRaceTime
        })

    return jsonify(race_data)

# Endpoint to get dog results
@app.route('/dog-results', methods=['GET'])
def get_dog_results():
    dog_name = request.args.get('dogName')  # Get dogName parameter from query
    if not dog_name:
        return jsonify({"error": "Please provide a dogName."}), 400

    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Query to get race results for a particular dog
        query = """
        SELECT * 
        FROM dbo.vwMain
        WHERE dogName = ?
        """
        cursor.execute(query, (dog_name,))
        results = cursor.fetchall()

        if results:
            columns = [column[0] for column in cursor.description]
            data = [dict(zip(columns, row)) for row in results]
            return jsonify(data)
        else:
            return jsonify({"message": f"No results found for dog: {dog_name}"}), 404

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        conn.close()
# Endpoint to get dog results and download CSV
@app.route('/dog-results-csv', methods=['GET'])
def download_dog_results_csv():
    dog_name = request.args.get('dogName')  # Get dogName parameter from query
    if not dog_name:
        return jsonify({"error": "Please provide a dogName."}), 400

    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Query to get race results for a particular dog
        query = """
        SELECT * 
        FROM dbo.vwMain
        WHERE dogName = ?
        """
        cursor.execute(query, (dog_name,))
        results = cursor.fetchall()

        if not results:
            return jsonify({"message": f"No results found for dog: {dog_name}"}), 404

        # Create a CSV in-memory stream
        output = io.StringIO()
        writer = csv.writer(output)

        # Write the header
        columns = [column[0] for column in cursor.description]
        writer.writerow(columns)

        # Write the data
        for row in results:
            writer.writerow(row)

        output.seek(0)  # Go to the beginning of the stream

        # Create a Flask response with the CSV content
        response = make_response(output.getvalue())
        response.headers['Content-Disposition'] = f'attachment; filename={dog_name}_results.csv'
        response.headers['Content-Type'] = 'text/csv'
        return response

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        conn.close()



# Endpoint to get trainer results and download CSV
@app.route('/trainer-results-csv', methods=['GET'])
def download_trainer_results_csv():
    trainer_name = request.args.get('trainerName')  # Get trainerName parameter from query
    if not trainer_name:
        return jsonify({"error": "Please provide a trainerName."}), 400

    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Query to get race results for a particular trainer
        query = """
        SELECT * 
        FROM dbo.vwMain
        WHERE trainerName = ?
        """
        cursor.execute(query, (trainer_name,))
        results = cursor.fetchall()

        if not results:
            return jsonify({"message": f"No results found for trainer: {trainer_name}"}), 404

        # Create a CSV in-memory stream
        output = io.StringIO()
        writer = csv.writer(output)

        # Write the header
        columns = [column[0] for column in cursor.description]
        writer.writerow(columns)

        # Write the data
        for row in results:
            writer.writerow(row)

        output.seek(0)  # Go to the beginning of the stream

        # Create a Flask response with the CSV content
        response = make_response(output.getvalue())
        response.headers['Content-Disposition'] = f'attachment; filename={trainer_name}_results.csv'
        response.headers['Content-Type'] = 'text/csv'
        return response

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        conn.close()



# API endpoint for querying trainer statistics filtered by trackName
@app.route('/trainer-stats', methods=['GET'])
def get_trainer_stats():
    track_name = request.args.get('trackName')  # Get trackName from the query string

    if not track_name:
        return jsonify({"error": "Please provide a trackName."}), 400

    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # SQL query with trackName as a parameter
        query = """
        SELECT TOP 20
            trackName, 
            trainerName, 
            COUNT(CASE WHEN resultPosition = 1 THEN 1 END) AS wins, 
            COUNT(*) AS totalRaces, 
            (COUNT(CASE WHEN resultPosition = 1 THEN 1 END) * 1.0 / COUNT(*)) * 100 AS winRate
        FROM 
            dbo.vwMain
        WHERE 
            trackName = ?
        GROUP BY 
            trackName, 
            trainerName
        HAVING 
            COUNT(*) > 10
        ORDER BY 
            winRate DESC;
        """

        # Execute the query and fetch results
        cursor.execute(query, (track_name,))
        results = cursor.fetchall()

        # Structure the results as a list of dictionaries
        trainer_stats = []
        for row in results:
            trainer_stats.append({
                "trackName": row[0],
                "trainerName": row[1],
                "wins": row[2],
                "totalRaces": row[3],
                "winRate": row[4]
            })

        return jsonify(trainer_stats), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        conn.close()

@app.route('/track-results-csv', methods=['GET'])
def download_track_results_csv():
    track_name = request.args.get('trackName')  # Get dogName parameter from query
    if not track_name:
        return jsonify({"error": "Please provide a trackName."}), 400

    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Query to get race results for a particular dog
        query = """
        SELECT TOP (1000) [meetingDate]
      ,[meetingId]
      ,[trackName]
      ,[raceTime]
      ,[raceDate]
      ,[raceId]
      ,[raceTitle]
      ,[raceNumber]
      ,[raceType]
      ,[raceHandicap]
      ,[raceClass]
      ,[raceDistance]
      ,[raceGoing]
      ,[trapNumber]
      ,[dogId]
      ,[dogName]
      ,[dogSire]
      ,[dogDam]
      ,[dogBorn]
      ,[dogColour]
      ,[dogSex]
      ,[dogSeason]
      ,[trainerName]
      ,[ownerName]
      ,[resultPosition]
      ,[resultMarketPos]
      ,[resultMarketCnt]
      ,[resultBtnDistance]
      ,[resultSectionalTime]
      ,[resultComment]
      ,[resultRunTime]
      ,[resultDogWeight]
      ,[resultAdjustedTime]
  FROM [GBGB].[dbo].[vwMain]

        WHERE trackName = ?
        """
        cursor.execute(query, (track_name,))
        results = cursor.fetchall()

        if not results:
            return jsonify({"message": f"No results found for track: {track_name}"}), 404

        # Create a CSV in-memory stream
        output = io.StringIO()
        writer = csv.writer(output)

        # Write the header
        columns = [column[0] for column in cursor.description]
        writer.writerow(columns)

        # Write the data
        for row in results:
            writer.writerow(row)

        output.seek(0)  # Go to the beginning of the stream

        # Create a Flask response with the CSV content
        response = make_response(output.getvalue())
        response.headers['Content-Disposition'] = f'attachment; filename={track_name}_results.csv'
        response.headers['Content-Type'] = 'text/csv'
        return response

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        conn.close()

# API to fetch distinct trackNames (no pagination as it's usually a small dataset)
@app.route('/api/tracknames', methods=['GET'])
def get_track_names():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        query = "SELECT DISTINCT trackName FROM dbo.vwMain"
        cursor.execute(query)
        rows = cursor.fetchall()

        track_names = [row[0] for row in rows if row[0] is not None]

        cursor.close()
        conn.close()

        return jsonify(track_names)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# API to fetch distinct dogNames with pagination
@app.route('/api/dognames', methods=['GET'])
def get_dog_names():
    try:
        # Get pagination parameters and search query from the request
        search_query = request.args.get('search', '')
        limit = int(request.args.get('limit', 20))  # Default 20 records
        offset = int(request.args.get('offset', 0))  # Default offset 0

        conn = get_db_connection()
        cursor = conn.cursor()

        # Modify query to filter dogNames based on search_query
        query = f"""
            SELECT DISTINCT dogName
            FROM dbo.vwMain
            WHERE dogName LIKE ?
            ORDER BY dogName
            OFFSET {offset} ROWS
            FETCH NEXT {limit} ROWS ONLY;
        """
        search_pattern = f"{search_query}%"  # Matches dog names starting with search_query
        cursor.execute(query, (search_pattern,))
        rows = cursor.fetchall()

        dog_names = [row[0] for row in rows if row[0] is not None]

        cursor.close()
        conn.close()

        return jsonify(dog_names)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# API to fetch distinct trainerNames with pagination
@app.route('/api/trainernames', methods=['GET'])
def get_trainer_names():
    try:
        # Get pagination parameters and search query from the request
        search_query = request.args.get('search', '')
        limit = int(request.args.get('limit', 20))  # Default 20 records
        offset = int(request.args.get('offset', 0))  # Default offset 0

        conn = get_db_connection()
        cursor = conn.cursor()

        # Modify query to filter trainerNames based on search_query
        query = f"""
            SELECT DISTINCT trainerName
            FROM dbo.vwMain
            WHERE trainerName LIKE ?
            ORDER BY trainerName
            OFFSET {offset} ROWS
            FETCH NEXT {limit} ROWS ONLY;
        """
        search_pattern = f"{search_query}%"  # Matches trainer names starting with search_query
        cursor.execute(query, (search_pattern,))
        rows = cursor.fetchall()

        trainer_names = [row[0] for row in rows if row[0] is not None]

        cursor.close()
        conn.close()

        return jsonify(trainer_names)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    
if __name__ == '__main__':
    app.run(debug=True)