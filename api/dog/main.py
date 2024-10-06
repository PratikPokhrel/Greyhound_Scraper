from flask_cors import CORS  # Import CORS
import uuid
from flask import Flask, jsonify, request, send_file
import pyodbc
import csv
from io import BytesIO, TextIOWrapper

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Database connection settings
connection_string = (
    r'DRIVER={SQL Server};'
    r'SERVER=DESKTOP-A0VMN38\SQLEXPRESS;'
    r'DATABASE=GBGB;'
    r'Trusted_Connection=yes;'
)

def get_latest_races(dog_name):
    query = """
    SELECT TOP 5 
        m.meetingDate,
        m.trackName,
        m.raceTime,
        m.raceDate,
        m.raceId,
        m.raceTitle,
        m.raceNumber,
        m.resultPosition,
        m.resultPriceNumerator,
        m.resultPriceDenominator,
        m.resultRunTime,
        m.resultComment,
        w.dogName AS Winner
    FROM 
        dbo.vwMain m
    LEFT JOIN 
        dbo.vwMain w ON m.raceId = w.raceId AND w.resultPosition = 1
    WHERE 
        m.dogName = ?
    ORDER BY 
        m.raceDate DESC, m.raceTime DESC;
    """

    with pyodbc.connect(connection_string) as conn:
        cursor = conn.cursor()
        cursor.execute(query, (dog_name,))
        return cursor.fetchall()

@app.route('/races/<dog_name>', methods=['GET'])
def get_races(dog_name):
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
            "raceNumber": race[6],
            "resultPosition": race[7],
            "resultPriceNumerator": race[8],
            "resultPriceDenominator": race[9],
            "resultRunTime": race[10],
            "resultComment": race[11],
            "winner": race[12]
        })
    return jsonify(races_list)

if __name__ == '__main__':
    app.run(debug=True)
