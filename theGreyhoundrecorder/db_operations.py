import pyodbc
import uuid
import logging

# Initialize DB connection


def init_db_connection(connection_string):
    return pyodbc.connect(connection_string)

# Close DB connection


def close_db_connection(conn):
    if conn:
        conn.close()

# Insert meeting information into UpcomingMeetings table


def insert_meeting(cursor, meeting_id, meeting_date_str, meeting_title):
    try:
        cursor.execute("""
            INSERT INTO UpcomingMeetings (MeetingID, MeetingDate, Name)
            VALUES (?, ?, ?)
        """, (meeting_id, meeting_date_str, meeting_title))
        logging.info(
            f"Meeting '{meeting_title}' inserted with ID: {meeting_id}")
    except pyodbc.Error as e:
        logging.error(f"Error inserting meeting '{meeting_title}': {e}")

# Insert race data into UpcomingMeetingRaces table


def insert_race_data(cursor, race_name, greyhound_name, trainer_name, meeting_id):
    try:
        cursor.execute("""
            INSERT INTO UpcomingMeetingRaces (ID, RaceName, GreyhoundName, TrainerName, UpcomingMeetingID)
            VALUES (?, ?, ?, ?, ?)
        """, (uuid.uuid4(), race_name, greyhound_name, trainer_name, meeting_id))
        logging.info(
            f"Race '{race_name}' inserted for meeting ID: {meeting_id}")
    except pyodbc.Error as e:
        logging.error(f"Error inserting race data for race '{race_name}': {e}")
