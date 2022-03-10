from pendulum import date
import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_ID = "osidekyle"
TOKEN = "BQA0qSQwWoLfet2ZxHRomESvvdsm5DdacSduj1jAsYmJZ1pqS6zKZjQ0oRa4735wwfNRZOMjpWdnB6NKYo44MbpBowWgG4JW9hEIAhQM5ito3WzEvtiOQFKnlhyKZsc0cmaG5PQ6Z0GUffVKr8E"

if __name__ == "__main__":
    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer " + TOKEN
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after=" + str(yesterday_unix_timestamp),headers = headers)

    data = r.json()

    song_names = []
    artist_names = []
    played_at = []
    timestamps = []

    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])


song_dict = {
    "song_name": song_names,
    "artist_name": artist_names,
    "played_at": played_at,
    "timestamp": timestamps
}

def check_if_valid_data(df: pd.DataFrame) -> bool:
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False
    
    if pd.Series(df["played_at"]).is_unique:
        pass
    else:
        raise Exception("Primary Key Check is Vioated")

    if df.isnull().values.any():
        raise Exception("Null values found")

    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    print(yesterday)
    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
            raise Exception("Song not from last 24 hours")

    return True

song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])


print(song_df)

if check_if_valid_data(song_df):
    print("Valid!")

engine = sqlalchemy.create_engine(DATABASE_LOCATION)
conn = sqlite3.connect("my_played_tracks.sqlite")
cursor = conn.cursor()

sql_query = """
CREATE TABLE IF NOT EXISTS my_played_tracks(
    song_name VARCHAR(200),
    artist_name VARCHAR(200),
    played_at VARCHAR(200),
    timestamp VARCHAR(200),
    CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
)
"""

cursor.execute(sql_query)
print("Opened databases successfully")

try: 
    song_df.to_sql("my_played_tracks", engine, index=False, if_exists="append")
except:
    print("Data already in DB")

conn.close()