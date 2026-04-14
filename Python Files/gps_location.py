import gpxpy
import psycopg2
import json
import os
from datetime import datetime

"""
this file is to save the original gps data to the database
"""

with open('../config.json') as f:
    config_data = json.load(f)

conn = psycopg2.connect(
    host=config_data['db_host'],
    database=config_data['database'],
    user=config_data['user'],
    password=config_data['password']
)
cursor = conn.cursor()


def write_query(vid: int, fid: int, t: datetime, lon: float, lat: float, elev: float) -> None:
    cursor.execute("""
               INSERT INTO gps_location(vehicle_id, frame_id, timestamp, longitude, latitude, elevation)
               VALUES (%s, %s, %s, %s, %s, %s)
           """, (vid, fid, t, lon, lat, elev))
    conn.commit()

def write_all_data(drive_id: int, file_name: str) -> None:
    with open(file_name, "r") as f:
        gpx = gpxpy.parse(f)

    for track in gpx.tracks:
        for segment in track.segments:
            for i, point in enumerate(segment.points):
                write_query(drive_id, i, point.time, point.longitude, point.latitude, point.elevation,)

def main() -> None:
    path = config_data['input_file_path']
    files = [path+f for f in os.listdir(path) if f.endswith(".gpx")]
    drive_ids = [3, 11, 23]  # bus numbers of the recorded lines could be any drive id

    for drive_id, file_name in zip(drive_ids, files):
        write_all_data(drive_id, file_name)  # get data from gpx file


if __name__ == '__main__':
    main()