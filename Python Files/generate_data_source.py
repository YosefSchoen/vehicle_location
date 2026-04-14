from typing import List, Tuple
import os
import json
import gpxpy
import numpy as np
import pandas as pd

with open('../config.json', 'r') as f:
    config = json.load(f)


"""
This file is to generate several data sources for the project
The original data is 3 gpx files with gps data from 3 distinct bus trips
The data recorded the latitude and longitude elevation and timestep each location of each bus trip
The project requires vehicle location to be:

A: local to the vehicles sensor,
not tied to a particular geo location in the world.
Solving the Ego Motion alignment problem will be an issue

B: rather than a vehicle being a 3d point, in needs to be a 3d point cloud of 100^3 points in a 1 meter box


The output is a matrix of n x 100^3 x 3
for each vehicle
"point_cloud": [[x, y, z], ...]
"""


size = config['n_points']
grid = np.indices((size, size, size)).reshape(3, -1).T


def read_data(drive_id: int, file_name: str) -> pd.DataFrame:
    """
    reads in the raw data of the vehicle and creates a dataframe
    of its id, latitude, longitude, elevation and timestep
    :param drive_id: the vehicle id
    :param file_name: the name of the gpx file for that vehicle
    :return: the dataframe above
    """
    rows = []
    print(file_name)
    with open(file_name, "r") as f:
        gpx = gpxpy.parse(f)

    for track in gpx.tracks:
        for segment in track.segments:
            for point in segment.points:
                rows.append({
                    "drive_id": drive_id,
                    "lat": point.latitude,
                    "lon": point.longitude,
                    "elevation": point.elevation,
                    "time": point.time
                })
    return pd.DataFrame(rows)


def convert_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    converts the data from geolocation (latitude longitude altitude)
    from degrees minutes seconds to cm
    also it orients the data from the first point at 0, 0, 0 to make it localized
    :param df: the data frame of points
    :return: the new data frame in cm
    """
    new_df = df.copy()

    lon_convertion_factor = config['scale'] * config['lon_convertion_factor'] * np.cos(new_df.lat)
    new_df.lon = lon_convertion_factor * (new_df.lon - new_df.lon.iloc[0])

    lat_convertion_factor = config['scale'] * config['lat_convertion_factor']
    new_df.lat = lat_convertion_factor * (new_df.lat - new_df.lat.iloc[0])

    new_df.elevation = config['scale'] * (new_df.elevation - new_df.elevation.iloc[0])
    x = np.round(new_df.lat.values).astype(int)
    y = np.round(new_df.lon.values).astype(int)
    z = np.round(new_df.elevation.values).astype(int)

    return pd.DataFrame({'x': x, 'y': y, 'z': z})


def generate_point_cloud(x: int, y: int, z: int) -> np.array:
    """
    generates a point cloud for each position of the vehicle
    :param x: x coord
    :param y: y coord
    :param z: z coord
    :return: the point cloud points as a array
    """
    center = np.array([x, y, z])
    point_cloud = center + grid.astype(int)
    return point_cloud


def generate_point_clouds(drive_id: int, df: pd.DataFrame) -> None:
    """

    :param drive_id: vehicle id
    :param df: the data frame of points
    :return: none save the point clouds to a np file
    """
    centers = df[['x', 'y', 'z']].to_numpy()  # shape (F, 3)
    point_clouds = centers[:, None, :] + grid[None, :, :]
    np.save(config['output_file_path']+str(drive_id), point_clouds)



def main():
    path = config['input_file_path']
    files = [path+f for f in os.listdir(path) if f.endswith(".gpx")]
    drive_ids = [3, 11, 23]  # bus numbers of the recorded lines could be any drive id

    for drive_id, file_name in zip(drive_ids, files):
        df = read_data(drive_id, file_name)  # get data from gpx file
        df = convert_data(df)  # convert data from gps coordinates to generic x, y, z coordinates
        generate_point_clouds(drive_id, df)

if __name__ == '__main__':
    main()