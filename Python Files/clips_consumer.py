from kafka import KafkaConsumer
import json
import numpy as np
import psycopg2
import open3d as o3d
import time
import warnings
from typing import List, Dict


with open('../config.json', 'r') as f:
    config_data = json.load(f)
"""
This is a consumer to take the point clouds and make clips
it saves all clips into the db
"""


TOPIC = config_data['topic']
KAFKA_BROKER = config_data['kafka_broker']
scale = config_data['scale']
FPS = config_data['sample_rate']
FRAME_INTERVAL = 1.0 / FPS

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


conn = psycopg2.connect(
    host=config_data['db_host'],
    database=config_data['database'],
    user=config_data['user'],
    password=config_data['password']
)
cursor = conn.cursor()
Data = {}



def write_query(vid: int, fid: int, t: float, cf: List) -> None:
    """
    will save the results to the postgres sink
    :param vid: vehicle id
    :param fid: frame id
    :param t: timestamp of event
    :param cf: clip frame, 1 frame of the clip
    :return: none will save results to the postgres sink
    """
    cursor.execute("""
            INSERT INTO vehicle_clip (vehicle_id, frame_id, timestamp, clip_frame)
            VALUES (%s, %s, %s, %s)
        """, (vid, fid, t, cf))
    conn.commit()



def frame_to_hull(vehicle_id: int, frame_id: int) -> List[float]:
    """
    will downsample the point cloud and then only return the outer hull
    :param vehicle_id: the id of the vehicle there can be many
    :param frame_id: the current frame from the vehicle
    :return: the outer hull of the point cloud
    """
    data = Data[vehicle_id][frame_id]
    points = np.asarray(data)

    pcd = o3d.geometry.PointCloud()
    pcd.points = o3d.utility.Vector3dVector(points)

    pcd = pcd.voxel_down_sample(voxel_size=10)
    hull, _ = pcd.compute_convex_hull()
    return np.asarray(hull.vertices).tolist()



def get_data(vehicle_id: int, frame_id: int) -> List[float]:
    """
    will load in the current point cloud from a vehicle and its current frame
    :param vehicle_id: the id of the vehicle there can be many
    :param frame_id: the current frame from the vehicle
    :return: see above
    """
    if vehicle_id not in Data:
        Data[vehicle_id] = np.load(f"processed_data/{vehicle_id}.npy", mmap_mode="r")
    return frame_to_hull(vehicle_id, frame_id)


def process(msg: Dict) -> None:
    """
    a consumer to read any new vehicle point cloud and return a clip of the hull
    :param msg: the live frame streamed from the vehicle
    :return: none will save results to postgres sink
    """
    start = time.time()
    vid = int(msg["vehicle_id"])
    fid = int(msg['frame_id'])
    t = float(msg["timestamp"])  # ensure float

    clip_frame = get_data(vid, fid)

    write_query(vid, fid, t, clip_frame)

    elapsed = time.time() - start
    if elapsed > FRAME_INTERVAL:
        warnings.warn(f'consumer too slow needs to be {FRAME_INTERVAL} seconds is currently {elapsed}')


for message in consumer:
    process(message.value)