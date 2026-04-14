from kafka import KafkaConsumer
import json
import numpy as np
import psycopg2
from typing import Tuple
import time
import warnings

with open('../config.json', 'r') as f:
    config_data = json.load(f)


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

state = {}
Data = {}


def update_state(vid: int, fid=-1, pos=np.zeros(3), vel=np.zeros(3), acc=np.zeros(3), t=0.0) -> None:
    """
    will save data in the consumer for the next vehicle position
    :param vid: vehicle id
    :param fid: frame id
    :param pos: [x, y, z] of vehicle position
    :param vel: [vx, vy, vz] of vehicle velocity
    :param acc: [ax, ay, az] of vehicle acceleration
    :param t: timestamp of event
    :return: none updates state for future calculations
    """
    state[vid] = {
        "frame_id": fid,
        "prev_pos": pos,
        "prev_vel": vel,
        "prev_acc": acc,
        "prev_time": t
    }

def write_query(vid: int, fid: int, t: float,
    pos: np.array, vel: np.array, acc:np.array,
    jerk: np.array, yaw: float, pitch: float) -> None:
    """
    will save the motion data to database sink
    :param vid: vehicle id
    :param fid: frame id
    :param t: timestamp of event
    :param pos: vehicle position [x, y, z]
    :param vel: vehicle velocity [vx, vy, vz]
    :param acc: vehicle acceleration [ax, ay, az]
    :param jerk: vehicle jerk [jx, yy, jz]
    :param yaw: rotation yaw
    :param pitch: inclination of vehicle
    :return: none saves data to sink
    """
    cursor.execute("""
        INSERT INTO vehicle_motion (
            vehicle_id, frame_id, timestamp,
            pos_x, pos_y, pos_z,
            vel_x, vel_y, vel_z,
            acc_x, acc_y, acc_z,
            jerk_x, jerk_y, jerk_z,
            yaw, pitch
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        vid, fid, t,
        float(pos[0]), float(pos[1]), float(pos[2]),
        float(vel[0]), float(vel[1]), float(vel[2]),
        float(acc[0]), float(acc[1]), float(acc[2]),
        float(jerk[0]), float(jerk[1]), float(jerk[2]),
        float(yaw), float(pitch)
    ))

    conn.commit()

def get_bbox(vehicle_id: int, frame_id: int) -> Tuple[np.array, np.array]:
    """
    will only use the bounding box data to speed up no need for entire cloud
    :param vehicle_id: vehicle id of this event
    :param frame_id: frame id of this event
    :return: the min and max values of the bounding box
    """
    points = Data[vehicle_id][frame_id]
    points = np.asarray(points)
    min_bound = points.min(axis=0)
    max_bound = points.max(axis=0)

    return min_bound, max_bound


def get_data(vehicle_id, frame_id) -> Tuple[np.array, np.array]:
    """
    gets the current pointcloud data and returns its bounding box
    :param vehicle_id: vehicle id of this event
    :param frame_id: frame id of this event
    :return: the min and max bounding boxes
    """
    if vehicle_id not in Data:
        Data[vehicle_id] = np.load(f"processed_data/{vehicle_id}.npy", mmap_mode="r")
    return get_bbox(vehicle_id, frame_id)


def process(msg)-> None:
    """
    will get motion data from the point cloud
    :param msg: the current vehicle point cloud data
    :return:
    """
    start = time.time()
    vid = int(msg["vehicle_id"])
    fid = int(msg['frame_id'])
    t = float(msg["timestamp"])  # ensure float

    min_bound, max_bound = get_data(vid, fid)
    print(min_bound, max_bound)

    # data sent in cm need it in m
    pos = (min_bound + max_bound) / 2 / scale

    if vid not in state:
       update_state(vid)
    prev = state[vid]

    if prev["frame_id"] != fid - 1:
        warnings.warn(f"a frame was skipped or sent out of order. current frame is {round(fid,3)}, previous frame is {round(prev['frame_id'], 3)}")

    dt = t - prev["prev_time"]
    if dt <= 0:
        return

    vel = (pos - prev["prev_pos"]) / dt
    vel = 0.8 * prev["prev_vel"] + 0.2 * vel
    acc = (vel - prev["prev_vel"]) / dt
    jerk = (acc - prev["prev_acc"]) / dt
    yaw = np.arctan2(vel[1], vel[0])
    pitch = np.arctan2(vel[2], np.linalg.norm(vel[:2]))

    write_query(vid, fid, t, pos, vel, acc, jerk, yaw, pitch)
    update_state(vid, fid, pos, vel, acc, t)
    elapsed = time.time() - start
    if elapsed > FRAME_INTERVAL:
        warnings.warn(f'consumer too slow needs to be {FRAME_INTERVAL} seconds is currently {elapsed}')

    print(vid, state[vid])


for message in consumer:
    process(message.value)