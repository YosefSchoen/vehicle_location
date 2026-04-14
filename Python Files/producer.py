from kafka import KafkaProducer
import json
import time
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import psycopg2

"""
This producer is a 'vehicle simulation'
it will send a point cloud of itself to whatever consumer is listening
I could not manage to get kafka to send an entire cloud so it sends the vehicle and frame id instead
any consumer can just load the cloud from those indexes
"""


with open('../config.json', 'r') as f:
    config_data = json.load(f)


size = config_data['n_points']
KAFKA_BROKER = config_data['kafka_broker']
TOPIC = config_data['topic']
DATA_DIR = config_data['output_file_path']
FPS = config_data['sample_rate']
FRAME_INTERVAL = 1.0 / FPS

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


conn = psycopg2.connect(
    host=config_data['db_host'],
    database=config_data['database'],
    user=config_data['user'],
    password=config_data['password']
)
cursor = conn.cursor()


def write_query(vid: int, fid: int, t: float) -> None:
    """
    will also make a table because later all this data will be shared across multiple tables
    :param vid:
    :param fid:
    :param t:
    :return: none save to sink
    """
    cursor.execute("""
                   INSERT INTO header_table(vehicle_id, frame_id, timestamp)
                   VALUES (%s, %s, %s)
               """, (vid, fid, t))
    conn.commit()


def stream_vehicle(vehicle_id: int, n_frames: int) -> None:
    """
    the main function to produce point clouds this acts like a real vehicle
    :param vehicle_id: the current vehicle id
    :param n_frames: the number of frames to send in deployment it will just continuously send
    :return: none save to sink and consumers can read from topic
    """
    for i in range(n_frames):
        start = time.time()
        message = {
            "vehicle_id": vehicle_id,
            "frame_id": i,
            "timestamp":start,
        }

        print(message)
        producer.send(TOPIC, value=message)
        write_query(vehicle_id, i, start)
        elapsed = time.time() - start
        sleep_time = FRAME_INTERVAL - elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)

    producer.flush()


def run_thread(vehicle_id: int) -> None:
    """
    in deployment each vehicle will have their own producer
    in this example it was simpler to run a few vehicle in parallel with threading
    :param vehicle_id:
    :return: none save to sink and consumers can read from topic
    """
    n_frames = len(np.load(f"processed_data/{vehicle_id}.npy", mmap_mode="r"))
    print(n_frames)
    stream_vehicle(vehicle_id, n_frames)


#simulating multiple parallel vehicles at the same time
if __name__ == '__main__':
    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(run_thread, [3, 11, 23])


