CREATE TABLE IF NOT EXISTS gps_location (
    vehicle_id int,
    frame_id int,
    timestamp Date,
    longitude float,
    latitude float,
    elevation float
);
