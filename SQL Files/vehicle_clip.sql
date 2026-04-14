CREATE TABLE IF NOT EXISTS vehicle_clip (
    vehicle_id int,
    frame_id int,
    timestamp DOUBLE PRECISION,
    clip_frame DOUBLE PRECISION[][]
);
