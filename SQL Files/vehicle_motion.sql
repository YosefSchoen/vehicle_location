CREATE TABLE IF NOT EXISTS vehicle_motion (
    vehicle_id int,
    frame_id int,
    timestamp DOUBLE PRECISION,

	pos_x DOUBLE PRECISION,
	pos_y DOUBLE PRECISION,
	pos_z DOUBLE PRECISION,

    vel_x DOUBLE PRECISION,
    vel_y DOUBLE PRECISION,
    vel_z DOUBLE PRECISION,

    acc_x DOUBLE PRECISION,
    acc_y DOUBLE PRECISION,
    acc_z DOUBLE PRECISION,

    jerk_x DOUBLE PRECISION,
    jerk_y DOUBLE PRECISION,
    jerk_z DOUBLE PRECISION,

    yaw DOUBLE PRECISION,
    pitch DOUBLE PRECISION
);