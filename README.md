# Vehicle location (VL) Based Data Product:

## There are many use cases for this project
Use Case 1: Driving Behavior Analysis
Use Case 2: Traffic Flow & Congestion Detection
Use Case 3: Autonomous Driving Validation
Use Case 4: Clip Extraction for Events

I implemented Case 1 and 2
Steps to running this project

1) you need to have a postgres database set up
2) in the config.json change the db_host, database, user, password accordingly
3) run the 4 sql files to create the tables
4) to get the original gps coordinates in the database run gps_location.py
4) run the docker-compose to get kafka to work in your venv
5) pip install the requierments.txt file
6) to generate the sample data run generate_data_source.py
7) if you have other data save it into processed_data as an npy file for the program to work
8) run both consumers first then run the producer
9) to see the dashboard run the power bi file and connect it to the database


## Errors to look out for
1 consumer to slow not managing to process 16 frames per second
2 possibility of skipping frames if the consumer misses a frame

## Project Design:
One kafka producer that simulates a vehicle on the road (can be a different producer for all vehicles on the same topic)
Two consumers that read the incoming vehicle data and each process the data according to there use case
A postgres database as a sink for the data
A power bi dashboard that shows driving anomalies per vehicle and a map of traffic                                                           

## Known Issues:
The Kafka consumers can not keep up with the incoming traffic
had package dependency conflicts if I tried to use flink
I could make the dashboard nicer

## Coming Soon:
will get more data
will change the consumers to flink instead of kafka consumers for true real time 
will migrate the architecture to AWS MSK and managed apache flink plus a bigger data warehouse
will try various ml predictions on the data such as predicting traffic and route times
will learn more about open3d and vl and ego 