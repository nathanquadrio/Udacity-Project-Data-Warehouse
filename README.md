# Sparkify Data Warehouse Project

## Purpose

Sparkify, a music streaming startup, aims to analyze user activity and song data to enhance its services and understand user preferences better. The primary goal of this project is to build an ETL pipeline that extracts data from JSON files stored in S3, loads it into Redshift, and transforms it into a star schema optimized for queries on song play analysis. This will enable Sparkify to perform detailed analytics on user behavior and song usage, helping them make data-driven decisions.

## Database Schema Design

The database is designed using a star schema, which is optimized for complex queries and aggregations. The schema consists of one fact table and four dimension tables:

1. **Fact Table**
   - **songplays**: Records in event data associated with song plays (i.e., records with page `NextSong`).
     - Columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

2. **Dimension Tables**
   - **users**: Users in the app.
     - Columns: user_id, first_name, last_name, gender, level
   - **songs**: Songs in the music database.
     - Columns: song_id, title, artist_id, year, duration
   - **artists**: Artists in the music database.
     - Columns: artist_id, name, location, latitude, longitude
   - **time**: Timestamps of records in songplays broken down into specific units.
     - Columns: start_time, hour, day, week, month, year, weekday


## ETL Pipeline

The ETL pipeline involves the following steps:

1. **Extract**: Load JSON data from S3 into staging tables in Redshift.
2. **Transform**: Process the data in the staging tables to match the star schema.
3. **Load**: Insert the transformed data into the fact and dimension tables.

The pipeline is implemented in the `sql_queries.py` script, which includes functions to load data into staging tables and insert data into the final tables, and executed by the `etl.py` script.


## Additional Files

`RedshiftCuster.py`
	•	Create IAM role, Redshift cluster, and allow TCP connection from outside VPC
	•	Uncomment cleanup_on_exit to delete resources

`create_tables.py`
	•	Drop and recreate tables
	•	dwh.cfg
	•	Configure Redshift cluster and data import

`dwh.cfg`
	•	Configuration file

 `dhwFunctions.py`
	•	Functions for creating the cluster.


## How to Run the Scripts
1. Set environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in `dwh.cfg`.
2. Run `redshiftCuster.py` to create IAM role, Redshift cluster, and configure TCP connectivity.
3. Complete `dwh.cfg` with outputs from step 2, DWH ENDPOINT and IAM_ROLE ARN.
4. Run `create_tables.py` to drop and recreate tables.
5. Run ETL pipeline `etl.py`.
6. Uncomment cleanup_on_exit and re-run `redshiftCuster.py` to delete IAM role and Redshift cluster.
