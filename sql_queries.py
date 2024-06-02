import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist TEXT,
    auth TEXT,
    firstName TEXT,
    gender TEXT,
    itemInSession INT,
    lastName TEXT,
    length FLOAT,
    level TEXT,
    location TEXT,
    method TEXT,
    page TEXT,
    registration BIGINT,
    sessionId INT,
    song TEXT,
    status INT,
    ts BIGINT,
    userAgent TEXT,
    userId INT
) 
diststyle EVEN;
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INT,
    artist_id TEXT,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location TEXT,
    artist_name TEXT,
    song_id TEXT,
    title TEXT,
    duration FLOAT,
    year INT
) diststyle EVEN;
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
    level TEXT,
    song_id TEXT NOT NULL,
    artist_id TEXT NOT NULL,
    session_id INT,
    location TEXT,
    user_agent TEXT
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    gender TEXT,
    level TEXT
) diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id TEXT PRIMARY KEY,
    title TEXT,
    artist_id TEXT,
    year INT,
    duration FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id TEXT PRIMARY KEY,
    name TEXT,
    location TEXT,
    latitude FLOAT,
    longitude FLOAT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday TEXT
);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY {} FROM {}
    IAM_ROLE '{}'
    JSON {} region '{}';
""").format(
    'staging_events',
    config['S3']['LOG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['S3']['LOG_JSONPATH'],
    config['DWH']['REGION']
)

staging_songs_copy = ("""
    COPY {} FROM {}
    IAM_ROLE '{}'
    JSON 'auto' region '{}';
""").format(
    'staging_songs',
    config['S3']['SONG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['DWH']['REGION']
)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT
    TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' as start_time,
    se.userId as user_id,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionId as session_id,
    se.location,
    se.userAgent as user_agent
FROM staging_events se
JOIN staging_songs ss
ON se.song = ss.title
AND se.artist = ss.artist_name
WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId, firstName, lastName, gender, level
FROM staging_events
WHERE userId IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT
    start_time,
    EXTRACT(hour FROM start_time),
    EXTRACT(day FROM start_time),
    EXTRACT(week FROM start_time),
    EXTRACT(month FROM start_time),
    EXTRACT(year FROM start_time),
    EXTRACT(dow FROM start_time)
FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]