import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

# CREATE TABLES

# Create Staging tables
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR(250),
    auth VARCHAR(15),
    firstName VARCHAR(250),
    gender CHAR(1),
    itemInSession INTEGER,
    lastName VARCHAR(250),
    length NUMERIC(12,6),
    level VARCHAR(10),
    location VARCHAR(250),
    method VARCHAR(10),
    page VARCHAR(25),
    registration NUMERIC(15,1),
    sessionId INTEGER,
    song TEXT,
    status INTEGER,
    ts TIMESTAMP,
    userAgent TEXT,
    userId INTEGER)
    sortkey(artist, song)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    artist_id VARCHAR(50) distkey,
    artist_latitude NUMERIC(12,6),
    artist_location VARCHAR(250),
    artist_longitude NUMERIC(12,6),
    artist_name VARCHAR(250),
    duration NUMERIC(12,6),
    num_songs INTEGER,
    song_id VARCHAR(50),
    title TEXT,
    year INTEGER)
    sortkey(artist_name, title)
""")

# Create Dimension tables
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER sortkey, 
    first_name VARCHAR(250), 
    last_name VARCHAR(250), 
    gender CHAR(1), 
    level VARCHAR(10),
    primary key(user_id)
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(50) distkey, 
    name VARCHAR(250) sortkey, 
    location VARCHAR(250) default 'Unknown', 
    latitude NUMERIC(12,6), 
    longitude NUMERIC(12,6),
    primary key(artist_id)
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(50), 
    title TEXT sortkey, 
    artist_id VARCHAR(50) distkey, 
    year INTEGER, 
    duration NUMERIC(12,6),
    primary key(song_id),
    foreign key(artist_id) references artists(artist_id)
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP sortkey, 
    hour INTEGER, 
    day INTEGER, 
    week INTEGER, 
    month INTEGER, 
    year INTEGER, 
    weekday INTEGER,
    primary key(start_time)
)
""")

# Create Fact table
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id BIGINT IDENTITY(0,1), 
    start_time TIMESTAMP, 
    user_id INTEGER, 
    level VARCHAR(10), 
    song_id VARCHAR(50), 
    artist_id VARCHAR(50) distkey,
    item_in_session INTEGER,
    session_id INTEGER, 
    location VARCHAR(250), 
    user_agent TEXT,
    foreign key(start_time) references time(start_time),
    foreign key(user_id) references users(user_id),
    foreign key(song_id) references songs(song_id))
    interleaved sortkey(start_time, item_in_session)
""")


# STAGING TABLES

staging_events_copy = ("""
copy staging_events from 's3://udacity-dend/log_data' 
credentials 'aws_iam_role={}'
region 'us-west-2'
json 's3://udacity-dend/log_json_path.json'
timeformat 'epochmillisecs';
""").format(config.get('IAM_ROLE', 'ARN'))

staging_songs_copy = ("""
copy staging_songs from 's3://udacity-dend/song_data'
credentials 'aws_iam_role={}'
region 'us-west-2'
json 'auto' truncatecolumns; 
""").format(config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

#Insert data into Dimension tables
user_table_insert = ("""
INSERT INTO users ( user_id, first_name, last_name, gender, level )
SELECT se.userid, se.firstname, se.lastname, se.gender, se.level 
FROM (
	SELECT userid, MAX(ts) AS ts
	FROM staging_events
	WHERE ts IS NOT NULL
    AND userid IS NOT NULL
	GROUP BY userid 
) u2
INNER JOIN staging_events se ON ( se.userid = u2.userid )
AND ( se.ts = u2.ts )
""")

song_table_insert = ("""
INSERT INTO songs ( song_id, title, artist_id, year, duration )
SELECT DISTINCT(song_id), title, artist_id, year, duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists ( artist_id, name, location, latitude, longitude )
SELECT DISTINCT(artist_id), artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time ( start_time, hour, day, week, month, year, weekday )
SELECT DISTINCT(ts) as start_time,
    EXTRACT(hour FROM ts) AS hour,
    EXTRACT(day FROM ts) AS day,
    EXTRACT(week FROM ts) AS week,
    EXTRACT(month FROM ts) AS month,
    EXTRACT(year FROM ts) AS year,
    EXTRACT(weekday FROM ts) AS weekday
FROM staging_events 
WHERE ts IS NOT NULL
AND page = 'NextSong'
""")

# Fact table insert
songplay_table_insert = ("""
INSERT INTO songplays ( start_time, user_id, level, song_id, artist_id, item_in_session, session_id, location, user_agent )
SELECT DISTINCT se.ts,
         se.userId,
         se.level,
         ss.song_id,
         ss.artist_id,
         se.itemInSession,
         se.sessionId,
         se.location,
         se.userAgent
FROM staging_events AS se
JOIN staging_songs AS ss ON (se.artist = ss.artist_name)
AND (se.song = ss.title)
WHERE se.ts IS NOT NULL
AND ss.song_id IS NOT NULL
AND ss.artist_id IS NOT NULL
AND se.page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, artist_table_insert, song_table_insert, time_table_insert, songplay_table_insert]
