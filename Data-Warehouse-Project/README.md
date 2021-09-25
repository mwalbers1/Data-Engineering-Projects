# Creating a Data Warehouse on AWS Redshift

The goal of this project is to build an ETL pipeline for music streaming data which resides in AWS S3 as json files. There are two categories of data, the log events detailing songs listened to by users, and the song library consisting of song and artist details.  The ETL pipeline first extracts the json log event and song data from AWS S3, and copies the relevent data attributes into staging tables residing on an AWS Redshift cluster database server. The staging data is then transformed into a group of dimension tables and a fact table on the sparkify database on Redshift.


## Project Notebook and Python Scripts

The jupyter notebook called `project-dwh-redshift` is responsible for creating a Redshift cluster database called sparkify on
Amazon Web Services (AWS).  This jupyter notebook uses the Python AWS boto3 SDK for creating the Redshift cluster.

The project contains the following Python scripts.

- create_tables.py
- etl.py
- sql_queries.py

The `create_tables.py` script is run from the terminal. It calls functions to drop and create the staging tables
and the dimension and fact tables on the Redshift cluster database.  

The `etl.py` script is responsible for loading data into the staging and dimension tables, and fact table.

The `sql_queries.py` script defines the specific database DML statements for drop/create tables and insert statements. The COPY commands for loading json data into the staging tables are also defined in this script.

**Instructions**
1. Create AWS Redshift cluster using the `project-dwh-redshift` jupyter notebook. Or you can manually create the Redshift cluster from the AWS console based on the specifications provided in the dwh.cfg config file.

2. Open terminal and issue *python create-tables.py* command to create the tables in the sparkify database

3. Open terminal and issue *python etl.py* command to execute the Etl process

4. Go into AWS Redshift query explorer to run the queries provided

<font color='red'>Important Note</font><br>
Before running the `create_tables.py` and `etl.py` scripts, update the ***DWH_ENDPOINT*** constant in each script with the host name endpoint of your running Redshift cluster. This can be found in the AWS Redshift cluster details page.


### Staging Tables

The log events json data consists of listening activity of users on the music streaming service called Sparkify. This file gets loaded into the staging_events table using the Redshift COPY command. The song data is a collection of json music files which comprises the universe of songs and artists that can be listened to. These files are loaded into the staging_songs staging table using the Redshift COPY command. An excerpt of the song and log event data is listed below.

**Song json data** 

    {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

**Log event data**

    {"artist":"Stephen Lynch","auth":"Logged In","firstName":"Jayden","gender":"M","itemInSession":0,"lastName":"Bell","length":182.85669,"level":"free","location":"Dallas-Fort Worth-Arlington, TX","method":"PUT","page":"NextSong","registration":1540991795796.0,"sessionId":829,"song":"Jim Henson's Dead","status":200,"ts":1543537327796,"userAgent":"Mozilla\/5.0 (compatible; MSIE 10.0; Windows NT 6.2; WOW64; Trident\/6.0)","userId":"91"}


### Dimension Tables

The distinct users of the music streaming service are extracted from the staging_events table and loaded into the `users` dimension table.

The timestamps are extracted from staging_events and loaded into the `time` dimension table.

The artists information including artist id, artist name, and location are loaded into the `artists` table. Because this table is large, a distkey is defined for the artist_id column. The data will be distributed evenly across all nodes in the Redshift cluster based on the artist_id column. 

The song data such as song id, song title, duration and associated artist id are loaded into the `songs` dimension table. A distkey is assigned to the artist_id column to optimize joins between the artists and songs tables.


### Fact Table

The fact table called `songplays` is loaded from the `staging_events` and `staging_songs` tables. The `songplays` table has a distkey on the artist_id column so that joins between the artists and songs tables perform better. An interleave sort key is defined on the start_time and item_in_session columns in an effort to optimize queries on start_time or item_in_session, or both columns.  These two columns will play a role in future analytical queries on time ranges and for songs listened to in a particular session.  However, the initial load time into the `songplays` table may be slower as a result.


## Data Warehouse Queries

**artists and songs most listened to**

    SELECT a.name as artist, s.title as song_title, count(p.session_id) session_count
    FROM music.songplays p
    INNER JOIN music.artists a on a.artist_id = p.artist_id
    INNER JOIN music.songs s on s.artist_id = a.artist_id
    GROUP BY a.name, s.title
    ORDER BY count(p.session_id) DESC

**songs listened to for particular artist during time period**

    SELECT a.name as artist, s.title as song_title, p.start_time, p.item_in_session
    FROM music.songplays p
    INNER JOIN music.artists a on a.artist_id = p.artist_id
    INNER JOIN music.songs s on s.artist_id = a.artist_id
    INNER JOIN music.time t on t.start_time = p.start_time
    WHERE a.name = 'Metallica'
    AND t.month = 11
    ORDER BY p.start_time, p.item_in_session

