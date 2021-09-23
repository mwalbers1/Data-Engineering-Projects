import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Load songs and artists from the song track files

    :param cur:
    :param filepath:
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = next(zip(df['song_id'], df['title'], df['artist_id'], df['year'], df['duration']))
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = next(zip(df['artist_id'], df['artist_name'], df['artist_location'], df['artist_latitude'].fillna(0), df['artist_longitude'].fillna(0)))
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Load time, users, and songplays tables from the log event data files

    :param cur:
    :param filepath:
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ("start_time", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.DataFrame({c: d for c, d in zip(column_labels, time_data)}).dropna()

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = pd.DataFrame(df[['userId', 'firstName', 'lastName', 'gender', 'level']].values,
                           columns=['user_id', 'first_name', 'last_name', 'gender', 'level'])

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), int(row.userId), row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Iterate through all song and event files to load star schema database tables for songs, artists, users, time,
    and songplays.

    :param cur:
    :param conn:
    :param filepath:
    :param func:
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Load song and event log data into sparkifydb postgresql database
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()