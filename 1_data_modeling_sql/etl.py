import os
import glob
import psycopg2
import tqdm
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    '''
    Process song data file: Read data from filepath file, process and save data to database
        Parameters:
            cur (cursor): cursor of database
            filepath (string): path of data file, require json file
    '''
    df = pd.read_json(filepath, lines=True)

    song_data = [df['song_id'].values[0], df['title'].values[0], df['artist_id'].values[0], int(df['year'].values[0]), float(df['duration'].values[0])]
    
    cur.execute(song_table_insert, song_data)
    
    artist_data = [df['artist_id'].values[0], df['artist_name'].values[0], df['artist_location'].values[0], df['artist_latitude'].values[0], df['artist_longitude'].values[0]]

    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    Process log data file: Read data from filepath file, process and save data to database.
    Detail: insert data into time table, user table, song table and songplay table
        Parameters:
            cur (cursor): cursor of database
            filepath (string): path of data file, require json file
    '''
    df = pd.read_json(filepath, lines=True)

    df = df[df['page']=='NextSong']

    t = pd.to_datetime(df['ts'])
    
    time_data = [df['ts'], t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels =['start_time','hour','day','week','month','year','weekday']
    time_df = pd.concat(time_data, keys=column_labels, axis=1)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    user_df = df[['userId','firstName', 'lastName', 'gender', 'level']]

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)

        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    Process multi file by call func
        Parameters:
            cur (cursor): cursor of database
            conn (connection): connect to database
            filepath (string): path of data file, require directory
            func (function): function to execute
    '''
    all_files = []
    for root, dirs, files in tqdm.tqdm(os.walk(filepath)):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)

    for i, datafile in tqdm.tqdm(enumerate(all_files, 1)):
        func(cur, datafile)
        conn.commit()


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()

if __name__ == "__main__":
    main()