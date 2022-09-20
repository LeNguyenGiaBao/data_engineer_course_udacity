# DROP TABLES

songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

songplay_table_create = ("""
    create table songplays
    (
        songplay_id serial primary key,
        start_time varchar not null,
        user_id varchar not null,
        level varchar,
        song_id varchar,
        artist_id varchar,
        session_id int,
        location varchar,
        user_agent varchar,
        FOREIGN KEY(user_id) REFERENCES users(user_id),
        FOREIGN KEY(artist_id) REFERENCES artists(artist_id),
        FOREIGN KEY(song_id) REFERENCES songs(song_id),
        FOREIGN KEY(start_time) REFERENCES time(start_time)
    )
""")

user_table_create = ("""
    create table users
    (
        user_id varchar primary key, -- can int
        first_name varchar,
        last_name varchar,
        gender varchar,  
        level varchar
    )
""")

song_table_create = ("""
    create table songs
    (
        song_id varchar primary key,
        title varchar,
        artist_id varchar,
        year int,
        duration float
    )
""")

artist_table_create = ("""
    create table artists 
    (
        artist_id varchar primary key,
        name varchar,
        location varchar,
        latitude float,
        longitude float
    )
""")

time_table_create = ("""
    create table time
    (
        start_time varchar primary key, -- can modify
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int -- can varchar (Mon, Tue)
    )
""")

# INSERT RECORDS

songplay_table_insert = ("""
    insert into songplays 
    ( 
        start_time, 
        user_id, level, 
        song_id, artist_id, 
        session_id, location,
        user_agent
    ) 
    values (%s,%s,%s,%s,%s,%s,%s,%s)
""")

user_table_insert = ("""
    insert into users
    (
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level
    )
    values (%s,%s,%s,%s,%s)
    ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level
""")

song_table_insert = ("""
    insert into songs 
    (
        song_id,
        title,
        artist_id,
        year, 
        duration
    ) 
    values (%s,%s,%s,%s,%s)
    ON CONFLICT DO NOTHING
""")

artist_table_insert = ("""
    insert into artists 
    (
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    values (%s,%s,%s,%s,%s)
    ON CONFLICT DO NOTHING
""")


time_table_insert = ("""
    insert into time 
    (
        start_time, 
        hour, 
        day, 
        week,
        month,
        year, 
        weekday
    )
    values (%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT DO NOTHING
""")

# FIND SONGS

song_select = ("""
    select songs.song_id, artists.artist_id from songs join artists 
    on songs.artist_id = artists.artist_id 
    where songs.title = %s
    and artists.name = %s 
    and songs.duration = %s
""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]