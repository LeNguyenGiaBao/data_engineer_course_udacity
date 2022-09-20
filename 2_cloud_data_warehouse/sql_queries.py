import configparser

# this file contains the queries for create table, drop table, insert table and connect data from s3

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get("S3", "LOG_DATA")
SONG_DATA = config.get("S3", "SONG_DATA")
ARN = config.get("IAM_ROLE", "ARN")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
# print(LOG_DATA)
# DROP TABLES

staging_events_table_drop = " drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create= ("""
    create table staging_events
    (
        artist varchar,
        auth varchar,
        firstName varchar,
        gender varchar,
        itemInSession int,
        lastName varchar,
        length float,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration varchar,
        sessionId int,
        song varchar,
        status varchar,
        ts bigint,
        userAgent varchar,
        userId int
    )
""")

staging_songs_table_create = ("""
    create table staging_songs
    (
        num_songs int,
        artist_id varchar,
        artist_latitude float,
        artist_longitude float,
        artist_location varchar,
        artist_name varchar,
        song_id varchar,
        title varchar,
        duration float,
        year int
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

songplay_table_create = ("""
    create table songplays
    (
        songplay_id int IDENTITY(0,1) primary key,
        start_time timestamp not null,
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
        start_time timestamp primary key,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int
    )
""")

# STAGING TABLES
staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    compupdate off region 'us-west-2'
    timeformat as 'epochmillisecs'
    truncatecolumns blanksasnull emptyasnull 
    format as json {};
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs FROM {}
    credentials 'aws_iam_role={}'
    compupdate off region 'us-west-2'
    format as json 'auto' 
    truncatecolumns blanksasnull emptyasnull;
""").format(SONG_DATA, ARN)

# FINAL TABLES
songplay_table_insert = ("""
    insert into songplays 
    ( 
        start_time, 
        user_id, level, 
        song_id, artist_id, 
        session_id, location,
        user_agent
    ) 
    select distinct
    timestamp without time zone 'epoch' + (e.ts/1000)* interval '1 second',
    e.userId, 
    e.level,
    s.song_id, 
    s.artist_id, 
    e.sessionId, 
    e.location, 
    e.userAgent 
    from staging_events as e join staging_songs as s
    on s.artist_name = e.artist and s.title = e.song    
    where e.page = 'NextSong';
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
    select distinct(e.userID), e.firstName, e.lastName, e.gender, e.level
    from staging_events e
    where e.page = 'NextSong';
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
    select distinct(s.song_id), s.title, s.artist_id, s.year, s.duration
    from staging_songs s
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
    select distinct(s.artist_id), s.artist_name, s.artist_location, s.artist_latitude, s.artist_longitude
    from staging_songs s
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
    select distinct (sp.start_time), 
           extract(hour from sp.start_time),
           extract(day from sp.start_time),
           extract(week from sp.start_time),
           extract(month from sp.start_time),
           extract(year from sp.start_time),
           extract(weekday from sp.start_time)
    from songplays as sp;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create,  songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
