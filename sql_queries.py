import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "Drop TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS factSongplay"
user_table_drop = "DROP TABLE IF EXISTS dimUsers"
song_table_drop = "DROP TABLE IF EXISTS dimSongs"
artist_table_drop = "DROP TABLE IF EXISTS dimArtists"
time_table_drop = "DROP TABLE IF EXISTS dimTime"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events
    (
        artist            varchar(256),
        auth              varchar(256),
        first_name        varchar(256),
        gender            TEXT,
        item_in_session   INT,
        last_name         varchar(256),
        length            FLOAT,
        level             varchar(256),
        location          varchar(256),
        method            varchar(256),
        page              varchar(256),
        registration      BIGINT,
        session_id        BIGINT,
        song              varchar(256),
        status            INT,
        ts                BIGINT,
        user_agent        varchar(256),
        user_id           INT
    );    
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (
        num_songs            INT,
        artist_id            varchar(256),
        artist_latitude      varchar(256),
        artist_longitude     varchar(256),
        artist_location      varchar(256),
        artist_name          varchar(256),
        song_id              varchar(256),
        title                varchar(256),
        duration             FLOAT,
        year                 BIGINT
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS factSongplay
    (
      songplay_id        INT IDENTITY(0,1) PRIMARY KEY,
      start_time         TIMESTAMP NOT NULL,
      user_id            varchar(256) NOT NULL,
      level              varchar(256),
      song_id            varchar(256),
      artist_id          varchar(256),
      session_id         BIGINT,
      location           varchar(256),
      user_agent         varchar(256)
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS dimUsers
    (
      user_id        varchar(256) NOT NULL PRIMARY KEY,
      first_name     varchar(256),
      last_name      varchar(256),
      gender         TEXT,
      level          varchar(256)
    );
""")

song_table_create = ("""
    CREATE TABLE dimSongs
    (
      song_id        varchar(256) NOT NULL PRIMARY KEY,
      title          varchar(256) NOT NULL,
      artist_id      varchar(256),
      year           BIGINT,
      duration       FLOAT
    );
""")

artist_table_create = ("""
    CREATE TABLE dimArtists
    (
      artist_id            varchar(256) NOT NULL PRIMARY KEY,
      artist_name          varchar(256),
      artist_location      varchar(256),
      artist_latitude      varchar(256),
      artist_longitude     varchar(256)
    );
""")

time_table_create = ("""
    CREATE TABLE dimTime
    (
      start_time        TIMESTAMP NOT NULL PRIMARY KEY,
      hour              INT,
      day               INT,
      week              INT,
      month             INT,
      year              INT,
      weekday           INT
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    iam_role {}
    region 'us-west-2'
    TIMEFORMAT AS 'epochmillisecs'
    JSON {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    iam_role {}
    region 'us-west-2'
    TIMEFORMAT AS 'epochmillisecs'
    JSON 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO factSongplay (start_time, user_id, level, song_id, artist_id, session_id,   
    location, user_agent) 
    SELECT
        TIMESTAMP 'epoch' + (e.ts/1000*INTERVAL '1 second') AS start_time,
        e.user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.session_id,
        e.location,
        e.user_agent
    FROM staging_events e
    JOIN staging_songs s ON (e.song = s.title AND e.length = s.duration AND e.artist = s.artist_name)
    WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO dimUsers (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        user_id,
        first_name,
        last_name,
        gender,
        level
    FROM staging_events WHERE page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO dimSongs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO dimArtists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
    SELECT DISTINCT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO dimTime (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        start_time,
        extract(hour FROM start_time),
        extract(day FROM start_time),
        extract(week FROM start_time),
        extract(month FROM start_time),
        extract(year FROM start_time),
        extract(weekday FROM start_time)
    FROM factSongplay
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
