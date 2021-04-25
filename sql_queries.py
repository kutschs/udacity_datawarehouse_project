import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN                    = config.get("IAM_ROLE", "ARN")

LOG_DATA               = config.get("S3", "LOG_DATA")
LOG_JSONPATH           = config.get("S3", "LOG_JSONPATH")
SONG_DATA              = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist        varchar,
        auth          varchar,
        firstName     text,
        gender         char,
        itemInSession integer,
        lastName      text,
        length        numeric,
        level         text,
        location      varchar,
        method        varchar,
        page          text,
        registration  varchar,
        sessionId     integer,
        song          varchar,
        status        integer,
        ts            timestamp SORTKEY,
        userAgent     varchar,
        userId        integer
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs        integer,
        artist_id        varchar,
        artist_latitude  numeric,
        artist_longitude numeric,
        artist_location  varchar,
        artist_name      varchar,
        song_id          varchar,
        title            varchar,
        duration         numeric,
        year             integer   SORTKEY
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id int IDENTITY(0,1) NOT NULL, 
        start_time  timestamp         NOT NULL SORTKEY, 
        user_id     int               NOT NULL, 
        level       text              NOT NULL, 
        song_id     varchar           DISTKEY,
        artist_id   varchar, 
        session_id  int               NOT NULL, 
        location    varchar, 
        user_agent  varchar
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users ( 
        user_id    int  NOT NULL, 
        first_name text, 
        last_name  text SORTKEY, 
        gender     char, 
        level      text NOT NULL
    ) diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs ( 
        song_id   varchar NOT NULL DISTKEY, 
        title     varchar NOT NULL,
        artist_id varchar, 
        year      int     SORTKEY, 
        duration  numeric
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists ( 
        artist_id varchar NOT NULL, 
        name      varchar NOT NULL SORTKEY, 
        location  text, 
        latitude  numeric, 
        longitude numeric
    ) diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time ( 
        start_time timestamp NOT NULL SORTKEY, 
        hour       int       NOT NULL, 
        day        int       NOT NULL, 
        week       int       NOT NULL, 
        month      int       NOT NULL,
        year       int       NOT NULL, 
        weekday    int       NOT NULL
    ) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    json {}
    TIMEFORMAT 'epochmillisecs';
""").format(LOG_DATA,ARN,LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    json 'auto';
""").format(SONG_DATA,ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT DISTINCT se.ts, se.userId, se.level, ss.song_id, ss.artist_id, se.sessionId, se.location, se.userAgent
        FROM staging_events se LEFT JOIN staging_songs ss ON ( se.song = ss.title AND se.artist = ss.artist_name)
        WHERE se.page = 'NextSong'
""")

# ts = SELECT max(ts) Trick by Pablo J (https://knowledge.udacity.com/questions/39503)
user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT userId, firstName, lastName, gender, level
        FROM staging_events se1
        WHERE userId IS NOT null
        AND ts = (SELECT max(ts) FROM staging_events se2 WHERE se1.userId = se2.userId)
        ORDER BY userId DESC;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT DISTINCT song_id, title, artist_id, year, duration
        FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
        SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
        WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists);
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT DISTINCT ts, 
                        EXTRACT(hour FROM ts),
                        EXTRACT(day FROM ts),
                        EXTRACT(week FROM ts),
                        EXTRACT(month FROM ts),
                        EXTRACT(year FROM ts),
                        EXTRACT(weekday FROM ts)
        FROM staging_events
        WHERE page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
