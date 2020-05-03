import configparser
import boto3

# Getting the necessary credentials to retrieve
# the redshift cluster that was created with boto3
config = configparser.ConfigParser()
config.read('./RedshiftCluster/dwh.cfg')

Log_data = config.get('S3','LOG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
SONG_DATA = config.get('S3','SONG_DATA')

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')
DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

## Get iam role arn
iam = boto3.client('iam',
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name='us-west-2'
                  )

roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

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
    
    CREATE TABLE staging_events
    (
        artist_name         VARCHAR,
        auth                VARCHAR,
        firstName           VARCHAR,
        gender              VARCHAR,
        itemInSession       INTEGER,
        lastName            VARCHAR,
        length              FLOAT, 
        level               VARCHAR,
        location            VARCHAR,
        method              VARCHAR,
        page                VARCHAR,
        registration        FLOAT, 
        sessionId           INTEGER,
        song                VARCHAR,
        status              INTEGER,
        ts                  bigint,
        userAgent           VARCHAR,
        userId              INTEGER
    )
""")

staging_songs_table_create = ("""

    CREATE TABLE staging_songs
    (
        num_songs           INTEGER,
        artist_id           VARCHAR,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            FLOAT,
        year                INTEGER
     )

""")

songplay_table_create = ("""

    CREATE TABLE songplays 
    (
        songplay_id         BIGINT IDENTITY(0,1)    PRIMARY KEY,
        start_time          timestamp               NOT NULL sortkey distkey,
        user_id             INTEGER                 NOT NULL,
        level               VARCHAR,
        song_id             VARCHAR                 NOT NULL,
        artist_id           VARCHAR                 NOT NULL,
        session_id          INTEGER,
        user_agent          VARCHAR,
        location            VARCHAR
    )

""")

user_table_create = ("""

    CREATE TABLE users
    (
        user_id             INTEGER          NOT NULL sortkey PRIMARY KEY,
        first_name          VARCHAR,
        last_name           VARCHAR,
        gender              VARCHAR,
        level               VARCHAR
    )

""")

song_table_create = ("""
    
    CREATE TABLE songs
    (
        song_id             VARCHAR          NOT NULL sortkey PRIMARY KEY,
        song_title          VARCHAR          NOT NULL,
        artist_id           VARCHAR          NOT NULL,
        year                INTEGER          NOT NULL,
        duration            FLOAT
    
    )
    

""")

artist_table_create = ("""

    CREATE TABLE artists
    (
        artist_id                  VARCHAR         NOT NULL sortkey PRIMARY KEY,
        artist_name                VARCHAR         NOT NULL,
        artist_location            VARCHAR,
        artist_latitude            FLOAT,
        artist_longitude           FLOAT
    )

""")

time_table_create = ("""
    
    CREATE TABLE time
    (
        start_time          timestamp        NOT NULL distkey sortkey PRIMARY KEY,
        hour                INTEGER          NOT NULL,
        day                 INTEGER          NOT NULL,
        week                INTEGER          NOT NULL, 
        month               INTEGER          NOT NULL,
        year                INTEGER          NOT NULL,
        weekday             INTEGER          NOT NULL        
    )

""")

# STAGING TABLES

staging_events_copy = ("""

    COPY staging_events 
    FROM {}
    iam_role '{}'
    COMPUPDATE OFF 
    REGION 'us-west-2'
    FORMAT AS JSON {};

""").format(Log_data, roleArn, LOG_JSONPATH)

staging_songs_copy = ("""

    COPY staging_songs 
    FROM {}
    iam_role '{}'
    COMPUPDATE OFF 
    REGION 'us-west-2'
    FORMAT AS JSON 'auto';

""").format(SONG_DATA, roleArn)

# FINAL TABLES

songplay_table_insert = ("""

    INSERT INTO songplays (start_time,
                            user_id,    
                            level,     
                            song_id,    
                            artist_id,  
                            session_id, 
                            user_agent, 
                            location
                           )
                           
    SELECT          DISTINCT(timestamp 'epoch' + se.ts/1000 * interval '1 second')             AS start_time,
                    se.userId                              AS user_id,
                    se.level                               AS level, 
                    ss.song_id                             AS song_id,
                    ss.artist_id                           AS artist_id,
                    se.sessionId                           AS sessionId,
                    se.userAgent                           AS user_agent,
                    se.location                            AS location
    FROM staging_songs ss 
    JOIN staging_events se ON (ss.artist_name = se.artist_name
                               AND ss.title = se.song)

""")

## Make sure you include the upsert

user_table_insert = ("""

    INSERT INTO users (
                       user_id,   
                       first_name,
                       last_name, 
                       gender,   
                       level         
                      )
    SELECT      DISTINCT(se.userId)        AS user_id,
                se.firstName              AS first_name,
                se.lastName               AS last_name,
                se.gender                  AS gender,
                se.level                   AS level
                
    FROM staging_events se  
    WHERE se.userId IS NOT NULL



""")

##ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level;

song_table_insert = ("""

    INSERT INTO songs (
                                song_id,     
                                song_title,  
                                artist_id,   
                                year,        
                                duration          
                               )
    SELECT      DISTINCT(ss.song_id)  AS song_id,
                ss.title              AS song_title,
                ss.artist_id          AS artist_id,
                ss.year               AS year,
                ss.duration           AS duration
    FROM staging_songs ss 
    
""")

artist_table_insert = ("""

    INSERT INTO artists (
                                  artist_id,  
                                  artist_name,
                                  artist_location,   
                                  artist_latitude,   
                                  artist_longitude 
                                 )
    SELECT      DISTINCT(ss.artist_id)   AS artist_id,
                ss.artist_name           AS artist_name,
                ss.artist_location       AS artist_location,
                ss.artist_latitude       AS artist_latitude,
                ss.artist_longitude      AS artist_longitude
    FROM staging_songs ss 


""")

time_table_insert = ("""

    INSERT INTO time (
                                start_time,
                                hour,      
                                day,       
                                week,      
                                month,     
                                year,      
                                weekday   
                              )
    SELECT      DISTINCT(s.start_time)                  AS start_time,
                EXTRACT(hour  from s.start_time)        AS hour,
                EXTRACT(day   from s.start_time)        AS day,
                EXTRACT(week  from s.start_time)        AS week,
                EXTRACT(month from s.start_time)        AS month,
                EXTRACT(year  from s.start_time)        AS year,
                EXTRACT(dow   from s.start_time)        AS weekday
    FROM songplays s  
""")

## Querying the number of rows for each table that the data was inserted into
check_rows_songplays = ("""SELECT COUNT(*) as total_count FROM songplays""")
check_rows_users =     ("""SELECT COUNT(*) as total_count FROM users""")
check_rows_songs =     ("""SELECT COUNT(*) as total_count FROM songs""")
check_rows_artists =   ("""SELECT COUNT(*) as total_count FROM artists""")
check_rows_time =      ("""SELECT COUNT(*) as total_count FROM time""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

## Collecting all the queries into one array
row_checkers = [check_rows_songplays, check_rows_users, check_rows_songs, check_rows_artists, check_rows_time] 
