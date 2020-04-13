# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays (songplay_id serial PRIMARY KEY, \
                                                                   start_time timestamp, \
                                                                   user_id int, \
                                                                   level varchar NOT NULL,\
                                                                   song_id varchar, \
                                                                   artist_id varchar, \
                                                                   session_id int NOT NULL, \
                                                                   location varchar NOT NULL, \
                                                                   user_agent varchar NOT NULL)""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                                                          user_id int PRIMARY KEY, \
                                                          first_name varchar, \
                                                          last_name varchar, \
                                                          gender varchar, \
                                                          level varchar NOT NULL)""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY, \
                                                          title varchar, \
                                                          artist_id varchar, \
                                                          year int, \
                                                          duration float)""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY, \
                                                              artist_name varchar, \
                                                              artist_location varchar NOT NULL, \
                                                              artist_latitude numeric NOT NULL, \
                                                              artist_longitude numeric NOT NULL)""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time timestamp PRIMARY KEY, \
                                                         hour int, \
                                                         day int, \
                                                         week int, \
                                                         month int, \
                                                         year int, \
                                                         weekday varchar)""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (songplay_id, \
                                                   start_time, \
                                                   user_id, \
                                                   level, \
                                                   song_id, \
                                                   artist_id, \
                                                   session_id, \
                                                   location, \
                                                   user_agent) \
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) \
                            ON CONFLICT DO NOTHING;""")

user_table_insert = ("""INSERT INTO users (user_id, \
                                           first_name, \
                                           last_name, \
                                           gender, \
                                           level)\
                        VALUES (%s, %s, %s, %s, %s) \
                        ON CONFLICT (user_id) DO UPDATE 
                                                SET level = EXCLUDED.level;""")

song_table_insert = ("""INSERT INTO songs (song_id, \
                                           title, \
                                           artist_id, \
                                           year, \
                                           duration)\
                        VALUES (%s, %s, %s, %s, %s) \
                        ON CONFLICT DO NOTHING;""")

artist_table_insert = ("""INSERT INTO artists (artist_id, \
                                               artist_name, \
                                               artist_location, \
                                               artist_latitude, \
                                               artist_longitude)\
                        VALUES (%s, %s, %s, %s, %s) \
                        ON CONFLICT DO NOTHING;""")


time_table_insert = ("""INSERT INTO time (start_time, \
                                          hour, \
                                          day, \
                                          week, \
                                          month, \
                                          year, \
                                          weekday)\
                        VALUES (%s, %s, %s, %s, %s, %s, %s) \
                        ON CONFLICT DO NOTHING;""")

# FIND SONGS

song_select = ("""SELECT songs.song_id, artists .artist_id  \
                  FROM songs JOIN artists ON songs.artist_id = artists.artist_id
                  WHERE songs.title=%s \
                  AND artists.artist_name=%s \
                  AND songs.duration=%s \
                  ;""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]