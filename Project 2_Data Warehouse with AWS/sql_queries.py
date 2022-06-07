import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSON_PATH = config.get("S3", "LOG_JSON_PATH")
SONG_DATA = config.get("S3", "SONG_DATA")
ARN = config.get("IAM_ROLE", "ARN")
REGION = config.get('GEO', 'REGION')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events cascade;"
staging_songs_table_drop = "drop table if exists staging_songs cascade;"
songplay_table_drop = "drop table if exists f_songs cascade;"
user_table_drop = "drop table if exists dim_users cascade"
song_table_drop = "drop table if exists dim_songs cascade;"
artist_table_drop = "drop table if exists dim_artists cascade;"
time_table_drop = "drop table if exists time cascade;"

# CREATE TABLES

staging_events_table_create= ("""
create table if not exists staging_songs
(
  num_songs          int,
  artist_id          varchar,
  artist_latitude    decimal,
  artist_longitude   decimal,
  artist_location    varchar,
  artist_name        varchar,
  song_id            varchar,
  title              varchar,
  duration           decimal,
  year               int
);
""")

staging_songs_table_create = ("""
create table if not exists staging_events
(
    artist          varchar,
    auth            varchar, 
    firstName       varchar,
    gender          varchar,   
    itemInSession   int,
    lastName        varchar,
    length          decimal,
    level           varchar, 
    location        varchar,
    method          varchar,
    page            varchar,
    registration    varchar,
    sessionId       int,
    song            varchar,
    status          int,
    ts              timestamp,
    userAgent       varchar,
    userId          int
);
""")

songplay_table_create = ("""
create table if not exists f_songs (
    songplay_id     int          identity(0,1),
    start_time      timestamp    references time(start_time)    sortkey,
    user_id         int          references dim_users(user_id)      distkey,
    level           varchar,
    song_id         varchar      references dim_songs(song_id),
    artist_id       varchar      references dim_artists(artist_id),
    session_id      int          not null,
    location        varchar,
    user_agent      varchar,
    primary key (songplay_id)
);
""")

user_table_create = ("""
create table if not exists dim_users(
    user_id       int distkey,
    first_name    varchar,
    last_name     varchar,
    gender        varchar,
    level         varchar,
    primary key (user_id)
);
""")

song_table_create = ("""
create table if not exists dim_songs (
    song_id   varchar sortkey,
    title     varchar not null,
    artist_id varchar not null,
    year      int,
    duration  decimal,
    primary key (song_id)
);
""")

artist_table_create = ("""
create table if not exists dim_artists (
    artist_id   varchar sortkey,
    name        varchar not null,
    location    varchar,
    latitude    decimal,
    logitude    decimal,
    primary key (artist_id)
);
""")

time_table_create = ("""
create table if not exists time (
    start_time    timestamp sortkey,
    hour          int     not null,
    day           int     not null,
    week          int     not null,
    month         int     not null,
    year          int     not null,
    weekday       int     not null,
    primary key (start_time)
);
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events 
from {} 
    iam_role {} 
    region {}
    format as json {} 
    timeformat 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(LOG_DATA, ARN, REGION, LOG_JSON_PATH)

staging_songs_copy = ("""
copy staging_songs 
from {} 
    iam_role {} 
    region {}
    format as json 'auto'
    timeformat 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA, ARN, REGION)

# FINAL TABLES

songplay_table_insert = ("""
insert into f_songs (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    select distinct
        se.ts, 
        se.userId, 
        se.level, 
        ss.song_id, 
        ss.artist_id, 
        se.sessionId, 
        se.location, 
        se.userAgent
    from staging_events se 
    inner join staging_songs ss 
        on se.song = ss.title and se.artist = ss.artist_name
        where se.page = 'NextSong';
""")

user_table_insert = ("""
insert into dim_users (user_id, first_name, last_name, gender, level)
    select distinct 
        se.userId, 
        se.firstName, 
        se.lastName, 
        se.gender, 
        se.level
    from staging_events se
    where se.userId is not null;
""")

song_table_insert = ("""
insert into dim_songs (song_id, title, artist_id, year, duration) 
    select distinct 
        ss.song_id, 
        ss.title, 
        ss.artist_id, 
        ss.year, 
        ss.duration
    from staging_songs ss
    where ss.song_id is not null;
""")

artist_table_insert = ("""
insert into dim_artists (artist_id, name, location, latitude, logitude)
    select distinct
        ss.artist_id, 
        ss.artist_name, 
        ss.artist_location,
        ss.artist_latitude,
        ss.artist_longitude
    from staging_songs ss
    where ss.artist_id is not null;
""")

time_table_insert = ("""
insert into time (start_time, hour, day, week, month, year, weekday)
    select distinct 
        se.ts,
        extract(hour from se.ts),
        extract(day from se.ts),
        extract(week from se.ts),
        extract(month from se.ts),
        extract(year from se.ts),
        extract(weekday from se.ts)
    from staging_events se
    where se.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create,
                        song_table_create, artist_table_create, time_table_create, songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
