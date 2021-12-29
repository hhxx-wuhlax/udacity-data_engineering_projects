# Project-1: Data Modeling with Postgres

### Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently the data is in a directory of Json logs on user activity on the app and a directory with Json metadata on the songs in thier app. The data team does not have an easy way to query the data.

In this project, a Postgres database schema and ETL pipeline are created to optimize queries on song play analysis. Here a star schema, consisting of 1 fact table and 4 dimention tables, is used to create the Postgres relational database. This is the most widely used schema in the tech industry, as it makes queries easier and allows for denormalized tables and fast aggregations.


### Folder Structure 

```
Data Modeling with Postgres

|____notebook        # jupyter notebooks for developing and testing ETL
| |____etl.ipynb     # developing ETL pipeline
| |____test.ipynb    # testing ETL pipeline
|
|____src                  # source code
| |____etl.py             # ETL builder source codes
| |____sql_queries.py     # SQL query helper functions
| |____create_tables.py   # database/table creation codes
```



### Code Walkthrough

#### src
etl.py <br>
1. `process_song_file`
    * Read a single song file and insert data into _songs_ and _artists_ dimension tables.
2. `process_log_file`
    * Read a single log file.
    * Breakout timestamp and iteratively insert data into _time_ dimension table.
    * Iteratively insert data into _users_ dimension table and _songplays_ facts table.
3. `process_data`
    * Get all files from the directories.
    * Call the two functions above and iteratively process each file.

create_tables.py <br>
1. create the database and connection
2. create tables if not exists
3. drop tables if exists

sql_queries.py <br>
Helper SQL queries for `etl.py` and `create_tables.py`

#### notebook
etl.ipynb <br>
Buildind the ETL pipeline step by step

test.ipynb <br>
Test whether the tables are created and data is inserted successfully.

### Database Schema

#### Fact table
```
songplays
    - songplay_id   serial PRIMARY KEY 
    - start_time    timestamp
    - user_id       int
    - level         varchar
    - song_id       varchar
    - artist_id     varchar
    - session_id    int
    - location      text
    - user_agent    text
```

#### Dimension tables
```
users
    - user_id       int PRIMARY KEY
    - first_name    varchar 
    - last_name     varchar
    - gender        char(1)
    - level         varchar

songs
    - song_id       varchar PRIMARY KEY
    - title         varchar
    - artist_id     varcahr
    - year          int
    - duration      float

artists
    - artist_id     varchar PRIMARY KEY
    - name          varchar
    - location      text
    - latitude      float
    - longitude     float

time
    - start_time    timestamp PRIMARY KEY
    - hour          int
    - day           int
    - week          int
    - month         int
    - year          int 
    - weekday       varchar
```