# Project 3: Build a Data Warehouse with AWS Redshift 

## Project Structure

```
Cloud Data Warehouse
|____create_tables.py    # database/table creation script to create tables according to the schema
|____etl.py              # ELT builder to load and insert into tables.
|____sql_queries.py      # SQL queries for create, load, delete tables, and query collection creation.
|____dwh.cfg             # AWS configuration file
```

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I am tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 

The data to laod into the data warehouse resides in two puhlic S3 buckets:
. song data: `s3://udacity-dend/song_data`
. log data: `s3://udacity-dend/log_data`

The objects contained in both buckets are JSON files. The song bucket has all the files under the same directory but
the event ones don't, so we need a descriptor file ((s3://udacity-dend/log_json_path.json)) in order to extract data from the folders by path. We used a descriptor file because we don't have a common prefix on folders.

## Data Warehouse Schema Definition 

### Staging Tables

#### TABLE staging_songs

| COLUMN | TYPE | FEATURES |
| ------ | ---- | ------- |
|num_songs| int| |
|artist_id| varchar| |
|artist_latitude | decimal | |
|artist_longitude| decimal| |
|artist_location| varchar| |
|artist_name | varchar | |
|song_id| varchar| |
|title| varchar| |
|duration | decimal | |
|year | int | |


#### TABLE staging_events

| COLUMN | TYPE | FEATURES |
| ------ | ---- | ------- |
|artist| varchar| |
|auth| varchar| |
|firstName | varchar | |
|gender| varchar| |
|itemInSession | int| |
|lastName | varchar | |
|length| decimal| |
|level| varchar| |
|location | varchar| |
|method | varchar| |
|page | varchar | |
|registration| varchar| |
|sessionId| int| |
|song | varchar| |
|status| int| |
|ts| timestamp| |
|userAgent| varchar| |
|userId| int| |


### Dimension Tables

#### TABLE dim_users

| COLUMN | TYPE | FEATURES |
| ------ | ---- | ------- |
|user_id| int| distkey, primary key |
|first_name| varchar| |
|last_name | varchar | |
|gender| varchar| |
|level| varchar| |


#### TABLE dim_songs

| COLUMN | TYPE | FEATURES |
| ------ | ---- | ------- |
|song_id| varchar| sortkey, primary key |
|title| varchar| not null |
|artist_id | varchar | not null |
|duration| decimal| |


#### TABLE dim_artists

| COLUMN | TYPE | FEATURES |
| ------ | ---- | ------- |
|artist_id| varchar| sortkey, primary key |
|name| varchar| not null |
|location | varchar | |
|latitude| decimal| |
|logitude| decimal| |


#### TABLE time

| COLUMN | TYPE | FEATURES |
| ------ | ---- | ------- |
|start_time| timestamp| sortkey, primary key |
|hour| int| |
|day| int| |
|week| int| |
|month| int| |
|year| int| |
|weekday| int| |


### Fact Table

#### TABLE f_songs

| COLUMN | TYPE | FEATURES |
| ------ | ---- | ------- |
|songplay_id| int| IDENTITY (0,1), primary key |
|start_time| timestamp| REFERENCES  time(start_time) sortkey|
|user_id | int | REFERENCES  dim_users(user_id) distkey|
|level| varchar| |
|song_id| varchar| REFERENCES  dim_songs(song_id)|
|artist_id | varchar | REFERENCES  dim_artists(artist_id)|
|session_id| int| not null |
|location| varchar| |
|user_agent| varchar| |


---------------------------------------