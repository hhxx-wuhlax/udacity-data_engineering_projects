# Project 4: Create a Data Lake with Spark

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want 
to move their processes and data onto the cloud. Their data resides in S3, in a directory 
of JSON logs on user activity on the app, as well as a directory with JSON metadata 
on the songs in their app.

As their data engineer, I am tasked with building an ETL pipeline that extracts 
their data from S3, stages them in Redshift, and transforms data into a set of 
dimensional tables for their analytics team to continue finding insights in what songs 
their users are listening to. 

An ETL pipeline was built for a data lake hosted on S3. Data is loaded from S3, processed into tables using Spark, and loaded back to S3. This Spark process will be deployed on a Cluster on AWS.

The source data resides in two public S3 buckets:

1. Songs bucket (s3://udacity-dend/song_data), contains info about songs and artists. All files are in the same directory.
2. Event bucket (s3://udacity-dend/log_data), contains info about actions done by users, what song are listening.

## Database Schema

### Fact table

#### songplays table

|  songplays  |    type   |
|-------------|-----------|
| songplay_id | INT       |
| start_time  | TIMESTAMP |
| user_id     | INT       |
| level       | VARCHAR   |
| song_id     | VARCHAR   |
| artist_id   | VARCHAR   |
| session_id  | INT       |
| location    | TEXT      |
| user_agent  | TEXT      |


### Dimension tables

#### user table

|    users   |   type  |
|------------|---------|
| user_id    | INT     |
| first_name | VARCHAR |
| last_name  | VARCHAR |
| gender     | CHAR(1) |
| level      | VARCHAR |

#### songs table

|   songs   |   type  |
|-----------|---------|
| song_id   | VARCHAR |
| title     | VARCHAR |
| artist_id | VARCHAR |
| year      | INT     |
| duration  | FLOAT   |

#### artists table

|   artists  |   type  |
|------------|---------|
| artist_id  | VARCHAR |
| name       | VARCHAR |
| location   | TEXT    |
| latitude   | FLOAT   |
| logitude   | FLOAT   |

#### time table

|    time    |    type   |
|------------|-----------|
| start_time | TIMESTAMP |
| hour       | INT       |
| day        | INT       |
| week       | INT       |
| month      | INT       |
| year       | INT       |
| weekday    | VARCHAR   |


## ELT Pipeline
### etl.py
ELT pipeline builder

1. `process_song_data`
    * Load raw data from S3 buckets to Spark stonealone server and process song dataset to extract columns and create _songs_ and _artists_ dimension table

2. `process_log_data`
    * Load raw data from S3 buckets to Spark stonealone server and Process event(log) dataset to extract columns and create _time_ and _users_ dimensio table and _songplays_ fact table
    
    


