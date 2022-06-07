# Udacity-Data_Engineering_Projects
Repo of projects for [Udacity data engineering nano-degree](https://www.udacity.com/course/data-engineer-nanodegree--nd027)

## Project 1 - Data Modeling with Postgres
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. <br>

This project creates a Postgres database schema and ETL pipeline for song play analysis. Tests will also be conducted by running queries from the analytics team. Read more [here](https://github.com/hhxx-wuhlax/udacity-data_engineering_projects/blob/main/Project%201_Data%20Modeling%20with%20Postgres/README.md).

## Project 2 - Data Warehouse with AWS
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I am tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. Read more [here]()

## Project 3 - Data Lake with Spark
A music streaming startup, Sparkify, has grown their user base and song database and want 
to move their processes and data onto the cloud. Their data resides in S3, in a directory 
of JSON logs on user activity on the app, as well as a directory with JSON metadata 
on the songs in their app.

As their data engineer, I am tasked with building an ETL pipeline that extracts 
their data from S3, stages them in Redshift, and transforms data into a set of 
dimensional tables for their analytics team to continue finding insights in what songs 
their users are listening to. 

An ETL pipeline was built for a data lake hosted on S3. Data is loaded from S3, processed into tables using Spark, and loaded back to S3. This Spark process will be deployed on a Cluster on AWS. Read more [here]()

## Project 4 - Data Pipeline with Airflow
Using custom operators, I build a data pipeline to load and transform data, and perform data quality check in Redshift with Airflow.

## Project 5 - Capstone Project 
In this project a data warehouse as a single source of truth database will be built by integrating data from different data sources for data analysis purposes and future backend usage.

The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up