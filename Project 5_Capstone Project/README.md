# Data Engineering Capstone Project

## Project Summary
In this project a data warehouse as a single source of truth database will be built by integrating data from different data sources for data analysis purposes and future backend usage.

The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

## Step 1: Scope the Project and Gather Data

### Scope
In this project, I94 immigration data, world temperature data and US demographic data will be used to set up a data warehouse that comprises of dimension tables and facts tables.

### Describe and Gather Data

| Data Set | Format | Description |
| ---      | ---    | ---         |
|[I94 Immigration Data](https://travel.trade.gov/research/reports/i94/historical/2016.html)| SAS | This data is from US Department of Homeland Security and contains international visitor arrival statistics by world regions and select countries (including top 20), type of visa, mode of transportation, age groups, states visited (first intended address only), and the top ports of entry (for select countries).|
|[World Temperature Data](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data)| CSV | This dataset is from Kaggle and contains monthly average temperature data at different country in the world wide.|
|[U.S. City Demographic Data](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)| CSV | This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000.|


## Step 2: Explore and Assess the Data
### Explore the Data 
Identify data quality issues, like missing values, duplicate data, etc.
<br>
Please refer to `Capstone Project.ipynb` for more details

### Clean the Data
1. Transform sas datetime to pandas datetime which is human readable.
2. Parse I94 label description to get country, state and city code.
3. Change city and state to upper case to match those in city_code table and state_code_table

## Step 3: Define Data Model

Because we will use this dataware house for OLAP and BI app usage, stat schema is employed.

### Facts Table

| f_immi         | 
|----------------|
| case_id        |
| cic_id         |
| year           |
| month          |
| city_code      |
| state_code     |
| mode           |
| visa_type_code |
| arrival_date   |
| departure_date |

### Dimension Tables

| dim_immi_person  | 
|------------------|
| immi_person_id   |
| cic_id           |
| citizen_country  |
| residence_country|
| birth_year       |
| gender           |
| ins_num          |
| visa_type        |

| dim_immi_flight  | 
|------------------|
| imm_flight_id    |
| cic_id           |
| airline          |
| admin_num        |
| flight_nubmer    |

| dim_temp            | 
|---------------------|
| dt                  |
| city_code           |
| country             |
| avg_temp            |
| avg_temp_uncertainty|
| year                |
| month               |

| dim_demo_pop     | 
|------------------|
| demo_pop_id      |
| city_code        |
| state_code       |
| num_male         |
| num_female       |
| num_veterans     |
| num_foregin_born |

| dim_demo_race    |
|------------------|
| demo_race_id     |
| city             |
| state            |
| race             |
| num              |


| dim_demo_stats    | 
|-------------------|
| demo_stat_id      |
| city_code         |
| state_code        |
| median_age        |
| avg_household_size|

### Mapping Out Data Pipelines
1. Assume all data sets are stored in S3 buckets as below: (The etl.py is based on this, but the tests in [capstone_project_test.ipynb] used the sample data in the folder)
- [S3_Bucket]/immigration/18-83510-I94-Data-2016/*.sas7bdat
- [S3_Bucket]/I94_SAS_Labels_Descriptions.SAS
- [S3_Bucket]/temperature/GlobalLandTemperaturesByCity.csv
- [S3_Bucket]/demographics/us-cities-demographics.csv
2. Cleaning step to clean up data sets, deduplicate, deal with missing values
3. Transform immigration data to fact and dimension tables, temperature data to dimension table, and parse description to tables, and split demographics data to dimension tables
4. Store these tables back to target S3 bucket

## Step 4: Run Pipelines to Model the Data 

### Create the data model
Data models are created using Spark. Please refer to sample code in  `Capstone Project.ipynb` Step 1,2.

### Data Quality Check
Data quality check to ensure there are no empty tables. Please refer to sample code in  `Capstone Project.ipynb` Step 4.

### Data Dictionary 

f_immi:
    |-- case_id: int | primary key
    |-- ci_cid: bigint | CIC id from sas file
    |-- year: init| 4 digit year
    |-- month: int| numeric month
    |-- city_code: char(3) | abbreviation for USA city
    |-- state_code: char(2) | abbreviation for USA state
    |-- mode: int | traffic method
    |-- visa_type_code: int | visa category code
    |-- arrival_date: timestamp | arrival date
    |-- departure_date: timestamp | departure date
    

dim_immi_person:
    |-- immi_person_id: int | primary key
    |-- cic_id: bigint | CIC id from sas file
    |-- citizen_country: int| country code of citizenship
    |-- residence_country: int | country code of residence
    |-- birth_year: int | birth year
    |-- gender: char(1)| gender
    |-- ins_num: int| INS number
    |-- visa_type: varchar | type of visa
    

dim_immi_flight:
    |-- immi_flight_id: int | primary key
    |-- cic_id: bigint | CIC id from sas file
    |-- airline: varchar| airlines flown
    |-- admin_num: bigint | admission number
    |-- flight_number: varchar | flight number of airline flown


country_code:
    |-- code: int | country code
    |-- country: varchar | country name


city_code:
    |-- code: int | city code
    |-- city: varchar | city name
    
state_code:
    |-- code: int | state code
    |-- state: varchar | state name
    
dim_temp:
    |-- dt: timestamp | record time stamp
    |-- city: varchar | record city
    |-- country: varchar| record country
    |-- avg_temp: float | monthly average temperature
    |-- avg_temp_uncertainty: float | monthly average temperature uncertainty
    |-- year: int | year
    |-- month: int | month
    
dim_demo_pop:
    |-- demo_pop_id: int | primary key
    |-- city: varchar | city name
    |-- state_code: char(2) | abbreviation for USA state
    |-- state: varchar | state name
    |-- num_male: int | city male population
    |-- num_female: int | city female population
    |-- num_veterans: int | number of veterans in the city
    |-- num_foreign_born: int | number of foreign born babies
    
dim_demo_race:
    |-- city: varchar | city name
    |-- state: varchar | state name
    |-- race: varchar | race group
    |-- num: iint | count of population
    
dim_demo_stats:
    |-- demo_stats_id: int | primary key
    |-- city: varchar | city name
    |-- state: varchar | state name
    |-- median_age: int | city median age
    |-- avg_household_size: float | city average household size
    

## Step 5: Complete Project Write Up

### Tools and Technologies

1. AWS S3 for data storage
2. Pandas for sample data set exploratory data analysis
3. PySpark for large data set data processing to transform staging table to dimension tables and fact tables

### Data Update Frequency

1. Tables created from immigration and temperature data set should be updated monthly since the raw data set is updated monthly.
2. Tables created from demographics data set could be updated annually since demographics data collection usually takes place once a year by relevant government organization.
3. All tables should be update in an append-only mode.

### User Persona

#### Users of the Data Models
* US Department of Homeland Security to understand immigration trend, distribution, stats.
* US Census Bureau to understand population movement, and distribution.
* Airlines to understand most popular port of entry, arrival time etc.

#### Questions the Models Can Answer
1. Which ports of entry are the 5 most popular in 2016?
2. What has been the trend is number of entries through these 5 ports?
3. Citizens of which 5 countries enter the US the most in 2016?
4. Which state see the most population growth from 2010 to 2013?

### Future Design Considerations

1. The data was increased by 100x. <br>
    If Spark with standalone server mode can not process 100x data set, we could consider to put data in AWS EMR which is a distributed data cluster for processing large data sets on cloud
2. The data populates a dashboard that must be updated on a daily basis by 7am every day.
    Apache Airflow could be used for creating a dag to handle a ETL data pipeline and to set up scheduler to regularly update the date and populate a report. Apache Airflow also integrate with Python and AWS very well.
3. The database needed to be accessed by 100+ people.
    AWS Redshift can handle up to 500 connections. We can use Redshift with confidence to handle this request.