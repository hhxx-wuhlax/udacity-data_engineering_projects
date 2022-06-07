import configparser
import os
import logging
import pandas as pd
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from pyspark.sql.functions import udf, col, lit, year, month, upper, to_date
from pyspark.sql.functions import monotonically_increasing_id

# setup logging 
logger = logging.getLogger('general')
logger.setLevel(logging.INFO)

# set up AWS configuration
config = configparser.ConfigParser()
config.read('config.cfg', encoding='utf-8-sig')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
source_s3 = config['S3']['SOURCE_S3_BUCKET']
dest_s3 = config['S3']['DEST_S3_BUCKET']


### helper functions 

# create Spark session
def create_spark_session():
    spark = SparkSession.builder.\
    config("spark.jars.repositories", "https://repos.spark-packages.org/").\
    config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
    enableHiveSupport().getOrCreate()
    return spark

# transform date to human readable 
def sas_to_date(date):
    if date is not None:
        return pd.to_timedelta(date, unit='D') + pd.Timestamp('1960-1-1')
sas_to_date_udf = udf(sas_to_date, DateType())

# rename columns 
def rename_columns(table, new_columns):
    for original, new in zip(table.columns, new_columns):
        table = table.withColumnRenamed(original, new)
    return table


### start processing data

def process_immigration_data(spark, input_data, output_data):
    """
    Read immigration data and create fact and dimension tables:
    f_immi, dim_immi_person, and dim_immi_flight
    
    Parameters:
    :param spark: spark session object
    :param input_data: path of input data
    :param output_data: path of output data
    """
    
    logger.info("Start processing immigration data")
    
    # read immigration data file
    immi_path = os.path.join(input_data + 'immigration/18-83510-I94-Data-2016/*.sas7bdat')
    df = spark.read.format('com.github.saurfang.sas.spark').load(immi_path)
    
    logger.info("Complete processing immigration data")
    
    logger.info("Start creating f_immi table")
    
    # extract relevant columns to create f_immi table
    f_immi = df.select('cicid', 'i94yr', 'i94mon', 'i94port', 'i94addr', 'arrdate', 'depdate', 'i94mode', 'i94visa') \
                .distinct().withColumn("case_id", monotonically_increasing_id())
    
    # data cleaning
    new_columns = ['cic_id', 'year', 'month', 'city_code', 'state_code',\
                   'arrival_date', 'departure_date', 'mode', 'visa_type_code']
    f_immi = rename_columns(f_immi, new_columns)
    
    f_immi = f_immi.withColumn('arrival_date', sas_to_date_udf(col('arrival_date')))
    f_immi = f_immi.withColumn('departure_date', sas_to_date_udf(col('departure_date')))
    
    # write f_immi table to parquet files partitioned by state and city
    f_immi.write.mode("overwrite").partitionBy('state_code')\
                    .parquet(path=output_data + 'f_immi')
    
    logger.info("Created f_immi table")   
    
    logger.info("Start creating dim_immi_person table")
    
    # extract relevant columns to create dim_immi_person table
    dim_immi_person = df.select('cicid', 'i94cit', 'i94res',\
                                  'biryear', 'gender', 'insnum', 'visatype').distinct()\
                        .withColumn("immi_person_id", monotonically_increasing_id())
    
    # data cleaning
    new_columns = ['cic_id', 'citizen_country', 'residence_country',\
                   'birth_year', 'gender', 'ins_num', 'visa_type']
    dim_immi_person = rename_columns(dim_immi_person, new_columns)
    
    # write dim_immi_personal table to parquet files
    dim_immi_person.write.mode("overwrite").parquet(path=output_data + 'dim_immi_person')
    
    logger.info("Created dim_immi_person table")   
    
    logger.info("Start creating dim_immi_flight table")
    
    # extract relevant columns to create dim_immi_flight table
    dim_immi_flight = df.select('cicid', 'airline', 'admnum', 'fltno').distinct()\
                        .withColumn("immi_flight_id", monotonically_increasing_id())
    
    # data wrangling to match data model
    new_columns = ['cic_id', 'airline', 'admin_num', 'flight_number']
    dim_immi_flight = rename_columns(dim_immi_flight, new_columns)

    # write dim_immi_airline table to parquet files
    dim_immi_flight.write.mode("overwrite").parquet(path=output_data + 'dim_immi_flight')
    
    logger.info("Created dim_immi_flight table")
    
    
def process_label_data(spark, input_data, output_data):
    """
    Parse I94 label descriptions to get country codes, city codes and state codes
    
    Parameters:
    :param spark: spark session object
    :param input_data: path of input data
    :param output_data: path of output data
    """
    
    logger.info("Start processing label data")
    
    label_file = os.path.join(input_data + "I94_SAS_Labels_Descriptions.SAS")
    
    # open file
    with open(label_file) as f:
        contents = f.readlines()

    country_code = {}
    for countries in contents[10:298]:
        pair = countries.split('=')
        code, country = pair[0].strip(), pair[1].strip().strip("'")
        country_code[code] = country
    spark.createDataFrame(country_code.items(), ['code', 'country'])\
         .write.mode("overwrite")\
         .parquet(path=output_data + 'country_code')

    city_code = {}
    for cities in contents[303:962]:
        pair = cities.split('=')
        code, city = pair[0].strip("\t").strip().strip("'"),\
                     pair[1].strip('\t').strip().strip("''")
        city_code[code] = city
    spark.createDataFrame(city_code.items(), ['code', 'city'])\
         .write.mode("overwrite")\
         .parquet(path=output_data + 'city_code')

    state_code = {}
    for states in contents[982:1036]:
        pair = states.split('=')
        code, state = pair[0].strip('\t').strip("'"), pair[1].strip().strip("'")
        state_code[code] = state
    spark.createDataFrame(state_code.items(), ['code', 'state'])\
         .write.mode("overwrite")\
         .parquet(path=output_data + 'state_code')
    
    logger.info("Finish processing label data")
    
    
def process_temperature_data(spark, input_data, output_data):
    """
    Read temperature data from input, process and create dim_temp table for US
    
    Parameters:
    :param spark: spark session object
    :param input_data: path of input data
    :param output_data: path of output data
    """
    
    logger.info("Start processing temperature data")
    
    # read data file
    file_path = os.path.join(input_data + 'temperature/GlobalLandTemperaturesByCity.csv')
    df = spark.read.csv(file_path, header=True)

    # filter to US
    df = df.where(df['Country'] == 'United States')
    
    # extract relevant columns to create dim_temp
    dim_temp = df.select(['dt', 'AverageTemperature', 'AverageTemperatureUncertainty',\
                         'City', 'Country']).distinct()
    # rename columns
    new_columns = ['dt', 'avg_temp', 'avg_temp_uncertnty', 'city', 'country']
    dim_temp = rename_columns(dim_temp, new_columns)
    
    # transform to date and add month, year
    dim_temp = dim_temp.withColumn('dt', to_date(col('dt')))
    dim_temp = dim_temp.withColumn('year', year(dim_temp['dt']))
    dim_temp = dim_temp.withColumn('month', month(dim_temp['dt']))
    
    # write dim_temp table to parquet files
    dim_temp.write.mode("overwrite").parquet(path=output_data + 'dim_temp')
    
    logger.info("Finish processing temperature data")
    
def process_demographics_data(spark, input_data, output_data):
    """
    Read demographics data, process and create dimension tables:
    dim_demo_pop, dim_demo_race and dim_demo_stats_table
    
    Parameters:
    :param spark: spark session object
    :param input_data: path of input data
    :param output_data: path of output data
    """
    
    
    logger.info("Start processing demographics data")
    
    # read demographics file
    file_path = os.path.join(input_data + 'demographics/us-cities-demographics.csv')
    df = spark.read.format('csv').options(header=True, delimiter=';').load(file_path)

    # extract relevant columns to create dim_demo_pop table
    
    dim_demo_pop = df.select(['City', 'State', 'Male Population', 'Female Population', \
                        'Number of Veterans', 'Foreign-born']).distinct() \
                        .withColumn("demo_pop_id", monotonically_increasing_id())
    
    # rename columns
    new_columns = ['city', 'state', 'state_code', 'num_male', 'num_female', 'num_vetarans', 'num_foreign_born']
    dim_demo_pop = rename_columns(dim_demo_pop, new_columns)
    
    # write dim_demo_pop table to parquet files
    dim_demo_pop.write.mode("overwrite").parquet(path=output_data + 'dim_demo_pop')
    
    logger.info("Created dim_demo_pop table")
    
    # extract relevant columns to create dim_demo_race table
    dim_demo_race = df.select(['City', 'State', 'Race', 'Count']).distinct()\
                        .withColumn("demo_race_id", monotonically_increasing_id())
    
    # rename columns
    new_columns = ['city', 'state', 'race', 'num']
    dim_demo_race = rename_columns(dim_demo_race, new_columns)
    
    # write dim_demo_race table to parquet files
    dim_demo_race.write.mode("overwrite").parquet(path=output_data + 'dim_demo_race')
    
    logger.info("Created dim_demo_race table")
    
    # extract relevant columns to create dim_demo_stats table
    dim_demo_stats = df.select(['City', 'State', 'Median Age', 'Average Household Size']).distinct()\
                        .withColumn("demo_stats_id", monotonically_increasing_id())
    
    # rename columns
    new_columns = ['city', 'state', 'median_age', 'avg_household_size']
    dim_demo_stats = rename_columns(dim_demo_stats, new_columns)
    
    # change to upper case
    dim_demo_stats = dim_demo_stats.withColumn('city', upper(col('city')))
    dim_demo_stats = dim_demo_stats.withColumn('state', upper(col('state')))
    
    # write dim_demo_stats table to parquet files
    dim_demo_stats.write.mode("overwrite").parquet(path=output_data + 'dim_demo_stats')
    
    logger.info("Created dim_demo_stats table")
    logger.info("Finish processing demographics data")
    
def uniqueness_check(s3_bucket):
    """
    Iterate through all the file directories in an s3 bucket
    Read all the parquet files 
    Count rows of table before and after dropping duplicates
    
    Parameters:
    :param s3_bucket: dest s3_bucket where output files are saved
    """
    s3 = Path(s3_bucket)
    for file_dir in s3.iterdir():
        if file_dir.is_dir():
            path = str(file_dir)
            df = spark.read.parquet(path)
            before = df.count()
            after = df.dropDuplicates().count()
            
            if after < before:
                raise ValueError("Table: " + path.split('/')[-1] + "has duplicate rows.")
            else:
                print("Table: " + path.split('/')[-1] + "passed uniqueness check")
                
def completeness_check(s3_bucket):
    """
    Iterate through all the file directories in an s3 bucket
    Read all the parquet files 
    Count records
    
    Parameters:
    :param s3_bucket: dest s3_bucket where output files are saved
    """
    s3 = Path(s3_bucket)
    for file_dir in s3.iterdir():
        if file_dir.is_dir():
        path = str(file_dir)
        df = spark.read.parquet(path)
        record_num = df.count()
        if record_num <= 0:
            raise ValueError("Empty Table.")
        else:
            print("Table: " + path.split('/')[-1] + f"has total {record_num} records. Completeness check passed.")
    
def main():
    spark = create_spark_session()
    input_data = source_s3
    output_data = dest_s3
    
    process_immigration_data(spark, input_data, output_data)    
    process_label_data(spark, input_data, output_data)
    process_temperature_data(spark, input_data, output_data)
    process_demographics_data(spark, input_data, output_data)
    logging.info("Data processing completed")
    
    completeness_check(output_data)
    logging.info("Completeness check completed")
    uniqueness_check(output_data)
    logging.info("Uniqueness check completed")


if __name__ == "__main__":
    main()