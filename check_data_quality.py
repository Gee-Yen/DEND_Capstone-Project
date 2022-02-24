import configparser
import os
import time
from pyspark.sql import SparkSession

config = configparser.ConfigParser()
config.read('aws_credentials.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    Creates an entry point to work with RDD, DF and Dataset.

    Parameters:
      N/A    

    Returns: 
      SparkSession's object spark.
    """

    spark = SparkSession.builder \
                .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.2") \
                .config("spark.jars.repositories", "https://repos.spark-packages.org/") \
                .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']) \
                .config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY']) \
                .enableHiveSupport().getOrCreate()

    return spark


def check_null_record(table_df, table_name):
    """
    Check the number of records in the table.
    Raise error if there is no record in the table.
    
    Parameters:
      table_df - table dataframe.
      table_name - name of the table.
      
    Returns:
      None.
    """

    record_count = table_df.count()
    
    if record_count == 0:
        raise ValueError('Data quality check failed. {} returned no results.'.format(table_name))
        
    print('Data quality on table {} check passed with {} records.'.format(table_name, record_count))    
    

def check_null_record_in_pk_column(table_df, table_name, pk_column, condition):
    """
    Check the null records in the primary key column of the table.
    Raise error if there is an null record in the column.
    
    Parameters:
      table_df - table dataframe.
      table_name - name of the table.
      pk_column - primary key column name.
      condition - condition to filter.
      
    Returns:
      None
    """
    record_count = table_df.filter(condition).count()
    
    if record_count > 0:
        raise ValueError('Data quality check failed. {} primary key column of {} table returned with {} null records.' \
                         .format(pk_column, table_name, record_count))
        
    print('Data quality on {} primary key column of {} table check passed.'.format(pk_column, table_name)) 
    
    
def main():
    """
    Create spark session, reads data from S3, processes that data using Spark
    and writes them back to S3
    """
    main_start_time = time.time()
    
    spark_start_time = time.time()
    spark = create_spark_session()
    print('\n***** Create spark session took {0:.2f} seconds. ******\n'.format(time.time()-spark_start_time))
    
    input_path = "s3a://udac-capstone-bucket/outputs/"
    
    country_codes_df = spark.read.parquet(input_path+'country_codes_table/*.parquet')
    travel_mode_df = spark.read.parquet(input_path+'travel_mode_table/*.parquet')
    us_port_df = spark.read.parquet(input_path+'us_port_table/*.parquet')
    us_state_codes_df = spark.read.parquet(input_path+'us_state_codes_table/*.parquet')
    visa_category_df = spark.read.parquet(input_path+'visa_category_table/*.parquet')
    immigrants_df = spark.read.parquet(input_path+'immigrants_table/i94yr=2016/i94mon=4/*.parquet')
    us_airport_codes_df = spark.read.parquet(input_path+'us_airport_codes_table/*/*.parquet')

    dfs = [country_codes_df, travel_mode_df, us_port_df, us_state_codes_df, visa_category_df, immigrants_df, us_airport_codes_df]
    table_names = ['country_codes', 'travel_mode', 'us_port', 'us_state_codes', 'visa_category', 'immigrants', 'us_airport_codes']
    pk_columns = ['country_code', 'travel_mode_code', 'port_code', 'state_code', 'category_code', 'cicid', 'local_code']
    conditions = [
        'country_code IS NULL',
        'travel_mode_code IS NULL',
        'port_code IS NULL',
        'state_code IS NULL',
        'category_code IS NULL',
        'cicid IS NULL',
        'local_code IS NULL'        
    ]

    print('*****Check records are loaded into the tables.*****')
    for df, table_name in zip(dfs, table_names):
        check_null_record(df, table_name)
    
    print('\n*****Check there is no null primary key columns of the tables.*****')
    for df, table_name, pk_column, condition in zip(dfs, table_names, pk_columns, conditions):
        check_null_record_in_pk_column(df, table_name, pk_column, condition)

    print('***** Overall process took {0:.2f} seconds. ******\n'.format(time.time()-main_start_time))


if __name__ == "__main__":
    main()
