import configparser
from datetime import datetime, timedelta
import os
import time
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import udf, col
from pyspark.sql.types import DateType


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
#     spark.conf.set('mapreduce.fileoutputcommiter.algorithm.version', '2')
    return spark


def pull_state_from_region_code(col):
    """
    
    """
    return col.strip().split('-')[-1]

udf_pull_state_from_region_code = udf(lambda x: pull_state_from_region_code(x))


def convert_datetime(days):
    """
    
    """
    try:
        start = datetime(1960, 1, 1)
        return (start + timedelta(days=int(days))).date()
    except:
        return None
    
udf_convert_datetime = udf(lambda x: convert_datetime(x), DateType())    


def process_airport_codes(spark, input_path, output_path):
    """
    Reads the airport codes csv file, clean the data and then store the records in parquet file.
    
    Parameters:
      spark - SparkSession's object.
      input_path - path of the input file to read from.
      output_path = path of the output file to write to.
      
    Returns: None.
    """
    # Assign airport-codes_csv data file to a variable
    file_name = 'airport-codes_csv.csv'
    
    # get filepath to airport-codes_csv data file
    airport_data = os.path.join(input_path, file_name)
    print('**************',airport_data,'***********')
    # read airport-codes_csv data file
    airport_df = spark.read.csv(airport_data, header=True, inferSchema=True)
    
    # Retrieve us airport code and remove heliport and closed airports
    us_code_df = airport_df.filter((airport_df['iso_country'] == 'US') & (airport_df['type'] != 'heliport') & (airport_df['type'] != 'closed'))
    
    # Drop null local codes because this column used for joining with the immigrants
    us_code_df = us_code_df.na.drop(subset=['local_code'])
    
    # Remove duplicates from the local_code column and keep only one record
    us_code_df = us_code_df.drop_duplicates(['local_code'])
        
    # split the region code to pull the state code
    us_code_df = us_code_df.withColumn('state_code', udf_pull_state_from_region_code('iso_region'))
    
    # split the coordinates to latitude and longitude
    coord = f.split(us_code_df['coordinates'], ',')
    us_code_df = us_code_df.withColumn('latitude', coord.getItem(0))
    us_code_df = us_code_df.withColumn('longitude', coord.getItem(1))
    
    # Casting the right datatype
    us_code_df = us_code_df.withColumn('elevation_ft', us_code_df['elevation_ft'].cast('integer')) \
                           .withColumn('latitude', us_code_df['latitude'].cast('float')) \
                           .withColumn('longitude', us_code_df['longitude'].cast('float')) 
    
    # Sort the data by state_code and local_code
    us_code_df = us_code_df.orderBy(['state_code', 'local_code'])
    
    # Retrieve only the required columns
    us_code_df = us_code_df[['local_code', 'name', 'type', 'municipality', 'state_code', 'elevation_ft', 'latitude', 'longitude']]  
    
    # write us airport codes table to parquet files
    us_code_df.write.partitionBy('state_code').mode('overwrite').parquet(output_path+'us_airport_codes_table')
    
    print('***** Completed writing us_airports_codes table *****')


def process_metadata_from_SAS_desc_file(spark, input_path, output_path):
    """
    Pull metadata from the SAS description file
    
    Parameters:
      spark - SparkSession's object.
      input_path - path of the input file to read from.
      output_path - path of the output file to write to.
      
    Returns: None.
    """
    # Assign I94_SAS_Labels_Descriptions.SAS data file to a variable
    file_name = './I94_SAS_Labels_Descriptions.SAS'
    
    # get filepath to airport-codes_csv data file
    labels_data_path = os.path.join(input_path, file_name)

    print(labels_data_path)
    
    with open(file_name) as f:
        f_content = f.read()
        f_content = f_content.replace('\t', '')

    def code_mapper(file, idx):
        """
        Pull the metadata from the file
        """
        f_content2 = f_content[f_content.index(idx):]
        f_content2 = f_content2[:f_content2.index(';')].split('\n')
        f_content2 = [i.replace("'", "") for i in f_content2]
        dic = [i.split('=') for i in f_content2[1:]]
        dic = dict([i[0].strip(), i[1].strip()] for i in dic if len(i) == 2)
        return dic
    
    # Retrieve country codes
    i94cit_res = code_mapper(f_content, "i94cntyl")
    country_df = pd.DataFrame.from_dict(list(i94cit_res.items()))
    country_df.columns = ['country_code', 'country']

    #Convert pandas df to spark df
    spark_country_df = spark.createDataFrame(country_df)
    
    # convert the datatype of the country_code column
    spark_country_df = spark_country_df.withColumn('country_code', col('country_code').cast('integer'))
    
    # write country codes table to parquet files
    spark_country_df.write.mode('overwrite').parquet(output_path+'country_codes_table')
    
    print('***** Completed writing country codes table *****')
    
    
    # Retrieve US state codes
    i94addr = code_mapper(f_content, "i94addrl")
    us_states_df = pd.DataFrame.from_dict(list(i94addr.items()))
    us_states_df.columns = ['state_code', 'state']
    
    #Convert pandas df to spark df
    spark_us_states_df = spark.createDataFrame(us_states_df)

    # write US state codes table to parquet files
    spark_us_states_df.write.mode('overwrite').parquet(output_path+'us_state_codes_table')
    
    print('***** Completed writing US state codes table *****')    
    
    
    # Retrieve travel mode
    i94mode = code_mapper(f_content, "i94model")
    travel_mode_df = pd.DataFrame.from_dict(list(i94mode.items()))
    travel_mode_df.columns = ['travel_mode_code', 'travel_mode']

    #Convert pandas df to spark df
    spark_travel_mode_df = spark.createDataFrame(travel_mode_df) 
    
    # convert the datatype of the travel_mode_code column
    spark_travel_mode_df = spark_travel_mode_df.withColumn('travel_mode_code', col('travel_mode_code').cast('integer'))
    
    # write travel mode table to parquet files
    spark_travel_mode_df.write.mode('overwrite').parquet(output_path+'travel_mode_table')
    
    print('***** Completed writing travel mode table *****')   
    
    
    # Retrieve port codes
    i94port = code_mapper(f_content, "i94prtl")
    i94port_df = pd.DataFrame.from_dict(list(i94port.items()))
    i94port_df.columns = ['port_code', 'port']
    
    city_state_df = i94port_df['port'].str.split(',', expand=True)
    city_state_df.columns = ['port_city', 'port_state', '_']
    city_state_df['port_city'] = city_state_df['port_city'].str.strip()
    city_state_df['port_state'] = city_state_df['port_state'].str.strip()
    
    us_port_df = pd.concat([i94port_df, city_state_df], axis = 1)[['port_code', 'port_city', 'port_state']]
    
    # Filter US state codes. Also, remove Mexico and Nova Scotia
    us_port_df = us_port_df[(us_port_df['port_state'].str.len() < 3) & (us_port_df['port_state'] != 'MX') & (us_port_df['port_state'] != 'NS')]
    
    #Convert pandas df to spark df
    spark_us_port_df = spark.createDataFrame(us_port_df)
    
    # write us port table to parquet files
    spark_us_port_df.write.mode('overwrite').parquet(output_path+'us_port_table')
    
    print('***** Completed writing us port table *****') 
    
    
    # Create a dictionary of visa category
    i94visa = {'1':'Business',
               '2': 'Pleasure',
               '3' : 'Student'}

    visa_category_df = pd.DataFrame.from_dict(list(i94visa.items()))
    visa_category_df.columns = ['category_code', 'category']

    #Convert pandas df to spark df
    spark_visa_category_df = spark.createDataFrame(visa_category_df)
    
    # convert the datatype of the travel_mode_code column
    spark_visa_category_df = spark_visa_category_df.withColumn('category_code', col('category_code').cast('integer'))
    
    # write visa category table to parquet files
    spark_visa_category_df.write.mode('overwrite').parquet(output_path+'visa_category_table')
    
    print('***** Completed writing visa category table *****')
    

def process_immigrants(spark, input_path, output_path):
    """
    Reads the airport codes csv file, clean the data and then store the records in parquet file.
    
    Parameters:
      spark - SparkSession's object.
      input_path - path of the input file to read from.
      output_path = path of the output file to write to.
      
    Returns: None.
    """
    # Assign immigrants data file to a variable
    file_name = 'i94_apr16_sub.sas7bdat'
    
    print(spark)

    # get filepath to immigrants data file
    immigrants_data = os.path.join(input_path, file_name)
    print('**************',immigrants_data,'***********')
    
    # read immigrants data file
    immigrants_df = spark.read.format('com.github.saurfang.sas.spark').load(immigrants_data)
    
    # Convert arrdate and depdate to date
    immigrants_df = immigrants_df.withColumn('arrival_date', udf_convert_datetime(immigrants_df['arrdate'])) \
                                 .withColumn('departure_date', udf_convert_datetime(immigrants_df['depdate'])) 
    
    # Casting columns to integer
    int_cols = ['cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res', 'i94bir', 'i94mode', 'i94visa', 'biryear', 'admnum']
    for col in int_cols:
        immigrants_df = immigrants_df.withColumn(col, immigrants_df[col].cast('integer'))
        
    # Retrieve only the required columns
    immigrants_df = immigrants_df[['cicid', 'i94port', 'i94yr', 'i94mon', 'i94cit', 'i94res', 'arrival_date', 'i94addr', 'departure_date',\
                                   'i94bir', 'i94mode', 'i94visa', 'biryear', 'gender', 'airline', 'admnum', 'fltno' , 'visatype']]  
    
    # write us airport codes table to parquet files
    immigrants_df.write.partitionBy('i94yr', 'i94mon').mode('overwrite').parquet(output_path+'immigrants_table')
    
    print('***** Completed writing immigrants table *****')


def main():
    """
    Create spark session, reads data from S3, processes that data using Spark
    and writes them back to S3
    """
    main_start_time = time.time()
    
    spark_start_time = time.time()
    spark = create_spark_session()
    print('\n***** Create spark session took {0:.2f} seconds. ******\n'.format(time.time()-spark_start_time))
    
    input_path = "s3a://udac-capstone-bucket/"
    
    #Writing the resultsets to S3
    output_path = 's3a://udac-capstone-bucket/outputs/'
    
#     airport_data_start_time = time.time()
#     process_airport_codes(spark, input_path, output_path)    
#     print('***** Process airport code data took {0:.2f} seconds. ******\n'.format(time.time()-airport_data_start_time))
    
    metadata_start_time = time.time()
    process_metadata_from_SAS_desc_file(spark, input_path, output_path)
    print('***** Process metadata took {0:.2f} seconds. ******\n'.format(time.time()-metadata_start_time))
    
    immigrants_start_time = time.time()
    process_immigrants(spark, input_path, output_path)
    print('***** Process immigrants took {0:.2f} seconds. ******\n'.format(time.time()-immigrants_start_time))
    
    print('***** Overall process took {0:.2f} seconds. ******\n'.format(time.time()-main_start_time))


if __name__ == "__main__":
    main()
