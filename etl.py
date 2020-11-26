import configparser
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import month, col, mean, upper
#from pyspark.sql.types import 

import funcs

config = configparser.ConfigParser()
config.read('config.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['IAM_USER']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['IAM_USER']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark=SparkSession.builder.\
        config("spark.jars.packages","saurfang:spark-sas7bdat:3.0.0-s_2.12")\
        .enableHiveSupport().getOrCreate()
    return spark


def process_airport(table,spark,input1,output1):
    """read airport with spark and create dim_airport
    ---input:
    table: string of table name
    spark: spark session
    input1:path+csv
    ---return: dim table airport in parquet files 
    output1: destination of parquet files
    """
    print ('table',table)
    # read spark
    df=spark.read.csv(input1,header=True)
    
    # clean
    df=funcs.clean_spark(df,dropna_mode='all',idx=['ident'])
    
    # as we will link it with immigration data, we only need airports within US
    df=df.filter(df['iso_country']=='US')
    print ('df inside US',df.count())
    
    # as local_code is FK, we need to remove local_code=nan
    df=df.dropna(subset='local_code')
    print ('df local_code is not nan',df.count())

    # convert elevation_ft from string to int
    df=df.withColumn('elevation_ft',df.elevation_ft.cast('int'))

    # output data to parquet file(s)
    df.write.parquet(output1+table,mode='overwrite')
    print (table,'contains', df.count(),'records')
    df.printSchema()
    

def process_temp(table, spark,input1,input2,output1):
    """read global temp with spark and create dim_temp
    ---input:
    spark: spark session
    input1: path+csv
    input2: path+SAS description file
    ---return: dim table with avg. temp in Apr for each country, with country code as str
    output1: destination of parquet files
    """
    print ('dim table', table)
    # read spark
    df=spark.read.csv(input1,header=True)
    
    # clean
    df=funcs.clean_spark(df,dropna_mode='any',idx=['dt','City','Country'])
    
    # extract temperature of Apr only for comparison
    df=df.filter(month('dt')==4)
    print ('df.count month==4',df.count())
    
    # aggregate at country level calculate avg temp for each country in apr
    df=df.groupBy('Country').agg(mean('AverageTemperature').alias('avg_temp_april'))
    print ('df.count groupby country',df.count())
    
    # map country code from SAS description file
    # create a new column 'country_code' and map it with country_dict obtained from SAS
    # country name needs to be capitalised for exact matching
    df=df.withColumn('country_code',upper(col('country')))
    
    dict_country={v:k for (k,v) in process_i94des(input2,"i94cntyl").items()}

    df=df.replace(to_replace=dict_country,subset=['country_code'])
    
    # display first 5 rows
    #print (df.limit(5).toPandas())

    # output data to parquet file(s)
    # do not need to partition, as the data is aggregated at country-level
    df.write.parquet(output1+table,mode='overwrite')

    print (table,'contains', df.count(),'records')
    df.printSchema()
    
    
def process_i94des(file,idx):
    """extract dictionary from SAS description file
    ---input:
    file: path+SAS file
    idx: a string from colnames in immigration file ['i94cntyl','i94prtl','i94model','i94addrl',...]
    ---return:
    a dictionary of col=idx
    """
    with open(file) as f:
        f_content = f.read()
        f_content = f_content.replace('\t', '')
        f_content = f_content[f_content.index(idx):]
        f_content = f_content[:f_content.index(';')].split('\n')
        f_content = [i.replace("'", "") for i in f_content]
        dic = [i.split('=') for i in f_content[1:]]
        dic = dict([i[0].strip(), i[1].strip()] for i in dic if len(i) == 2)
    return dic

def process_demographics(table,spark,input1,output1):
    """clean demographics and create dim_demographics
    ---input:
    table: name of dim table
    spark: spark session
    """

    print ('dim table',table)
    # read spark
    df=spark.read.csv(input1,sep=';',header=True, inferSchema=True)
    
    # clean
    df=funcs.clean_spark(df,dropna_mode='any',idx=['City','State','Race'])

    # output data to parquet file(s)
    # columns need to be rename for writing into parquet if necessary
    # to distribute the data, partition it at state-level
    replacements = {c:c.replace(' ','_').replace('-','_') for c in df.columns}
    df=df.select([col(c).alias(replacements.get(c,c)) for c in df.columns])
    df.write.parquet(output1+table,partitionBy=['State'],mode='overwrite')
    
    print (table,'contains', df.count(),'records')
    df.printSchema()

def process_immigration(table1,table2,spark,input1,output1):
    """clean immigration and return fact_immigration and dim_visa
    ---input:
    table1: name of fact table
    table2: name of dim table
    spark: spark session
    ---return: 
    dim_visa: PK=visatype
    fact_immigration
    output1: destination of parquet files

    """

    print ('table',table1)
    print ('table',table2)
    # read spark
    df=spark.read.format('com.github.saurfang.sas.spark').load(input1)
    
    # clean
    df=funcs.clean_spark(df,dropna_mode='all',idx=['cicid'])

    # convert country code from double to str so that it can be join with dim_temp
    df=df.withColumn('i94cit',df.i94cit.cast('int').cast('string'))
    df=df.withColumn('i94res',df.i94res.cast('int').cast('string'))
    #print (df.printSchema())
    #print (df.limit(5).toPandas())

    # create dim_visa and normalise the fact_demographics
    df_visa=df.select(['i94visa','visatype'])
    df_visa=df_visa.dropDuplicates(subset=['visatype'])

    # drop col=i94visa because we no longer need it in fact table
    df=df.drop('i94visa')

    # map i94 visa
    dict_visa={'1':'Business',
        '2': 'Pleasure',
        '3' : 'Student'}
    df_visa=df_visa.withColumn('i94_visa_info',df_visa.i94visa.cast('int').cast('string'))
    df_visa=df_visa.replace(to_replace=dict_visa,subset=['i94_visa_info'])
    
    print (table1,'contains', df.count(),'records')
    print (table2,'contains', df_visa.count(),'records')
    #print (df_visa.limit(3).toPandas())

    
    # output fact_demographics to parquet file(s)
    # to distribute the data, partition at arr-date (assume even arrival each day)
    df.write.parquet(output1+table1,partitionBy=['arrdate'],mode='overwrite')
    df.printSchema()

    # output dim_visa
    df_visa.write.parquet(output1+table2,mode='overwrite')
    df_visa.printSchema()
def main(mode):
    if mode=='local':
        data_in='data/'
        data_out='parquet/'
        data_immigration='immigration/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
    elif mode=='aws':
        data_in="s3://htl_capstone_spark/"
        data_out="s3://htl_capstone_spark/parquet/"
        data_immigration='immigration/*/*.sas7bdat'

    spark=create_spark_session()
    
    data_airport='airport-codes_csv.csv'
    #process_airport('dim_airport', spark,data_in+data_airport,data_out)

    data_temp='GlobalLandTemperaturesByCity.csv'
    i94des='immigration/I94_SAS_Labels_Descriptions.SAS'
    #process_temp('dim_temp',spark,data_in+data_temp,data_in+i94des,data_out)

    data_demographics='us-cities-demographics.csv'
    process_demographics('dim_demographics',spark, data_in+data_demographics,data_out)

    
    process_immigration(table1='fact_immigration',table2='dim_visa',spark=spark,input1=data_in+data_immigration,output1=data_out)

      
    
if __name__ == "__main__":
    # 'local' to run monthly immigration test locally
    # 'aws' to run all immigration data on AWS
    mode=str(sys.argv[1])
    main(mode)
    
    
    