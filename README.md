<h1>
  <center>DATA LAKES WITH SPARK: WORKING WITH I94 IMMIGRATION DATA</center>
</h1>




Author: Tianlin He

Date: 25 Nov 2020

Tag: #Udacity #Data Engineering #AWS #S3 #Spark #Capstone Project #I94 Immigration

# Project Overview

By creating an ETL pipeline of I94 immigration data of the U.S. in year 2016, the aim of this project is to gain an analytical insight of the immigrants and their immigration destinations to U.S..  The questions we are interested are:

* Where did the immigrants come from? A cold place or a hot place?
* What were the purposes of immigration? What kind of visa did they hold?
* What were their favourite immigration destination(s)? 
* Are there anything special (attitude, composition of the population) about these destinations?

As a data engineer, you are tasked with building an ETL pipeline that 

1. load the data from sources to (format=sas7bdat, csv) to **S3**, 
2. process the data with **Spark** , 
3. load the data back to S3 (format=Parquet) as fact and dimension tables. 



# Dataset

## I94 immigration data in year 2016

* `18-83510-I94-Data-2016` containining `sas7bat` files by month: came from the US National Tourism and Trade Office.  You do not have to use the entire dataset, just use what you need to accomplish the goal you set at the beginning of the project.

### Supplementary data:

* `I94_SAS_Labels_Descriptions.SAS`: a dictionary of  column names in I94 data
* `GlobalLandTemperaturesByCity.csv`: World Temperature Data: comes from Kaggle. It reports the daily temperature since 1700s for most cities in the world.
* `us-cities-demographics.csv`: comes from OpenSoft. It records the demographics data (e.g. % female, mean age, etc) of all cities in U.S.
* `airport-codes_csv`: comes from [here](https://datahub.io/core/airport-codes#data). It records the airport in U.S with correponding information.

# Project Development

The technologies used in this project are **AWS EMR**, **AWS S3** and **Apache Spark**.

## Overview of the steps

1. On **AWS console**, create **IAM user**
    * save the access key and secrete in `config.cfg`
2. On **AWS console**, create a **S3 bucket** and upload datasets
    * The name of my S3 bucket is `htl_capstone_spark`
    * The output from this project will be saved in the subfolder `s3a://htl_capstone_spark/parquet/`
3. **Exploratory analysis** on local notebook `capstone_project_submission`
    * visualise missing data
    * examine number of records in tables
4. Develop and test the pipeline with **local data**, to check the correctness of codes:
    1. Run `etl_local.py local` on April immigration data **local**
5. Run the pipeline with **data stored in S3**
    * Run `etl.py aws` on an **emr cluster**

## Files

* `etl.py`: **ETL pipeline**
* `funcs.py`: contains helper functions needed for exploratory analysis and ETL
* `capstone_project_submission.ipynb`: contains exploratory analysis and project requirements
* `config.cfg`contains **key and secret** of AWS, it ought NOT to be accessible to the public

# Data Model

The ETL pipeline builds **one fact table** and **four dimension tables**.  The dimension tables can be joined to the fact table via shared foreign key in later data analytics steps.

#### Fact table
The fact table `fact_immigration` is created from the immigration data. It include columns:


| colname  | dtype          | key  | value                               |
| -------- | -------------- | ---- | ----------------------------------- |
| cicid    | double         | PK   | identifier                          |
| i94cit   | double->string | FK   | 3-digit code country of citizenship |
| i94res   | double->string | FK   | 3-digit code country of residence   |
| i94addr  | string         | FK   | destination state in US             |
| ...      | ...            | ...  | ...                                 |
| visatype | string         | FK   | visa type                           |


#### Dimension tables

* `dim_temp` is created from global temperature data. It is aggregated on country-level average temperature in April of all cities. It connects to `i94cit` and `i94res` in fact table  via column `country_code`.

| colname        | dtype  | key  | value                                                        |
| -------------- | ------ | ---- | ------------------------------------------------------------ |
| country_code   | string | FK   | 3-digit code of country                                      |
| Country        | string | PK   | country name in BLOCK letters                                |
| avg_temp_april | double |      | average temperature in april for all recorded dates, aggregated at country level |

* `dim_airport` is created from airport data. It shows only airport inside US. It connects to `i94_port` in fact table via column `local_code`.

| colname      | dtype       | key  | value                                             |
| ------------ | ----------- | ---- | ------------------------------------------------- |
| ident        | string      | PK   | identifier of airport                             |
| local_code   | string      | FK   | non-nan local code of airport                     |
| iso_country  | string      |      | country name in BLOCK letters, all equals to 'US' |
| elevation_ft | string->int |      | elevation of airport                              |
| ...          | ...         | ...  | ...                                               |

* `dim_demographics` is created from demographics data in US. It shows cellular demographical information of up to state-city-race level. It connects to `i94addr`in fact table via column `State_code`.

| colname    | dtype       | key  | value                                                        |
| ---------- | ----------- | ---- | ------------------------------------------------------------ |
| State_code | string      | FK   | 2-syllable code of states in US                              |
| State      | string      |      | State in US                                                  |
| Race       | string      |      | categorical, one of 'Hispanic or Latino', 'White', 'Asian', 'Black or African-American', and 'American Indian and Alaska Native' |
| City       | string->int |      | City in a US state                                           |
| ...        | ...         | ...  | ...                                                          |

* `dim_visa` is created from immigration data during the normalisation of fact table. It provides supplementary information about the type of visa issue. It connects to `visatype` in fact table via column `visatype`.

| colname       | dtype          | key  | value                                                        |
| ------------- | -------------- | ---- | ------------------------------------------------------------ |
| visatype      | string         | FK   | 2-syllable code of visa types issued in US                   |
| i94_visa      | string->string |      | one-digit string of three ('1','2','3') condensed visa types, |
| i94_visa_info | string         |      | description of 'i94_visa'                                    |
| ...           | ...            | ...  | ...                                                          |

