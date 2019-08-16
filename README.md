# SparkifyDataPipeline
A datapipeline on songs dataset built using airflow 
It will look like following
![alt text](https://github.com/ausurya/SparkifyDataPipeline/blob/master/Airflow_project_pipeline.PNG)


## Goal
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines.

We need to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Dag properties
1. Doesn't depend on past executions
2. Retries 3 times
3. Retries on a timedelta of 5 minutes
4. Doesn't email on retries
5. Scheduled to run every hour

## Prerequisites
1. Redshift cluster in aws
2. A connection to this redshift created in airflow ui

## Files and what they do briefly
1. Operators 
    a. stage_redshift.py -> copies data from s3 to redshift tables[staging_songs,staging_events]
    b. load_fact.py -> creates and load data to fact table[songplays]
    c. load_dimension.py -> creates and load data to dimension tables[users,songs,artists,time]
    d. data_quality.py -> performs data quality checks on redshift tables
2. dags/Udac_dag.py
    This file contains the code of all the operators,sequence of pipeline and executes the dag with the default arguments
3. plugins/helpers/sql_queries.py
    This file contains all the sql queries which creates tables and inserts data into them
    
## Data model
![alt text](https://github.com/ausurya/SparkifyDataPipeline/blob/master/songplays_DM.PNG)