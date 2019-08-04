from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'sparkify',
    'start_date': datetime(2019, 1, 12), 
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('songs_etl_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          catchup=False,
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id = "redshift",
    table ="staging_events",
    aws_credentials_id ="aws_credentials",
    file_typ = "json",
    s3_bucket = "udacity-dend",
    s3_key = "log_data",
    sql = SqlQueries.create_staging_events_table,
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id ='Stage_songs',
    redshift_conn_id = "redshift",
    table ="staging_songs",
    aws_credentials_id ="aws_credentials",
    file_typ = "json",
    s3_bucket = "udacity-dend",
    s3_key = "song_data",
    sql = SqlQueries.create_staging_songs_table,
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    table = "songplays",
    redshift_conn_id="redshift",
    create_table_sql=SqlQueries.create_songplays_table,
    insert_table_sql=SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id="redshift",
    #aws_credentials_id="aws_credentials",
    create_table_sql=SqlQueries.create_users_table,
    insert_table_sql=SqlQueries.user_table_insert,
    table="users",
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id="redshift",
    #aws_credentials_id="aws_credentials",
    create_table_sql=SqlQueries.create_songs_table,
    insert_table_sql=SqlQueries.song_table_insert,
    table="songs",
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id="redshift",
    #aws_credentials_id="aws_credentials",
    create_table_sql=SqlQueries.create_artists_table,
    insert_table_sql=SqlQueries.artist_table_insert,
    table="artists",
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    #aws_credentials_id="aws_credentials",
    create_table_sql=SqlQueries.create_times_table,
    insert_table_sql=SqlQueries.time_table_insert,
    table="time",
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    table="songplays",
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

#another way to give dependency 
[load_artist_dimension_table, load_song_dimension_table, load_time_dimension_table,
 load_user_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator