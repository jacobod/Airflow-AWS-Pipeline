from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')
aws_credentials_id = 'aws_credentials'
redshift_credentials_id = 'redshift'
s3_bucket = 'udacity-dend'
s3_logs = 'log_data'
s3_songs = 'song_data'
# TODO:
# update operators below with parameters

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 7, 24),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    redshift_conn_id=redshift_credentials_id,
    aws_credentials_id=aws_credentials_id,
    target_table="staging_events",
    s3_bucket=s3_bucket,
    s3_key=s3_logs,
    file_format='csv'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    redshift_conn_id=redshift_credentials_id,
    aws_credentials_id=aws_credentials_id,
    target_table="staging_songs",
    s3_bucket=s3_bucket,
    s3_key=s3_songs,
    file_format='csv'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id=redshift_credentials_id,
    target_table='songplays',
    sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id=redshift_credentials_id,
    target_table='users',
    sql=SqlQueries.user_table_insert,
    insert_mode='truncate'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id=redshift_credentials_id,
    target_table='songs',
    sql=SqlQueries.song_table_insert,
    insert_mode='truncate'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id=redshift_credentials_id,
    target_table='artists',
    sql=SqlQueries.artist_table_insert,
    insert_mode='truncate'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id=redshift_credentials_id,
    target_table='time',
    sql=SqlQueries.time_table_insert,
    insert_mode='truncate'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    provide_context=True,
    retries=3,
    redshift_conn_id=redshift_credentials_id,
    test_sql='',
    test_tbl='',
    expcted_results=0
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

###################
# add ordering
###################
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_time_dimension_table
load_songplays_table >> load_artist_dimension_table

load_artist_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
