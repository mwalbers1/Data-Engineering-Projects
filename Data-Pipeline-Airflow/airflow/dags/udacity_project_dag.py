from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from staging_subdag import get_s3_to_redshift_dag
from helpers import sql_statements

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

start_date = datetime.utcnow()
parent_dag_name = 'udacity_project_dag'

default_args = {
    'owner': 'michael_albers',
    'start_date': start_date,
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(parent_dag_name,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Call staging subdag for events staging data 
events_task_id = "Stage_events_subdag"
stage_events_to_redshift = SubDagOperator(
    subdag = get_s3_to_redshift_dag(
        parent_dag_name = parent_dag_name,
        task_id = events_task_id,
        redshift_conn_id = "redshift",
        aws_credentials_id = "aws_credentials",
        table = "public.staging_events",
        create_sql_stmt = sql_statements.CREATE_STAGING_EVENTS_TABLE,
        s3_bucket = "udacity-dend",
        s3_key = "log_data",
        json = "s3://udacity-dend/log_json_path.json",
        start_date=start_date,
    ),
    task_id=events_task_id,
    dag=dag
)

# Call staging subdag for songs staging data
songs_task_id = "Stage_songs_subdag"
stage_songs_to_redshift = SubDagOperator(
    subdag = get_s3_to_redshift_dag(
        parent_dag_name = parent_dag_name,
        task_id = songs_task_id,
        redshift_conn_id = "redshift",
        aws_credentials_id = "aws_credentials",
        table = "public.staging_songs",
        create_sql_stmt = sql_statements.CREATE_STAGING_SONGS_TABLE,
        s3_bucket = "udacity-dend",
        s3_key = "song_data",
        json = "auto",
        start_date=start_date,
    ),
    task_id=songs_task_id,
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id=f"Load_songplays_fact_table",
    dag=dag,
    redshift_conn_id="redshift",
    database="sparkify",
    table="songplays",
    sql_statement=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id="Load_user_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    database="sparkify",
    table="users",
    sql_statement=SqlQueries.user_table_insert,
    append_data=False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    database="sparkify",
    table="songs",
    sql_statement=SqlQueries.song_table_insert,
    append_data=False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    database="sparkify",
    table="artists",
    sql_statement=SqlQueries.artist_table_insert,
    append_data=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    database="sparkify",
    table="time",
    sql_statement=SqlQueries.time_table_insert,
    append_data=False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table_names = ["songplays", "artists", "songs", "time", "users"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# define task dependencies
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
