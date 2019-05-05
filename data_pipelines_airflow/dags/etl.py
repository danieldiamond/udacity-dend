import os
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)

from helpers import SqlQueries

# Global Variables
TABLES = ["staging_events", "staging_songs", "users", "songs",
          "artists", "time", "songplays"]

# Convert SQL commands from create_tables file into dictionary of commands
sql_commands = {}
filename = os.path.join(Path(__file__).parents[1], 'create_tables.sql')
with open(filename, 'r') as sqlfile:
    commands = s = " ".join(sqlfile.readlines())
    for idx, sql_stmt in enumerate(commands.split(';')[:-1]):
        table = sql_stmt.split('.')[-1].split(' ')[0].strip('"').strip('\n')
        sql_commands[table] = sql_stmt


default_args = {
    'owner': 'danieldiamond',
    'depends_on_past': False,
    'catchup': False,
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('etl_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
          )

start_operator = DummyOperator(task_id='start_etl',  dag=dag)
load_staging_tables = DummyOperator(task_id='load_staging_tables',  dag=dag)
etl_success = DummyOperator(task_id='etl_success',  dag=dag)

# Drop & Create Tables
for table in TABLES:
    # Drop Table
    drop_table_task = PostgresOperator(
        task_id=f"drop_{table}",
        postgres_conn_id="redshift",
        sql=f"DROP table IF EXISTS {table}",
        dag=dag
    )

    # Create Table
    create_table_task = PostgresOperator(
        task_id=f"create_{table}",
        postgres_conn_id="redshift",
        sql=sql_commands[table],
        dag=dag
    )

    start_operator >> drop_table_task
    drop_table_task >> create_table_task
    create_table_task >> load_staging_tables

# Stage Events Data
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="dend",
    s3_key="log_data",
    file_format="CSV"
)

# Stage Song Data
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="dend",
    s3_key="song_data",
    file_format="JSON"
)

# Insert Fact Tables
load_songplays_table = LoadFactOperator(
    task_id=f'load_songplays',
    redshift_conn_id="redshift",
    table='songplays',
    sql_stmt=SqlQueries.songplay_table_insert,
    dag=dag
)

# Insert DIM Tables
load_users_table = LoadDimensionOperator(
    task_id=f'load_users',
    redshift_conn_id="redshift",
    table='users',
    truncate=True,
    sql_stmt=SqlQueries.user_table_insert,
    dag=dag
)

load_songs_table = LoadDimensionOperator(
    task_id=f'load_songs',
    redshift_conn_id="redshift",
    table='songs',
    truncate=True,
    sql_stmt=SqlQueries.song_table_insert,
    dag=dag
)

load_artists_table = LoadDimensionOperator(
    task_id=f'load_artists',
    redshift_conn_id="redshift",
    table='artists',
    truncate=True,
    sql_stmt=SqlQueries.artist_table_insert,
    dag=dag
)

load_time_table = LoadDimensionOperator(
    task_id=f'load_time',
    redshift_conn_id="redshift",
    table='time',
    truncate=True,
    sql_stmt=SqlQueries.time_table_insert,
    dag=dag
)

songplays_data_quality = DataQualityOperator(
    task_id=f"data_quality_check_on_songplays",
    redshift_conn_id="redshift",
    table='songplays',
    dag=dag
)

artists_data_quality = DataQualityOperator(
    task_id=f"data_quality_check_on_artists",
    redshift_conn_id="redshift",
    table='artists',
    test_stmt=SqlQueries.artist_table_data_quality,
    result=(1,),
    dag=dag
)

users_data_quality = DataQualityOperator(
    task_id=f"data_quality_check_on_users",
    redshift_conn_id="redshift",
    table='users',
    dag=dag
)

songs_data_quality = DataQualityOperator(
    task_id=f"data_quality_check_on_songs",
    redshift_conn_id="redshift",
    table='songs',
    dag=dag
)

time_data_quality = DataQualityOperator(
    task_id=f"data_quality_check_on_time",
    redshift_conn_id="redshift",
    table='time',
    dag=dag
)

load_staging_tables >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
stage_songs_to_redshift >> [load_songs_table, load_artists_table]
stage_events_to_redshift >> [load_users_table, load_time_table]
load_songplays_table >> songplays_data_quality
load_users_table >> users_data_quality
load_artists_table >> artists_data_quality
load_songs_table >> songs_data_quality
load_time_table >> time_data_quality

[songplays_data_quality,
    users_data_quality,
    artists_data_quality,
    songs_data_quality,
    time_data_quality] >> etl_success
