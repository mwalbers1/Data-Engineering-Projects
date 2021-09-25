import logging
import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.udacity_plugin import DataQualityOperator
from airflow.operators.udacity_plugin import StageToRedshiftOperator
from helpers import sql_statements

def get_s3_to_redshift_dag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        aws_credentials_id,
        table,
        create_sql_stmt,
        s3_bucket,
        s3_key,
        json,
        *args, **kwargs):
    """
    Return subdag to songs_dag and events dag which loads songs and events json data
    from S3 to Redshift staging tables.
    
    arguments:
        parent_dag_name:    name of parent dag
        task_id:            task_id for subdag
        redshift_conn_id:   airflow redshift connection id
        aws_credentials_id: airflow aws credentials id for aws key id and secret
        table:              staging table name
        create_sql_stmt:    create table sql
        s3_bucket:          name of S3 bucket for source json data
        s3_key:             name of S3 key for source json data
        json:               json argument for Redshift Copy command
        
    return:
        subdag
    """
    
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

    create_task = PostgresOperator(
        task_id=f"create_{table}_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=create_sql_stmt
    )

    copy_task = StageToRedshiftOperator(
        task_id=f"load_{table}_from_s3_to_redshift",
        dag=dag,
        table=table,
        redshift_conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        json=json
    )

    check_task = DataQualityOperator(
        task_id=f"check_{table}_data",
        redshift_conn_id=redshift_conn_id,
        dag=dag,
        table_names=[table]
    )

    create_task >> copy_task
    copy_task >> check_task

    return dag
