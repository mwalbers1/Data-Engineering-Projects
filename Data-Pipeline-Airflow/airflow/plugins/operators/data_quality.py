import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_names=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_names = table_names
        
    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for table in self.table_names:
            logging.info(f"Checking data load for {table} table")
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed for {table}. The {table} table select returned no results.")

            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed for {table}. The {table} table contained 0 rows.")
            
            logging.info(f"Data quality check on {table} passed with {num_records} records")
