from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 database="",
                 table="",
                 sql_statement="",
                 append_data=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.database = database
        self.table = table
        self.sql_statement = sql_statement
        self.append_data = append_data

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.append_data == True:
            self.log.info(f"Append data to {self.table} dimension table in {self.database} Redshift database")
        else:
            self.log.info(f"Truncate {self.table} dimension table in {self.database} Redshift database")
            delete_statement = "DELETE FROM {}".format(self.table)
            self.log.info(f"Delete statement: {delete_statement}")
            redshift.run(delete_statement)

        self.log.info(f"Load {self.table} dimension table in {self.database} Redshift database")
        insert_sql_statement = "INSERT INTO {} {}".format(self.table, self.sql_statement)
        self.log.info(f"Insert statement: {insert_sql_statement}")
        redshift.run(insert_sql_statement)
        
