from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 table="",
                 sql_stmt="",
                 truncate=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_stmt = sql_stmt
        self.truncate = truncate

    def execute(self, context):
        """
        Insert data into dimensional tables from staging events and song data.
        Using a truncate-insert method to empty target tables prior to load.

        Parameters:
        ----------
        redshift_conn_id: string
            airflow connection to redshift cluster
        table: string
            target table located in redshift cluster
        sql_stmt: string
            SQL command to generate insert data.
        truncate: boolean
            Flag to truncate target table prior to load.
        """
        pg_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate:
            pg_hook.run(f"TRUNCATE TABLE {self.table}")
        formatted_sql = f"INSERT INTO {self.table} ({self.sql_stmt})"
        pg_hook.run(formatted_sql)
        self.log.info(f"Success: {self.task_id}")
