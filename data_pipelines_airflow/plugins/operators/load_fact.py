from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 table="",
                 sql_stmt="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_stmt = sql_stmt

    def execute(self, context):
        """
        Insert data into fact tables from staging events and song data.
        Typically fact tables are significantly large and thus append only
        methods should be utilized.

        Parameters:
        ----------
        redshift_conn_id: string
            airflow connection to redshift cluster
        table: string
            target table located in redshift cluster
        sql_stmt: string
            SQL command to generate insert data.
        """
        pg_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        formatted_sql = f"INSERT INTO {self.table} ({self.sql_stmt})"
        # formatted_sql = f"""ALTER TABLE {self.table}
        #                     APPEND FROM ({self.sql_stmt})
        #                     IGNOREEXTRA"""
        pg_hook.run(formatted_sql)
        self.log.info(f"Success: {self.task_id}")
