from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {0}
        FROM '{1}'
        ACCESS_KEY_ID '{2}'
        SECRET_ACCESS_KEY '{3}'
        {4}
        {5}
        {6}
        TIMEFORMAT as 'epochmillisecs'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 table="staging_temp",
                 s3_bucket="dend",
                 s3_key="temp",
                 file_format="JSON",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format

    def execute(self, context):
        """
        Copy data from S3 buckets to redshift cluster into staging tables.

        Parameters:
        ----------
        redshift_conn_id: string
            airflow connection to redshift cluster
        aws_credentials_id: string
            airflow connection to AWS
        table: string
            target table located in redshift cluster
        s3_bucket: string
            bucket location of staging data
        s3_key: boolean
            path location of staging data
        file_format: string
            file format to copy data, default JSON
        """
        self.log.info(f"Begin {self.table} StageToRedshiftOperator")
        pg_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        s3_hook = S3Hook(aws_conn_id=self.aws_credentials_id)
        credentials = s3_hook.get_credentials()

        self.log.info(f"Copying {self.table} from S3 to Redshift")
        s3_path = "s3://{}/{}/".format(self.s3_bucket, self.s3_key)
        if self.file_format == 'CSV':
            format = "CSV"
            delimiter = "DELIMITER ','"
            header = "IGNOREHEADER 1"
        else:
            format = "FORMAT AS JSON 'auto'"
            delimiter = ""
            header = ""

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            format,
            delimiter,
            header
        )
        self.log.info(f"QUERY: {formatted_sql}")
        pg_hook.run(formatted_sql)
        self.log.info(f"Success: Copying {self.table} from S3 to Redshift")
