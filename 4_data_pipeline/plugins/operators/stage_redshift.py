from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="",
                 aws_key_id="",
                 aws_secret_id="",
                 region="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_key_id = aws_key_id
        self.aws_secret_id = aws_secret_id
        self.region=region
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Clearing data from destination Redshift table {}".format(self.table))
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Copying data from S3 to Redshift with table {}".format(self.table))
        rendered_key = self.s3_key.format(**context)
        
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = """
                COPY {}
                FROM '{}'
                ACCESS_KEY_ID '{}'
                SECRET_ACCESS_KEY '{}'
                REGION '{}'
                IGNOREHEADER {}
                DELIMITER '{}'
            """.format(
                    self.table,
                    s3_path,
                    self.aws_key_id,
                    self.aws_secret_id,
                    self.ignore_headers,
                    self.delimiter
                )
        redshift.run(formatted_sql)
        self.log.info(f'StageToRedshiftOperator is implemented with {self.table} table')
        
#         self.log.info('StageToRedshiftOperator not implemented yet')





