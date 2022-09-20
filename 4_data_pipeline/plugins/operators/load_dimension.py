from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 sql_query="",
                 table="",
                 append=True
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.sql_query=sql_query
        self.table=table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append == False:
            self.log.info("Truncate and insert table {}".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))
        else:
            self.log.info("Append more table {}".format(self.table))
            
        self.log.info("Load data to dimension with table {}".format(self.table))
        redshift.run(self.sql_query)
        
        self.log.info("Finish Loading data to dimension with table {}".format(self.table))
        
