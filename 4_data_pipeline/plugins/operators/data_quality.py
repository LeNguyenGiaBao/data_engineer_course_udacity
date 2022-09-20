from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 testcases=[]
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.testcases=testcases

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        op = {'>': lambda x, y: x > y,
              '>=': lambda x, y: x >= y,
              '<': lambda x, y: x < y,
              '<=': lambda x, y: x <= y,
              '==': lambda x, y: x == y,
              '!=': lambda x, y: x != y}
        
        for testcase in self.testcases:
            query = testcase['test_sql']
            expected_result = testcase['expected_result']
            comparison = testcase['comparison']
            
            query_result = redshift_hook.run(query)
            result = op[comparison](query_result,expected_result)
            
            if result == False:
                raise ValueError('Error with query {}, expect result {} and comparison operator is {}'.format(query, expected_result, comparison))
            