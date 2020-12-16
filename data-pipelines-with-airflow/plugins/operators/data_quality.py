from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 check_sqls=[],
                 expected_results=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.check_sqls=check_sqls
        self.expected_results=expected_results

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Checking data quality')
        for index, check_sql in enumerate(self.check_sqls):
            expected_result=self.expected_results[index]
            records=redshift.get_records(check_sql)
            
            if (expected_result(records) == False):
                raise ValueError(f"Data quality check for SQL {check_sql} failed.")