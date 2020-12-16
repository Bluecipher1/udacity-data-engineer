from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    insert_sql = """
        INSERT INTO {} {}
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 select_sql="",
                 destination_table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.select_sql=select_sql
        self.destination_table=destination_table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        formatted_sql=LoadFactOperator.insert_sql.format(self.destination_table, self.select_sql)

        self.log.info(f'Loading data into {self.destination_table}')
        redshift.run(formatted_sql)
