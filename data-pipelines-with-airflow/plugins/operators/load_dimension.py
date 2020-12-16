from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_sql = """
        INSERT INTO {} {}
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 select_sql="",
                 destination_table="",
                 truncate_insert=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.select_sql=select_sql
        self.destination_table=destination_table
        self.truncate_insert=truncate_insert

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate_insert:
            self.log.info(f'Emptying table {self.destination_table}')
            redshift.run(f'TRUNCATE TABLE {self.destination_table}')
                         
        formatted_sql=LoadDimensionOperator.insert_sql.format(self.destination_table, self.select_sql)

        self.log.info(f'Loading data into {self.destination_table}')
        redshift.run(formatted_sql)
        
