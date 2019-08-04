from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 create_table_sql="",
                 insert_table_sql="",
                 table="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.create_table_sql = create_table_sql
        self.insert_table_sql = insert_table_sql
        self.table = table

    def execute(self, context):
        #get the redshift hook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Create Fact Table')
        redshift.run(format(self.create_table_sql))
        
        #deleting and inserting here because it's a dimension table
        self.log.info('Insert Data to Fact Table')
        insert_sql = f"DELETE FROM {self.table}; INSERT INTO {self.table} {self.insert_table_sql}"
        redshift.run(format(insert_sql))
