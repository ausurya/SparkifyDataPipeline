from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    #color of the opeartor
    ui_color = '#358140'
    
    #copy command for copying songs from s3 to redshift
    copy_songsql_json = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            TIMEFORMAT as 'epochmillisecs'
            REGION 'us-west-2'
            FORMAT AS JSON 'auto'
        """
    #copy command for copying events from s3 to redshift
    copy_eventsql_json = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            TIMEFORMAT as 'epochmillisecs'
            TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
            REGION 'us-west-2'
            FORMAT AS JSON 's3://udacity-dend/log_json_path.json'
        """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 file_typ="",
                 ignore_headers=1,
                 s3_bucket="",
                 s3_key="",
                 sql="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_typ = file_typ
        self.ignore_headers = ignore_headers
        self.sql = sql

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        #create table before loading
        self.log.info('Creating staging table in redshift')
        redshift.run(format(self.sql))
        
        #delete records if present from the table for fresh insert
        self.log.info('Deleteing the records of staging table')
        redshift.run("DELETE from {}".format(self.table))
        
        #copy records from s3 to redshift staging table
        self.log.info('Copy data from s3 to redshift')
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        if self.table == "staging_songs":
            formatted_sql = StageToRedshiftOperator.copy_songsql_json.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key
            )
            
        else:
            formatted_sql = StageToRedshiftOperator.copy_eventsql_json.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key
            )
        redshift.run(formatted_sql)
        





