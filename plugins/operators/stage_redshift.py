from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    
    """
    Creates staging table in the Redshift Cluster from json files in S3.
    
    Params:
        redshift_conn_id: a reference to a specific postgres database, type: str
        aws_credentials_id: a reference to a specific AWS account, type: str
        table: staging table name to be created, type: str
        s3_bucket: s3 bucket name, type: str
        s3_key: s3 folder name of data, type: str
        json: how json is organized, can be 'auto' or a path to a file, type: str
        
    Return:
        Nothing
    """
    ui_color = '#358140'
    
    
    
    copy_sql = """
        COPY public.{}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        delimeter '::';
    """


    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 table="",
                 s3_bucket="udacitytluo",
                 s3_key="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        
            
            
            

    def execute(self, context):
        self.log.info("Connect to AWS")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        self.log.info("Connecting to Redshift")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket,rendered_key)
        
        self.log.info("Truncating table {}...".format(self.table))
        redshift.run("TRUNCATE {}".format(self.table))
        
        self.log.info("Creating table {}...".format(self.table))
        redshift.run(StageToRedshiftOperator.copy_sql.format(self.table, s3_path, credentials.access_key, credentials.secret_key))




