from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# specify where in S3 the file is loaded and what is the target table.

# The parameters should be used to distinguish between JSON and CSV file.
# Another important requirement of the stage operator is containing a templated
#  field that allows it to load timestamped files from S3 based on the
#  execution time and run backfills.

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    # create SQL to format
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {} {}
        '{}' '{}';
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 create_sql="",
                 target_table="",
                 s3_bucket='',
                 s3_key="",
                 file_format='csv',
                 file_timestamp ='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.create_sql = create_sql
        self.target_table = target_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
        self.file_timestamp = file_timestamp

    def execute(self, context):
        # TODO
        # get all files in s3 bucket based on credentials and file format and optionally timestamp
        # format SQL based on file type & Timestamp???

        # initialize the connections
        self.log.info("Making Connections to Redshift..")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing out table if exists in Postgres")
        redshift.run("DROP {} IF EXISTS;".format(self.table))

        self.log.info("Creating table {} in Redshift".format(self.target_table))
        redshift.run(create_sql)

        self.log.info('Loading the data from {} to Redshift'.format(self.s3_bucket))
        # creating vars to format with
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        # setting the ignore headers to none if not a csv
        ignore_headers = None
        ignore_rows = None
        load_format = 'json'
        load_delim = 'auto'
        if self.format == 'csv':
            ignore_headers = 'IGNOREHEADER'
            ignore_rows = 1
            load_format = 'csv'
            load_delim = None
        # formatting sql
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            ignore_headers,
            ignore_rows,
            load_format,
            load_delim
        )
        redshift.run(formatted_sql)
