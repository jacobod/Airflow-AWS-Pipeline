from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.helpers.sql_queries import SqlQueries
# With dimension and fact operators, you can utilize the provided
# SQL helper class to run data transformations.
# Most of the logic is within the SQL transformations and the operator
# is expected to take as input a SQL statement and target database on
# which to run the query against.
# You can also define a target table that will contain the
# results of the transformation.

# Fact tables are usually so massive that they should only
# allow append type functionality.

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    # create insert sql query
    upsert_sql = """
        INSERT INTO {}
        FROM ({});
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 target_table='',
                 sql='',
                 create_table='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = origin_table
        self.sql = create_sql
        self.create_table = create_table

    def execute(self, context):
        self.log.info("Making Connections to Redshift..")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # create target table if setting set to True
        if self.create_table:
            self.log.info("Creating table {} in Redshift".format(self.target_table))
            redshift.run("DROP TABLE IF EXISTS {}".format(self.target_table))
            redshift.run(create_sql)
        # moving data over from redshift
        insert_sql = SqlQueries.songplay_table_insert
        self.log.info("Inserting into {}".format(self.target_table))
        redshift.run(upsert_sql.format(self.target_table,insert_sql))
