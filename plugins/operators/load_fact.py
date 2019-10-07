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
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.sql = sql

    def execute(self, context):
        self.log.info("Making Connections to Redshift..")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # moving data over from redshift
        insert_sql = SqlQueries.songplay_table_insert
        self.log.info("Inserting into {}".format(self.target_table))
        redshift.run(upsert_sql.format(self.target_table,sql))
