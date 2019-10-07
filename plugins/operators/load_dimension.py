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

#Dimension loads are often done with the truncate-insert pattern
# where the target table is emptied before the load.
# Thus, you could also have a parameter that allows switching
# between insert modes when loading dimensions.
class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
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
                 insert_mode='truncate', # or insert or anything else but delete
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.sql = sql
        self.insert_mode = insert_mode


    def execute(self, context):
        self.log.info("Making Connections to Redshift..")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # dropping table if exists already
        if self.insert_mode == 'truncate':
            redshift.run("TRUNCATE {};".format(self.target_table))
        # create target table
        self.log.info("Inserting into table {} in Redshift".format(self.target_table))
        # insert data into table
        redshift.run(upsert_sql.format(self.target_table,sql))
