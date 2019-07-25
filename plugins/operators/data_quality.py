from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

#####
# Data Quality Operator
#
# The final operator to create is the data quality operator,
#   which is used to run checks on the data itself.
#
# The operator's main functionality is to receive one or more SQL
#   based test cases along with the expected results and execute the tests.
#
# For each test, the test result and expected result needs to be
#   checked and if there is no match, the operator should raise an exception
#   and the task should retry and fail eventually.
#
# For example one test could be a SQL statement that checks if certain
#   column contains NULL values by counting all the rows that have NULL
#   in the column. We do not want to have any NULLs so expected result
#   would be 0 and the test would compare the SQL statement's outcome
#   to the expected result.
#####

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self='',
                 redshift_conn_id='',
                 test_sql='',
                 test_tbl='',
                 expected_results=0,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_sql = test_sql
        self.test_tbl = test_tbl
        self.expected_results = expected_results

    def execute(self, context):
        self.log.info("Making Connections to Redshift..")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # run the given sql
        records = redshift.get_records(test_sql)
        # check if matches expected result
        if len(records) != expected_results:
            raise ValueError(
                """Data Quality check on {} failed with result of {} records.
                 Expected result was {} records""".format(
                    self.test_tbl,len(records)))
        else:
            self.log.info(
                "Data Quality Check on {} passed with {} records".format(
                    self.test_tbl,len(records)))
