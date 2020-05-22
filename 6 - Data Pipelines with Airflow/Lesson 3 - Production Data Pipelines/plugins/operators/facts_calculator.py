import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class FactsCalculatorOperator(BaseOperator):
    facts_sql_template = """
    DROP TABLE IF EXISTS {destination_table};
    CREATE TABLE {destination_table} AS
    SELECT
        {groupby_column},
        MAX({fact_column}) AS max_{fact_column},
        MIN({fact_column}) AS min_{fact_column},
        AVG({fact_column}) AS average_{fact_column}
    FROM {origin_table}
    GROUP BY {groupby_column};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 origin_table="",
                 destination_table="",
                 fact_column="",
                 groupby_column="",
                 *args, **kwargs):

        super(FactsCalculatorOperator, self).__init__(*args, **kwargs)
        #
        # TODO: Set attributes from __init__ instantiation arguments
        #
        self.redshift_conn_id = redshift_conn_id
        self.origin_table = origin_table
        self.destination_table = destination_table
        self.fact_column = fact_column
        self.groupby_column = groupby_column

    def execute(self, context):
        #
        # TODO: Fetch the redshift hook
        #
        redshift_hook = PostgresHook(self.redshift_conn_id)
        #
        # TODO: Format the `facts_sql_template` and run the query against redshift
        #
        facts_sql = FactsCalculatorOperator.facts_sql_template.format(
            destination_table = self.destination_table,
            groupby_column = self.groupby_column,
            fact_column = self.fact_column,
            origin_table = self.origin_table
        )
        redshift_hook.run(facts_sql)
