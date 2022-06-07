from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 tables,
                 columns,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.columns = columns

    def execute(self, context):
        self.log.info('Connect to Redshift')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if len(self.tables) != len(self.columns):
            raise ValueError('Number of tables and number of tested columns do not match')
        
        for i in range(len(self.tables)):
            table = self.tables[i]
            column = self.columns[i]
            record_rows = redshift.get_records(f'select count(*) from {table}')
            record_nulls = redshift.get_records(f'SELECT COUNT(*) FROM {table} where {column} is null')
            
            if len(record_rows) <1 or len(record_rows[0])<1:
                raise ValueError(f'Data quality check failed. {table} returned no results')
            
            num_records = record_rows[0][0]
            num_nulls = record_nulls[0][0]
            
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            if num_nulls > 0:
                raise ValueError(f"Data quality check failed. In {table} column {column} contained nulls")
                
            self.log.info(f"Data quality on table {table} and column {column} check passed with {num_records} records")