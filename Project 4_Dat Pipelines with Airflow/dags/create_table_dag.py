from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from udac_example_dag import default_args

# run this dag first to create the tables
dag = DAG(
    "create_table_dag",
    start_date = datetime.now(),
    default_args = default_args
)

create_table_task = PostgresOperator(
    task_id = "create_table",
    dag = dag,
    postgres_conn_id = "redshift",
    sql = 'create_tables.sql'
)

