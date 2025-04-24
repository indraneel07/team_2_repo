from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime, duration

_SNOWFLAKE_CONN_ID = "snowflake_conn"


@dag(
    dag_display_name="Snowflake Tutorial DAG TEAM-1",
    start_date=datetime(2024, 9, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "airflow", "retries": 1, "retry_delay": duration(seconds=5)},
    doc_md=__doc__,
    tags=["tutorial"],
)
def my_snowflake_dag():

    # you can execute SQL queries directly using the SQLExecuteQueryOperator
    run_query = SQLExecuteQueryOperator(
        task_id="run_query",
        conn_id=_SNOWFLAKE_CONN_ID,
        database="HQ",
        sql="select * from HQ.model_astro.alerts limit 10;"
    )

    run_query

my_snowflake_dag()
