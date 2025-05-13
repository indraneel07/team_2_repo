from airflow.sdk import Asset, DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="my_consumer_dag",
    schedule=[Asset("my_asset")]
):

    EmptyOperator(task_id="empty_task")
