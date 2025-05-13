from airflow.sdk import Asset, DAG
from airflow.providers.standard.operators.python import PythonOperator
test_asset = Asset("my_asset")

with DAG(dag_id="my_producer_dag"):

    def my_function(**context):
        context["outlet_events"][test_asset].extra = {"my_num": 1233}

    my_task = PythonOperator(
        task_id="my_producer_task",
        python_callable=my_function,
        outlets=[test_asset],
    )
