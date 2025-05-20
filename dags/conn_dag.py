from airflow.decorators import dag, task
from pendulum import datetime
# from airflow.hooks.base import BaseHook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2025, 4, 20),
    schedule="0 18 * * *",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 0},
    tags=["example"]
)
def example_dag1():
    # Define tasks
    @task
    def get_astronauts(**context):
        print("HELLO FROM REMOTE!")
        hk = WasbHook(wasb_conn_id="astro_azure_logs_override")
        conn = hk.get_conn()

        # conn = BaseHook.get_connection("astro_azure_logs_override")
        # print(type(conn))
        # context["ti"].xcom_push(key="my_explicitly_pushed_xcom", value=conn.get_uri())

        # print(conn.get_uri())
        # print(conn.password)
        # print(hk.test_connection())
        print(hk.get_blobs_list(container_name="relogs"))

    @task
    def print_astronaut_craft(**context) -> None:
        print("WHY AM I HERE!")

    get_astronauts() >> print_astronaut_craft()


# Instantiate the DAG
example_dag1()
