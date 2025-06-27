from datetime import datetime
from cosmos import DbtDag, ProjectConfig
from pathlib import Path
from cosmos import ExecutionConfig
from cosmos import ProfileConfig
# from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

jaffle_shop_path = Path("/usr/local/airflow/dbt/jaffle_shop")
# dbt_executable = Path("/usr/local/airflow/dbt_venv/bin/dbt")
# dbt_executable = Path("/opt/astro/dbt_venv/bin/dbt")


# profile_config = ProfileConfig(
#     profile_name="airflow_db",
#     target_name="dev",
#     profile_mapping=PostgresUserPasswordProfileMapping(
#         conn_id="airflow_metadata_db",
#         profile_args={"schema": "dbt"},
#     ),
# )

profile_config = ProfileConfig(
    profile_name="snowflake_demo",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",
        profile_args={
            "schema": "dbt",
            "threads": 4,
        },
    )
)

# venv_execution_config = ExecutionConfig(
#     dbt_executable_path=str(dbt_executable),
# )

simple_dag = DbtDag(
    project_config=ProjectConfig(jaffle_shop_path),
    profile_config=profile_config,
    # execution_config=venv_execution_config,
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    dag_id="simple_dag_2",
    tags=["simple"],
)
