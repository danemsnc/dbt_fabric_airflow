
# dags/dbt_jaffle_shop_dag.py
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = "/opt/airflow/dags/dbt/jaffle_shop-main"   # path within the synced repo
DBT_PROFILE_DIR = "/opt/airflow/dags/dbt"               # where profiles.yml lives

default_args = {"owner": "data-eng", "depends_on_past": False}
with DAG(
    dag_id="dbt_jaffle_shop_fabric",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # or "@daily"
    catchup=False,
    default_args=default_args,
    tags=["dbt","fabric","warehouse"]
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILE_DIR}"
    )

    # Optional: if you use seeds in your project
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt seed --profiles-dir {DBT_PROFILE_DIR} --target fabric_dev"
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROFILE_DIR} --target fabric_dev"
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILE_DIR} --target fabric_dev"
    )

    dbt_deps >> dbt_seed >> dbt_run >> dbt_test

