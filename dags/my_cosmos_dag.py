
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

DBT_PROJECT_DIR = "/opt/airflow/dags/jaffle-shop-main"   # <-- adjust
DBT_PROFILE_DIR = "/opt/airflow/dags"               # <-- adjust

with DAG(
    dag_id="dbt_jaffle_shop_fabric_test",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dbt","fabric","warehouse"],
) as dag:

    diag = BashOperator(
        task_id="diag_list_paths",
        bash_command="""
          set -euxo pipefail
          pwd
          ls -la /opt/airflow/dags
          find /opt/airflow/dags -maxdepth 3 -type d -print
        """,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"""
          set -euxo pipefail
          cd {DBT_PROJECT_DIR}
          dbt deps --profiles-dir {DBT_PROFILE_DIR}
        """,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
          set -euxo pipefail
          cd {DBT_PROJECT_DIR}
          dbt run --profiles-dir {DBT_PROFILE_DIR} --target fabric_dev
        """,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
          set -euxo pipefail
          cd {DBT_PROJECT_DIR}
          dbt test --profiles-dir {DBT_PROFILE_DIR} --target fabric_dev
        """,
    )

    diag >> dbt_deps >> dbt_run >> dbt_test
