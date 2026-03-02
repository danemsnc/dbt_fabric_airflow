from datetime import datetime
from cosmos import DbtDag
from cosmos.profiles import ProfileConfig, EnvVar
from cosmos.constants import LoadMode

# Path inside the Fabric Airflow container after Git-sync pulls your repo:
DBT_ROOT = "/opt/airflow/dags/../dbt/jaffle_shop"  # adjust if your repo layout differs

profile = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="prod",
    env={
        "FABRIC_SQL_ENDPOINT": EnvVar("FABRIC_SQL_ENDPOINT"),
        "FABRIC_DATABASE": EnvVar("FABRIC_DATABASE"),
        "FABRIC_TENANT_ID": EnvVar("FABRIC_TENANT_ID"),
        "FABRIC_SP_CLIENT_ID": EnvVar("FABRIC_SP_CLIENT_ID"),
        "FABRIC_SP_CLIENT_SECRET": EnvVar("FABRIC_SP_CLIENT_SECRET"),
    },
)

dag = DbtDag(
    dag_id="dbt_jaffle_shop_prod",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    dbt_root_path=DBT_ROOT,
    dbt_project_name="jaffle_shop",
    profile_config=profile,
    load_mode=LoadMode.DBT_LS,
)
