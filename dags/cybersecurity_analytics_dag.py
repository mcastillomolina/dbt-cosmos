from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# Path to dbt project
DBT_PROJECT_PATH = Path("/usr/local/airflow/include/dbt")

# Cosmos configuration
profile_config = ProfileConfig(
    profile_name="cybersecurity",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",
        profile_args={
            "database": "{{ var.value.snowflake_database }}",
            "schema": "{{ var.value.snowflake_schema }}",
        },
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
)

# Create the Cosmos DAG
cybersecurity_dbt_dag = DbtDag(
    project_config=ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH,
    ),
    profile_config=profile_config,
    execution_config=execution_config,
    # DAG configuration
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dag_id="cybersecurity_analytics_dbt",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="dbt models for cybersecurity incidents analytics",
    tags=["dbt", "cosmos", "cybersecurity", "snowflake"],
)
