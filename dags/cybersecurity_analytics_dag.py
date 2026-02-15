from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, TestBehavior
from cosmos.profiles import PostgresUserPasswordProfileMapping

# Path to dbt project
DBT_PROJECT_PATH = Path("/opt/airflow/include/dbt")

# Profile configuration using Airflow connection
# This works with any warehouse - just change the Airflow connection
profile_config = ProfileConfig(
    profile_name="cybersecurity",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="dwh_default",  # Generic data warehouse connection
        profile_args={
            "schema": "public",
        },
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path="/home/airflow/.local/bin/dbt",
    # Show tests as task groups for better visibility
    test_behavior=TestBehavior.AFTER_EACH,
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
    start_date=datetime(2026, 1, 1),
    catchup=False,
    dag_id="cybersecurity_analytics_dbt",
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    description="dbt models for cybersecurity incidents analytics - warehouse agnostic",
    tags=["dbt", "cosmos", "cybersecurity"],
)
