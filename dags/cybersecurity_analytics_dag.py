from datetime import datetime, timedelta
from pathlib import Path
import os

from airflow import DAG
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# Path to dbt project
DBT_PROJECT_PATH = Path("/opt/airflow/include/dbt")

# Cosmos configuration
profile_config = ProfileConfig(
    profile_name="cybersecurity",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",
        profile_args={
            "database": os.getenv("SNOWFLAKE_DATABASE", "CYBERSECURITY_DB"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        },
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path="/home/airflow/.local/bin/dbt",
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
        "retry_delay": timedelta(minutes=0.15),
    },
    description="dbt models for cybersecurity incidents analytics",
    tags=["dbt", "cosmos", "cybersecurity", "snowflake"],
)
