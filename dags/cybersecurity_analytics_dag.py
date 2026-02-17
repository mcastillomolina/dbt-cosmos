from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from cosmos import DbtDag, ProjectConfig, ProfileConfig, RenderConfig, LoadMode, TestBehavior
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# Path to dbt project
DBT_PROJECT_PATH = Path("/opt/airflow/include/dbt")

# Profile configuration - Cosmos will mock this during DAG parsing
# At runtime, it will use the actual Snowflake connection from profiles.yml
profile_config = ProfileConfig(
    profile_name="cybersecurity",
    target_name="dev",
    profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",
)

# Render configuration for better DAG visualization and performance
render_config = RenderConfig(
    # Emit datasets for data-aware scheduling (Airflow 2.4+)
    emit_datasets=True,

    # Use DBT_LS with mock profile - Cosmos will mock credentials during parsing
    # This allows DAG to parse without needing real warehouse credentials
    load_method=LoadMode.DBT_LS,

    # Enable mock profile (default=True) - mocks profile during dbt ls
    # Set to False only if you want to use partial parsing with real credentials
    enable_mock_profile=True,


    # Select specific models/tags (optional - comment out to run all)
    # select=["+critical_incidents"],  # Run critical_incidents and all upstream deps
    # exclude=["tag:deprecated"],  # Exclude deprecated models
)

# Create the Cosmos DAG
cybersecurity_dbt_dag = DbtDag(
    project_config=ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH,
    ),
    profile_config=profile_config,
    # execution_config=execution_config,
    render_config=render_config,
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
    description="dbt models for cybersecurity incidents analytics - uses mocked profile for parsing",
    tags=["dbt", "cosmos", "cybersecurity"],
)
