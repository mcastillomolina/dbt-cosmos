"""
Weekly Analytics DAG - Multi-Dataset Processing

This DAG processes multiple datasets (Cybersecurity + Premier League) on a weekly cadence.
It uses Cosmos RenderConfig with tag-based selection to run only models tagged as 'weekly'.

Tagging Strategy:
- 'weekly': Models that run on weekly schedule
- 'daily': Models for daily schedule (future)
- 'monthly': Models for monthly schedule (future)
- 'cybersecurity': Cybersecurity domain models
- 'premier_league': Premier League domain models

To add new cadences, simply:
1. Tag models with 'daily' or 'monthly'
2. Create daily_analytics_dag.py or monthly_analytics_dag.py
3. Use RenderConfig select=['tag:daily'] or select=['tag:monthly']
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from cosmos import DbtDag, ProjectConfig, ProfileConfig, RenderConfig, LoadMode, TestBehavior

# Path to dbt project
DBT_PROJECT_PATH = Path("/opt/airflow/include/dbt")

# Profile configuration - Cosmos will mock this during DAG parsing
profile_config = ProfileConfig(
    profile_name="cybersecurity",
    target_name="dev",
    profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",
)

# Render configuration with tag-based selection
render_config = RenderConfig(
    # Emit datasets for data-aware scheduling
    emit_datasets=True,

    # Use DBT_LS with mock profile
    load_method=LoadMode.DBT_LS,
    enable_mock_profile=True,

    # ⭐ KEY FEATURE: Select only models tagged as 'weekly'
    # This allows the same dbt project to serve multiple DAGs with different cadences
    select=["tag:weekly"],

    # You can also combine multiple selections:
    # select=["tag:weekly", "tag:cybersecurity"],  # Only weekly cybersecurity models
    # select=["tag:weekly", "tag:premier_league"],  # Only weekly premier league models
)

# Create the Weekly Analytics DAG
weekly_analytics_dag = DbtDag(
    project_config=ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH,
    ),
    profile_config=profile_config,
    render_config=render_config,

    # DAG configuration
    schedule_interval="@weekly",  # Runs every Monday at midnight
    start_date=datetime(2026, 1, 5),  # Start on a Monday
    catchup=False,
    dag_id="weekly_analytics",

    default_args={
        "owner": "data_engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,  # Set to True in production
        "email_on_retry": False,
    },

    description="Weekly analytics processing for Cybersecurity and Premier League datasets",
    tags=["weekly", "multi-dataset", "analytics", "dbt", "cosmos"],

    # Set max active runs to prevent overlapping weekly runs
    max_active_runs=1,
)
