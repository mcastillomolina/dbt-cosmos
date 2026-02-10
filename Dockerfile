FROM apache/airflow:2.10.4-python3.12

USER airflow


RUN pip install --no-cache-dir 'dbt-core>=1.7.0,<1.8.0' 'dbt-snowflake>=1.7.0,<1.8.0'


COPY requirements.txt .
RUN pip install --no-cache-dir astronomer-cosmos==1.9.0