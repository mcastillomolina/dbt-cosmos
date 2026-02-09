FROM quay.io/astronomer/astro-runtime:12.7.1

# Install dbt in a virtual environment to avoid dependency conflicts
RUN python -m venv dbt_venv && \
    . dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake==1.9.0 && \
    deactivate
