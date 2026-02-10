# Cybersecurity Analytics - Airflow + Cosmos + dbt + Snowflake PoC

A proof-of-concept data pipeline using Apache Airflow, Astronomer Cosmos, dbt, and Snowflake to analyze cybersecurity incidents.

## Architecture

- **Apache Airflow**: Orchestration platform
- **Astronomer Cosmos**: Seamless dbt integration with Airflow
- **dbt**: Data transformation and modeling
- **Snowflake**: Cloud data warehouse
- **Docker**: Containerization

## Project Structure

```
.
├── dags/                          # Airflow DAGs
│   └── cybersecurity_analytics_dag.py
├── include/
│   ├── data/                      # Raw data files
│   │   └── cybersecurity_incidents.csv
│   └── dbt/                       # dbt project
│       ├── dbt_project.yml
│       ├── profiles.yml
│       ├── seeds/
│       │   └── cybersecurity_incidents.csv
│       ├── models/
│       │   ├── staging/
│       │   │   └── stg_cybersecurity_incidents.sql
│       │   └── marts/
│       │       ├── incident_summary_by_type.sql
│       │       ├── critical_incidents.sql
│       │       └── regional_analysis.sql
│       ├── macros/
│       ├── tests/
│       └── snapshots/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

## dbt Models

### Staging
- `stg_cybersecurity_incidents`: Cleaned and standardized incident data

### Marts (Analytics)
- `incident_summary_by_type`: Aggregated statistics by incident type
- `critical_incidents`: Filtered view of critical severity incidents
- `regional_analysis`: Regional and industry-based incident analysis

## Setup Instructions

### Prerequisites
- Docker and Docker Compose installed
- Snowflake free account
- Basic understanding of Airflow and dbt

### 1. Clone and Configure

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your Snowflake credentials
nano .env
```

### 2. Snowflake Setup

Log into your Snowflake account and run:

```sql
-- Create database and schema
CREATE DATABASE CYBERSECURITY_DB;
CREATE SCHEMA CYBERSECURITY_DB.PUBLIC;

-- Grant permissions (adjust as needed)
GRANT USAGE ON DATABASE CYBERSECURITY_DB TO ROLE ACCOUNTADMIN;
GRANT USAGE ON SCHEMA CYBERSECURITY_DB.PUBLIC TO ROLE ACCOUNTADMIN;
GRANT CREATE TABLE ON SCHEMA CYBERSECURITY_DB.PUBLIC TO ROLE ACCOUNTADMIN;
GRANT CREATE VIEW ON SCHEMA CYBERSECURITY_DB.PUBLIC TO ROLE ACCOUNTADMIN;
```

### 3. Start the Environment

```bash
# Build and start services
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f airflow-scheduler
```

### 4. Initialize Airflow Database

```bash
# Run Airflow DB initialization (first time only)
docker-compose run --rm airflow-webserver airflow db init

# Create admin user
docker-compose run --rm airflow-webserver airflow users create \
    --username airflow \
    --password airflow \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

### 5. Access Airflow UI

Open your browser and navigate to: http://localhost:8080

- **Username**: `airflow`
- **Password**: `airflow`

### 6. Run the DAG

1. In the Airflow UI, locate the `cybersecurity_analytics_dbt` DAG
2. Enable the DAG by toggling the switch
3. Click "Trigger DAG" to run it manually

The Cosmos DAG will automatically:
- Run dbt seed (load CSV data into Snowflake)
- Execute staging models
- Execute mart models
- Handle dependencies automatically

## Data Pipeline Flow

```
CSV Data (seed)
    ↓
stg_cybersecurity_incidents (view)
    ↓
    ├── incident_summary_by_type (table)
    ├── critical_incidents (table)
    └── regional_analysis (table)
```

## Dataset

The PoC uses a sample cybersecurity incidents dataset with 20 records covering:
- Incident types: Malware, DDoS, Ransomware, Phishing, etc.
- Severity levels: Low, Medium, High, Critical
- Geographic regions: North America, Europe, Asia
- Industries: Finance, Healthcare, Technology, Manufacturing, etc.

## dbt Commands (Manual Execution)

If you want to run dbt commands manually:

```bash
# Enter the container
docker-compose exec airflow-scheduler bash

# Activate dbt virtual environment
source /usr/local/airflow/dbt_venv/bin/activate

# Navigate to dbt project
cd /usr/local/airflow/include/dbt

# Run dbt commands
dbt seed
dbt run
dbt test
dbt docs generate
```

## Stopping the Environment

```bash
# Stop services
docker-compose down

# Stop and remove volumes (WARNING: deletes all data)
docker-compose down -v
```

## Troubleshooting

### Connection Issues
- Verify Snowflake credentials in `.env`
- Check that your Snowflake account allows connections from your IP
- Ensure warehouse is running in Snowflake

### dbt Errors
- Check `airflow-scheduler` logs: `docker-compose logs airflow-scheduler`
- Verify dbt project structure
- Ensure profiles.yml has correct environment variables

### Airflow Issues
- Check if postgres is healthy: `docker-compose ps`
- View webserver logs: `docker-compose logs airflow-webserver`
- Restart services: `docker-compose restart`

## Next Steps

- Add more complex dbt models and transformations
- Implement dbt tests for data quality
- Add incremental models for larger datasets
- Set up alerting and monitoring
- Create dbt documentation site
- Add CI/CD pipeline

## Resources

- [Apache Airflow Documentation](https://airflow.apache.org/)
- [Astronomer Cosmos](https://astronomer.github.io/astronomer-cosmos/)
- [dbt Documentation](https://docs.getdbt.com/)
- [Snowflake Documentation](https://docs.snowflake.com/)
