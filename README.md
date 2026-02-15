# dbt-Cosmos: Cybersecurity Analytics with Apache Airflow

Orchestration of dbt transformations using Apache Airflow and Astronomer Cosmos. This project is **warehouse-agnostic** and works with Snowflake, BigQuery, Postgres, Redshift, and other data platforms.

## TO DO:

Add incident notification on dag and dbt test failures. (incident.io or PagerDuty)

## Architecture

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│   Airflow   │─────▶│   Cosmos    │─────▶│     dbt     │
│  Scheduler  │      │  Framework  │      │   Models    │
└─────────────┘      └─────────────┘      └─────────────┘
                                                  │
                                                  ▼
                                           ┌─────────────┐
                                           │    Data     │
                                           │  Warehouse  │
                                           │ (Any Type)  │
                                           └─────────────┘
```

## Prerequisites

- **Docker Desktop** (recommended) or Docker CLI
- A data warehouse account (Snowflake, BigQuery, Postgres, etc.)
- 5 minutes to set up

## Quick Start

### 1. Clone the Repository

```bash
git clone <your-repo-url>
cd dbt-cosmos
```

### 2. Configure Environment Variables

```bash
# Copy the example environment file
cp .env.example .env

# Generate a Fernet key
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

**Edit `.env` file** with your credentials:

**Example for Snowflake:**
```bash
AIRFLOW_FERNET_KEY=sH8hAE7X6f01LtwEPzcqtiA5vnJFBnj8IVtJBz5SEG4=
DBT_TARGET_TYPE=snowflake
AIRFLOW_CONN_DWH_DEFAULT=snowflake://USER:PASS@account/database/schema?warehouse=WH&role=ROLE
```

**Example for BigQuery:**
```bash
AIRFLOW_FERNET_KEY=sH8hAE7X6f01LtwEPzcqtiA5vnJFBnj8IVtJBz5SEG4=
DBT_TARGET_TYPE=bigquery
AIRFLOW_CONN_DWH_DEFAULT=google-cloud-platform://?extra__google_cloud_platform__project=my-project&extra__google_cloud_platform__key_path=/opt/airflow/keys/gcp-key.json
```

**Example for Postgres:**
```bash
AIRFLOW_FERNET_KEY=sH8hAE7X6f01LtwEPzcqtiA5vnJFBnj8IVtJBz5SEG4=
DBT_TARGET_TYPE=postgres
AIRFLOW_CONN_DWH_DEFAULT=postgres://username:password@hostname:5432/database
```

### 3. Start the Services

```bash
# Build and start all containers
docker-compose up -d

# Check status
docker-compose ps

# View logs (optional)
docker-compose logs -f
```

### 4. Access Airflow

1. Open your browser to **http://localhost:8080**
2. Login with:
   - **Username**: `admin`
   - **Password**: `admin`
3. Find the `cybersecurity_analytics_dbt` DAG
4. Unpause the DAG (toggle switch) and trigger it

### 5. View dbt Test Results

When the DAG runs, you'll see **task groups** for each model:
- **Model execution** (seed/run tasks)
- **Test execution** (shown after each model completes)

This provides clear visibility into which tests passed or failed!

## dbt Models

### Data Pipeline Flow

```
cybersecurity_incidents.csv (seed)
          ↓
stg_cybersecurity_incidents (staging view)
          ↓
    ┌─────┴─────┬──────────────┐
    ↓           ↓              ↓
critical_   incident_    regional_
incidents   summary      analysis
(table)     (table)      (table)
```

### Staging Layer
- **stg_cybersecurity_incidents**: Cleaned and standardized incident data with quality tests

### Mart Layer
- **critical_incidents**: Filtered view of critical severity incidents
- **incident_summary_by_type**: Aggregated statistics by incident type
- **regional_analysis**: Geographic distribution of incidents

### dbt Tests Included
All models include comprehensive data quality tests:
- ✅ Uniqueness checks
- ✅ Not-null constraints
- ✅ Accepted values validation
- ✅ Referential integrity

## Switching Data Warehouses

To use a different data warehouse:

1. **Update `.env` file** with your new connection string
2. **Update `DBT_TARGET_TYPE`** to match your warehouse
3. **Restart services**:
   ```bash
   docker-compose down
   docker-compose up -d
   ```

The DAG will automatically adapt to your warehouse - no code changes needed!

## Troubleshooting

### Logs Not Visible in Airflow UI

**Issue**: Getting 403 errors when viewing logs
**Solution**: Ensure `AIRFLOW_FERNET_KEY` is set in your `.env` file. Generate one with:
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### DAG Not Appearing

**Check for syntax errors:**
```bash
docker-compose exec airflow-scheduler airflow dags list
```

**View scheduler logs:**
```bash
docker-compose logs airflow-scheduler
```

### Connection Errors

**Verify your connection string:**
```bash
docker-compose exec airflow-webserver airflow connections get dwh_default
```

**Test connection manually:**
```bash
docker-compose exec airflow-scheduler bash
cd /opt/airflow/include/dbt
dbt debug
```

### Database Reset

If you need to completely reset:
```bash
docker-compose down -v  # Remove all volumes
docker-compose up -d    # Recreate everything fresh
```

### Tasks Failing

**View task logs in terminal:**
```bash
docker-compose exec airflow-scheduler bash
cat logs/dag_id=cybersecurity_analytics_dbt/run_id=<run_id>/task_id=<task_id>/attempt=1.log
```

## Development

### Adding New dbt Models

1. Create your `.sql` file in `include/dbt/models/`
2. Add tests in the corresponding `schema.yml`
3. Cosmos will automatically detect and add it to the DAG - no code changes needed!

### Modifying the DAG

Edit `dags/cybersecurity_analytics_dag.py` and the scheduler will automatically pick up changes.

### Running dbt Commands Manually

```bash
# Access the scheduler container
docker-compose exec airflow-scheduler bash

# Navigate to dbt project
cd /opt/airflow/include/dbt

# Run dbt commands
dbt run
dbt test
dbt docs generate
dbt docs serve
```

## Dataset

The PoC uses a sample cybersecurity incidents dataset with 20 records covering:
- **Incident types**: Malware, DDoS, Ransomware, Phishing, Data Breach, etc.
- **Severity levels**: Low, Medium, High, Critical
- **Geographic regions**: North America, Europe, Asia, South America
- **Industries**: Finance, Healthcare, Technology, Manufacturing, Retail

## Stopping the Environment

```bash
# Stop services (keeps data)
docker-compose down

# Stop and remove volumes (WARNING: deletes all data)
docker-compose down -v
```

**Built with**:
- Apache Airflow 2.10.4
- Astronomer Cosmos 1.9.0
- dbt Core 1.7.x
- Docker & Docker Compose


## Project Structure

```
dbt-cosmos/
├── dags/
│   └── cybersecurity_analytics_dag.py    # Airflow DAG definition
├── include/
│   └── dbt/
│       ├── models/
│       │   ├── staging/
│       │   │   ├── stg_cybersecurity_incidents.sql
│       │   │   └── schema.yml            # Tests for staging models
│       │   └── marts/
│       │       ├── critical_incidents.sql
│       │       ├── incident_summary_by_type.sql
│       │       ├── regional_analysis.sql
│       │       └── schema.yml            # Tests for mart models
│       ├── seeds/
│       │   └── cybersecurity_incidents.csv
│       └── dbt_project.yml
├── Dockerfile                             # Custom Airflow image with dbt
├── docker-compose.yml                     # Service orchestration
├── requirements.txt                       # Python dependencies
├── .env.example                           # Example environment config
└── README.md                              # This file
```